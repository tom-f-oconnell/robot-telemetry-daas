// kafka_bridge: generic ROS 2 topic -> Avro (schema registry) -> Kafka bridge.
//
// For every configured mapping the node creates a GenericSubscription (no compile-time
// message dependency), deserializes with the rosidl_typesupport_cpp handle, walks the
// message with rosidl_typesupport_introspection_cpp, builds an Avro GenericDatum that
// matches the schema registered under "<kafka_topic>-value", and produces the record in
// Confluent wire format (0x00 + big-endian schema id + Avro binary) with key = robot_id.
//
// Parameters (all may be given with --params-file, see config/bridge.yaml):
//   robot_id                  (string, required)
//   kafka_bootstrap           (string, default "localhost:9092")
//   schema_registry_url       (string, default "http://localhost:8081/apis/ccompat/v7")
//   config_file               (string, optional plain-YAML topic mapping file)
//   topics.<name>.{ros_topic,kafka_topic,type,qos}   (parameter-tree topic mappings)
//   flush_interval_ms         (int, default 100: delivery-report polling period)
//   queue_max_messages        (int, default 100000: librdkafka local queue size)
//   compression               (string, default "lz4")
//   stats_period_s            (double, default 10.0)
//   registry_connect_retries  (int, default 30), registry_retry_delay_ms (int, default 2000)

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <avro/Compiler.hh>
#include <nlohmann/json.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/serialization.hpp>
#include <rclcpp/generic_subscription.hpp>
#include <rclcpp/version.h>
#include <rcpputils/shared_library.hpp>
#include <rosidl_runtime_cpp/message_initialization.hpp>
#include <rosidl_typesupport_cpp/identifier.hpp>
#include <rosidl_typesupport_introspection_cpp/identifier.hpp>
#include <rosidl_typesupport_introspection_cpp/message_introspection.hpp>
#include <std_msgs/msg/string.hpp>

// rclcpp has shipped the typesupport helpers since Iron (RCLCPP_VERSION_MAJOR >= 21); older
// distros only have them in rosbag2_cpp. Jazzy is 28.x.
#if defined(RCLCPP_VERSION_MAJOR) && RCLCPP_VERSION_MAJOR >= 21
#include <rclcpp/typesupport_helpers.hpp>
#define RTDAAS_TYPESUPPORT_FROM_RCLCPP 1
#else
#include <rosbag2_cpp/typesupport_helpers.hpp>
#define RTDAAS_TYPESUPPORT_FROM_RCLCPP 0
#endif

#include "avro_mapper.hpp"
#include "bridge_config.hpp"
#include "kafka_sink.hpp"
#include "registry_client.hpp"

namespace rtdaas_kafka_bridge
{

namespace intro = rosidl_typesupport_introspection_cpp;

namespace
{

struct TypeSupport
{
  std::shared_ptr<rcpputils::SharedLibrary> library;
  const rosidl_message_type_support_t * handle{nullptr};
};

TypeSupport load_typesupport(const std::string & type, const std::string & identifier)
{
  TypeSupport ts;
#if RTDAAS_TYPESUPPORT_FROM_RCLCPP
  ts.library = rclcpp::get_typesupport_library(type, identifier);
  ts.handle = rclcpp::get_message_typesupport_handle(type, identifier, *ts.library);
#else
  ts.library = rosbag2_cpp::get_typesupport_library(type, identifier);
  ts.handle = rosbag2_cpp::get_typesupport_handle(type, identifier, ts.library);
#endif
  if (ts.handle == nullptr) {
    throw std::runtime_error("no " + identifier + " type support for " + type);
  }
  return ts;
}

int64_t unix_now_ns()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

}  // namespace

class KafkaBridgeNode : public rclcpp::Node
{
public:
  explicit KafkaBridgeNode(const rclcpp::NodeOptions & options)
  : rclcpp::Node("kafka_bridge", options)
  {
    robot_id_ = param_or<std::string>("robot_id", "");
    if (robot_id_.empty()) {
      throw std::invalid_argument("parameter 'robot_id' is required");
    }
    const auto bootstrap = param_or<std::string>("kafka_bootstrap", "localhost:9092");
    registry_url_ = param_or<std::string>(
      "schema_registry_url", "http://localhost:8081/apis/ccompat/v7");
    const auto config_file = param_or<std::string>("config_file", "");
    const int flush_interval_ms = param_or<int>("flush_interval_ms", 100);
    const int queue_max = param_or<int>("queue_max_messages", 100000);
    const auto compression = param_or<std::string>("compression", "lz4");
    const double stats_period_s = param_or<double>("stats_period_s", 10.0);
    registry_retries_ = param_or<int>("registry_connect_retries", 30);
    registry_retry_delay_ms_ = param_or<int>("registry_retry_delay_ms", 2000);

    // --- mappings -------------------------------------------------------------------------
    std::vector<TopicMapping> mappings;
    if (!config_file.empty()) {
      mappings = load_config_file(config_file).topics;
      RCLCPP_INFO(get_logger(), "loaded %zu topic mappings from %s", mappings.size(),
                  config_file.c_str());
    }
    merge_mappings(mappings, mappings_from_parameter_tree());
    if (mappings.empty()) {
      throw std::invalid_argument(
              "no topic mappings configured (set 'config_file' or 'topics.<name>.*' parameters)");
    }

    // --- sinks ----------------------------------------------------------------------------
    KafkaSinkConfig sink_cfg;
    sink_cfg.bootstrap = bootstrap;
    sink_cfg.client_id = "rtdaas-kafka-bridge-" + robot_id_;
    sink_cfg.queue_max_messages = queue_max;
    sink_cfg.compression = compression;
    sink_ = std::make_unique<KafkaSink>(sink_cfg, get_logger());
    registry_ = std::make_unique<RegistryClient>(registry_url_);

    for (const auto & m : mappings) {
      setup_route(m);
    }

    // --- timers & stats -------------------------------------------------------------------
    poll_timer_ = create_wall_timer(
      std::chrono::milliseconds(std::max(flush_interval_ms, 10)),
      [this]() {sink_->poll(0);});
    stats_timer_ = create_wall_timer(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::duration<double>(stats_period_s)),
      [this]() {publish_stats();});
    stats_pub_ = create_publisher<std_msgs::msg::String>("~/stats", rclcpp::QoS(1));

    RCLCPP_INFO(get_logger(), "kafka_bridge for robot '%s' up: %zu routes -> %s",
                robot_id_.c_str(), routes_.size(), bootstrap.c_str());
  }

  ~KafkaBridgeNode() override
  {
    if (sink_) {
      RCLCPP_INFO(get_logger(), "flushing Kafka producer...");
      sink_->flush(10000);
    }
  }

private:
  struct Route
  {
    TopicMapping mapping;
    TypeSupport serialization_ts;    // rosidl_typesupport_cpp (for rmw deserialization)
    TypeSupport introspection_ts;    // rosidl_typesupport_introspection_cpp (for field walking)
    const intro::MessageMembers * members{nullptr};
    std::unique_ptr<rclcpp::SerializationBase> serializer;
    std::vector<uint8_t> message_buffer;   // one initialized ROS message, reused per callback
    std::shared_ptr<AvroMapper> mapper;
    int32_t schema_id{0};
    std::shared_ptr<rclcpp::GenericSubscription> subscription;
    uint64_t messages{0};
    uint64_t bytes{0};
    uint64_t errors{0};

    ~Route()
    {
      subscription.reset();
      if (members != nullptr && !message_buffer.empty() && members->fini_function != nullptr) {
        members->fini_function(message_buffer.data());
      }
    }
  };

  template<typename T>
  T param_or(const std::string & name, const T & default_value)
  {
    if (!has_parameter(name)) {
      declare_parameter<T>(name, default_value);
    }
    return get_parameter(name).get_value<T>();
  }

  /// Collect "topics.<name>.<key>" parameters (declared automatically from the params file).
  std::vector<TopicMapping> mappings_from_parameter_tree()
  {
    std::map<std::string, std::string> flat;
    // depth 0 == rcl_interfaces::srv::ListParameters::Request::DEPTH_RECURSIVE
    const auto listed = list_parameters({"topics"}, 0);
    for (const auto & name : listed.names) {
      const rclcpp::Parameter p = get_parameter(name);
      flat[name] = p.get_type() == rclcpp::ParameterType::PARAMETER_STRING ?
        p.as_string() : p.value_to_string();
    }
    return mappings_from_parameters(flat);
  }

  std::pair<int32_t, std::shared_ptr<AvroMapper>> mapper_for(const std::string & kafka_topic)
  {
    auto it = mappers_.find(kafka_topic);
    if (it != mappers_.end()) {
      return it->second;
    }
    const std::string subject = RegistryClient::subject_for_topic(kafka_topic);
    RegisteredSchema reg;
    for (int attempt = 1;; ++attempt) {
      try {
        reg = registry_->latest(subject);
        break;
      } catch (const std::exception & e) {
        if (attempt >= registry_retries_) {
          throw;
        }
        RCLCPP_WARN(get_logger(), "schema registry not ready for %s (%s); retry %d/%d",
                    subject.c_str(), e.what(), attempt, registry_retries_);
        std::this_thread::sleep_for(std::chrono::milliseconds(registry_retry_delay_ms_));
      }
    }
    auto mapper = std::make_shared<AvroMapper>(avro::compileJsonSchemaFromString(reg.schema_json));
    RCLCPP_INFO(get_logger(), "subject %s -> schema id %d (version %d)", subject.c_str(),
                reg.id, reg.version);
    auto entry = std::make_pair(reg.id, mapper);
    mappers_[kafka_topic] = entry;
    return entry;
  }

  static rclcpp::QoS qos_for(const std::string & qos)
  {
    if (qos == "reliable") {
      return rclcpp::QoS(rclcpp::KeepLast(200)).reliable().durability_volatile();
    }
    // Best effort is compatible with both reliable and best-effort publishers.
    return rclcpp::SensorDataQoS().keep_last(50);
  }

  void setup_route(const TopicMapping & mapping)
  {
    auto route = std::make_unique<Route>();
    route->mapping = mapping;
    route->mapping.type = normalize_type(mapping.type);

    route->serialization_ts = load_typesupport(
      route->mapping.type, rosidl_typesupport_cpp::typesupport_identifier);
    route->introspection_ts = load_typesupport(
      route->mapping.type, intro::typesupport_identifier);
    route->members = static_cast<const intro::MessageMembers *>(route->introspection_ts.handle->data);
    if (route->members == nullptr) {
      throw std::runtime_error("introspection members missing for " + route->mapping.type);
    }
    route->serializer = std::make_unique<rclcpp::SerializationBase>(route->serialization_ts.handle);
    route->message_buffer.assign(route->members->size_of_, 0);
    route->members->init_function(
      route->message_buffer.data(), rosidl_runtime_cpp::MessageInitialization::ALL);

    const auto [schema_id, mapper] = mapper_for(route->mapping.kafka_topic);
    route->schema_id = schema_id;
    route->mapper = mapper;

    Route * raw = route.get();
    route->subscription = create_generic_subscription(
      route->mapping.ros_topic, route->mapping.type, qos_for(route->mapping.qos),
      [this, raw](std::shared_ptr<const rclcpp::SerializedMessage> msg) {
        on_message(*raw, msg);
      });

    RCLCPP_INFO(get_logger(), "route %s: %s [%s, %s] -> %s", route->mapping.name.c_str(),
                route->mapping.ros_topic.c_str(), route->mapping.type.c_str(),
                route->mapping.qos.c_str(), route->mapping.kafka_topic.c_str());
    routes_.push_back(std::move(route));
  }

  void on_message(Route & route, const std::shared_ptr<const rclcpp::SerializedMessage> & msg)
  {
    const int64_t recv_ns = unix_now_ns();
    try {
      route.serializer->deserialize_message(msg.get(), route.message_buffer.data());
      const std::vector<uint8_t> payload = route.mapper->serialize(
        route.schema_id, robot_id_, route.mapping.ros_topic, recv_ns,
        route.message_buffer.data(), route.members);
      const KafkaSink::Headers headers = {
        {"robot_id", robot_id_},
        {"ros_topic", route.mapping.ros_topic},
        {"msg_type", route.mapping.type},
        {"schema_id", std::to_string(route.schema_id)},
      };
      if (sink_->produce(route.mapping.kafka_topic, robot_id_, payload, headers)) {
        ++route.messages;
        route.bytes += payload.size();
      } else {
        ++route.errors;
      }
    } catch (const std::exception & e) {
      ++route.errors;
      RCLCPP_ERROR_THROTTLE(get_logger(), *get_clock(), 5000, "route %s: %s",
                            route.mapping.name.c_str(), e.what());
    }
  }

  void publish_stats()
  {
    const KafkaSinkStats s = sink_->stats();
    nlohmann::json j;
    j["robot_id"] = robot_id_;
    j["produced"] = s.produced;
    j["delivered"] = s.delivered;
    j["failed"] = s.failed;
    j["dropped"] = s.dropped;
    j["bytes"] = s.bytes;
    nlohmann::json routes = nlohmann::json::array();
    for (const auto & r : routes_) {
      routes.push_back({
        {"name", r->mapping.name},
        {"ros_topic", r->mapping.ros_topic},
        {"kafka_topic", r->mapping.kafka_topic},
        {"messages", r->messages},
        {"bytes", r->bytes},
        {"errors", r->errors},
        {"schema_id", r->schema_id},
      });
    }
    j["routes"] = routes;
    std_msgs::msg::String out;
    out.data = j.dump();
    stats_pub_->publish(out);
    RCLCPP_INFO(get_logger(), "stats: produced=%lu delivered=%lu failed=%lu dropped=%lu bytes=%lu",
                static_cast<unsigned long>(s.produced), static_cast<unsigned long>(s.delivered),
                static_cast<unsigned long>(s.failed), static_cast<unsigned long>(s.dropped),
                static_cast<unsigned long>(s.bytes));
  }

  std::string robot_id_;
  std::string registry_url_;
  int registry_retries_{30};
  int registry_retry_delay_ms_{2000};
  std::unique_ptr<RegistryClient> registry_;
  std::unique_ptr<KafkaSink> sink_;
  std::map<std::string, std::pair<int32_t, std::shared_ptr<AvroMapper>>> mappers_;
  std::vector<std::unique_ptr<Route>> routes_;
  rclcpp::TimerBase::SharedPtr poll_timer_;
  rclcpp::TimerBase::SharedPtr stats_timer_;
  rclcpp::Publisher<std_msgs::msg::String>::SharedPtr stats_pub_;
};

}  // namespace rtdaas_kafka_bridge

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);
  rclcpp::NodeOptions options;
  // Let the whole --params-file (including the topics.* tree) be declared automatically.
  options.allow_undeclared_parameters(true).automatically_declare_parameters_from_overrides(true);
  int rc = 0;
  try {
    auto node = std::make_shared<rtdaas_kafka_bridge::KafkaBridgeNode>(options);
    rclcpp::spin(node);
  } catch (const std::exception & e) {
    RCLCPP_FATAL(rclcpp::get_logger("kafka_bridge"), "fatal: %s", e.what());
    rc = 1;
  }
  rclcpp::shutdown();
  return rc;
}
