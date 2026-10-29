// Topic-mapping configuration for the ROS 2 -> Kafka bridge.
//
// A mapping may come from either source (both are merged):
//   1. a plain YAML file given by the `config_file` parameter:
//        topics:
//          - name: psm1_joint_states          # optional
//            ros_topic: /dv-001/PSM1/measured_js
//            kafka_topic: rtdaas.telemetry.joint_states
//            type: sensor_msgs/msg/JointState
//            qos: best_effort                 # best_effort | reliable (default best_effort)
//   2. ROS parameters under the `topics.` prefix (so a single --params-file works):
//        topics:
//          psm1_joint_states:
//            ros_topic: ...
//            kafka_topic: ...
//            type: ...
//            qos: ...
#pragma once

#include <map>
#include <string>
#include <vector>

namespace rtdaas_kafka_bridge
{

struct TopicMapping
{
  std::string name;          // human-readable identifier (defaults to ros_topic)
  std::string ros_topic;     // absolute ROS topic, e.g. /dv-001/PSM1/measured_js
  std::string kafka_topic;   // e.g. rtdaas.telemetry.joint_states
  std::string type;          // normalized to pkg/msg/Type
  std::string qos{"best_effort"};
};

struct BridgeConfig
{
  std::vector<TopicMapping> topics;
};

/// "sensor_msgs/JointState" -> "sensor_msgs/msg/JointState"; already-normalized strings pass through.
std::string normalize_type(const std::string & type);

/// Throws std::invalid_argument when a mapping is incomplete or malformed.
void validate(const TopicMapping & mapping);

/// Parse the plain-YAML form (see header comment). Throws on malformed input.
BridgeConfig parse_config_yaml(const std::string & yaml_text);

/// Read and parse `path` with parse_config_yaml.
BridgeConfig load_config_file(const std::string & path);

/// Build mappings from flattened ROS parameters, keyed "topics.<name>.<key>" -> string value.
/// Entries whose name does not start with "topics." are ignored.
std::vector<TopicMapping> mappings_from_parameters(const std::map<std::string, std::string> & flat);

/// Append mappings from `extra` to `base`, skipping duplicates (same ros_topic + kafka_topic).
void merge_mappings(std::vector<TopicMapping> & base, const std::vector<TopicMapping> & extra);

}  // namespace rtdaas_kafka_bridge
