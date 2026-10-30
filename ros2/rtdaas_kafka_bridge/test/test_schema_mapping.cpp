// Unit tests that need neither a ROS graph nor Kafka: the ROS->Avro primitive mapping table,
// the Confluent wire header, UTF-16 conversion, and topic-mapping configuration parsing.
#include <gtest/gtest.h>

#include <map>
#include <stdexcept>
#include <string>

#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <rosidl_typesupport_introspection_cpp/field_types.hpp>

#include "../src/avro_mapper.hpp"
#include "../src/bridge_config.hpp"

namespace intro = rosidl_typesupport_introspection_cpp;
using namespace rtdaas_kafka_bridge;  // NOLINT

TEST(AvroTypeMapping, FollowsSpecTable)
{
  EXPECT_EQ(avro_type_for_ros_type(intro::ROS_TYPE_BOOLEAN), avro::AVRO_BOOL);
  for (uint8_t t : {intro::ROS_TYPE_INT8, intro::ROS_TYPE_UINT8, intro::ROS_TYPE_INT16,
      intro::ROS_TYPE_UINT16, intro::ROS_TYPE_INT32, intro::ROS_TYPE_CHAR,
      intro::ROS_TYPE_OCTET, intro::ROS_TYPE_WCHAR})
  {
    EXPECT_EQ(avro_type_for_ros_type(t), avro::AVRO_INT) << "type id " << int{t};
  }
  for (uint8_t t : {intro::ROS_TYPE_UINT32, intro::ROS_TYPE_INT64, intro::ROS_TYPE_UINT64}) {
    EXPECT_EQ(avro_type_for_ros_type(t), avro::AVRO_LONG) << "type id " << int{t};
  }
  EXPECT_EQ(avro_type_for_ros_type(intro::ROS_TYPE_FLOAT), avro::AVRO_FLOAT);
  EXPECT_EQ(avro_type_for_ros_type(intro::ROS_TYPE_DOUBLE), avro::AVRO_DOUBLE);
  EXPECT_EQ(avro_type_for_ros_type(intro::ROS_TYPE_LONG_DOUBLE), avro::AVRO_DOUBLE);
  EXPECT_EQ(avro_type_for_ros_type(intro::ROS_TYPE_STRING), avro::AVRO_STRING);
  EXPECT_EQ(avro_type_for_ros_type(intro::ROS_TYPE_WSTRING), avro::AVRO_STRING);
  EXPECT_THROW(avro_type_for_ros_type(intro::ROS_TYPE_MESSAGE), std::invalid_argument);
  EXPECT_THROW(avro_type_for_ros_type(250), std::invalid_argument);
}

TEST(WireFormat, HeaderIsMagicPlusBigEndianId)
{
  const auto h = wire_header(0x01020304);
  ASSERT_EQ(h.size(), 5u);
  EXPECT_EQ(h[0], 0x00);
  EXPECT_EQ(h[1], 0x01);
  EXPECT_EQ(h[2], 0x02);
  EXPECT_EQ(h[3], 0x03);
  EXPECT_EQ(h[4], 0x04);
  const auto one = wire_header(1);
  EXPECT_EQ(one[4], 0x01);
  EXPECT_EQ(one[1], 0x00);
}

TEST(Utf16, ConvertsBmpAndSurrogatePairs)
{
  EXPECT_EQ(utf16_to_utf8(u"hello"), "hello");
  EXPECT_EQ(utf16_to_utf8(u"é"), "\xC3\xA9");             // é
  EXPECT_EQ(utf16_to_utf8(u"€"), "\xE2\x82\xAC");         // €
  EXPECT_EQ(utf16_to_utf8(u"\U0001F600"), "\xF0\x9F\x98\x80");  // 😀 (surrogate pair)
}

TEST(FillPrimitive, HonoursSchemaPromotions)
{
  // Schema asks for long, message provides int32 -> promoted.
  avro::ValidSchema long_schema = avro::compileJsonSchemaFromString(R"({"type":"long"})");
  avro::GenericDatum d_long(long_schema);
  const int32_t v32 = -42;
  fill_primitive(d_long, &v32, intro::ROS_TYPE_INT32);
  EXPECT_EQ(d_long.value<int64_t>(), -42);

  avro::ValidSchema double_schema = avro::compileJsonSchemaFromString(R"({"type":"double"})");
  avro::GenericDatum d_double(double_schema);
  const float f = 1.5f;
  fill_primitive(d_double, &f, intro::ROS_TYPE_FLOAT);
  EXPECT_DOUBLE_EQ(d_double.value<double>(), 1.5);

  avro::ValidSchema str_schema = avro::compileJsonSchemaFromString(R"({"type":"string"})");
  avro::GenericDatum d_str(str_schema);
  const std::string s = "PSM1";
  fill_primitive(d_str, &s, intro::ROS_TYPE_STRING);
  EXPECT_EQ(d_str.value<std::string>(), "PSM1");

  avro::ValidSchema int_schema = avro::compileJsonSchemaFromString(R"({"type":"int"})");
  avro::GenericDatum d_int(int_schema);
  const uint8_t sev = 3;
  fill_primitive(d_int, &sev, intro::ROS_TYPE_UINT8);
  EXPECT_EQ(d_int.value<int32_t>(), 3);

  // uint32 into an int schema is a narrowing the spec forbids at generation time; the mapper
  // still copies (truncating) rather than throwing, so a stale schema does not stop the bridge.
  const uint32_t u = 7;
  fill_primitive(d_int, &u, intro::ROS_TYPE_UINT32);
  EXPECT_EQ(d_int.value<int32_t>(), 7);
}

TEST(AvroMapperCtor, RequiresEnvelopeFields)
{
  const std::string good = R"({"type":"record","name":"XRecord","namespace":"rtdaas.kafka","fields":[
    {"name":"robot_id","type":"string"},{"name":"ros_topic","type":"string"},
    {"name":"recv_time_ns","type":"long"},
    {"name":"msg","type":{"type":"record","name":"X","namespace":"p","fields":[{"name":"a","type":"int"}]}}]})";
  EXPECT_NO_THROW(AvroMapper(avro::compileJsonSchemaFromString(good)));

  const std::string missing = R"({"type":"record","name":"Y","fields":[{"name":"robot_id","type":"string"}]})";
  EXPECT_THROW(AvroMapper(avro::compileJsonSchemaFromString(missing)), std::invalid_argument);
}

TEST(Config, NormalizeType)
{
  EXPECT_EQ(normalize_type("sensor_msgs/JointState"), "sensor_msgs/msg/JointState");
  EXPECT_EQ(normalize_type("sensor_msgs/msg/JointState"), "sensor_msgs/msg/JointState");
  EXPECT_THROW(normalize_type("JointState"), std::invalid_argument);
  EXPECT_THROW(normalize_type("sensor_msgs/srv/Foo"), std::invalid_argument);
}

TEST(Config, ParsesSequenceForm)
{
  const std::string yaml = R"(
topics:
  - name: psm1_js
    ros_topic: /dv_001/PSM1/measured_js
    kafka_topic: rtdaas.telemetry.joint_states
    type: sensor_msgs/JointState
    qos: best_effort
  - ros_topic: /dv_001/rtdaas/events
    kafka_topic: rtdaas.telemetry.events
    type: rtdaas_msgs/msg/SystemEvent
    qos: reliable
)";
  const BridgeConfig cfg = parse_config_yaml(yaml);
  ASSERT_EQ(cfg.topics.size(), 2u);
  EXPECT_EQ(cfg.topics[0].name, "psm1_js");
  EXPECT_EQ(cfg.topics[0].type, "sensor_msgs/msg/JointState");
  EXPECT_EQ(cfg.topics[0].qos, "best_effort");
  EXPECT_EQ(cfg.topics[1].name, "/dv_001/rtdaas/events");  // defaults to ros_topic
  EXPECT_EQ(cfg.topics[1].qos, "reliable");
}

TEST(Config, ParsesMapFormAndDefaultsQos)
{
  const std::string yaml = R"(
topics:
  heartbeat:
    ros_topic: /dv_001/rtdaas/heartbeat
    kafka_topic: rtdaas.telemetry.heartbeat
    type: rtdaas_msgs/Heartbeat
)";
  const BridgeConfig cfg = parse_config_yaml(yaml);
  ASSERT_EQ(cfg.topics.size(), 1u);
  EXPECT_EQ(cfg.topics[0].name, "heartbeat");
  EXPECT_EQ(cfg.topics[0].qos, "best_effort");
  EXPECT_EQ(cfg.topics[0].type, "rtdaas_msgs/msg/Heartbeat");
}

TEST(Config, RejectsInvalidMappings)
{
  EXPECT_THROW(parse_config_yaml("foo: bar"), std::invalid_argument);
  EXPECT_THROW(parse_config_yaml(R"(
topics:
  - ros_topic: relative/topic
    kafka_topic: t
    type: a/msg/B
)"), std::invalid_argument);
  EXPECT_THROW(parse_config_yaml(R"(
topics:
  - ros_topic: /t
    kafka_topic: t
    type: a/msg/B
    qos: transient_local
)"), std::invalid_argument);
}

TEST(Config, BuildsMappingsFromParameterTree)
{
  const std::map<std::string, std::string> flat = {
    {"topics.psm1_js.ros_topic", "/dv_001/PSM1/measured_js"},
    {"topics.psm1_js.kafka_topic", "rtdaas.telemetry.joint_states"},
    {"topics.psm1_js.type", "sensor_msgs/JointState"},
    {"topics.psm1_js.qos", "best_effort"},
    {"topics.events.ros_topic", "/dv_001/rtdaas/events"},
    {"topics.events.kafka_topic", "rtdaas.telemetry.events"},
    {"topics.events.type", "rtdaas_msgs/msg/SystemEvent"},
    {"topics.events.qos", "reliable"},
    {"robot_id", "dv_001"},   // ignored: not under topics.
  };
  const auto mappings = mappings_from_parameters(flat);
  ASSERT_EQ(mappings.size(), 2u);
  // std::map iterates alphabetically: "events" before "psm1_js"
  EXPECT_EQ(mappings[0].name, "events");
  EXPECT_EQ(mappings[1].type, "sensor_msgs/msg/JointState");

  std::vector<TopicMapping> merged = mappings;
  merge_mappings(merged, mappings);   // duplicates are skipped
  EXPECT_EQ(merged.size(), 2u);
}

int main(int argc, char ** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
