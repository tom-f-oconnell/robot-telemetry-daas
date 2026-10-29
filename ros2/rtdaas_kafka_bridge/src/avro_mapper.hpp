// Introspection-driven ROS 2 message -> Avro GenericDatum mapping (spec §2.3).
//
// The Avro schema is the authority: it is fetched from the schema registry and the
// mapper walks the *schema's* record fields, looking up each one in the ROS
// introspection members by name. Fields present in the schema but missing from the
// message keep their Avro default; message fields absent from the schema are
// dropped. This makes the bridge tolerant of BACKWARD-compatible schema evolution.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <avro/Generic.hh>
#include <avro/Node.hh>
#include <avro/Types.hh>
#include <avro/ValidSchema.hh>
#include <rosidl_typesupport_introspection_cpp/message_introspection.hpp>

namespace rtdaas_kafka_bridge
{

/// Avro primitive for a ROS introspection field type id (ROS_TYPE_*), per spec §2.3:
///   bool -> AVRO_BOOL; int8/16/32, uint8/16, char, octet, wchar -> AVRO_INT;
///   int64, uint32, uint64 -> AVRO_LONG; float -> AVRO_FLOAT; double, long double -> AVRO_DOUBLE;
///   string, wstring -> AVRO_STRING.
/// Throws std::invalid_argument for ROS_TYPE_MESSAGE or unknown ids.
avro::Type avro_type_for_ros_type(uint8_t ros_type_id);

/// UTF-16 (ROS wstring, std::u16string) -> UTF-8.
std::string utf16_to_utf8(const std::u16string & in);

/// Confluent wire-format header: 0x00 magic + big-endian int32 schema id.
std::vector<uint8_t> wire_header(int32_t schema_id);

/// Fill `datum` (which must be a record, or a union containing one) from a ROS message.
void fill_record(
  avro::GenericDatum & datum, const avro::NodePtr & node, const void * ros_message,
  const rosidl_typesupport_introspection_cpp::MessageMembers * members);

/// Fill a primitive datum from a pointer to a ROS primitive of type `ros_type_id`.
/// Numeric promotions allowed by Avro (int->long, int/long->float/double, float->double)
/// are applied when the schema asks for a wider type than the message provides.
void fill_primitive(avro::GenericDatum & datum, const void * value_ptr, uint8_t ros_type_id);

class AvroMapper
{
public:
  explicit AvroMapper(avro::ValidSchema envelope_schema);

  /// Build the envelope record {robot_id, ros_topic, recv_time_ns, msg}.
  avro::GenericDatum build(
    const std::string & robot_id, const std::string & ros_topic, int64_t recv_time_ns,
    const void * ros_message,
    const rosidl_typesupport_introspection_cpp::MessageMembers * members) const;

  /// Avro binary encoding of `datum` (no wire header).
  std::vector<uint8_t> encode(const avro::GenericDatum & datum) const;

  /// wire_header(schema_id) + encode(build(...)) in one call.
  std::vector<uint8_t> serialize(
    int32_t schema_id, const std::string & robot_id, const std::string & ros_topic,
    int64_t recv_time_ns, const void * ros_message,
    const rosidl_typesupport_introspection_cpp::MessageMembers * members) const;

  const avro::ValidSchema & schema() const {return schema_;}

private:
  avro::ValidSchema schema_;
};

}  // namespace rtdaas_kafka_bridge
