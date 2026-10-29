#include "avro_mapper.hpp"

#include <cstring>
#include <memory>
#include <stdexcept>

#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>
#include <rosidl_typesupport_introspection_cpp/field_types.hpp>

namespace rtdaas_kafka_bridge
{

namespace intro = rosidl_typesupport_introspection_cpp;

avro::Type avro_type_for_ros_type(uint8_t ros_type_id)
{
  switch (ros_type_id) {
    case intro::ROS_TYPE_BOOLEAN:
      return avro::AVRO_BOOL;
    case intro::ROS_TYPE_INT8:
    case intro::ROS_TYPE_UINT8:
    case intro::ROS_TYPE_INT16:
    case intro::ROS_TYPE_UINT16:
    case intro::ROS_TYPE_INT32:
    case intro::ROS_TYPE_CHAR:
    case intro::ROS_TYPE_WCHAR:
    case intro::ROS_TYPE_OCTET:
      return avro::AVRO_INT;
    case intro::ROS_TYPE_UINT32:
    case intro::ROS_TYPE_INT64:
    case intro::ROS_TYPE_UINT64:
      return avro::AVRO_LONG;
    case intro::ROS_TYPE_FLOAT:
      return avro::AVRO_FLOAT;
    case intro::ROS_TYPE_DOUBLE:
    case intro::ROS_TYPE_LONG_DOUBLE:
      return avro::AVRO_DOUBLE;
    case intro::ROS_TYPE_STRING:
    case intro::ROS_TYPE_WSTRING:
      return avro::AVRO_STRING;
    case intro::ROS_TYPE_MESSAGE:
      throw std::invalid_argument("ROS_TYPE_MESSAGE is not a primitive");
    default:
      throw std::invalid_argument(
              "unknown ROS introspection type id " + std::to_string(ros_type_id));
  }
}

std::string utf16_to_utf8(const std::u16string & in)
{
  std::string out;
  out.reserve(in.size() * 3);
  for (size_t i = 0; i < in.size(); ++i) {
    uint32_t cp = in[i];
    if (cp >= 0xD800 && cp <= 0xDBFF && i + 1 < in.size()) {
      const uint32_t lo = in[i + 1];
      if (lo >= 0xDC00 && lo <= 0xDFFF) {
        cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
        ++i;
      }
    }
    if (cp < 0x80) {
      out.push_back(static_cast<char>(cp));
    } else if (cp < 0x800) {
      out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
      out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
      out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
      out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
      out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
      out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
  }
  return out;
}

std::vector<uint8_t> wire_header(int32_t schema_id)
{
  const auto id = static_cast<uint32_t>(schema_id);
  return {
    0x00,
    static_cast<uint8_t>((id >> 24) & 0xFF),
    static_cast<uint8_t>((id >> 16) & 0xFF),
    static_cast<uint8_t>((id >> 8) & 0xFF),
    static_cast<uint8_t>(id & 0xFF),
  };
}

namespace
{

bool is_numeric(avro::Type t)
{
  return t == avro::AVRO_INT || t == avro::AVRO_LONG || t == avro::AVRO_FLOAT ||
         t == avro::AVRO_DOUBLE;
}

/// If `node` is a union, select the branch best matching `wanted` on `datum` and return the
/// branch node; otherwise return `node` unchanged.
avro::NodePtr select_branch(avro::GenericDatum & datum, const avro::NodePtr & node, avro::Type wanted)
{
  if (node->type() != avro::AVRO_UNION) {
    return node;
  }
  const size_t n = node->leaves();
  // 1. exact type match
  for (size_t b = 0; b < n; ++b) {
    if (node->leafAt(b)->type() == wanted) {
      datum.selectBranch(b);
      return node->leafAt(b);
    }
  }
  // 2. any numeric branch when a numeric was wanted (Avro promotion rules apply later)
  if (is_numeric(wanted)) {
    for (size_t b = 0; b < n; ++b) {
      if (is_numeric(node->leafAt(b)->type())) {
        datum.selectBranch(b);
        return node->leafAt(b);
      }
    }
  }
  // 3. first non-null branch
  for (size_t b = 0; b < n; ++b) {
    if (node->leafAt(b)->type() != avro::AVRO_NULL) {
      datum.selectBranch(b);
      return node->leafAt(b);
    }
  }
  throw std::runtime_error("union has no usable branch for ROS field");
}

void set_integer(avro::GenericDatum & datum, int64_t v)
{
  switch (datum.type()) {
    case avro::AVRO_INT:
      datum.value<int32_t>() = static_cast<int32_t>(v);
      break;
    case avro::AVRO_LONG:
      datum.value<int64_t>() = v;
      break;
    case avro::AVRO_FLOAT:
      datum.value<float>() = static_cast<float>(v);
      break;
    case avro::AVRO_DOUBLE:
      datum.value<double>() = static_cast<double>(v);
      break;
    case avro::AVRO_BOOL:
      datum.value<bool>() = (v != 0);
      break;
    case avro::AVRO_STRING:
      datum.value<std::string>() = std::to_string(v);
      break;
    default:
      throw std::runtime_error("cannot assign integer to Avro type " + avro::toString(datum.type()));
  }
}

void set_floating(avro::GenericDatum & datum, double v)
{
  switch (datum.type()) {
    case avro::AVRO_FLOAT:
      datum.value<float>() = static_cast<float>(v);
      break;
    case avro::AVRO_DOUBLE:
      datum.value<double>() = v;
      break;
    case avro::AVRO_STRING:
      datum.value<std::string>() = std::to_string(v);
      break;
    default:
      throw std::runtime_error("cannot assign float to Avro type " + avro::toString(datum.type()));
  }
}

void set_string(avro::GenericDatum & datum, const std::string & v)
{
  if (datum.type() == avro::AVRO_STRING) {
    datum.value<std::string>() = v;
  } else if (datum.type() == avro::AVRO_BYTES) {
    datum.value<std::vector<uint8_t>>().assign(v.begin(), v.end());
  } else {
    throw std::runtime_error("cannot assign string to Avro type " + avro::toString(datum.type()));
  }
}

const intro::MessageMember * find_member(
  const intro::MessageMembers * members, const std::string & name)
{
  for (uint32_t i = 0; i < members->member_count_; ++i) {
    if (name == members->members_[i].name_) {
      return &members->members_[i];
    }
  }
  return nullptr;
}

const intro::MessageMembers * nested_members(const intro::MessageMember & member)
{
  if (member.members_ == nullptr || member.members_->data == nullptr) {
    throw std::runtime_error(std::string("nested message member without type support: ") +
                             member.name_);
  }
  return static_cast<const intro::MessageMembers *>(member.members_->data);
}

/// Pointer to the i-th element of an array/sequence member. For std::vector<bool> the
/// introspection layer provides no get_const_function, only fetch_function; in that case
/// the value is copied into `scratch` and a pointer to it is returned.
const void * element_ptr(
  const intro::MessageMember & member, const void * array_ptr, size_t index, bool & scratch)
{
  if (member.get_const_function != nullptr) {
    return member.get_const_function(array_ptr, index);
  }
  if (member.fetch_function != nullptr) {
    member.fetch_function(array_ptr, index, &scratch);
    return &scratch;
  }
  throw std::runtime_error(std::string("array member without element accessors: ") + member.name_);
}

void fill_member(
  avro::GenericDatum & field, const avro::NodePtr & field_node, const void * member_ptr,
  const intro::MessageMember & member)
{
  if (member.is_array_) {
    const avro::NodePtr node = select_branch(field, field_node, avro::AVRO_ARRAY);
    if (node->type() != avro::AVRO_ARRAY) {
      throw std::runtime_error(std::string("ROS array field '") + member.name_ +
                               "' mapped to non-array Avro type " + avro::toString(node->type()));
    }
    auto & arr = field.value<avro::GenericArray>();
    auto & items = arr.value();
    items.clear();
    const avro::NodePtr elem_node = node->leafAt(0);
    const size_t n = member.size_function ? member.size_function(member_ptr) : member.array_size_;
    items.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      avro::GenericDatum elem(elem_node);
      bool scratch = false;
      const void * ep = element_ptr(member, member_ptr, i, scratch);
      if (member.type_id_ == intro::ROS_TYPE_MESSAGE) {
        fill_record(elem, elem_node, ep, nested_members(member));
      } else {
        select_branch(elem, elem_node, avro_type_for_ros_type(member.type_id_));
        fill_primitive(elem, ep, member.type_id_);
      }
      items.push_back(std::move(elem));
    }
    return;
  }

  if (member.type_id_ == intro::ROS_TYPE_MESSAGE) {
    fill_record(field, field_node, member_ptr, nested_members(member));
    return;
  }

  select_branch(field, field_node, avro_type_for_ros_type(member.type_id_));
  fill_primitive(field, member_ptr, member.type_id_);
}

}  // namespace

void fill_primitive(avro::GenericDatum & datum, const void * p, uint8_t ros_type_id)
{
  switch (ros_type_id) {
    case intro::ROS_TYPE_BOOLEAN:
      if (datum.type() == avro::AVRO_BOOL) {
        datum.value<bool>() = *static_cast<const bool *>(p);
      } else {
        set_integer(datum, *static_cast<const bool *>(p) ? 1 : 0);
      }
      break;
    case intro::ROS_TYPE_INT8:
      set_integer(datum, *static_cast<const int8_t *>(p));
      break;
    case intro::ROS_TYPE_UINT8:
    case intro::ROS_TYPE_OCTET:   // rosidl C++ maps octet to unsigned char
    case intro::ROS_TYPE_CHAR:    // rosidl C++ maps char to unsigned char
      set_integer(datum, *static_cast<const uint8_t *>(p));
      break;
    case intro::ROS_TYPE_WCHAR:   // rosidl C++ maps wchar to char16_t
      set_integer(datum, *static_cast<const char16_t *>(p));
      break;
    case intro::ROS_TYPE_INT16:
      set_integer(datum, *static_cast<const int16_t *>(p));
      break;
    case intro::ROS_TYPE_UINT16:
      set_integer(datum, *static_cast<const uint16_t *>(p));
      break;
    case intro::ROS_TYPE_INT32:
      set_integer(datum, *static_cast<const int32_t *>(p));
      break;
    case intro::ROS_TYPE_UINT32:
      set_integer(datum, static_cast<int64_t>(*static_cast<const uint32_t *>(p)));
      break;
    case intro::ROS_TYPE_INT64:
      set_integer(datum, *static_cast<const int64_t *>(p));
      break;
    case intro::ROS_TYPE_UINT64:
      // Values above INT64_MAX wrap to negative; the spec maps uint64 -> long knowingly.
      set_integer(datum, static_cast<int64_t>(*static_cast<const uint64_t *>(p)));
      break;
    case intro::ROS_TYPE_FLOAT:
      set_floating(datum, *static_cast<const float *>(p));
      break;
    case intro::ROS_TYPE_DOUBLE:
      set_floating(datum, *static_cast<const double *>(p));
      break;
    case intro::ROS_TYPE_LONG_DOUBLE:
      set_floating(datum, static_cast<double>(*static_cast<const long double *>(p)));
      break;
    case intro::ROS_TYPE_STRING:
      set_string(datum, *static_cast<const std::string *>(p));
      break;
    case intro::ROS_TYPE_WSTRING:
      set_string(datum, utf16_to_utf8(*static_cast<const std::u16string *>(p)));
      break;
    default:
      throw std::invalid_argument(
              "fill_primitive: unsupported ROS type id " + std::to_string(ros_type_id));
  }
}

void fill_record(
  avro::GenericDatum & datum, const avro::NodePtr & node, const void * ros_message,
  const intro::MessageMembers * members)
{
  const avro::NodePtr rec_node = select_branch(datum, node, avro::AVRO_RECORD);
  if (rec_node->type() != avro::AVRO_RECORD) {
    throw std::runtime_error("ROS message mapped to non-record Avro type " +
                             avro::toString(rec_node->type()));
  }
  auto & rec = datum.value<avro::GenericRecord>();
  for (size_t i = 0; i < rec.fieldCount(); ++i) {
    const std::string & field_name = rec_node->nameAt(i);
    const intro::MessageMember * member = find_member(members, field_name);
    if (member == nullptr) {
      continue;  // schema-only field: keep the Avro default
    }
    const void * member_ptr = static_cast<const uint8_t *>(ros_message) + member->offset_;
    fill_member(rec.fieldAt(i), rec_node->leafAt(i), member_ptr, *member);
  }
}

AvroMapper::AvroMapper(avro::ValidSchema envelope_schema)
: schema_(std::move(envelope_schema))
{
  const avro::NodePtr root = schema_.root();
  if (root->type() != avro::AVRO_RECORD) {
    throw std::invalid_argument("envelope schema must be a record");
  }
  for (const char * required : {"robot_id", "ros_topic", "recv_time_ns", "msg"}) {
    size_t idx = 0;
    if (!root->nameIndex(required, idx)) {
      throw std::invalid_argument(std::string("envelope schema lacks field '") + required + "'");
    }
  }
}

avro::GenericDatum AvroMapper::build(
  const std::string & robot_id, const std::string & ros_topic, int64_t recv_time_ns,
  const void * ros_message, const intro::MessageMembers * members) const
{
  avro::GenericDatum datum(schema_);
  auto & rec = datum.value<avro::GenericRecord>();
  const avro::NodePtr root = schema_.root();

  set_string(rec.field("robot_id"), robot_id);
  set_string(rec.field("ros_topic"), ros_topic);
  set_integer(rec.field("recv_time_ns"), recv_time_ns);

  size_t msg_idx = 0;
  root->nameIndex("msg", msg_idx);
  fill_record(rec.fieldAt(msg_idx), root->leafAt(msg_idx), ros_message, members);
  return datum;
}

std::vector<uint8_t> AvroMapper::encode(const avro::GenericDatum & datum) const
{
  std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
  avro::EncoderPtr encoder = avro::binaryEncoder();
  encoder->init(*out);
  avro::encode(*encoder, datum);
  encoder->flush();
  std::shared_ptr<std::vector<uint8_t>> bytes = avro::snapshot(*out);
  return *bytes;
}

std::vector<uint8_t> AvroMapper::serialize(
  int32_t schema_id, const std::string & robot_id, const std::string & ros_topic,
  int64_t recv_time_ns, const void * ros_message, const intro::MessageMembers * members) const
{
  std::vector<uint8_t> out = wire_header(schema_id);
  const std::vector<uint8_t> body = encode(build(robot_id, ros_topic, recv_time_ns, ros_message, members));
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

}  // namespace rtdaas_kafka_bridge
