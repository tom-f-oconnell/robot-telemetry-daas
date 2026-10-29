#include "bridge_config.hpp"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include <yaml-cpp/yaml.h>

namespace rtdaas_kafka_bridge
{

std::string normalize_type(const std::string & type)
{
  // Count '/' separators. "pkg/Type" -> insert "/msg"; "pkg/msg/Type" unchanged.
  const auto first = type.find('/');
  if (first == std::string::npos) {
    throw std::invalid_argument("message type must be 'pkg/Type' or 'pkg/msg/Type': " + type);
  }
  const auto second = type.find('/', first + 1);
  if (second == std::string::npos) {
    return type.substr(0, first) + "/msg/" + type.substr(first + 1);
  }
  const std::string middle = type.substr(first + 1, second - first - 1);
  if (middle != "msg") {
    throw std::invalid_argument("only message interfaces are supported (pkg/msg/Type): " + type);
  }
  if (type.find('/', second + 1) != std::string::npos) {
    throw std::invalid_argument("malformed message type: " + type);
  }
  return type;
}

void validate(const TopicMapping & mapping)
{
  if (mapping.ros_topic.empty() || mapping.ros_topic.front() != '/') {
    throw std::invalid_argument(
            "mapping '" + mapping.name + "': ros_topic must be an absolute topic name");
  }
  if (mapping.kafka_topic.empty()) {
    throw std::invalid_argument("mapping '" + mapping.name + "': kafka_topic is required");
  }
  if (mapping.type.empty()) {
    throw std::invalid_argument("mapping '" + mapping.name + "': type is required");
  }
  normalize_type(mapping.type);  // throws on malformed type
  if (mapping.qos != "best_effort" && mapping.qos != "reliable") {
    throw std::invalid_argument(
            "mapping '" + mapping.name + "': qos must be 'best_effort' or 'reliable', got '" +
            mapping.qos + "'");
  }
}

namespace
{

TopicMapping mapping_from_node(const YAML::Node & node, const std::string & default_name)
{
  if (!node.IsMap()) {
    throw std::invalid_argument("each topic mapping must be a YAML map");
  }
  TopicMapping m;
  m.name = node["name"] ? node["name"].as<std::string>() : default_name;
  if (node["ros_topic"]) {m.ros_topic = node["ros_topic"].as<std::string>();}
  if (node["kafka_topic"]) {m.kafka_topic = node["kafka_topic"].as<std::string>();}
  if (node["type"]) {m.type = normalize_type(node["type"].as<std::string>());}
  if (node["qos"]) {m.qos = node["qos"].as<std::string>();}
  if (m.name.empty()) {m.name = m.ros_topic;}
  validate(m);
  return m;
}

}  // namespace

BridgeConfig parse_config_yaml(const std::string & yaml_text)
{
  BridgeConfig cfg;
  YAML::Node root = YAML::Load(yaml_text);
  if (!root || !root["topics"]) {
    throw std::invalid_argument("config must contain a top-level 'topics' key");
  }
  const YAML::Node topics = root["topics"];
  if (topics.IsSequence()) {
    size_t i = 0;
    for (const auto & item : topics) {
      cfg.topics.push_back(mapping_from_node(item, "topic_" + std::to_string(i++)));
    }
  } else if (topics.IsMap()) {
    for (const auto & kv : topics) {
      cfg.topics.push_back(mapping_from_node(kv.second, kv.first.as<std::string>()));
    }
  } else {
    throw std::invalid_argument("'topics' must be a sequence or a map");
  }
  return cfg;
}

BridgeConfig load_config_file(const std::string & path)
{
  std::ifstream in(path);
  if (!in) {
    throw std::invalid_argument("cannot open config file: " + path);
  }
  std::stringstream buf;
  buf << in.rdbuf();
  return parse_config_yaml(buf.str());
}

std::vector<TopicMapping> mappings_from_parameters(const std::map<std::string, std::string> & flat)
{
  // Group "topics.<name>.<key>" by <name>, preserving first-seen order.
  std::vector<std::string> order;
  std::map<std::string, TopicMapping> by_name;
  const std::string prefix = "topics.";
  for (const auto & [param, value] : flat) {
    if (param.rfind(prefix, 0) != 0) {continue;}
    const std::string rest = param.substr(prefix.size());
    const auto dot = rest.find('.');
    if (dot == std::string::npos) {continue;}
    const std::string name = rest.substr(0, dot);
    const std::string key = rest.substr(dot + 1);
    if (by_name.find(name) == by_name.end()) {
      order.push_back(name);
      by_name[name].name = name;
    }
    TopicMapping & m = by_name[name];
    if (key == "ros_topic") {m.ros_topic = value;} else if (key == "kafka_topic") {
      m.kafka_topic = value;
    } else if (key == "type") {m.type = normalize_type(value);} else if (key == "qos") {
      m.qos = value;
    } else {
      throw std::invalid_argument("unknown topic mapping key '" + key + "' in parameter " + param);
    }
  }
  std::vector<TopicMapping> out;
  for (const auto & name : order) {
    validate(by_name[name]);
    out.push_back(by_name[name]);
  }
  return out;
}

void merge_mappings(std::vector<TopicMapping> & base, const std::vector<TopicMapping> & extra)
{
  for (const auto & m : extra) {
    const bool dup = std::any_of(
      base.begin(), base.end(), [&](const TopicMapping & b) {
        return b.ros_topic == m.ros_topic && b.kafka_topic == m.kafka_topic;
      });
    if (!dup) {base.push_back(m);}
  }
}

}  // namespace rtdaas_kafka_bridge
