// Minimal Confluent-compatible schema registry client (libcurl + nlohmann/json).
// Works against Confluent Schema Registry and Apicurio Registry's ccompat API
// (e.g. http://apicurio:8080/apis/ccompat/v7).
#pragma once

#include <cstdint>
#include <string>

namespace rtdaas_kafka_bridge
{

struct RegisteredSchema
{
  int32_t id{0};
  int32_t version{0};
  std::string schema_json;   // the Avro schema document as a JSON string
};

class RegistryClient
{
public:
  explicit RegistryClient(std::string base_url, long timeout_ms = 5000);

  /// GET /subjects/{subject}/versions/latest. Throws std::runtime_error on HTTP/transport error.
  RegisteredSchema latest(const std::string & subject) const;

  /// GET /schemas/ids/{id}. Throws on error.
  std::string schema_by_id(int32_t id) const;

  /// Spec §2.2: subject name is "<kafka topic>-value".
  static std::string subject_for_topic(const std::string & kafka_topic);

  const std::string & base_url() const {return base_url_;}

private:
  std::string get(const std::string & path) const;

  std::string base_url_;
  long timeout_ms_;
};

}  // namespace rtdaas_kafka_bridge
