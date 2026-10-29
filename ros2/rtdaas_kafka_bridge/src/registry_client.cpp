#include "registry_client.hpp"

#include <mutex>
#include <stdexcept>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

namespace rtdaas_kafka_bridge
{

namespace
{

void ensure_curl_global_init()
{
  static std::once_flag once;
  std::call_once(once, []() {curl_global_init(CURL_GLOBAL_DEFAULT);});
}

size_t write_cb(char * ptr, size_t size, size_t nmemb, void * userdata)
{
  auto * out = static_cast<std::string *>(userdata);
  out->append(ptr, size * nmemb);
  return size * nmemb;
}

std::string strip_trailing_slash(std::string s)
{
  while (!s.empty() && s.back() == '/') {
    s.pop_back();
  }
  return s;
}

}  // namespace

RegistryClient::RegistryClient(std::string base_url, long timeout_ms)
: base_url_(strip_trailing_slash(std::move(base_url))), timeout_ms_(timeout_ms)
{
  ensure_curl_global_init();
}

std::string RegistryClient::subject_for_topic(const std::string & kafka_topic)
{
  return kafka_topic + "-value";
}

std::string RegistryClient::get(const std::string & path) const
{
  CURL * curl = curl_easy_init();
  if (curl == nullptr) {
    throw std::runtime_error("curl_easy_init failed");
  }
  std::string body;
  const std::string url = base_url_ + path;
  char errbuf[CURL_ERROR_SIZE] = {0};

  struct curl_slist * headers = nullptr;
  headers = curl_slist_append(headers, "Accept: application/vnd.schemaregistry.v1+json, application/json");

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms_);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);

  const CURLcode rc = curl_easy_perform(curl);
  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  if (rc != CURLE_OK) {
    throw std::runtime_error("registry GET " + url + " failed: " +
                             (errbuf[0] ? std::string(errbuf) : curl_easy_strerror(rc)));
  }
  if (http_code < 200 || http_code >= 300) {
    throw std::runtime_error("registry GET " + url + " returned HTTP " + std::to_string(http_code) +
                             ": " + body);
  }
  return body;
}

RegisteredSchema RegistryClient::latest(const std::string & subject) const
{
  CURL * curl = curl_easy_init();
  char * escaped = curl_easy_escape(curl, subject.c_str(), static_cast<int>(subject.size()));
  const std::string subj = escaped ? std::string(escaped) : subject;
  curl_free(escaped);
  curl_easy_cleanup(curl);

  const std::string body = get("/subjects/" + subj + "/versions/latest");
  const nlohmann::json j = nlohmann::json::parse(body);

  RegisteredSchema out;
  out.id = j.at("id").get<int32_t>();
  out.version = j.value("version", 0);
  const auto & schema = j.at("schema");
  // Confluent/Apicurio return the schema as an escaped JSON string; be lenient if an
  // implementation returns it as an embedded object.
  out.schema_json = schema.is_string() ? schema.get<std::string>() : schema.dump();
  return out;
}

std::string RegistryClient::schema_by_id(int32_t id) const
{
  const std::string body = get("/schemas/ids/" + std::to_string(id));
  const nlohmann::json j = nlohmann::json::parse(body);
  const auto & schema = j.at("schema");
  return schema.is_string() ? schema.get<std::string>() : schema.dump();
}

}  // namespace rtdaas_kafka_bridge
