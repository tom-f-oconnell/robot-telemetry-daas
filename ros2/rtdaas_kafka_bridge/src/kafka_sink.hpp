// Thin librdkafka producer wrapper: idempotent, compressed, with delivery-report accounting.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <librdkafka/rdkafkacpp.h>
#include <rclcpp/clock.hpp>
#include <rclcpp/logger.hpp>

namespace rtdaas_kafka_bridge
{

struct KafkaSinkConfig
{
  std::string bootstrap{"localhost:9092"};
  std::string client_id{"rtdaas-kafka-bridge"};
  int queue_max_messages{100000};
  int linger_ms{20};
  int batch_num_messages{10000};
  std::string compression{"lz4"};
  bool idempotent{true};
  int message_timeout_ms{30000};
};

struct KafkaSinkStats
{
  uint64_t produced{0};
  uint64_t delivered{0};
  uint64_t failed{0};
  uint64_t dropped{0};     // rejected locally (queue full after retry)
  uint64_t bytes{0};
};

class KafkaSink
{
public:
  using Headers = std::vector<std::pair<std::string, std::string>>;

  KafkaSink(const KafkaSinkConfig & cfg, rclcpp::Logger logger);
  ~KafkaSink();

  KafkaSink(const KafkaSink &) = delete;
  KafkaSink & operator=(const KafkaSink &) = delete;

  /// Enqueue one record. Returns false if the local queue rejected it.
  bool produce(
    const std::string & topic, const std::string & key, const std::vector<uint8_t> & payload,
    const Headers & headers);

  /// Serve delivery reports; call frequently (e.g. from a timer).
  void poll(int timeout_ms = 0);

  /// Block until all queued messages are delivered or `timeout_ms` elapses.
  void flush(int timeout_ms);

  KafkaSinkStats stats() const;

private:
  class DeliveryReport : public RdKafka::DeliveryReportCb
  {
public:
    DeliveryReport(KafkaSink & owner, rclcpp::Logger logger)
    : owner_(owner), logger_(logger) {}
    void dr_cb(RdKafka::Message & message) override;

private:
    KafkaSink & owner_;
    rclcpp::Logger logger_;
  };

  class EventCb : public RdKafka::EventCb
  {
public:
    explicit EventCb(rclcpp::Logger logger)
    : logger_(logger) {}
    void event_cb(RdKafka::Event & event) override;

private:
    rclcpp::Logger logger_;
  };

  rclcpp::Logger logger_;
  rclcpp::Clock clock_{RCL_STEADY_TIME};   // for throttled logging
  DeliveryReport dr_cb_;
  EventCb event_cb_;
  std::unique_ptr<RdKafka::Producer> producer_;
  mutable std::mutex mutex_;
  std::atomic<uint64_t> produced_{0};
  std::atomic<uint64_t> delivered_{0};
  std::atomic<uint64_t> failed_{0};
  std::atomic<uint64_t> dropped_{0};
  std::atomic<uint64_t> bytes_{0};
};

}  // namespace rtdaas_kafka_bridge
