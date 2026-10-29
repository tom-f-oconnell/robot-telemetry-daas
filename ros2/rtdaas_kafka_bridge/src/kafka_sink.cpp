#include "kafka_sink.hpp"

#include <stdexcept>

#include <rclcpp/logging.hpp>

namespace rtdaas_kafka_bridge
{

namespace
{

void set_or_throw(RdKafka::Conf & conf, const std::string & key, const std::string & value)
{
  std::string errstr;
  if (conf.set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error("librdkafka conf '" + key + "'='" + value + "': " + errstr);
  }
}

}  // namespace

void KafkaSink::DeliveryReport::dr_cb(RdKafka::Message & message)
{
  if (message.err() != RdKafka::ERR_NO_ERROR) {
    owner_.failed_.fetch_add(1, std::memory_order_relaxed);
    RCLCPP_WARN_THROTTLE(
      logger_, owner_.clock_, 5000,
      "delivery failed on %s: %s", message.topic_name().c_str(), message.errstr().c_str());
  } else {
    owner_.delivered_.fetch_add(1, std::memory_order_relaxed);
  }
}

void KafkaSink::EventCb::event_cb(RdKafka::Event & event)
{
  switch (event.type()) {
    case RdKafka::Event::EVENT_ERROR:
      RCLCPP_ERROR(logger_, "kafka error: %s (%s)",
                   RdKafka::err2str(event.err()).c_str(), event.str().c_str());
      break;
    case RdKafka::Event::EVENT_LOG:
      RCLCPP_DEBUG(logger_, "kafka log [%s]: %s", event.fac().c_str(), event.str().c_str());
      break;
    default:
      break;
  }
}

KafkaSink::KafkaSink(const KafkaSinkConfig & cfg, rclcpp::Logger logger)
: logger_(logger), dr_cb_(*this, logger), event_cb_(logger)
{
  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  set_or_throw(*conf, "bootstrap.servers", cfg.bootstrap);
  set_or_throw(*conf, "client.id", cfg.client_id);
  set_or_throw(*conf, "queue.buffering.max.messages", std::to_string(cfg.queue_max_messages));
  set_or_throw(*conf, "linger.ms", std::to_string(cfg.linger_ms));
  set_or_throw(*conf, "batch.num.messages", std::to_string(cfg.batch_num_messages));
  set_or_throw(*conf, "compression.type", cfg.compression);
  set_or_throw(*conf, "enable.idempotence", cfg.idempotent ? "true" : "false");
  set_or_throw(*conf, "acks", "all");
  set_or_throw(*conf, "message.timeout.ms", std::to_string(cfg.message_timeout_ms));

  std::string errstr;
  if (conf->set("dr_cb", &dr_cb_, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error("librdkafka dr_cb: " + errstr);
  }
  if (conf->set("event_cb", &event_cb_, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error("librdkafka event_cb: " + errstr);
  }

  producer_.reset(RdKafka::Producer::create(conf.get(), errstr));
  if (!producer_) {
    throw std::runtime_error("failed to create Kafka producer: " + errstr);
  }
  RCLCPP_INFO(logger_, "Kafka producer connected to %s (compression=%s, idempotent=%s)",
              cfg.bootstrap.c_str(), cfg.compression.c_str(), cfg.idempotent ? "yes" : "no");
}

KafkaSink::~KafkaSink()
{
  if (producer_) {
    producer_->flush(5000);
  }
  RdKafka::wait_destroyed(1000);
}

bool KafkaSink::produce(
  const std::string & topic, const std::string & key, const std::vector<uint8_t> & payload,
  const Headers & headers)
{
  std::lock_guard<std::mutex> lock(mutex_);
  for (int attempt = 0; attempt < 2; ++attempt) {
    RdKafka::Headers * hdrs = RdKafka::Headers::create();
    for (const auto & [name, value] : headers) {
      hdrs->add(name, value);
    }
    // RK_MSG_COPY: librdkafka copies the payload, so the caller's buffer may be reused.
    const RdKafka::ErrorCode err = producer_->produce(
      topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
      const_cast<uint8_t *>(payload.data()), payload.size(),
      key.data(), key.size(),
      0 /* timestamp: now */, hdrs, nullptr);
    if (err == RdKafka::ERR_NO_ERROR) {
      produced_.fetch_add(1, std::memory_order_relaxed);
      bytes_.fetch_add(payload.size(), std::memory_order_relaxed);
      producer_->poll(0);
      return true;
    }
    // On failure librdkafka does not take ownership of the headers.
    delete hdrs;
    if (err == RdKafka::ERR__QUEUE_FULL && attempt == 0) {
      producer_->poll(10);   // let delivery reports drain the queue, then retry once
      continue;
    }
    RCLCPP_WARN_THROTTLE(
      logger_, clock_, 5000,
      "produce to %s failed: %s", topic.c_str(), RdKafka::err2str(err).c_str());
    dropped_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  return false;
}

void KafkaSink::poll(int timeout_ms)
{
  std::lock_guard<std::mutex> lock(mutex_);
  producer_->poll(timeout_ms);
}

void KafkaSink::flush(int timeout_ms)
{
  std::lock_guard<std::mutex> lock(mutex_);
  const RdKafka::ErrorCode err = producer_->flush(timeout_ms);
  if (err != RdKafka::ERR_NO_ERROR) {
    RCLCPP_WARN(logger_, "flush incomplete: %s (%d messages still queued)",
                RdKafka::err2str(err).c_str(), producer_->outq_len());
  }
}

KafkaSinkStats KafkaSink::stats() const
{
  KafkaSinkStats s;
  s.produced = produced_.load();
  s.delivered = delivered_.load();
  s.failed = failed_.load();
  s.dropped = dropped_.load();
  s.bytes = bytes_.load();
  return s;
}

}  // namespace rtdaas_kafka_bridge
