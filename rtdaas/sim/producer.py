"""Kafka producer for simulated telemetry (requires ``confluent-kafka``).

Fetches the latest envelope schema for every topic from the schema registry at
start-up, then publishes Confluent wire-format Avro values keyed by ``robot_id``,
with informational headers (``robot_id``, ``ros_topic``, ``msg_type``, ``schema_id``).
"""

from __future__ import annotations

import logging
from typing import Any

from rtdaas.contracts.registry import RegistryClient, subject_for_topic
from rtdaas.contracts.wire import encode_wire

log = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, bootstrap: str, registry_url: str, topics: dict[str, str], extra_config: dict[str, Any] | None = None) -> None:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:  # pragma: no cover
            raise ImportError("confluent-kafka is required for --kafka: pip install 'rtdaas[kafka]'") from exc
        self.topics = dict(topics)
        self.registry = RegistryClient(registry_url)
        self._schemas: dict[str, tuple[int, dict[str, Any]]] = {}
        for topic in self.topics:
            schema_id, _version, schema = self.registry.latest(subject_for_topic(topic))
            self._schemas[topic] = (schema_id, schema)
            log.info("topic %s uses schema id %d", topic, schema_id)
        conf = {
            "bootstrap.servers": bootstrap,
            "linger.ms": 20,
            "batch.num.messages": 5000,
            "compression.type": "lz4",
            "enable.idempotence": True,
            "acks": "all",
        }
        conf.update(extra_config or {})
        self._producer = Producer(conf)
        self.sent = 0
        self.errors = 0

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.errors += 1
            log.error("delivery failed: %s", err)

    def send(self, kafka_topic: str, robot_id: str, ros_topic: str, msg: dict[str, Any], recv_time_ns: int) -> None:
        schema_id, schema = self._schemas[kafka_topic]
        envelope = {"robot_id": robot_id, "ros_topic": ros_topic, "recv_time_ns": int(recv_time_ns), "msg": msg}
        value = encode_wire(schema_id, schema, envelope)
        headers = [
            ("robot_id", robot_id.encode()),
            ("ros_topic", ros_topic.encode()),
            ("msg_type", self.topics[kafka_topic].encode()),
            ("schema_id", str(schema_id).encode()),
        ]
        while True:
            try:
                self._producer.produce(kafka_topic, key=robot_id.encode(), value=value, headers=headers, on_delivery=self._on_delivery)
                break
            except BufferError:
                self._producer.poll(0.05)
        self.sent += 1
        if self.sent % 1000 == 0:
            self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> None:
        self._producer.flush(timeout)
