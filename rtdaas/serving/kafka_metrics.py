"""Kafka's health for the UI, from Prometheus (``GET /api/kafka/metrics``).

Reads kafka-exporter's metrics, which both the exporter Strimzi runs on Kubernetes and the compose
stack's expose. Bytes per second come from the brokers' own JMX metrics, so they are only there where
those are scraped: on Kubernetes (``infra/k8s/manifests/kafka/metrics.yaml``), not in compose.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from datetime import UTC, datetime

import requests

log = logging.getLogger(__name__)

TOPICS = 'topic=~"rtdaas.*"'
MESSAGE_RATE = f"sum by (topic) (rate(kafka_topic_partition_current_offset{{{TOPICS}}}[2m]))"
QUERIES = {
    "brokers": "max(kafka_brokers)",
    "partitions": f"max by (topic) (kafka_topic_partitions{{{TOPICS}}})",
    "messages_per_s": MESSAGE_RATE,
    "retained_messages": (
        f"sum by (topic) (kafka_topic_partition_current_offset{{{TOPICS}}}"
        f" - kafka_topic_partition_oldest_offset{{{TOPICS}}})"
    ),
    "under_replicated": f"sum by (topic) (kafka_topic_partition_under_replicated_partition{{{TOPICS}}})",
    "offline_partitions": f"count(kafka_topic_partition_leader{{{TOPICS}}} == -1)",
    "consumer_lag": "sum by (consumergroup, topic) (kafka_consumergroup_lag)",
    "bytes_in_per_s": "sum(rate(kafka_server_brokertopicmetrics_bytesinpersec_total[2m]))",
    "bytes_out_per_s": "sum(rate(kafka_server_brokertopicmetrics_bytesoutpersec_total[2m]))",
}


def _get(url: str, path: str, params: dict) -> list[dict]:
    response = requests.get(f"{url.rstrip('/')}{path}", params=params, timeout=5.0)
    response.raise_for_status()
    body = response.json()
    if body.get("status") != "success":
        raise ValueError(f"Prometheus said: {body.get('error')}")
    return body["data"]["result"]


def _number(raw: str | float) -> float | None:
    value = float(raw)
    return round(value, 3) if math.isfinite(value) else None


def _scalar(rows: list[dict]) -> float | None:
    return _number(rows[0]["value"][1]) if rows else None


def _by_topic(rows: list[dict]) -> dict[str, float | None]:
    return {row["metric"]["topic"]: _number(row["value"][1]) for row in rows}


def _series(rows: list[dict], start: float, end: float, step: int) -> dict:
    """Per-topic values on one shared time axis, null where a topic has no sample."""
    stamps = [start + i * step for i in range(int((end - start) // step) + 1)]
    topics = {}
    for row in rows:
        values = {round(float(t) - start): _number(v) for t, v in row["values"]}
        topics[row["metric"]["topic"]] = [values.get(round(s - start)) for s in stamps]
    return {
        "t": [datetime.fromtimestamp(s, UTC).strftime("%Y-%m-%dT%H:%M:%SZ") for s in stamps],
        "topics": dict(sorted(topics.items())),
    }


def kafka_overview(
    prometheus_url: str,
    minutes: int = 30,
    now: float | None = None,
    get: Callable[[str, str, dict], list[dict]] = _get,
) -> dict:
    """Brokers, per-topic rates and sizes, consumer lag and recent history.

    Returns ``{"available": False, "reason": ...}`` rather than raising when there is nothing to
    show: no Prometheus configured, one that cannot be reached, or one with no Kafka metrics.
    """
    if not prometheus_url:
        return {"available": False, "reason": "PROMETHEUS_URL is not set."}
    end = time.time() if now is None else now
    start = end - minutes * 60
    step = max(15, minutes * 60 // 120)
    try:
        latest = {
            name: get(prometheus_url, "/api/v1/query", {"query": query, "time": end})
            for name, query in QUERIES.items()
        }
        history = get(
            prometheus_url,
            "/api/v1/query_range",
            {"query": MESSAGE_RATE, "start": start, "end": end, "step": step},
        )
    except (requests.RequestException, ValueError, KeyError) as exc:
        log.warning("cannot read Kafka metrics from %s: %s", prometheus_url, exc)
        return {"available": False, "reason": f"Prometheus cannot be read at {prometheus_url}."}

    brokers = _scalar(latest["brokers"])
    if brokers is None:
        return {
            "available": False,
            "reason": f"Prometheus at {prometheus_url} has no Kafka metrics, so nothing is scraping them.",
        }
    rates = _by_topic(latest["messages_per_s"])
    retained = _by_topic(latest["retained_messages"])
    under_replicated = _by_topic(latest["under_replicated"])
    partitions = _by_topic(latest["partitions"])
    return {
        "available": True,
        "minutes": minutes,
        "brokers": int(brokers),
        "messages_per_s": round(sum(v for v in rates.values() if v is not None), 3),
        "bytes_in_per_s": _scalar(latest["bytes_in_per_s"]),
        "bytes_out_per_s": _scalar(latest["bytes_out_per_s"]),
        "under_replicated_partitions": int(sum(v or 0 for v in under_replicated.values())),
        "offline_partitions": int(_scalar(latest["offline_partitions"]) or 0),
        "topics": [
            {
                "topic": topic,
                "partitions": int(partitions[topic]) if partitions.get(topic) is not None else None,
                "messages_per_s": rates.get(topic),
                "retained_messages": int(retained[topic])
                if retained.get(topic) is not None
                else None,
                "under_replicated_partitions": int(under_replicated.get(topic) or 0),
            }
            for topic in sorted({*partitions, *rates, *retained})
        ],
        "consumer_groups": sorted(
            (
                {
                    "group": row["metric"].get("consumergroup"),
                    "topic": row["metric"].get("topic"),
                    "lag": int(_number(row["value"][1]) or 0),
                }
                for row in latest["consumer_lag"]
            ),
            key=lambda row: (str(row["group"]), str(row["topic"])),
        ),
        "history": _series(history, start, end, step),
    }
