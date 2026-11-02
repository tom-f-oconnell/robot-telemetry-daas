from __future__ import annotations

import pytest
import requests

from rtdaas.serving.kafka_metrics import QUERIES, kafka_overview

NOW = 1_800_000_000.0
JOINT, POSE = "rtdaas.telemetry.joint_states", "rtdaas.telemetry.tool_pose"


def _row(value, **labels):
    return {"metric": labels, "value": [NOW, str(value)]}


LATEST = {
    "brokers": [_row(3)],
    "partitions": [_row(12, topic=JOINT), _row(12, topic=POSE)],
    "messages_per_s": [_row(100.5, topic=JOINT), _row("NaN", topic=POSE)],
    "retained_messages": [_row(5000, topic=JOINT), _row(0, topic=POSE)],
    "under_replicated": [_row(0, topic=JOINT), _row(2, topic=POSE)],
    "offline_partitions": [],
    "consumer_lag": [_row(7, consumergroup="g", topic=POSE)],
    "bytes_in_per_s": [_row(2048)],
    "bytes_out_per_s": [],
}


def _fake(history=None):
    def get(url, path, params):
        if path == "/api/v1/query_range":
            return history or []
        name = next(n for n, q in QUERIES.items() if q == params["query"])
        return LATEST[name]

    return get


def test_overview_shapes_what_prometheus_returns():
    m = kafka_overview("http://p", minutes=5, now=NOW, get=_fake())
    assert m["available"] and m["brokers"] == 3
    assert m["messages_per_s"] == 100.5  # a NaN rate is not a number to add up
    assert m["bytes_in_per_s"] == 2048 and m["bytes_out_per_s"] is None
    assert m["under_replicated_partitions"] == 2 and m["offline_partitions"] == 0
    assert m["consumer_groups"] == [{"group": "g", "topic": POSE, "lag": 7}]
    pose = next(t for t in m["topics"] if t["topic"] == POSE)
    assert pose == {
        "topic": POSE,
        "partitions": 12,
        "messages_per_s": None,
        "retained_messages": 0,
        "under_replicated_partitions": 2,
    }


def test_history_shares_one_time_axis_with_gaps_for_missing_samples():
    start = NOW - 300
    history = [
        {"metric": {"topic": POSE}, "values": [[start, "1"], [start + 30, "2"], [start + 300, "3"]]}
    ]
    h = kafka_overview("http://p", minutes=5, now=NOW, get=_fake(history))["history"]
    assert len(h["t"]) == 21 and h["t"][0] == "2027-01-15T07:55:00Z"
    series = h["topics"][POSE]
    assert series[0] == 1 and series[2] == 2 and series[-1] == 3 and series[1] is None


@pytest.mark.parametrize(
    "url,get,reason",
    [
        ("", None, "PROMETHEUS_URL is not set"),
        (
            "http://p",
            lambda *a: (_ for _ in ()).throw(requests.ConnectionError("down")),
            "cannot be read",
        ),
        ("http://p", lambda *a: [], "no Kafka metrics"),
    ],
)
def test_nothing_to_show_says_why(url, get, reason):
    m = kafka_overview(url, now=NOW, **({"get": get} if get else {}))
    assert m["available"] is False and reason in m["reason"]


def test_route_reports_when_prometheus_is_not_configured(client):
    assert client.get("/api/kafka/metrics").json()["available"] is False
    assert client.get("/api/kafka/metrics", params={"minutes": 1}).status_code == 400
