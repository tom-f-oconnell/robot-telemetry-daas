"""Minimal Elasticsearch read client for the API (search only; indexing lives in Spark jobs).

Uses ``requests`` directly so the API image does not need the ``elasticsearch`` package.
Every method returns ``None`` when the cluster is unreachable so callers can fall back to SQL.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

EVENTS_INDEX = "rtdaas-events"
GESTURES_INDEX = "rtdaas-gestures"


class ElasticsearchClient:
    def __init__(self, base_url: str, timeout: float = 3.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def ping(self) -> bool:
        try:
            r = requests.get(f"{self.base_url}/_cluster/health", timeout=1.5)
            return r.ok
        except requests.RequestException:
            return False

    def _search(self, index: str, body: dict) -> list[dict] | None:
        try:
            r = requests.post(f"{self.base_url}/{index}/_search", json=body, timeout=self.timeout)
            if r.status_code == 404:
                return []
            r.raise_for_status()
            return [h["_source"] for h in r.json().get("hits", {}).get("hits", [])]
        except requests.RequestException as exc:
            log.warning("elasticsearch search failed: %s", exc)
            return None

    @staticmethod
    def _range(field: str, start: str | None, end: str | None) -> dict | None:
        if not start and not end:
            return None
        rng: dict[str, Any] = {}
        if start:
            rng["gte"] = start
        if end:
            rng["lte"] = end
        return {"range": {field: rng}}

    def search_events(
        self, q=None, robot_id=None, severity_min=None, start=None, end=None, limit=200
    ):
        must: list[dict] = []
        if q:
            must.append(
                {
                    "query_string": {
                        "query": q,
                        "fields": ["code", "message", "arm"],
                        "default_operator": "AND",
                    }
                }
            )
        if robot_id:
            must.append({"term": {"robot_id": robot_id}})
        if severity_min is not None:
            must.append({"range": {"severity": {"gte": int(severity_min)}}})
        rng = self._range("ts", start, end)
        if rng:
            must.append(rng)
        body = {
            "size": int(limit),
            "sort": [{"ts": "desc"}],
            "query": {"bool": {"must": must or [{"match_all": {}}]}},
        }
        return self._search(EVENTS_INDEX, body)

    def search_gestures(self, task=None, gesture=None, skill_level=None, limit=500):
        must: list[dict] = [
            {"term": {k: v}}
            for k, v in (("task", task), ("gesture", gesture), ("skill_level", skill_level))
            if v
        ]
        body = {
            "size": int(limit),
            "sort": [{"task": "asc"}, {"trial_id": "asc"}, {"start_frame": "asc"}],
            "query": {"bool": {"must": must or [{"match_all": {}}]}},
        }
        return self._search(GESTURES_INDEX, body)
