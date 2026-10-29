"""Client for a Confluent-compatible schema registry.

Works against Confluent Schema Registry and Apicurio Registry's ``ccompat`` API
(``http://apicurio:8080/apis/ccompat/v7``). Only the handful of endpoints the
platform needs are wrapped; responses are plain dicts.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
SUBJECT_SUFFIX = "-value"


class RegistryError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"registry returned HTTP {status}: {message}")
        self.status = status


def subject_for_topic(topic: str) -> str:
    return f"{topic}{SUBJECT_SUFFIX}"


def topic_for_subject(subject: str) -> str:
    return subject[: -len(SUBJECT_SUFFIX)] if subject.endswith(SUBJECT_SUFFIX) else subject


class RegistryClient:
    def __init__(
        self, base_url: str, timeout: float = 10.0, session: requests.Session | None = None
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = session or requests.Session()
        self._session.headers.update({"Accept": CONTENT_TYPE, "Content-Type": CONTENT_TYPE})

    # -- low level ----------------------------------------------------------------------
    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None, ok_404: bool = False
    ) -> Any:
        url = f"{self.base_url}{path}"
        resp = self._session.request(
            method, url, data=json.dumps(body) if body is not None else None, timeout=self.timeout
        )
        if resp.status_code == 404 and ok_404:
            return None
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("message", resp.text)
            except Exception:
                detail = resp.text
            raise RegistryError(resp.status_code, detail)
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    # -- API ----------------------------------------------------------------------------
    def set_compatibility(self, subject: str, mode: str) -> None:
        self._request("PUT", f"/config/{subject}", {"compatibility": mode.upper()})

    def get_compatibility(self, subject: str) -> str | None:
        data = self._request("GET", f"/config/{subject}", ok_404=True)
        if not data:
            return None
        return data.get("compatibilityLevel") or data.get("compatibility")

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        data = self._request(
            "POST",
            f"/subjects/{subject}/versions",
            {"schema": json.dumps(schema, separators=(",", ":")), "schemaType": "AVRO"},
        )
        return int(data["id"])

    def latest(self, subject: str) -> tuple[int, int, dict[str, Any]]:
        data = self._request("GET", f"/subjects/{subject}/versions/latest")
        return int(data["id"]), int(data["version"]), json.loads(data["schema"])

    def by_id(self, schema_id: int) -> dict[str, Any]:
        data = self._request("GET", f"/schemas/ids/{schema_id}")
        return json.loads(data["schema"])

    def subjects(self) -> list[str]:
        return list(self._request("GET", "/subjects") or [])

    def test_compatibility(self, subject: str, schema: dict[str, Any]) -> bool:
        data = self._request(
            "POST",
            f"/compatibility/subjects/{subject}/versions/latest",
            {"schema": json.dumps(schema, separators=(",", ":")), "schemaType": "AVRO"},
            ok_404=True,
        )
        if data is None:  # no versions yet: anything is compatible
            return True
        return bool(data.get("is_compatible", False))

    def schema_cache(self) -> SchemaCache:
        return SchemaCache(self)


class SchemaCache:
    """Memoising ``schema_for_id`` suitable for :func:`rtdaas.contracts.wire.decode_wire`."""

    def __init__(self, client: RegistryClient) -> None:
        self._client = client
        self._by_id: dict[int, dict[str, Any]] = {}

    def __call__(self, schema_id: int) -> dict[str, Any]:
        schema = self._by_id.get(schema_id)
        if schema is None:
            schema = self._client.by_id(schema_id)
            self._by_id[schema_id] = schema
        return schema
