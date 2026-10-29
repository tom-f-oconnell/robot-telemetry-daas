"""RegistryClient against an in-process fake of the Confluent-compatible REST API."""

from __future__ import annotations

import json
from typing import Any

import pytest
import requests

from rtdaas.contracts.registry import (
    RegistryClient,
    RegistryError,
    SchemaCache,
    subject_for_topic,
    topic_for_subject,
)


class _Resp:
    def __init__(self, status: int, body: Any = None) -> None:
        self.status_code = status
        self._body = body
        self.content = b"" if body is None else json.dumps(body).encode()
        self.text = self.content.decode()

    def json(self) -> Any:
        if self._body is None:
            raise ValueError("no body")
        return self._body


class FakeRegistry:
    """Just enough of the ccompat API for the client: subjects, versions, ids, config, compat."""

    def __init__(self) -> None:
        self.schemas: dict[int, str] = {}
        self.subjects: dict[str, list[tuple[int, int]]] = {}  # subject -> [(version, id)]
        self.config: dict[str, str] = {}
        self.calls: list[tuple[str, str]] = []

    def request(
        self, method: str, url: str, data: str | None = None, timeout: float | None = None
    ) -> _Resp:
        path = url.split("/apis/ccompat/v7", 1)[1]
        self.calls.append((method, path))
        parts = path.strip("/").split("/")
        body = json.loads(data) if data else None
        if method == "PUT" and parts[0] == "config":
            self.config[parts[1]] = body["compatibility"]
            return _Resp(200, {"compatibility": body["compatibility"]})
        if method == "GET" and parts[0] == "config":
            if parts[1] not in self.config:
                return _Resp(404, {"message": "no config"})
            return _Resp(200, {"compatibilityLevel": self.config[parts[1]]})
        if method == "POST" and parts[0] == "subjects" and parts[2] == "versions":
            subject = parts[1]
            text = body["schema"]
            sid = next((i for i, s in self.schemas.items() if s == text), None)
            if sid is None:
                sid = len(self.schemas) + 100
                self.schemas[sid] = text
            versions = self.subjects.setdefault(subject, [])
            if not any(i == sid for _, i in versions):
                versions.append((len(versions) + 1, sid))
            return _Resp(200, {"id": sid})
        if method == "GET" and parts[0] == "subjects" and len(parts) == 1:
            return _Resp(200, sorted(self.subjects))
        if method == "GET" and parts[0] == "subjects" and parts[2] == "versions":
            versions = self.subjects.get(parts[1])
            if not versions:
                return _Resp(404, {"error_code": 40401, "message": "Subject not found"})
            version, sid = versions[-1]
            return _Resp(
                200,
                {"subject": parts[1], "version": version, "id": sid, "schema": self.schemas[sid]},
            )
        if method == "GET" and parts[0] == "schemas":
            sid = int(parts[2])
            if sid not in self.schemas:
                return _Resp(404, {"message": "Schema not found"})
            return _Resp(200, {"schema": self.schemas[sid]})
        if method == "POST" and parts[0] == "compatibility":
            subject = parts[2]
            if subject not in self.subjects:
                return _Resp(404, {"message": "Subject not found"})
            old = json.loads(self.schemas[self.subjects[subject][-1][1]])
            new = json.loads(body["schema"])
            from rtdaas.contracts.compat import is_compatible

            return _Resp(
                200,
                {"is_compatible": is_compatible(new, old, self.config.get(subject, "BACKWARD"))},
            )
        return _Resp(500, {"message": f"unhandled {method} {path}"})


@pytest.fixture
def fake(monkeypatch) -> FakeRegistry:
    fr = FakeRegistry()
    monkeypatch.setattr(
        requests.Session,
        "request",
        lambda self, method, url, data=None, timeout=None, **kw: fr.request(
            method, url, data, timeout
        ),
    )
    return fr


SCHEMA_V1 = {
    "type": "record",
    "name": "R",
    "namespace": "t",
    "fields": [{"name": "a", "type": "int"}],
}
SCHEMA_V2 = {
    "type": "record",
    "name": "R",
    "namespace": "t",
    "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "string", "default": ""}],
}
SCHEMA_BAD = {
    "type": "record",
    "name": "R",
    "namespace": "t",
    "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}],
}


def test_subject_naming():
    assert subject_for_topic("rtdaas.telemetry.tool_pose") == "rtdaas.telemetry.tool_pose-value"
    assert topic_for_subject("rtdaas.telemetry.tool_pose-value") == "rtdaas.telemetry.tool_pose"


def test_register_latest_by_id_and_compat(fake: FakeRegistry):
    client = RegistryClient("http://registry:8080/apis/ccompat/v7/")
    subject = subject_for_topic("rtdaas.telemetry.events")
    assert client.get_compatibility(subject) is None
    client.set_compatibility(subject, "backward")
    assert client.get_compatibility(subject) == "BACKWARD"
    assert client.test_compatibility(subject, SCHEMA_V1) is True  # no versions yet
    sid1 = client.register(subject, SCHEMA_V1)
    assert client.register(subject, SCHEMA_V1) == sid1  # idempotent
    assert client.test_compatibility(subject, SCHEMA_V2) is True
    assert client.test_compatibility(subject, SCHEMA_BAD) is False
    sid2 = client.register(subject, SCHEMA_V2)
    assert sid2 != sid1
    got_id, version, schema = client.latest(subject)
    assert (got_id, version, schema) == (sid2, 2, SCHEMA_V2)
    assert client.by_id(sid1) == SCHEMA_V1
    assert client.subjects() == [subject]
    assert ("PUT", f"/config/{subject}") in fake.calls
    cache = SchemaCache(client)
    assert cache(sid1) == SCHEMA_V1
    n = len(fake.calls)
    assert cache(sid1) == SCHEMA_V1
    assert len(fake.calls) == n  # memoised


def test_errors_surface_status(fake: FakeRegistry):
    client = RegistryClient("http://registry:8080/apis/ccompat/v7")
    with pytest.raises(RegistryError) as exc:
        client.latest("missing-value")
    assert exc.value.status == 404
    with pytest.raises(RegistryError):
        client.by_id(999)
