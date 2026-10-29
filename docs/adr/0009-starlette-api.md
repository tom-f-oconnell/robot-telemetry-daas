# ADR 0009: Starlette API with a hand-maintained OpenAPI document

**Status:** accepted · **Date:** 2026-10-29

## Context

The serving layer needs JSON endpoints, static UI hosting, video streaming with range requests,
a Prometheus endpoint, and an OpenAPI document for discoverability. FastAPI generates OpenAPI
from type hints but adds a dependency and a second schema language (Pydantic models) on top of
the spec's table definitions.

## Decision

Starlette routes with explicit query-parameter parsing; `openapi.yaml` is maintained by hand
next to the routes and served at `/openapi.json` with Swagger UI at `/docs`. A unit test asserts
that every mounted route (method + path) is documented and vice versa, so the document cannot
drift from the implementation.

## Consequences

- Lighter runtime, and the OpenAPI document reads as deliberate API design rather than generated
  boilerplate.
- Request validation is explicit code; invalid parameters return structured 400s.
- Switching to FastAPI later is mechanical (same ASGI, same backends).
