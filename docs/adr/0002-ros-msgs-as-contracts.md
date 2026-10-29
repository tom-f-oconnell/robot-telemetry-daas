# ADR 0002: ROS `.msg` definitions are the source of truth for data contracts

**Status:** accepted · **Date:** 2026-10-29

## Context

Telemetry originates in ROS 2 nodes whose interfaces are `.msg` files. The DaaS platform needs
Avro schemas in a registry, and the two must never disagree. Hand-maintaining both invites drift;
Intuitive's posting explicitly asks for "evolving data contracts" and "automated tests for schema
changes".

## Decision

`contracts/msgs/` holds the `.msg` files (standard types vendored, custom types owned here).
`rtdaas-contracts gen` derives Avro schemas deterministically using a fixed type-mapping table
(spec §2.3) and wraps each message in a Kafka envelope record (`robot_id`, `ros_topic`,
`recv_time_ns`, `msg`). Generated files are committed; CI fails if they are stale. A pure-Python
implementation of Avro schema-resolution rules checks every PR against `main` under the subject's
compatibility mode (BACKWARD) before anything reaches the registry. Producers and consumers fetch
schemas from the registry at startup and never embed them.

## Consequences

- A `.msg` change is a reviewed contract change: adding a field with a default passes, removing
  or narrowing a field fails CI with a precise path (`msg.position.x: type double→float`).
- ROS constants are documentation only (Avro has no constants); enums stay strings.
- The ROS 2 C++ bridge must walk messages generically (rosidl introspection) so a new message
  type needs no bridge code, only a config entry.
