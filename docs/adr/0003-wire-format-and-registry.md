# ADR 0003: Confluent wire format with Apicurio Registry in compatibility mode

**Status:** accepted · **Date:** 2026-10-29

## Context

Spark's `from_avro`, Kafka Connect, kcat and most tooling understand the Confluent wire format
(magic byte + schema id + Avro binary). Confluent Schema Registry is the de-facto API but has a
community licence; Apicurio Registry is Apache-2.0 and exposes the same REST API under
`/apis/ccompat/v7`.

## Decision

Values use the Confluent wire format. The registry is Apicurio in ccompat mode locally and on
Kubernetes; the client (`rtdaas.contracts.registry.RegistryClient`) only uses the Confluent-compatible
endpoints so swapping to Confluent Cloud or Redpanda's registry is a URL change.

## Consequences

- Spark jobs strip the 5-byte header (`substring(value, 6, length-5)`) and call `from_avro` with
  the schema JSON fetched for the subject at job start; schema ids are not resolved per record.
  A producer upgrading its schema mid-run requires a job restart (documented in the runbook).
- Keys are plain UTF-8 `robot_id` strings (no key schema) so partitioning stays readable in tooling.
