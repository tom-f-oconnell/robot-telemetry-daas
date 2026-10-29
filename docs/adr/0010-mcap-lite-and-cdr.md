# ADR 0010: In-house minimal MCAP and CDR codecs

**Status:** accepted · **Date:** 2026-10-29

## Context

Reading ROS 2 bags needs an MCAP container reader and a CDR deserializer driven by message
definitions. The `mcap` + `mcap-ros2-support` Python packages exist, but pulling them into
Spark executors adds dependencies, and the ROS-level deserializer in those packages is itself a
pure-Python CDR walker. The simulator also needs to *write* bags that `ros2 bag` tooling accepts.

## Decision

`rtdaas.mcap_lite` implements the MCAP v1 container (unchunked, uncompressed, with summary and
statistics records) and `rtdaas.contracts.cdr` implements XCDR1-LE encode/decode from parsed
`.msg` definitions. Both are small, dependency-free and unit-tested with golden byte vectors.
Chunked bags (what `ros2 bag record` writes) are unwrapped by the reader itself; zstd and lz4
chunk compression use the optional `zstandard` / `lz4` packages, which the Spark image installs.

## Consequences

- Executors need only the repo wheel; bag decoding is testable on a laptop.
- Compressed chunks need `zstandard` / `lz4` on the executors (part of the `spark` extra).
- The simulator's bags are byte-for-byte valid MCAP and open in Foxglove / `mcap info`.
