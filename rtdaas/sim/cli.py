"""``rtdaas-sim`` command line.

    rtdaas-sim run --robots 3 --duration 60 [--rate 50] [--seed 0] [--kafka] [--mcap-out DIR]
                   [--realtime | --no-realtime] [--start-time 2026-10-29T00:00:00Z] [--arms PSM1,PSM2,ECM]

With ``--kafka`` messages are published through :class:`rtdaas.sim.producer.KafkaProducer`;
with ``--mcap-out`` each robot session is recorded to
``<DIR>/<robot_id>/<yyyy>/<mm>/<dd>/<session_id>.mcap``. Real-time pacing is on by
default when publishing to Kafka and off when only recording.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from rtdaas.config import DEFAULT_MSGS_DIR, configure_logging, get_settings
from rtdaas.contracts.msgspec import MsgRegistry
from rtdaas.sim.recorder import McapRecorder
from rtdaas.sim.robot import NS, TOPIC_TYPES, RobotSim, SimConfig

log = logging.getLogger(__name__)


def session_id(robot_id: str, start_ns: int) -> str:
    start = datetime.fromtimestamp(start_ns / NS, tz=UTC)
    return f"{robot_id}-{start:%Y%m%dT%H%M%S}"


def bag_path(root: Path, robot_id: str, start_ns: int) -> Path:
    start = datetime.fromtimestamp(start_ns / NS, tz=UTC)
    return (
        root
        / robot_id
        / f"{start:%Y}"
        / f"{start:%m}"
        / f"{start:%d}"
        / f"{session_id(robot_id, start_ns)}.mcap"
    )


def _parse_start(value: str | None) -> int:
    if value is None:
        return time.time_ns()
    text = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * NS)


def cmd_run(args: argparse.Namespace) -> int:
    if not args.kafka and not args.mcap_out:
        print("nothing to do: pass --kafka and/or --mcap-out", file=sys.stderr)
        return 1
    settings = get_settings()
    registry = MsgRegistry.load_dir(args.msgs)
    arms = tuple(a.strip() for a in args.arms.split(",") if a.strip())
    start_ns = _parse_start(args.start_time)
    realtime = args.realtime if args.realtime is not None else bool(args.kafka)

    producer = None
    if args.kafka:
        from rtdaas.sim.producer import KafkaProducer

        producer = KafkaProducer(
            args.bootstrap or settings.kafka_bootstrap,
            args.registry or settings.schema_registry_url,
            TOPIC_TYPES,
        )

    sims: list[RobotSim] = []
    recorders: dict[str, McapRecorder] = {}
    for i in range(args.robots):
        rid = f"dv_{i + 1:03d}"
        sims.append(
            RobotSim(SimConfig(robot_id=rid, arms=arms, rate_hz=args.rate, seed=args.seed + i))
        )
        if args.mcap_out:
            recorders[rid] = McapRecorder(
                bag_path(Path(args.mcap_out), rid, start_ns), registry, TOPIC_TYPES
            )

    n_ticks = int(round(args.duration * args.rate))
    dt_ns = int(round(NS / args.rate))
    counts: dict[str, int] = {}
    wall_start = time.monotonic()

    def emit(sim: RobotSim, batch, t_ns: int) -> None:
        for kafka_topic, ros_topic, msg in batch:
            counts[kafka_topic] = counts.get(kafka_topic, 0) + 1
            if producer is not None:
                producer.send(
                    kafka_topic,
                    sim.cfg.robot_id,
                    ros_topic,
                    msg,
                    time.time_ns() if realtime else t_ns,
                )
            rec = recorders.get(sim.cfg.robot_id)
            if rec is not None:
                rec.record(ros_topic, TOPIC_TYPES[kafka_topic], msg, t_ns)

    try:
        for k in range(n_ticks):
            t_ns = start_ns + k * dt_ns
            for sim in sims:
                emit(sim, sim.tick(t_ns), t_ns)
            if realtime:
                target = wall_start + (k + 1) / args.rate
                delay = target - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
        t_end = start_ns + n_ticks * dt_ns
        for sim in sims:
            emit(sim, sim.end(t_end), t_end)
    finally:
        for rec in recorders.values():
            rec.close()
        if producer is not None:
            producer.flush()

    summary = {
        "robots": [s.cfg.robot_id for s in sims],
        "duration_s": args.duration,
        "rate_hz": args.rate,
        "messages": counts,
        "bags": [str(bag_path(Path(args.mcap_out), s.cfg.robot_id, start_ns)) for s in sims]
        if args.mcap_out
        else [],
        "kafka": bool(producer),
        "elapsed_s": round(time.monotonic() - wall_start, 3),
    }
    print(json.dumps(summary, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="rtdaas-sim", description="Simulate a fleet of dVRK-like robots."
    )
    p.add_argument("--log-level", default=None)
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="run the simulator")
    r.add_argument("--robots", type=int, default=1)
    r.add_argument(
        "--duration", type=float, required=True, help="seconds of simulated time per robot"
    )
    r.add_argument("--rate", type=float, default=50.0, help="tick rate in Hz")
    r.add_argument("--seed", type=int, default=0)
    r.add_argument("--arms", default="PSM1,PSM2,ECM")
    r.add_argument("--kafka", action="store_true", help="publish to Kafka")
    r.add_argument("--bootstrap", default=None, help="override KAFKA_BOOTSTRAP")
    r.add_argument("--registry", default=None, help="override SCHEMA_REGISTRY_URL")
    r.add_argument("--mcap-out", default=None, help="record MCAP bags under this directory")
    rt = r.add_mutually_exclusive_group()
    rt.add_argument("--realtime", dest="realtime", action="store_true", default=None)
    rt.add_argument("--no-realtime", dest="realtime", action="store_false")
    r.add_argument(
        "--start-time", default=None, help="ISO-8601 start of simulated time (default: now)"
    )
    r.add_argument("--msgs", default=str(DEFAULT_MSGS_DIR))
    r.set_defaults(func=cmd_run)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return int(args.func(args))
    except (OSError, ValueError, ImportError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
