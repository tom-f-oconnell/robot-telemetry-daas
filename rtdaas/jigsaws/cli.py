"""``rtdaas-jigsaws`` command line: generate a synthetic dataset, or inspect or import a real one."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from rtdaas.config import configure_logging

log = logging.getLogger(__name__)


def _cmd_synth(args: argparse.Namespace) -> int:
    from rtdaas.jigsaws.synth import generate_dataset

    tasks = tuple(args.task) if args.task else None
    kwargs = {} if tasks is None else {"tasks": tasks}
    trials = generate_dataset(
        Path(args.out),
        n_trials_per_task=args.trials,
        seed=args.seed,
        duration_s=args.duration,
        fps=args.fps,
        video=not args.no_video,
        **kwargs,
    )
    sys.stdout.write(f"wrote {len(trials)} trials under {args.out}\n")
    return 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    from rtdaas.jigsaws.layout import discover_trials, parse_kinematics, parse_transcription

    trials = discover_trials(Path(args.root), tasks=args.task or None)
    rows = []
    for t in trials:
        kin = parse_kinematics(t.kinematics_path)
        n_gestures = len(parse_transcription(t.transcription_path)) if t.transcription_path else 0
        rows.append(
            {
                "trial_id": t.trial_id,
                "task": t.task,
                "subject": t.subject,
                "repetition": t.repetition,
                "skill_level": t.skill_level,
                "grs_total": t.grs_total,
                "n_samples": int(len(kin)),
                "duration_s": float(kin["t_s"].iloc[-1]) if len(kin) else 0.0,
                "n_gestures": n_gestures,
                "video": str(t.video_path) if t.video_path else None,
            }
        )
    if args.json:
        json.dump(rows, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        for r in rows:
            sys.stdout.write(
                f"{r['trial_id']:<24} {r['skill_level']:<12} samples={r['n_samples']:<6} "
                f"gestures={r['n_gestures']:<3} video={'yes' if r['video'] else 'no'}\n"
            )
        sys.stdout.write(f"{len(rows)} trials\n")
    return 0


def _cmd_import(args: argparse.Namespace) -> int:
    from rtdaas.jigsaws.importer import import_dataset

    result = import_dataset(Path(args.root), Path(args.out), tasks=args.task, jobs=args.jobs)
    sys.stdout.write(
        f"{result['trials']} trials under {args.out} ({result['transcoded']} videos transcoded); "
        f"skipped {len(result['skipped'])} incomplete: {' '.join(result['skipped'])}\n"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rtdaas-jigsaws", description=__doc__)
    p.add_argument("--log-level", default=None)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("synth", help="write a synthetic JIGSAWS-layout dataset")
    s.add_argument("--out", required=True)
    s.add_argument("--trials", type=int, default=2, help="trials per task")
    s.add_argument("--seed", type=int, default=0)
    s.add_argument("--duration", type=float, default=6.0, help="seconds per trial")
    s.add_argument("--fps", type=float, default=30.0)
    s.add_argument("--task", action="append", help="restrict to task (repeatable)")
    s.add_argument("--no-video", action="store_true")
    s.set_defaults(func=_cmd_synth)

    i = sub.add_parser("inspect", help="list trials found under a JIGSAWS root")
    i.add_argument("--root", required=True)
    i.add_argument("--task", action="append")
    i.add_argument("--json", action="store_true")
    i.set_defaults(func=_cmd_inspect)

    m = sub.add_parser("import", help="write a browser-ready copy of a downloaded dataset")
    m.add_argument("--root", required=True, help="the download's folder")
    m.add_argument("--out", required=True)
    m.add_argument("--task", action="append", help="restrict to task (repeatable)")
    m.add_argument("--jobs", type=int, default=4, help="videos transcoded at once")
    m.set_defaults(func=_cmd_import)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return int(args.func(args))
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        log.error("%s", exc)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
