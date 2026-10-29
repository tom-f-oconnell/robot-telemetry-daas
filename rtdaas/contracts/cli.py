"""``rtdaas-contracts`` command line (spec §4.1).

    rtdaas-contracts gen      [--msgs DIR] [--registry-yml FILE] [--out DIR] [--check]
    rtdaas-contracts check    --base REF_OR_DIR [--head DIR] [--mode BACKWARD]
    rtdaas-contracts diff     --base REF_OR_DIR [--head DIR]
    rtdaas-contracts register --registry URL [--avro DIR] [--registry-yml FILE]

``--base`` is either a directory holding ``.avsc`` files (same layout as
``contracts/avro``) or a git ref such as ``origin/main``; in the latter case the
schemas are read with ``git show <ref>:<path>``.

Exit codes: 0 ok, 1 usage / runtime error, 2 contract violation.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

from rtdaas.config import (
    DEFAULT_AVRO_DIR,
    DEFAULT_MSGS_DIR,
    DEFAULT_REGISTRY_YML,
    configure_logging,
)
from rtdaas.contracts import avro_gen, compat
from rtdaas.contracts.msgspec import MsgRegistry
from rtdaas.contracts.registry import RegistryClient, subject_for_topic

log = logging.getLogger(__name__)

EXIT_OK, EXIT_ERROR, EXIT_VIOLATION = 0, 1, 2


def _load_head(head_dir: Path) -> dict[str, dict[str, Any]]:
    """``{relative_path: schema}`` for every .avsc under ``head_dir``."""
    out: dict[str, dict[str, Any]] = {}
    for path in sorted(head_dir.rglob("*.avsc")):
        out[path.relative_to(head_dir).as_posix()] = json.loads(path.read_text(encoding="utf-8"))
    return out


def _git_root(start: Path) -> Path:
    res = subprocess.run(
        ["git", "-C", str(start), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(res.stdout.strip())


def _load_base(base: str, head_dir: Path, rel_paths: list[str]) -> dict[str, dict[str, Any] | None]:
    """Load the base version of each head schema. ``None`` means "did not exist in base"."""
    base_path = Path(base)
    out: dict[str, dict[str, Any] | None] = {}
    if base_path.is_dir():
        for rel in rel_paths:
            p = base_path / rel
            out[rel] = json.loads(p.read_text(encoding="utf-8")) if p.exists() else None
        return out
    root = _git_root(head_dir)
    head_rel = head_dir.resolve().relative_to(root).as_posix()
    for rel in rel_paths:
        spec = f"{base}:{head_rel}/{rel}"
        res = subprocess.run(["git", "-C", str(root), "show", spec], capture_output=True, text=True)
        out[rel] = json.loads(res.stdout) if res.returncode == 0 else None
    return out


def cmd_gen(args: argparse.Namespace) -> int:
    registry = MsgRegistry.load_dir(args.msgs)
    topics = avro_gen.load_topics(args.registry_yml)
    out_dir = Path(args.out)
    if args.check:
        expected = avro_gen.render_all(registry, topics)
        stale: list[str] = []
        for rel, text in expected.items():
            p = out_dir / rel
            if not p.exists() or p.read_text(encoding="utf-8") != text:
                stale.append(rel)
        extra = [
            p.relative_to(out_dir).as_posix()
            for p in out_dir.rglob("*.avsc")
            if p.relative_to(out_dir).as_posix() not in expected
        ]
        if stale or extra:
            for rel in stale:
                print(f"STALE   {rel}")
            for rel in extra:
                print(f"ORPHAN  {rel}")
            print("generated schemas are out of date; run `rtdaas-contracts gen`")
            return EXIT_VIOLATION
        print(f"{len(expected)} schemas up to date")
        return EXIT_OK
    written = avro_gen.generate_all(registry, topics, out_dir)
    for p in written:
        print(p.relative_to(out_dir.parent) if out_dir.parent in p.parents else p)
    print(f"wrote {len(written)} schemas to {out_dir}")
    return EXIT_OK


def cmd_check(args: argparse.Namespace) -> int:
    head_dir = Path(args.head)
    head = _load_head(head_dir)
    if not head:
        print(f"no .avsc files under {head_dir}", file=sys.stderr)
        return EXIT_ERROR
    base = _load_base(args.base, head_dir, list(head))
    violations = 0
    for rel, new_schema in head.items():
        old_schema = base.get(rel)
        if old_schema is None:
            print(f"NEW     {rel}")
            continue
        problems = compat.check(new_schema, old_schema, args.mode)
        if problems:
            violations += 1
            print(f"BREAK   {rel}")
            for p in problems:
                print(f"          {p}")
        else:
            print(f"OK      {rel}")
    if violations:
        print(f"{violations} schema(s) violate {args.mode} compatibility")
        return EXIT_VIOLATION
    print(f"all {len(head)} schemas are {args.mode}-compatible with base {args.base}")
    return EXIT_OK


def cmd_diff(args: argparse.Namespace) -> int:
    head_dir = Path(args.head)
    head = _load_head(head_dir)
    base = _load_base(args.base, head_dir, list(head))
    for rel, new_schema in head.items():
        old_schema = base.get(rel)
        new_fields = compat.flatten_fields(new_schema)
        old_fields = compat.flatten_fields(old_schema) if old_schema else {}
        added = sorted(set(new_fields) - set(old_fields))
        removed = sorted(set(old_fields) - set(new_fields))
        changed = sorted(
            k for k in set(new_fields) & set(old_fields) if new_fields[k] != old_fields[k]
        )
        if not (added or removed or changed):
            continue
        print(f"== {rel}")
        for k in added:
            print(f"  + {k}: {new_fields[k]}")
        for k in removed:
            print(f"  - {k}: {old_fields[k]}")
        for k in changed:
            print(f"  ~ {k}: {old_fields[k]} -> {new_fields[k]}")
    return EXIT_OK


def cmd_register(args: argparse.Namespace) -> int:
    import yaml

    with open(args.registry_yml, encoding="utf-8") as fh:
        reg_cfg = yaml.safe_load(fh)
    mode = str(reg_cfg.get("compatibility", "BACKWARD"))
    client = RegistryClient(args.registry)
    avro_dir = Path(args.avro)
    failures = 0
    for topic in sorted(reg_cfg["topics"]):
        subject = subject_for_topic(topic)
        schema = json.loads((avro_dir / "kafka" / f"{topic}.avsc").read_text(encoding="utf-8"))
        client.set_compatibility(subject, mode)
        if not client.test_compatibility(subject, schema):
            print(f"INCOMPATIBLE {subject}")
            failures += 1
            continue
        schema_id = client.register(subject, schema)
        print(f"REGISTERED   {subject} id={schema_id} compatibility={mode}")
    return EXIT_VIOLATION if failures else EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="rtdaas-contracts", description="Generate, check and register data contracts."
    )
    p.add_argument("--log-level", default=None)
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("gen", help="generate Avro schemas from .msg definitions")
    g.add_argument("--msgs", default=str(DEFAULT_MSGS_DIR))
    g.add_argument("--registry-yml", default=str(DEFAULT_REGISTRY_YML))
    g.add_argument("--out", default=str(DEFAULT_AVRO_DIR))
    g.add_argument(
        "--check",
        action="store_true",
        help="exit 2 if committed schemas differ from generated ones",
    )
    g.set_defaults(func=cmd_gen)

    c = sub.add_parser("check", help="check compatibility of head schemas against a base")
    c.add_argument("--base", required=True, help="directory or git ref (e.g. origin/main)")
    c.add_argument("--head", default=str(DEFAULT_AVRO_DIR))
    c.add_argument("--mode", default="BACKWARD", choices=list(compat.MODES))
    c.set_defaults(func=cmd_check)

    d = sub.add_parser("diff", help="human-readable field diff between base and head")
    d.add_argument("--base", required=True)
    d.add_argument("--head", default=str(DEFAULT_AVRO_DIR))
    d.set_defaults(func=cmd_diff)

    r = sub.add_parser("register", help="register envelope schemas with the schema registry")
    r.add_argument("--registry", required=True, help="e.g. http://localhost:8081/apis/ccompat/v7")
    r.add_argument("--avro", default=str(DEFAULT_AVRO_DIR))
    r.add_argument("--registry-yml", default=str(DEFAULT_REGISTRY_YML))
    r.set_defaults(func=cmd_register)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return int(args.func(args))
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
