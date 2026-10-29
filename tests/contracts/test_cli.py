from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from rtdaas.contracts import cli
from rtdaas.contracts.msgspec import MsgRegistry


def test_gen_and_gen_check(tmp_path: Path, msgs_dir: Path, registry_yml: Path, capsys):
    out = tmp_path / "avro"
    assert (
        cli.main(
            ["gen", "--msgs", str(msgs_dir), "--registry-yml", str(registry_yml), "--out", str(out)]
        )
        == 0
    )
    assert (out / "kafka" / "rtdaas.telemetry.tool_pose.avsc").exists()
    assert len(list(out.glob("*.avsc"))) == 10
    assert (
        cli.main(
            [
                "gen",
                "--msgs",
                str(msgs_dir),
                "--registry-yml",
                str(registry_yml),
                "--out",
                str(out),
                "--check",
            ]
        )
        == 0
    )
    (out / "kafka" / "rtdaas.telemetry.tool_pose.avsc").write_text("{}")
    assert (
        cli.main(
            [
                "gen",
                "--msgs",
                str(msgs_dir),
                "--registry-yml",
                str(registry_yml),
                "--out",
                str(out),
                "--check",
            ]
        )
        == 2
    )
    assert "STALE" in capsys.readouterr().out


def _breaking_copy(src: Path, dst: Path) -> None:
    shutil.copytree(src, dst)
    p = dst / "kafka" / "rtdaas.telemetry.tool_pose.avsc"
    schema = json.loads(p.read_text())
    for f in schema["fields"][3]["type"]["fields"]:
        if f["name"] == "jaw_angle":
            f["type"] = "float"
    p.write_text(json.dumps(schema, indent=2) + "\n")


def test_check_against_directory_base(tmp_path: Path, repo_root: Path, capsys):
    base = repo_root / "contracts" / "avro"
    assert cli.main(["check", "--base", str(base), "--head", str(base)]) == 0
    head = tmp_path / "head"
    _breaking_copy(base, head)
    assert cli.main(["check", "--base", str(base), "--head", str(head), "--mode", "BACKWARD"]) == 2
    out = capsys.readouterr().out
    assert "BREAK   kafka/rtdaas.telemetry.tool_pose.avsc" in out and "$.msg.jaw_angle" in out
    # a brand-new schema in head is reported as NEW, not a violation
    (head / "kafka" / "rtdaas.telemetry.extra.avsc").write_text(
        (base / "kafka" / "rtdaas.telemetry.events.avsc").read_text()
    )
    shutil.copy(
        base / "kafka" / "rtdaas.telemetry.tool_pose.avsc",
        head / "kafka" / "rtdaas.telemetry.tool_pose.avsc",
    )
    assert cli.main(["check", "--base", str(base), "--head", str(head)]) == 0
    assert "NEW     kafka/rtdaas.telemetry.extra.avsc" in capsys.readouterr().out


def test_check_against_git_ref(tmp_path: Path, repo_root: Path, capsys):
    """`--base <ref>` reads schemas with `git show`; build a throwaway repo with two commits."""
    repo = tmp_path / "repo"
    (repo / "contracts").mkdir(parents=True)
    shutil.copytree(repo_root / "contracts" / "avro", repo / "contracts" / "avro")
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-qm", "base"], cwd=repo, check=True, env={**env, "HOME": str(tmp_path)}
    )
    p = repo / "contracts" / "avro" / "kafka" / "rtdaas.telemetry.heartbeat.avsc"
    schema = json.loads(p.read_text())
    schema["fields"][3]["type"]["fields"].append({"name": "site", "type": "string", "default": ""})
    p.write_text(json.dumps(schema, indent=2) + "\n")
    assert cli.main(["check", "--base", "main", "--head", str(repo / "contracts" / "avro")]) == 0
    schema["fields"][3]["type"]["fields"].append({"name": "no_default", "type": "string"})
    p.write_text(json.dumps(schema, indent=2) + "\n")
    assert cli.main(["check", "--base", "main", "--head", str(repo / "contracts" / "avro")]) == 2
    assert "reader_field_missing_default" in capsys.readouterr().out
    assert cli.main(["diff", "--base", "main", "--head", str(repo / "contracts" / "avro")]) == 0
    out = capsys.readouterr().out
    assert "+ msg.site: string" in out and "+ msg.no_default: string" in out


def test_register_uses_registry_client(monkeypatch, repo_root: Path, registry_yml: Path, capsys):
    calls: list[tuple[str, str]] = []

    class FakeClient:
        def __init__(self, url: str) -> None:
            calls.append(("init", url))

        def set_compatibility(self, subject: str, mode: str) -> None:
            calls.append(("compat", f"{subject}:{mode}"))

        def test_compatibility(self, subject: str, schema: dict) -> bool:
            return "events" not in subject

        def register(self, subject: str, schema: dict) -> int:
            calls.append(("register", subject))
            return 7

    monkeypatch.setattr(cli, "RegistryClient", FakeClient)
    rc = cli.main(
        [
            "register",
            "--registry",
            "http://r/apis/ccompat/v7",
            "--avro",
            str(repo_root / "contracts" / "avro"),
            "--registry-yml",
            str(registry_yml),
        ]
    )
    assert rc == 2  # the events subject was reported incompatible by the fake
    out = capsys.readouterr().out
    assert "INCOMPATIBLE rtdaas.telemetry.events-value" in out
    assert "REGISTERED   rtdaas.telemetry.tool_pose-value id=7 compatibility=BACKWARD" in out
    assert ("compat", "rtdaas.telemetry.heartbeat-value:BACKWARD") in calls


def test_main_reports_errors(tmp_path: Path):
    assert cli.main(["gen", "--msgs", str(tmp_path / "missing"), "--out", str(tmp_path)]) == 1


def test_msg_registry_fixture_matches_gen(msg_registry: MsgRegistry):
    assert isinstance(msg_registry, MsgRegistry)


@pytest.mark.parametrize("cmd", [["check"], ["bogus"]])
def test_usage_errors_exit_nonzero(cmd: list[str]):
    with pytest.raises(SystemExit) as exc:
        cli.main(cmd)
    assert exc.value.code != 0
