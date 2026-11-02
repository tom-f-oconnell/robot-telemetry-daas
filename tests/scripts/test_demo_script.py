"""scripts/demo.sh's control flow, with docker and curl stubbed.

A copy runs in a temporary directory, because the script deletes demo data under its own parent and
reads the checkout's ``.env``.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
STUB = """#!/usr/bin/env bash
echo "SERVE_JIGSAWS=${{SERVE_JIGSAWS:-unset}} {name} $*" >> "$CALLS"
[[ "{name}" == curl && "$*" == *http_code* ]] && printf 404
exit 0
"""


def _executable(path: Path, text: str) -> None:
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def _run(tmp_path: Path, command: str, extra_env: dict | None = None) -> list[str]:
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    (root / "infra" / "compose").mkdir(parents=True)
    shutil.copy(REPO / "scripts" / "demo.sh", root / "scripts" / "demo.sh")
    _executable(root / "scripts" / "wait-for.sh", "#!/usr/bin/env bash\nexit 0\n")
    stubs = tmp_path / "bin"
    stubs.mkdir()
    for name in ("docker", "curl"):
        _executable(stubs / name, STUB.format(name=name))
    calls = tmp_path / "calls.log"
    env = {"PATH": f"{stubs}:{os.environ['PATH']}", "HOME": str(tmp_path), "CALLS": str(calls)}
    subprocess.run(
        ["bash", str(root / "scripts" / "demo.sh"), command],
        env={**env, **(extra_env or {})},
        check=True,
        capture_output=True,
        text=True,
    )
    return calls.read_text().splitlines()


JIGSAWS_WORK = ("rtdaas-jigsaws", "s3://rtdaas-raw/jigsaws", "procedures/tables", "rtdaas-gestures")


def _trigger(lines: list[str], dag: str) -> bool:
    return any(f"/dags/{dag}/dagRuns" in line for line in lines)


def _patch(lines: list[str], dag: str, paused: str) -> bool:
    return any(f"/dags/{dag} " in line and f'"is_paused": {paused}' in line for line in lines)


def test_demo_prepares_and_ingests_jigsaws(tmp_path):
    lines = _run(tmp_path, "up")
    for word in JIGSAWS_WORK:
        assert any(word in line for line in lines), word
    assert _trigger(lines, "jigsaws_ingest") and _patch(lines, "jigsaws_ingest", "false")
    assert all("SERVE_JIGSAWS=unset" in line for line in lines)  # compose then defaults it to true


def test_no_jigsaws_touches_none_of_it(tmp_path):
    lines = _run(tmp_path, "up-no-jigsaws")
    for word in JIGSAWS_WORK:
        assert not any(word in line for line in lines), word
    assert not _trigger(lines, "jigsaws_ingest")
    assert _patch(lines, "jigsaws_ingest", "true")  # cannot run on its own and fill the tables


def test_no_jigsaws_hands_the_setting_to_compose_and_runs_the_rest(tmp_path):
    lines = _run(tmp_path, "up-no-jigsaws")
    docker = [line for line in lines if " docker " in line]
    assert docker and all("SERVE_JIGSAWS=false" in line for line in docker)
    assert any("--profile core up -d" in line for line in docker)
    assert any("up -d sim streaming bag-lander" in line for line in docker)
    assert _trigger(lines, "contracts_sync") and _patch(lines, "telemetry_bag_ingest", "false")


@pytest.mark.parametrize("value", ["false", "False", "0", "no", "off"])
def test_the_setting_in_dot_env_or_the_shell_works_the_same(tmp_path, value):
    lines = _run(tmp_path, "up", {"SERVE_JIGSAWS": value})
    assert not any("rtdaas-jigsaws" in line for line in lines)
    assert not _trigger(lines, "jigsaws_ingest")


def test_the_make_target_runs_it():
    out = subprocess.run(
        ["make", "-n", "demo-no-jigsaws"], cwd=REPO, check=True, capture_output=True, text=True
    ).stdout
    assert "scripts/demo.sh up-no-jigsaws" in out


def test_compose_passes_the_setting_to_the_api_and_airflow():
    compose = (REPO / "infra" / "compose" / "docker-compose.yml").read_text()
    assert "SERVE_JIGSAWS: ${SERVE_JIGSAWS:-true}" in compose
    assert "AIRFLOW_VAR_RTDAAS_JIGSAWS: ${SERVE_JIGSAWS:-true}" in compose
