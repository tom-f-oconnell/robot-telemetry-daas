"""Backend protocol for the data API (spec §4.10).

Every method returns plain JSON-serialisable Python (lists of dicts, dicts, ``None``).
Timestamps are ISO-8601 UTC strings; arrays are lists.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Backend(Protocol):
    name: str

    # ---- catalog / contracts ------------------------------------------------------------
    def catalog(self) -> list[dict]: ...
    def contracts(self) -> list[dict]: ...
    def freshness(self) -> dict[str, dict]: ...

    # ---- telemetry ----------------------------------------------------------------------
    def robots(self) -> list[dict]: ...
    def sessions(
        self,
        robot_id: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict]: ...
    def session(self, session_id: str) -> dict | None: ...
    def trajectory(self, session_id: str, arm: str, downsample: int = 1) -> list[dict]: ...
    def session_kpis(self, session_id: str) -> list[dict]: ...
    def events(
        self,
        q: str | None = None,
        robot_id: str | None = None,
        severity_min: int | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 200,
    ) -> list[dict]: ...

    # ---- procedures ---------------------------------------------------------------------
    def trials(
        self, task: str | None = None, skill_level: str | None = None, subject: str | None = None
    ) -> list[dict]: ...
    def trial(self, trial_id: str) -> dict | None: ...
    def trial_kinematics(
        self, trial_id: str, arms: list[str], downsample: int = 1
    ) -> list[dict]: ...
    def trial_frames(self, trial_id: str) -> list[dict]: ...
    def gestures(
        self,
        task: str | None = None,
        gesture: str | None = None,
        skill_level: str | None = None,
        limit: int = 500,
    ) -> list[dict]: ...

    # ---- quality / jobs -----------------------------------------------------------------
    def quality_runs(self, limit: int = 50) -> list[dict]: ...
    def quality_results(
        self,
        run_id: str | None = None,
        table_name: str | None = None,
        status: str | None = None,
        limit: int = 500,
    ) -> list[dict]: ...
    def record_job(self, kind: str, target: str, dag_run_id: str | None, status: str) -> dict: ...
    def jobs(self, limit: int = 50) -> list[dict]: ...


ARM_FIELDS: tuple[str, ...] = (
    "x",
    "y",
    "z",
    "r11",
    "r12",
    "r13",
    "r21",
    "r22",
    "r23",
    "r31",
    "r32",
    "r33",
    "vx",
    "vy",
    "vz",
    "wx",
    "wy",
    "wz",
    "gripper",
)
KINEMATIC_ARMS: tuple[str, ...] = ("mtml", "mtmr", "psml", "psmr")
