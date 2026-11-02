"""Keeping JIGSAWS data out of what the API serves, when ``SERVE_JIGSAWS`` is off.

The rule lives on the server, not in the UI, because a page that hides data the API still serves is
one ``curl`` from showing it. Which tables count comes from ``catalog.yml``'s ``dataset`` field,
which also tells the UI what to label when the setting is on.
"""

from __future__ import annotations

import copy

from rtdaas.serving.backends._sqlbase import load_catalog_yml

JIGSAWS_TABLES: tuple[str, ...] = tuple(
    e["name"] for e in load_catalog_yml() if e.get("dataset") == "jigsaws"
)
JIGSAWS_JOB_KINDS: tuple[str, ...] = ("trial",)
PROCEDURES_TAG = "procedures"
_SCHEMAS = ("Trial", "TrialDetail", "KinematicSample", "FrameFeature", "Gesture")


class WithoutJigsaws:
    """A backend with JIGSAWS data removed; everything else passes through.

    The routes that only serve JIGSAWS data are not mounted at all in this mode. The empty methods
    here are a second line of defence, so that a route added later cannot leak by forgetting.
    """

    def __init__(self, backend):
        self._backend = backend

    def __getattr__(self, name):
        return getattr(self._backend, name)

    def catalog(self) -> list[dict]:
        return [t for t in self._backend.catalog() if t["name"] not in JIGSAWS_TABLES]

    def freshness(self) -> dict[str, dict]:
        return {k: v for k, v in self._backend.freshness().items() if k not in JIGSAWS_TABLES}

    def health(self) -> dict:
        info = self._backend.health()
        info["tables"] = {k: v for k, v in info["tables"].items() if k not in JIGSAWS_TABLES}
        return info

    def quality_runs(self, limit: int = 50) -> list[dict]:
        return self._backend.quality_runs(limit=limit, exclude_tables=JIGSAWS_TABLES)

    def quality_results(self, run_id=None, table_name=None, status=None, limit=500) -> list[dict]:
        return self._backend.quality_results(
            run_id=run_id,
            table_name=table_name,
            status=status,
            limit=limit,
            exclude_tables=JIGSAWS_TABLES,
        )

    def jobs(self, limit: int = 50) -> list[dict]:
        return self._backend.jobs(limit=limit, exclude_kinds=JIGSAWS_JOB_KINDS)

    def trials(self, task=None, skill_level=None, subject=None) -> list[dict]:
        return []

    def trial(self, trial_id: str) -> None:
        return None

    def trial_kinematics(self, trial_id: str, arms: list[str], downsample: int = 1) -> list[dict]:
        return []

    def trial_frames(self, trial_id: str) -> list[dict]:
        return []

    def gestures(self, task=None, gesture=None, skill_level=None, limit=500) -> list[dict]:
        return []


def without_jigsaws_openapi(doc: dict) -> dict:
    """The API's document with the JIGSAWS routes, schemas and wording taken out."""
    out = copy.deepcopy(doc)
    out["paths"] = {
        path: ops
        for path, ops in out["paths"].items()
        if not any(PROCEDURES_TAG in op.get("tags", []) for op in ops.values())
    }
    out["tags"] = [t for t in out.get("tags", []) if t["name"] != PROCEDURES_TAG]
    components = out.get("components", {})
    for name in _SCHEMAS:
        components.get("schemas", {}).pop(name, None)
    components.get("parameters", {}).pop("TrialId", None)
    out["info"]["description"] = out["info"]["description"].replace(
        "telemetry sessions and KPIs, JIGSAWS procedures and gestures",
        "telemetry sessions and KPIs",
    )
    return out
