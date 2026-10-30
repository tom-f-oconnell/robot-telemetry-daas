"""Starlette data API (spec §4.10, ADR 0009).

Routes are thin: parse and validate query parameters, call the backend, return JSON. The
OpenAPI document lives in ``openapi.yaml`` next to this module and a test asserts that every
mounted route is documented (and vice versa). Static UI files are served from ``static/``.
"""

from __future__ import annotations

import logging
import mimetypes
import re
import time
from pathlib import Path

import yaml
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from rtdaas import __version__
from rtdaas.config import Settings, get_settings
from rtdaas.serving import airflow_client
from rtdaas.serving.backends import Backend, make_backend
from rtdaas.serving.metrics import Metrics

log = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
STATIC_DIR = HERE / "static"
OPENAPI_PATH = HERE / "openapi.yaml"

VALID_ARMS = {"PSM1", "PSM2", "PSM3", "ECM", "MTML", "MTMR"}
KINEMATIC_ARMS = ("mtml", "mtmr", "psml", "psmr")
JOB_KINDS = {"session", "trial", "date"}
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ApiError(Exception):
    def __init__(self, status: int, error: str, detail: str = ""):
        super().__init__(error)
        self.status = status
        self.error = error
        self.detail = detail


def error_response(status: int, error: str, detail: str = "") -> JSONResponse:
    return JSONResponse({"error": error, "detail": detail}, status_code=status)


# --------------------------------------------------------------------------- parameter parsing
def q_str(request: Request, name: str, default: str | None = None) -> str | None:
    value = request.query_params.get(name)
    return default if value is None or value == "" else value


def q_int(request: Request, name: str, default: int, lo: int = 0, hi: int = 1_000_000) -> int:
    raw = request.query_params.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ApiError(400, "invalid_parameter", f"{name} must be an integer") from exc
    if not lo <= value <= hi:
        raise ApiError(400, "invalid_parameter", f"{name} must be between {lo} and {hi}")
    return value


def q_ts(request: Request, name: str) -> str | None:
    raw = q_str(request, name)
    if raw is None:
        return None
    try:
        import pandas as pd

        if pd.isna(pd.Timestamp(raw)):
            raise ValueError
    except Exception as exc:  # noqa: BLE001
        raise ApiError(
            400, "invalid_parameter", f"{name} must be an ISO-8601 date or datetime"
        ) from exc
    return raw


def q_list(request: Request, name: str, default: list[str]) -> list[str]:
    raw = q_str(request, name)
    if raw is None:
        return default
    return [v.strip() for v in raw.split(",") if v.strip()]


# --------------------------------------------------------------------------- middleware
class MetricsMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, metrics: Metrics):
        super().__init__(app)
        self.metrics = metrics

    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)
        self.metrics.observe(
            request.method, route_path, response.status_code, time.perf_counter() - start
        )
        return response


# --------------------------------------------------------------------------- app factory
def create_app(settings: Settings | None = None, backend: Backend | None = None) -> Starlette:
    settings = settings or get_settings()
    backend = backend or make_backend(settings)
    metrics = Metrics()
    openapi_doc = yaml.safe_load(OPENAPI_PATH.read_text())
    openapi_doc.setdefault("info", {})["version"] = __version__
    media_root = Path(settings.media_root).resolve()

    # ---- meta --------------------------------------------------------------
    # Plain `def` handlers run in Starlette's thread pool: the backends block (DuckDB over S3 can take
    # seconds), and on the event loop that would stall every other request, /api/live included.
    async def live(request: Request):
        """For probes: answers while the process is up, without touching the backend."""
        return JSONResponse({"status": "ok"})

    def health(request: Request):
        info = backend.health()
        info.update({"version": __version__, "media_root": str(media_root)})
        return JSONResponse(info)

    def catalog(request: Request):
        return JSONResponse({"tables": backend.catalog()})

    def contracts(request: Request):
        return JSONResponse({"contracts": backend.contracts()})

    def openapi(request: Request):
        return JSONResponse(openapi_doc)

    def docs(request: Request):
        return HTMLResponse(SWAGGER_HTML)

    def prometheus(request: Request):
        body, content_type = metrics.render()
        return Response(body, media_type=content_type)

    # ---- telemetry ---------------------------------------------------------
    def robots(request: Request):
        return JSONResponse({"robots": backend.robots()})

    def sessions(request: Request):
        rows = backend.sessions(
            robot_id=q_str(request, "robot_id"),
            start=q_ts(request, "start"),
            end=q_ts(request, "end"),
            limit=q_int(request, "limit", 200, 1, 5000),
            offset=q_int(request, "offset", 0, 0),
        )
        return JSONResponse({"sessions": rows, "count": len(rows)})

    def session(request: Request):
        row = backend.session(request.path_params["session_id"])
        if row is None:
            raise ApiError(404, "not_found", "unknown session_id")
        return JSONResponse(row)

    def trajectory(request: Request):
        session_id = request.path_params["session_id"]
        arm = q_str(request, "arm", "PSM1")
        if arm not in VALID_ARMS:
            raise ApiError(400, "invalid_parameter", f"arm must be one of {sorted(VALID_ARMS)}")
        if backend.session(session_id) is None:
            raise ApiError(404, "not_found", "unknown session_id")
        points = backend.trajectory(
            session_id, arm, downsample=q_int(request, "downsample", 1, 1, 10_000)
        )
        return JSONResponse(
            {"session_id": session_id, "arm": arm, "points": points, "count": len(points)}
        )

    def session_kpis(request: Request):
        session_id = request.path_params["session_id"]
        if backend.session(session_id) is None:
            raise ApiError(404, "not_found", "unknown session_id")
        rows = backend.session_kpis(session_id)
        return JSONResponse({"session_id": session_id, "kpis": rows, "count": len(rows)})

    def events(request: Request):
        sev = q_str(request, "severity_min")
        severity_min = None
        if sev is not None:
            try:
                severity_min = int(sev)
            except ValueError as exc:
                raise ApiError(400, "invalid_parameter", "severity_min must be 0..4") from exc
        rows = backend.events(
            q=q_str(request, "q"),
            robot_id=q_str(request, "robot_id"),
            severity_min=severity_min,
            start=q_ts(request, "start"),
            end=q_ts(request, "end"),
            limit=q_int(request, "limit", 200, 1, 5000),
        )
        return JSONResponse({"events": rows, "count": len(rows)})

    # ---- procedures --------------------------------------------------------
    def trials(request: Request):
        rows = backend.trials(
            task=q_str(request, "task"),
            skill_level=q_str(request, "skill_level"),
            subject=q_str(request, "subject"),
        )
        return JSONResponse({"trials": rows, "count": len(rows)})

    def trial(request: Request):
        row = backend.trial(request.path_params["trial_id"])
        if row is None:
            raise ApiError(404, "not_found", "unknown trial_id")
        return JSONResponse(row)

    def trial_kinematics(request: Request):
        trial_id = request.path_params["trial_id"]
        arms = [a.lower() for a in q_list(request, "arms", ["psml", "psmr"])]
        bad = [a for a in arms if a not in KINEMATIC_ARMS]
        if bad:
            raise ApiError(400, "invalid_parameter", f"arms must be from {KINEMATIC_ARMS}")
        if backend.trial(trial_id) is None:
            raise ApiError(404, "not_found", "unknown trial_id")
        rows = backend.trial_kinematics(
            trial_id, arms, downsample=q_int(request, "downsample", 1, 1, 10_000)
        )
        return JSONResponse(
            {"trial_id": trial_id, "arms": arms, "samples": rows, "count": len(rows)}
        )

    def trial_frames(request: Request):
        trial_id = request.path_params["trial_id"]
        if backend.trial(trial_id) is None:
            raise ApiError(404, "not_found", "unknown trial_id")
        rows = backend.trial_frames(trial_id)
        return JSONResponse({"trial_id": trial_id, "frames": rows, "count": len(rows)})

    def trial_video(request: Request):
        trial_id = request.path_params["trial_id"]
        row = backend.trial(trial_id)
        if row is None:
            raise ApiError(404, "not_found", "unknown trial_id")
        path = resolve_media(row.get("video_path"), media_root)
        if path is None:
            raise ApiError(404, "not_found", "video file not available")
        return range_file_response(request, path)

    def gestures(request: Request):
        rows = backend.gestures(
            task=q_str(request, "task"),
            gesture=q_str(request, "gesture"),
            skill_level=q_str(request, "skill_level"),
            limit=q_int(request, "limit", 500, 1, 10_000),
        )
        return JSONResponse({"gestures": rows, "count": len(rows)})

    # ---- quality / jobs ----------------------------------------------------
    def quality_runs(request: Request):
        rows = backend.quality_runs(limit=q_int(request, "limit", 50, 1, 1000))
        return JSONResponse({"runs": rows, "count": len(rows)})

    def quality_results(request: Request):
        status = q_str(request, "status")
        if status is not None and status not in {"pass", "fail", "error", "skipped"}:
            raise ApiError(400, "invalid_parameter", "status must be pass|fail|error|skipped")
        rows = backend.quality_results(
            run_id=q_str(request, "run_id"),
            table_name=q_str(request, "table_name"),
            status=status,
            limit=q_int(request, "limit", 500, 1, 10_000),
        )
        return JSONResponse({"results": rows, "count": len(rows)})

    def jobs(request: Request):
        rows = backend.jobs(limit=q_int(request, "limit", 50, 1, 1000))
        return JSONResponse({"jobs": rows, "count": len(rows)})

    async def reprocess(request: Request):
        try:
            body = await request.json()
        except Exception as exc:  # noqa: BLE001
            raise ApiError(400, "invalid_body", "body must be JSON") from exc
        if not isinstance(body, dict):
            raise ApiError(400, "invalid_body", "body must be a JSON object")
        kind = body.get("kind")
        target = body.get("target")
        if kind not in JOB_KINDS:
            raise ApiError(400, "invalid_body", f"kind must be one of {sorted(JOB_KINDS)}")
        if not isinstance(target, str) or not target:
            raise ApiError(400, "invalid_body", "target must be a non-empty string")
        if kind == "date" and not DATE_RE.match(target):
            raise ApiError(400, "invalid_body", "target must be YYYY-MM-DD for kind=date")
        robot_id = None
        if kind == "session":
            sess = backend.session(target)
            if sess is None:
                raise ApiError(404, "not_found", "unknown session_id")
            robot_id = sess.get("robot_id")
        if kind == "trial" and backend.trial(target) is None:
            raise ApiError(404, "not_found", "unknown trial_id")
        dag_id, conf = airflow_client.build_conf(kind, target, robot_id=robot_id)
        try:
            dag_run_id = airflow_client.trigger_dag(settings, dag_id, conf)
            status, note = "triggered", "Airflow DAG run created"
        except airflow_client.AirflowUnavailable as exc:
            dag_run_id, status = None, "queued_local"
            note = f"Airflow not reachable ({exc}); request recorded locally"
        job = backend.record_job(kind, target, dag_run_id, status)
        job.update({"dag_id": dag_id, "conf": conf, "note": note})
        return JSONResponse(job, status_code=202)

    # ---- UI ----------------------------------------------------------------
    async def index(request: Request):
        return FileResponse(STATIC_DIR / "index.html")

    routes = [
        Route("/api/live", live),
        Route("/api/health", health),
        Route("/api/catalog", catalog),
        Route("/api/contracts", contracts),
        Route("/api/robots", robots),
        Route("/api/sessions", sessions),
        Route("/api/sessions/{session_id}", session),
        Route("/api/sessions/{session_id}/trajectory", trajectory),
        Route("/api/sessions/{session_id}/kpis", session_kpis),
        Route("/api/events", events),
        Route("/api/trials", trials),
        Route("/api/trials/{trial_id}", trial),
        Route("/api/trials/{trial_id}/kinematics", trial_kinematics),
        Route("/api/trials/{trial_id}/frames", trial_frames),
        Route("/api/trials/{trial_id}/video", trial_video),
        Route("/api/gestures", gestures),
        Route("/api/quality/runs", quality_runs),
        Route("/api/quality/results", quality_results),
        Route("/api/jobs", jobs),
        Route("/api/jobs/reprocess", reprocess, methods=["POST"]),
        Route("/openapi.json", openapi),
        Route("/docs", docs),
        Route("/metrics", prometheus),
        Route("/", index),
        Mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static"),
    ]

    middleware = [
        Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]),
        Middleware(MetricsMiddleware, metrics=metrics),
    ]

    async def on_api_error(request: Request, exc: ApiError):
        return error_response(exc.status, exc.error, exc.detail)

    async def on_unhandled(request: Request, exc: Exception):
        log.exception("unhandled error on %s %s", request.method, request.url.path)
        return error_response(500, "internal_error", str(exc))

    app = Starlette(
        routes=routes,
        middleware=middleware,
        exception_handlers={
            ApiError: on_api_error,
            404: lambda r, e: error_response(404, "not_found", str(r.url.path)),
            Exception: on_unhandled,
        },
    )
    app.state.backend = backend
    app.state.settings = settings
    app.state.metrics = metrics
    return app


# --------------------------------------------------------------------------- media helpers
def resolve_media(video_path: str | None, media_root: Path) -> Path | None:
    """Resolve a trial's video path to a readable file.

    Absolute paths are honoured if they exist (local pipeline output); otherwise the path is
    taken relative to ``MEDIA_ROOT`` and must stay inside it (no ``..`` escapes). Object-store
    URIs (``s3a://<bucket>/<key>``, as written by the Spark jobs) resolve to ``MEDIA_ROOT/<key>``,
    i.e. ``MEDIA_ROOT`` is treated as a local mirror of the raw bucket.
    """
    if not video_path:
        return None
    scheme, sep, rest = video_path.partition("://")
    if sep and scheme in ("s3", "s3a", "s3n"):
        video_path = rest.partition("/")[2]  # drop the bucket
        if not video_path:
            return None
    p = Path(video_path)
    if p.is_absolute() and p.is_file():
        return p
    candidate = (media_root / p.name if p.is_absolute() else media_root / p).resolve()
    try:
        candidate.relative_to(media_root)
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def range_file_response(request: Request, path: Path, chunk_size: int = 1 << 20) -> Response:
    """Serve a file with HTTP Range support so ``<video>`` can seek (RFC 7233, single range)."""
    size = path.stat().st_size
    media_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
    range_header = request.headers.get("range")
    headers = {"Accept-Ranges": "bytes", "Content-Type": media_type}
    if not range_header:
        return FileResponse(str(path), media_type=media_type, headers={"Accept-Ranges": "bytes"})
    m = re.match(r"bytes=(\d*)-(\d*)$", range_header.strip())
    if not m:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
    start_s, end_s = m.groups()
    if start_s == "" and end_s == "":
        return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
    if start_s == "":  # suffix range
        length = min(int(end_s), size)
        start, end = size - length, size - 1
    else:
        start = int(start_s)
        end = min(int(end_s), size - 1) if end_s else size - 1
    if start >= size or start > end:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{size}"})
    length = end - start + 1
    headers.update({"Content-Range": f"bytes {start}-{end}/{size}", "Content-Length": str(length)})

    def iterator():
        with open(path, "rb") as fh:
            fh.seek(start)
            remaining = length
            while remaining > 0:
                chunk = fh.read(min(chunk_size, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk

    return StreamingResponse(iterator(), status_code=206, headers=headers, media_type=media_type)


SWAGGER_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>rtdaas API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css"></head>
<body><div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>window.onload=()=>{SwaggerUIBundle({url:'/openapi.json',dom_id:'#swagger-ui',deepLinking:true});};</script>
</body></html>"""


def documented_routes(app: Starlette) -> set[tuple[str, str]]:
    """(method, path) pairs for every API route — used by the OpenAPI parity test."""
    out: set[tuple[str, str]] = set()
    for r in app.routes:
        if isinstance(r, Route):
            for m in r.methods or {"GET"}:
                if m in {"GET", "POST", "PUT", "DELETE", "PATCH"}:
                    out.add((m.lower(), r.path))
    return out
