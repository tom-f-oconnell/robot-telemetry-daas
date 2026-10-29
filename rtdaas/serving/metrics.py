"""Prometheus metrics for the API.

Uses ``prometheus_client`` when installed; otherwise a tiny in-process registry renders the
same text exposition format (request counts by route/status and a latency histogram) so the
``/metrics`` endpoint and the ServiceMonitor work in every environment.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict

try:  # pragma: no cover - exercised only when the package is installed
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

    _HAVE_PROM = True
except ImportError:  # pragma: no cover
    _HAVE_PROM = False
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"

BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


class _FallbackMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: dict[tuple[str, str, str], int] = defaultdict(int)
        self._lat_sum: dict[tuple[str, str], float] = defaultdict(float)
        self._lat_count: dict[tuple[str, str], int] = defaultdict(int)
        self._lat_buckets: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0] * (len(BUCKETS) + 1))

    def observe(self, method: str, route: str, status: str, seconds: float) -> None:
        with self._lock:
            self._requests[(method, route, status)] += 1
            key = (method, route)
            self._lat_sum[key] += seconds
            self._lat_count[key] += 1
            b = self._lat_buckets[key]
            for i, le in enumerate(BUCKETS):
                if seconds <= le:
                    b[i] += 1
            b[-1] += 1

    def render(self) -> str:
        lines = [
            "# HELP rtdaas_api_requests_total HTTP requests by method, route and status.",
            "# TYPE rtdaas_api_requests_total counter",
        ]
        with self._lock:
            for (m, r, s), n in sorted(self._requests.items()):
                lines.append(f'rtdaas_api_requests_total{{method="{m}",route="{r}",status="{s}"}} {n}')
            lines += [
                "# HELP rtdaas_api_request_duration_seconds Request latency.",
                "# TYPE rtdaas_api_request_duration_seconds histogram",
            ]
            for (m, r), b in sorted(self._lat_buckets.items()):
                for i, le in enumerate(BUCKETS):
                    lines.append(f'rtdaas_api_request_duration_seconds_bucket{{method="{m}",route="{r}",le="{le}"}} {b[i]}')
                lines.append(f'rtdaas_api_request_duration_seconds_bucket{{method="{m}",route="{r}",le="+Inf"}} {b[-1]}')
                lines.append(f'rtdaas_api_request_duration_seconds_sum{{method="{m}",route="{r}"}} {self._lat_sum[(m, r)]:.6f}')
                lines.append(f'rtdaas_api_request_duration_seconds_count{{method="{m}",route="{r}"}} {self._lat_count[(m, r)]}')
        return "\n".join(lines) + "\n"


class Metrics:
    def __init__(self) -> None:
        if _HAVE_PROM:  # pragma: no cover
            self._req = Counter("rtdaas_api_requests_total", "HTTP requests", ["method", "route", "status"])
            self._lat = Histogram("rtdaas_api_request_duration_seconds", "Request latency", ["method", "route"], buckets=BUCKETS)
        else:
            self._fb = _FallbackMetrics()

    def observe(self, method: str, route: str, status: int, seconds: float) -> None:
        if _HAVE_PROM:  # pragma: no cover
            self._req.labels(method, route, str(status)).inc()
            self._lat.labels(method, route).observe(seconds)
        else:
            self._fb.observe(method, route, str(status), seconds)

    def render(self) -> tuple[bytes, str]:
        if _HAVE_PROM:  # pragma: no cover
            return generate_latest(), CONTENT_TYPE_LATEST
        return self._fb.render().encode(), CONTENT_TYPE_LATEST


class Timer:
    def __enter__(self) -> Timer:
        self.start = time.perf_counter()
        return self

    def __exit__(self, *exc) -> None:
        self.seconds = time.perf_counter() - self.start
