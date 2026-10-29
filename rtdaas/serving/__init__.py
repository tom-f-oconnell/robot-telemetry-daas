"""Serving layer: Starlette data API, query backends and the web UI (spec §4.10)."""

from __future__ import annotations

__all__ = ["create_app"]


def create_app(*args, **kwargs):  # pragma: no cover - thin re-export
    from rtdaas.serving.app import create_app as _create_app

    return _create_app(*args, **kwargs)
