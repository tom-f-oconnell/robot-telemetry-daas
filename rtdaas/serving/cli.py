"""Console script ``rtdaas-api``: run the data API with uvicorn."""

from __future__ import annotations

import argparse
import sys

from rtdaas.config import configure_logging, get_settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rtdaas-api", description="Serve the rtdaas data API and UI")
    parser.add_argument("--host", default=None, help="bind host (default API_HOST)")
    parser.add_argument("--port", type=int, default=None, help="bind port (default API_PORT)")
    parser.add_argument("--reload", action="store_true", help="auto-reload (development)")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(settings.log_level)
    try:
        import uvicorn
    except ImportError:
        print("uvicorn is not installed; pip install 'rtdaas[api]'", file=sys.stderr)
        return 1
    uvicorn.run(
        "rtdaas.serving.app:create_app",
        factory=True,
        host=args.host or settings.api_host,
        port=args.port or settings.api_port,
        reload=args.reload,
        log_level=settings.log_level.lower(),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
