# Serving API + web UI (Starlette).
# Build context: repository root.
#   docker build -f infra/docker/api.Dockerfile -t ghcr.io/<owner>/rtdaas-api:dev .
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --gid 10001 rtdaas \
 && useradd --uid 10001 --gid rtdaas --create-home --shell /usr/sbin/nologin rtdaas

WORKDIR /opt/rtdaas
COPY pyproject.toml README.md ./
COPY rtdaas ./rtdaas
COPY contracts ./contracts
COPY quality ./quality
RUN pip install "/opt/rtdaas[api]"

# Data directory for SQLite mirror / media (mounted in compose and k8s).
RUN mkdir -p /data && chown -R rtdaas:rtdaas /data /opt/rtdaas
VOLUME ["/data"]

ENV API_HOST=0.0.0.0 \
    API_PORT=8080 \
    MEDIA_ROOT=/data \
    SERVING_SQLITE_PATH=/data/rtdaas.sqlite \
    SERVING_DUCKDB_PATH=/data/rtdaas.duckdb

USER rtdaas
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
  CMD curl -fsS "http://127.0.0.1:${API_PORT}/api/health" || exit 1

CMD ["rtdaas-api"]
