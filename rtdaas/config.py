"""Runtime configuration, read from environment variables (see docs/architecture/spec.md §9)."""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field

from rtdaas import __version__


def _find_repo_root() -> Path:
    """Locate the checkout that holds ``contracts/`` and ``quality/``.

    Editable installs find them next to the package; the container images install the package
    from ``/opt/rtdaas`` and keep the checkout there; ``RTDAAS_ROOT`` overrides both.
    """
    candidates: list[Path] = []
    if os.environ.get("RTDAAS_ROOT"):
        candidates.append(Path(os.environ["RTDAAS_ROOT"]))
    candidates += [Path(__file__).resolve().parent.parent, Path("/opt/rtdaas"), Path.cwd()]
    for c in candidates:
        if (c / "contracts" / "registry.yml").is_file():
            return c
    return candidates[0]


REPO_ROOT = _find_repo_root()
DEFAULT_MSGS_DIR = REPO_ROOT / "contracts" / "msgs"
DEFAULT_AVRO_DIR = REPO_ROOT / "contracts" / "avro"
DEFAULT_REGISTRY_YML = REPO_ROOT / "contracts" / "registry.yml"
DEFAULT_CHECKS_DIR = REPO_ROOT / "quality" / "checks"


class Settings(BaseModel):
    """All settings have defaults that work against ``infra/compose``."""

    kafka_bootstrap: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081/apis/ccompat/v7"
    es_url: str = "http://localhost:9200"

    s3_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket_raw: str = "rtdaas-raw"
    s3_bucket_lake: str = "rtdaas-lake"

    iceberg_rest_uri: str = "http://localhost:8181"
    iceberg_warehouse: str = "s3a://rtdaas-lake/warehouse"

    snowflake_account: str | None = None
    snowflake_user: str | None = None
    snowflake_password: str | None = None
    snowflake_warehouse: str | None = None
    snowflake_database: str = "RTDAAS"
    snowflake_role: str | None = None

    airflow_api_url: str = "http://localhost:8080/api/v1"
    airflow_user: str = "airflow"
    airflow_password: str = "airflow"

    serving_backend: str = "sqlite"
    serving_sqlite_path: str = "./data/rtdaas.sqlite"
    serving_duckdb_path: str = "./data/rtdaas.duckdb"
    media_root: str = "./data"
    api_host: str = "0.0.0.0"
    api_port: int = 8080

    pipeline_version: str = Field(default=__version__)
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        values: dict[str, object] = {}
        for name, field in cls.model_fields.items():
            key = name.upper()
            if key in env and env[key] != "":
                raw = env[key]
                ann = field.annotation
                if ann is int:
                    values[name] = int(raw)
                else:
                    values[name] = raw
        return cls(**values)

    @property
    def raw_bags_uri(self) -> str:
        return f"s3a://{self.s3_bucket_raw}/bags"

    @property
    def raw_jigsaws_uri(self) -> str:
        return f"s3a://{self.s3_bucket_raw}/jigsaws"

    @property
    def checkpoint_uri(self) -> str:
        return f"s3a://{self.s3_bucket_lake}/checkpoints"

    @property
    def snowflake_stage_uri(self) -> str:
        return f"s3a://{self.s3_bucket_lake}/stage/snowflake"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()


def configure_logging(level: str | None = None) -> None:
    logging.basicConfig(
        level=(level or get_settings().log_level).upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
