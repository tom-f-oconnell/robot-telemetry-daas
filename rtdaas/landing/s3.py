"""S3 (or MinIO) :class:`rtdaas.landing.core.Store` on boto3 (``pip install rtdaas[land]``)."""

from __future__ import annotations

from pathlib import Path

from rtdaas.config import Settings


class S3Store:
    def __init__(self, bucket: str, settings: Settings, region: str = "us-east-1"):
        try:
            import boto3
            from botocore.config import Config
        except ImportError as exc:  # pragma: no cover - depends on the environment
            raise ImportError("rtdaas-land needs boto3 (pip install rtdaas[land])") from exc
        self.bucket = bucket
        self._s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=region,
            config=Config(s3={"addressing_style": "path"}),  # MinIO
        )

    def size(self, key: str) -> int | None:
        from botocore.exceptions import ClientError

        try:
            return int(self._s3.head_object(Bucket=self.bucket, Key=key)["ContentLength"])
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise

    def put(self, path: Path, key: str) -> None:
        self._s3.upload_file(str(path), self.bucket, key)
