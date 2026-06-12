from __future__ import annotations

from datetime import timedelta
from io import BytesIO
from typing import Any

from utils.config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)


class Storage:
    def __init__(self) -> None:
        self._client: Any | None = None
        self._bucket: str = MINIO_BUCKET
        self._ready: bool = False
        self._init_client()

    def _init_client(self) -> None:
        try:
            from minio import Minio

            self._client = Minio(
                endpoint=MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE,
            )
            self._ensure_bucket()
            self._ready = True
        except Exception as e:
            print(f"[WARN] MinIO init failed: {e}")
            self._ready = False

    def _ensure_bucket(self) -> None:
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    def upload_bytes(self, object_name: str, data: bytes) -> bool:
        if not self._ready:
            return False
        try:
            self._client.put_object(
                self._bucket,
                object_name,
                BytesIO(data),
                length=len(data),
            )
            return True
        except Exception as e:
            print(f"[WARN] MinIO upload failed: {e}")
            return False

    def presigned_url(self, object_name: str, expires: int = 3600) -> str | None:
        if not self._ready:
            return None
        try:
            return self._client.presigned_get_object(
                self._bucket, object_name, expires=timedelta(seconds=expires)
            )
        except Exception as e:
            print(f"[WARN] MinIO presigned URL failed: {e}")
            return None

    def is_ready(self) -> bool:
        return self._ready


_storage: Storage | None = None


def get_storage() -> Storage:
    global _storage
    if _storage is None:
        _storage = Storage()
    return _storage
