import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SAVE_DIR: Path = Path(__file__).parent.parent / "model" / "bin"

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "screenshots")
MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
