import os
from pathlib import Path

SAVE_DIR: Path = Path(__file__).parent.parent / "model" / "bin"
CHAR_MAX_SEQ_LEN: int = 150
CACHE_TTL: int = int(os.getenv("REDIS_CACHE_TTL", "86400"))
