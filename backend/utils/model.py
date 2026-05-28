import pickle
from pathlib import Path
from typing import Any

import tensorflow as tf
from keras import Model
from keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.preprocessing.text import Tokenizer  # type: ignore[import-untyped]

from .config import SAVE_DIR, CHAR_MAX_SEQ_LEN
from .helpers import prepare_url_for_cnn


_model: Model | None = None
_tokenizer: Tokenizer | None = None
_threshold: float = 0.5


def load() -> bool:
    global _model, _tokenizer, _threshold
    model_path: Path = SAVE_DIR / "cnn_model_char.keras"
    tokenizer_path: Path = SAVE_DIR / "tokenizer.pkl"
    threshold_path: Path = SAVE_DIR / "best_threshold.pkl"

    if not all(p.exists() for p in [model_path, tokenizer_path, threshold_path]):
        print(
            f"[WARN] Model files not found in {SAVE_DIR} — /classify/url will return 503"
        )
        return False

    _model = tf.keras.models.load_model(str(model_path))
    with open(tokenizer_path, "rb") as f:
        _tokenizer = pickle.load(f)
    with open(threshold_path, "rb") as f:
        _threshold = pickle.load(f)
    print(f"[INFO] Model loaded from {SAVE_DIR} (threshold={_threshold:.4f})")

    return True


def is_loaded() -> bool:
    return _model is not None and _tokenizer is not None


def infer(url: str) -> dict[str, Any]:
    assert _tokenizer is not None
    assert _model is not None

    cleaned: str = prepare_url_for_cnn(url)
    seq = pad_sequences(
        _tokenizer.texts_to_sequences([cleaned]),
        maxlen=CHAR_MAX_SEQ_LEN,
    )
    prob: float = float(_model.predict(seq, verbose="0")[0][0])
    category: str = "gambling" if prob > _threshold else "non-gambling"

    return {"url": cleaned, "category": category, "gambling_score": round(prob, 4)}


_model_loaded: bool = load()
