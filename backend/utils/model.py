import pickle
from pathlib import Path
from typing import Any

import tensorflow as tf
from keras import Model

from .config import SAVE_DIR
from .helpers import clean_url


_model: Model | None = None
_vectorizer: Any | None = (
    None  # Penyesuaian: Tokenizer diganti menjadi Vectorizer (FeatureUnion)
)
_threshold: float = 0.5


def load() -> bool:
    global _model, _vectorizer, _threshold
    model_path: Path = SAVE_DIR / "dl_model.keras"
    vectorizer_path: Path = SAVE_DIR / "tfidf_vectorizer.pkl"
    threshold_path: Path = SAVE_DIR / "best_threshold.pkl"

    if not all(p.exists() for p in [model_path, vectorizer_path, threshold_path]):
        print(
            f"[WARN] Model files not found in {SAVE_DIR} — /classify/url will return 503"
        )
        return False

    _model = tf.keras.models.load_model(str(model_path))
    with open(vectorizer_path, "rb") as f:
        _vectorizer = pickle.load(f)
    with open(threshold_path, "rb") as f:
        _threshold = pickle.load(f)
    print(f"[INFO] Model loaded from {SAVE_DIR} (threshold={_threshold:.4f})")

    return True


def is_loaded() -> bool:
    return _model is not None and _vectorizer is not None


def infer(url: str) -> dict[str, Any]:
    assert _vectorizer is not None
    assert _model is not None

    cleaned: str = clean_url(url)

    seq_tfidf = _vectorizer.transform([cleaned])

    seq_tfidf.sort_indices()

    prob: float = float(_model.predict(seq_tfidf, verbose=0)[0][0])
    category: str = "gambling" if prob > _threshold else "non-gambling"

    return {"url": url, "category": category, "gambling_score": round(prob, 4)}


_model_loaded: bool = load()
