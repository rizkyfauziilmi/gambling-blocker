import logging
import os


def setup_logging(
    name: str = "unknown",
    log_level: int = logging.DEBUG,
) -> logging.Logger:
    """Setup logging configuration."""
    # make logs folder
    os.makedirs("logs", exist_ok=True)
    path = os.path.join("logs", f"{name}.log")

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # File handler
    file_handler = logging.FileHandler(path, encoding="utf-8")
    file_handler.setLevel(log_level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # Formatter
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
