import logging
import os
from typing import Optional

_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def get_logger(name: Optional[str] = None) -> logging.Logger:
    logger = logging.getLogger(name if name else "vacation")
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
    logger.setLevel(_LOG_LEVEL)
    return logger
