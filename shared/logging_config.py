import logging
import json
import sys
from datetime import datetime, timezone
from shared.config import settings


class JSONFormatter(logging.Formatter):
    """
    Strukturirani JSON logging — svaki log red je valjan JSON.
    Idealno za agregaciju logova u distribuiranom sustavu.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "node_id": settings.node_id,
            "level": record.levelname,
            "service": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra"):
            log_entry.update(record.extra)

        return json.dumps(log_entry)


def setup_logging(service_name: str, level: str = "INFO") -> logging.Logger:
    """
    Postavi strukturirani logging za servis.
    Koristi JSON format u produkciji, human-readable u developmentu.
    """
    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)

    # JSON format u produkciji, human-readable lokalno
    if os.getenv("ENV", "development") == "production":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%H:%M:%S"
        ))

    logger.addHandler(handler)
    return logger


import os