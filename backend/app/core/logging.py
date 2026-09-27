"""Structured logging with request/message correlation.

Emits one JSON object per line so the container logs drop straight into
Elasticsearch or ClickHouse later without a parsing stage. A correlation id is
carried in a ``ContextVar`` and stamped onto every record automatically, which
is what makes it possible to follow one citizen report from the HTTP handler,
through Kafka, into the normalizer worker.
"""

from __future__ import annotations

import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

import orjson

_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)

# Attributes LogRecord always carries; anything else was passed via `extra=`
# and should be promoted into the JSON payload.
_RESERVED: frozenset[str] = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }
)


def new_correlation_id() -> str:
    """Generate and bind a fresh correlation id to the current context."""
    cid = uuid.uuid4().hex[:16]
    _correlation_id.set(cid)
    return cid


def set_correlation_id(cid: str | None) -> None:
    _correlation_id.set(cid)


def get_correlation_id() -> str | None:
    return _correlation_id.get()


class JsonFormatter(logging.Formatter):
    """Render records as single-line JSON."""

    def __init__(self, service: str = "backend") -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service": self.service,
            "message": record.getMessage(),
        }

        cid = _correlation_id.get()
        if cid:
            payload["correlation_id"] = cid

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value

        # `default=str` keeps a stray UUID/datetime/Path from killing a log line.
        return orjson.dumps(payload, default=str).decode("utf-8")


class HumanFormatter(logging.Formatter):
    """Readable formatter for local terminals."""

    _FMT = "%(asctime)s | %(levelname)-8s | %(name)-34s | %(message)s"

    def __init__(self) -> None:
        super().__init__(fmt=self._FMT, datefmt="%H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        cid = _correlation_id.get()
        return f"[{cid}] {base}" if cid else base


def configure_logging(
    *,
    level: str = "INFO",
    json_output: bool = True,
    service: str = "backend",
) -> None:
    """Install the root handler. Idempotent — safe to call from every entrypoint."""
    root = logging.getLogger()
    root.setLevel(level.upper())

    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter(service=service) if json_output else HumanFormatter())
    root.addHandler(handler)

    # These libraries are chatty at INFO and drown out pipeline events.
    for noisy in ("aiokafka", "aiokafka.consumer.group_coordinator", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
