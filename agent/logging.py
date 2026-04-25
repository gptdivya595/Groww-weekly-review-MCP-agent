from __future__ import annotations

import logging
from typing import Any

import structlog


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        format="%(message)s",
        level=level,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso", utc=False),
            structlog.stdlib.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def bind_run_context(run_id: str, **extras: Any) -> None:
    from agent.telemetry import bind_context as bind_telemetry_context

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(run_id=run_id, **extras)
    bind_telemetry_context(run_id=run_id, **extras)


def clear_run_context() -> None:
    from agent.telemetry import clear_context as clear_telemetry_context

    structlog.contextvars.clear_contextvars()
    clear_telemetry_context()


def get_logger(name: str = "pulse") -> structlog.stdlib.BoundLogger:
    return structlog.stdlib.get_logger(name)
