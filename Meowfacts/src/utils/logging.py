"""Logging utilities shared across the application."""

from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from pathlib import Path

try:
    from rich.logging import RichHandler
except ImportError:  # pragma: no cover - optional pretty logging
    RichHandler = None  # type: ignore

workflow_run_id_var: ContextVar[str] = ContextVar("workflow_run_id", default="n/a")


class WorkflowRunIdFilter(logging.Filter):
    """Inject workflow_run_id into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.workflow_run_id = workflow_run_id_var.get()
        return True


class ExactLevelFilter(logging.Filter):
    """Pass records matching a specific level number only."""

    def __init__(self, level: int) -> None:
        """Initialize the level filter.

        Args:
            level: Exact level number to allow through.
        """
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        return record.levelno == self.level


def setup_logging(level: int = logging.INFO, log_path: Path | None = None) -> None:
    """Configure root logging with run ID context and optional file output.

    Args:
        level: Logging level to configure for the root logger.
        log_path: Optional path to a log file; overwritten each session.
    """

    # Handlers with distinct formats per level family
    if RichHandler:
        info_handler = RichHandler(
            level=logging.INFO,
            console=None,
            show_time=True,
            show_level=True,
            show_path=False,
            rich_tracebacks=False,
            log_time_format="%Y-%m-%d %H:%M:%S",
        )
        info_handler.addFilter(ExactLevelFilter(logging.INFO))
        info_handler.setFormatter(logging.Formatter("%(message)s"))
    else:
        info_handler = logging.StreamHandler(sys.stdout)
        info_handler.setLevel(logging.INFO)
        info_handler.addFilter(ExactLevelFilter(logging.INFO))
        info_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )

    debug_handler = (
        RichHandler(
            level=logging.DEBUG,
            console=None,
            show_time=True,
            show_level=True,
            show_path=True,
            rich_tracebacks=False,
            log_time_format="%Y-%m-%d %H:%M:%S",
        )
        if RichHandler
        else logging.StreamHandler(sys.stdout)
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.addFilter(ExactLevelFilter(logging.DEBUG))
    debug_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s [%(workflow_run_id)s] - %(message)s"
        )
    )

    handlers: list[logging.Handler] = [info_handler, debug_handler]

    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s [%(workflow_run_id)s] - %(message)s"
            )
        )
        handlers.append(file_handler)

    logging.basicConfig(level=level, handlers=handlers, force=True)

    for handler in logging.getLogger().handlers:
        handler.addFilter(WorkflowRunIdFilter())

    httpx_logger = logging.getLogger("httpx")
    httpx_logger.setLevel(logging.DEBUG if level <= logging.DEBUG else logging.WARNING)
