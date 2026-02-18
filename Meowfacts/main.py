"""Run a single Meowfacts workflow by name from settings."""

from __future__ import annotations

import uuid
from typing import Optional

import logging

from src import workflows
from src.core import Settings
from src.utils.logging import setup_logging, workflow_run_id_var


# Select the workflow to run here:
# Options:
# "DAILY_ELT" (default),
# "TEST_RUN",
# "TRANSFORM_ONLY",
# "PUBLISH_ONLY".
# Leave as None to use the default.
SELECTED_WORKFLOW: Optional[str] = None


def main() -> None:
    """Bootstrap settings/logging and run the configured workflow once."""

    cfg = Settings()  # Reads MEOW_* env vars; MEOW_WORKFLOW_NAME picks the flow.
    setup_logging(log_path=cfg.log_path)

    run_id = str(uuid.uuid4())
    token = workflow_run_id_var.set(run_id)
    logger = logging.getLogger(__name__)
    workflow_name = SELECTED_WORKFLOW or cfg.workflow_name
    logger.info("Starting workflow '%s' (MEOW_WORKFLOW_NAME)", workflow_name)

    try:
        result = workflows.run_sync(workflow_name, settings=cfg)
        logger.info("Workflow '%s' completed", workflow_name)
        logger.debug("Workflow result: %s", result)
    except KeyError as err:
        available = ", ".join(workflows.list_workflows()) or "(none registered)"
        logger.error("Workflow '%s' not found. Available: %s", workflow_name, available)
        raise err
    finally:
        workflow_run_id_var.reset(token)


if __name__ == "__main__":
    main()
