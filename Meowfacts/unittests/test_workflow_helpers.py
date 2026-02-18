import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src.core import Settings, WorkflowType
from src.core import workflows


class TestWorkflowHelpers(unittest.TestCase):
    def test_pull_runs_fetcher(self) -> None:
        with patch(
            "src.core.workflows.FactFetcher.fetch_all",
            new=AsyncMock(return_value=[{"language": "eng-us", "fact": "cat"}]),
        ) as fetch:
            result = workflows.pull(["eng-us"])

        fetch.assert_awaited_once_with(["eng-us"])
        assert result[0]["fact"] == "cat"

    def test_pull_async_runs_fetcher(self) -> None:
        async def _run() -> None:
            with patch(
                "src.core.workflows.FactFetcher.fetch_all",
                new=AsyncMock(return_value=[{"language": "eng-us", "fact": "cat"}]),
            ) as fetch:
                result = await workflows.pull_async(["eng-us"])

            fetch.assert_awaited_once_with(["eng-us"])
            assert result[0]["fact"] == "cat"

        asyncio.run(_run())

    def test_save_raw_writes_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Settings().model_copy(
                update={
                    "data_path": Path(tmp) / "data.json",
                    "raw_data_path": Path(tmp) / "raw.json",
                    "log_path": Path(tmp) / "log.txt",
                }
            )

            path = workflows.save_raw(
                [{"language": "eng-us", "fact": "cat"}],
                settings=cfg,
            )

            assert path.exists()
            payload = json.loads(path.read_text(encoding="utf-8"))
            assert payload[0]["fact"] == "cat"

    def test_save_clean_writes_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Settings().model_copy(
                update={
                    "data_path": Path(tmp) / "data.json",
                    "raw_data_path": Path(tmp) / "raw.json",
                    "log_path": Path(tmp) / "log.txt",
                }
            )

            path = workflows.save_clean(
                [
                    {
                        "language": "eng-us",
                        "fact": "cat",
                        "fact_length": 3,
                    }
                ],
                settings=cfg,
            )

            assert path.exists()
            payload = json.loads(path.read_text(encoding="utf-8"))
            assert payload[0]["fact_length"] == 3
            assert "fact_hash" not in payload[0]

    def test_build_workflow_creates_expected_types(self) -> None:
        cfg = Settings()
        assert isinstance(
            workflows.build_workflow(WorkflowType.DAILY_ELT, cfg),
            workflows.FullEltWorkflow,
        )
        assert isinstance(
            workflows.build_workflow(WorkflowType.TEST_RUN, cfg),
            workflows.ExtractTransformWorkflow,
        )
        assert isinstance(
            workflows.build_workflow(WorkflowType.TRANSFORM_ONLY, cfg),
            workflows.TransformWorkflow,
        )
        assert isinstance(
            workflows.build_workflow(WorkflowType.PUBLISH_ONLY, cfg),
            workflows.PublishWorkflow,
        )


class TestWorkflowRegistry(unittest.TestCase):
    def test_run_registers_builtins_on_get(self) -> None:
        workflows._registry.clear()  # type: ignore[attr-defined]
        workflows._display_names.clear()  # type: ignore[attr-defined]

        workflows.list_workflows()
        assert workflows.get(WorkflowType.DAILY_ELT)

    def test_run_async_returns_summary(self) -> None:
        async def _run() -> None:
            cfg = Settings().model_copy(
                update={
                    "data_path": Path("output/test.json"),
                    "raw_data_path": Path("output/raw.json"),
                    "log_path": Path("log/test.log"),
                }
            )

            with patch("src.core.workflows.build_workflow") as builder:
                mock_flow = AsyncMock()
                mock_flow.run = AsyncMock(
                    return_value=workflows.WorkflowResult(
                        "x", {"ok": True}, workflows.PipelineContext()
                    )
                )
                builder.return_value = mock_flow

                result = await workflows.run(WorkflowType.DAILY_ELT, settings=cfg)

            builder.assert_called_once()
            assert result == {"ok": True}

        asyncio.run(_run())

    def test_run_sync_wraps_async(self) -> None:
        with patch(
            "src.core.workflows.run", new=AsyncMock(return_value={"ok": True})
        ) as run_async:
            out = workflows.run_sync("custom")

        run_async.assert_awaited_once()
        assert out == {"ok": True}

    def test_register_builtins_adds_display_names(self) -> None:
        workflows._registry.clear()  # type: ignore[attr-defined]
        workflows._display_names.clear()  # type: ignore[attr-defined]

        workflows.register_builtins()
        assert WorkflowType.DAILY_ELT.value in workflows.list_workflows()
