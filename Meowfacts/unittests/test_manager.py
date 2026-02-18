import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from src.core import Settings, WorkflowType
from src import workflows as manager


class ManagerRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        manager._registry.clear()  # type: ignore[attr-defined]
        manager._display_names.clear()  # type: ignore[attr-defined]

    def test_register_and_get_sync(self) -> None:
        @manager.register("sync_fn")
        def fn():
            return 1

        fetched = manager.get("sync_fn")
        self.assertEqual(fetched(), 1)

    def test_register_and_get_async(self) -> None:
        @manager.register("async_fn")
        async def fn():
            return 2

        fetched = manager.get("async_fn")
        result = asyncio.run(fetched())
        self.assertEqual(result, 2)

    def test_register_accepts_enum(self) -> None:
        @manager.register(WorkflowType.DAILY_ELT)
        def fn():
            return "ok"

        self.assertEqual(manager.get("DAILY_ELT")(), "ok")

    def test_get_unknown_raises_keyerror(self) -> None:
        with self.assertRaises(KeyError):
            manager.get("missing")

    def test_run_sync_calls_registered_sync(self) -> None:
        @manager.register("hello")
        def fn():
            return {"result": "hi"}

        out = manager.run_sync("hello")
        self.assertEqual(out["result"], "hi")

    def test_run_sync_calls_registered_async(self) -> None:
        @manager.register("hello_async")
        async def fn():
            return {"result": "hi"}

        out = manager.run_sync("hello_async")
        self.assertEqual(out["result"], "hi")

    def test_run_async_calls_registered_sync(self) -> None:
        @manager.register("hello_sync")
        def fn(settings: Settings):
            return settings.base_url

        out = asyncio.run(manager.run("hello_sync"))
        self.assertTrue(out)

    def test_run_async_calls_registered_async_with_settings(self) -> None:
        @manager.register("hello_async_settings")
        async def fn(settings: Settings):
            return settings.facts_per_language

        out = asyncio.run(manager.run("hello_async_settings"))
        self.assertIsInstance(out, int)

    def test_list_workflows_returns_sorted_names(self) -> None:
        @manager.register("b")
        def _b(): ...

        @manager.register("a")
        def _a(): ...

        self.assertEqual(manager.list_workflows(), ["a", "b"])


class ManagerBuiltInsTests(unittest.IsolatedAsyncioTestCase):
    def _tmp_settings(self) -> Settings:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        base = Settings()
        return base.model_copy(
            update={
                "data_path": Path(tmp.name) / "meowfacts.json",
                "raw_data_path": Path(tmp.name) / "raw.json",
                "log_path": Path(tmp.name) / "meowfacts.log",
            }
        )

    async def asyncSetUp(self) -> None:
        manager._registry.clear()  # type: ignore[attr-defined]
        manager._display_names.clear()  # type: ignore[attr-defined]

    async def test_daily_elt_returns_summary(self) -> None:
        cfg = self._tmp_settings()
        with (
            patch(
                "src.core.services.FactFetcher.fetch_all",
                new=AsyncMock(return_value=[{"language": "eng-us", "fact": "cat"}]),
            ) as fetch,
            patch(
                "src.core.services.FactCleaner.normalize",
                return_value=(
                    [
                        {
                            "language": "eng-us",
                            "fact": "cat",
                            "fact_length": 3,
                        }
                    ],
                    None,
                ),
            ) as clean,
        ):
            result = await manager.run(WorkflowType.DAILY_ELT, settings=cfg)

        fetch.assert_awaited_once()
        clean.assert_called_once()
        self.assertIn("fetched_records", result)
        self.assertIn("json_path", result)
        self.assertIn("raw_path", result)

    async def test_test_run_returns_summary(self) -> None:
        cfg = self._tmp_settings()
        with (
            patch(
                "src.core.services.FactFetcher.fetch_all",
                new=AsyncMock(return_value=[{"language": "eng-us", "fact": "cat"}]),
            ) as fetch,
            patch(
                "src.core.services.FactCleaner.normalize",
                return_value=(
                    [
                        {
                            "language": "eng-us",
                            "fact": "cat",
                            "fact_length": 3,
                        }
                    ],
                    None,
                ),
            ) as clean,
        ):
            result = await manager.run(WorkflowType.TEST_RUN, settings=cfg)

        fetch.assert_awaited_once()
        clean.assert_called_once()
        self.assertIn("cleaned_records", result)

    async def test_publish_only_raises(self) -> None:
        cfg = self._tmp_settings()
        with self.assertRaises(ValueError):
            await manager.run(WorkflowType.PUBLISH_ONLY, settings=cfg)


if __name__ == "__main__":
    unittest.main()
