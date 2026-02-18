"""Workflows, registry, and helpers for the Meowfacts ELT pipeline."""

from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Protocol

import pandas as pd

from src.core import (
    ArtifactKey,
    FactFields,
    CleanRecord,
    PipelineContext,
    RawRecord,
    Settings,
    WorkflowType,
    ensure_output_dirs,
)
from src.core.services import FactCleaner, FactFetcher, FactPublisher

# ---------------------------------------------------------------------------
# Workflow protocol + result


@dataclass
class WorkflowResult:
    """Standard workflow result structure.

    Attributes:
        name: Workflow name.
        summary: Arbitrary summary payload (counts, paths, etc.).
        context: Shared pipeline context carrying artifacts and dataframes.
    """

    name: str
    summary: Dict[str, Any]
    context: PipelineContext


class Workflow(Protocol):
    """Protocol for runnable workflows."""

    name: str

    async def run(
        self, ctx: PipelineContext
    ) -> WorkflowResult:  # pragma: no cover - protocol
        ...


# ---------------------------------------------------------------------------
# ELT workflow implementations


class ExtractLoadWorkflow(Workflow):
    """Extract raw facts and load them into a staging file."""

    name = "extract_load"

    def __init__(
        self,
        fetcher: FactFetcher,
        publisher: FactPublisher,
        languages: list[str] | None,
        raw_path: Path,
    ):
        """Configure extract+load step.

        Args:
            fetcher: Fact fetcher dependency.
            publisher: Publisher used to persist raw data.
            languages: Optional language overrides; None triggers discovery.
            raw_path: Output path for staged raw JSON.
        """
        self.fetcher = fetcher
        self.publisher = publisher
        self.languages = languages
        self.raw_path = raw_path

    async def run(self, ctx: PipelineContext) -> WorkflowResult:
        """Fetch raw facts and persist them.

        Args:
            ctx: Pipeline context to mutate and return.

        Returns:
            WorkflowResult containing counts, languages, and raw path.
        """
        raw_records = await self.fetcher.fetch_all(self.languages)
        raw_path = self.publisher.write_raw_json(raw_records, path=self.raw_path)

        ctx.raw_records = raw_records
        ctx.output_paths[ArtifactKey.RAW_JSON] = raw_path

        used_languages = sorted({rec[FactFields.LANGUAGE.value] for rec in raw_records})

        summary: Dict[str, Any] = {
            "fetched_records": len(raw_records),
            "languages": used_languages,
            "raw_path": str(raw_path),
        }
        return WorkflowResult(name=self.name, summary=summary, context=ctx)


class TransformWorkflow(Workflow):
    """Transform staged raw facts into cleaned records."""

    name = "transform"

    def __init__(self, cleaner: FactCleaner, raw_path: Path):
        """Configure transform step.

        Args:
            cleaner: Fact cleaner dependency.
            raw_path: Default path to the raw staging file.
        """
        self.cleaner = cleaner
        self.raw_path = raw_path

    async def run(self, ctx: PipelineContext) -> WorkflowResult:
        """Clean raw facts from the staging file into structured records.

        Args:
            ctx: Pipeline context with paths and artifacts.

        Raises:
            ValueError: If the raw JSON file cannot be located.

        Returns:
            WorkflowResult with cleaned counts and dataframe info.
        """
        raw_path = ctx.output_paths.get(ArtifactKey.RAW_JSON, self.raw_path)
        if raw_path is None or not Path(raw_path).exists():
            raise ValueError("Transform workflow requires a staged raw JSON file")

        with Path(raw_path).open(encoding="utf-8") as fp:
            raw_records = json.load(fp)

        cleaned_records, frame = self.cleaner.normalize(raw_records)

        ctx.cleaned_records = cleaned_records
        ctx.dataframe = frame

        summary: Dict[str, Any] = {
            "cleaned_records": len(cleaned_records),
            "raw_path": str(raw_path),
            "dataframe_rows": 0 if frame is None else len(frame),
        }
        return WorkflowResult(name=self.name, summary=summary, context=ctx)


class PublishWorkflow(Workflow):
    """Publish cleaned records to JSON."""

    name = "publish"

    def __init__(self, publisher: FactPublisher):
        """Configure publish step.

        Args:
            publisher: Fact publisher dependency.
        """
        self.publisher = publisher

    async def run(self, ctx: PipelineContext) -> WorkflowResult:
        """Persist cleaned records to the configured JSON path.

        Args:
            ctx: Pipeline context containing cleaned_records.

        Raises:
            ValueError: If cleaned records are missing from context.

        Returns:
            WorkflowResult summarizing output path and record count.
        """
        cleaned_records = ctx.cleaned_records
        if cleaned_records is None:
            raise ValueError("Publish workflow requires cleaned_records in context")

        json_path = self.publisher.write_json(cleaned_records)
        ctx.output_paths[ArtifactKey.JSON] = json_path

        summary: Dict[str, Any] = {
            "json_path": str(json_path),
            "record_count": len(cleaned_records),
        }
        return WorkflowResult(name=self.name, summary=summary, context=ctx)


class ExtractTransformWorkflow(Workflow):
    """Extract+load then transform, without publishing."""

    name = "extract_transform"

    def __init__(
        self,
        fetcher: FactFetcher,
        cleaner: FactCleaner,
        publisher: FactPublisher,
        languages: list[str] | None,
        raw_path: Path,
    ):
        """Configure combined extract+transform workflow."""
        self.extract_load = ExtractLoadWorkflow(fetcher, publisher, languages, raw_path)
        self.transform = TransformWorkflow(cleaner, raw_path)

    async def run(self, ctx: PipelineContext) -> WorkflowResult:
        """Run extract+load followed by transform.

        Args:
            ctx: Pipeline context shared across steps.

        Returns:
            Combined WorkflowResult summary from extract and transform.
        """
        extract_summary = await self.extract_load.run(ctx)
        transform_summary = await self.transform.run(ctx)

        combined: Dict[str, Any] = {
            **extract_summary.summary,
            **transform_summary.summary,
        }
        return WorkflowResult(name=self.name, summary=combined, context=ctx)


class FullEltWorkflow(Workflow):
    """End-to-end ELT: extract+load, transform, publish."""

    name = "full_elt"

    def __init__(
        self,
        fetcher: FactFetcher,
        cleaner: FactCleaner,
        publisher: FactPublisher,
        languages: list[str] | None,
        raw_path: Path,
    ) -> None:
        """Configure full ELT workflow."""
        self.extract_load = ExtractLoadWorkflow(fetcher, publisher, languages, raw_path)
        self.transform = TransformWorkflow(cleaner, raw_path)
        self.publish = PublishWorkflow(publisher)

    async def run(self, ctx: PipelineContext) -> WorkflowResult:
        """Run the full extract, transform, and publish chain.

        Args:
            ctx: Pipeline context shared across steps.

        Returns:
            WorkflowResult that merges summaries from all steps.
        """
        extract_summary = await self.extract_load.run(ctx)
        transform_summary = await self.transform.run(ctx)
        publish_summary = await self.publish.run(ctx)

        combined: Dict[str, Any] = {
            **extract_summary.summary,
            **transform_summary.summary,
            **publish_summary.summary,
        }
        return WorkflowResult(name=self.name, summary=combined, context=ctx)


# ---------------------------------------------------------------------------
# Workflow factory

WorkflowFactory = Callable[[Settings], Workflow]


def build_workflow(workflow_type: WorkflowType, cfg: Settings) -> Workflow:
    """Instantiate a workflow by type using the given config.

    Args:
        workflow_type: Enum indicating which workflow to build.
        cfg: Settings object providing dependencies and paths.

    Returns:
        Concrete Workflow instance.

    Raises:
        ValueError: If the workflow_type is not handled.
    """

    fetcher = FactFetcher(
        base_url=cfg.base_url,
        timeout=cfg.request_timeout,
        retries=cfg.request_retries,
        backoff_seconds=cfg.backoff_seconds,
        facts_per_language=cfg.facts_per_language,
    )
    cleaner = FactCleaner()
    publisher = FactPublisher(data_path=cfg.data_path)
    languages = cfg.target_languages or None

    factories: dict[WorkflowType, WorkflowFactory] = {
        WorkflowType.DAILY_ELT: lambda c=cfg: FullEltWorkflow(
            fetcher, cleaner, publisher, languages, c.raw_data_path
        ),
        WorkflowType.TEST_RUN: lambda c=cfg: ExtractTransformWorkflow(
            fetcher, cleaner, publisher, languages, c.raw_data_path
        ),
        WorkflowType.TRANSFORM_ONLY: lambda c=cfg: TransformWorkflow(
            cleaner, c.raw_data_path
        ),
        WorkflowType.PUBLISH_ONLY: lambda c=cfg: PublishWorkflow(publisher),
    }

    if workflow_type not in factories:
        raise ValueError(f"Unhandled workflow '{workflow_type}'")

    return factories[workflow_type](cfg)


# ---------------------------------------------------------------------------
# Decorator-based registry + runner

RegistryFunc = Callable[..., Any]

_registry: Dict[str, RegistryFunc] = {}
_display_names: Dict[str, str] = {}
_builtins: Dict[str | WorkflowType, RegistryFunc] = {}


def _normalize_name(name: str | WorkflowType) -> str:
    """Convert workflow names or enums to a normalized string key."""
    if isinstance(name, WorkflowType):
        return name.value
    normalized = str(name).strip()
    if not normalized:
        raise ValueError("Workflow name must be a non-empty string")
    return normalized


def _key(name: str | WorkflowType) -> str:
    """Lowercase normalization wrapper for registry keys."""
    return _normalize_name(name).casefold()


def register(name: str | WorkflowType) -> Callable[[RegistryFunc], RegistryFunc]:
    """Decorator to register a workflow function under a loose string key.

    Args:
        name: Registry key or WorkflowType.

    Returns:
        Decorated function with registration side effects.
    """

    def decorator(func: RegistryFunc) -> RegistryFunc:
        key = _key(name)
        _registry[key] = func
        _display_names[key] = _normalize_name(name)
        return func

    return decorator


def _register_builtins_if_missing(target: str | WorkflowType | None = None) -> None:
    """Ensure built-ins are registered when explicitly requested."""

    if not _builtins:
        return

    names = _builtins.keys() if target is None else [target]

    for raw_name in names:
        if raw_name not in _builtins:
            continue
        func = _builtins[raw_name]
        key = _key(raw_name)
        if key not in _registry:
            _registry[key] = func
            _display_names[key] = _normalize_name(raw_name)


def get(name: str | WorkflowType) -> RegistryFunc:
    """Retrieve a registered workflow factory by name.

    Args:
        name: Workflow registry key or enum.

    Returns:
        Registered callable.

    Raises:
        KeyError: If the workflow is not registered.
    """
    _register_builtins_if_missing(name)
    key = _key(name)
    if key not in _registry:
        raise KeyError(f"Workflow '{name}' is not registered")
    return _registry[key]


def list_workflows() -> list[str]:
    """List registered workflow display names."""
    return sorted(_display_names.values())


def register_builtins(settings: Settings | None = None) -> None:
    """Force built-ins to be registered (handy for REPL/debug)."""

    for name in list(_builtins.keys()):
        _register_builtins_if_missing(name)


async def run(name: str | WorkflowType, settings: Settings | None = None) -> Any:
    """Run a registered workflow by name (sync or async functions supported).

    Args:
        name: Workflow name or enum.
        settings: Optional settings override.

    Returns:
        Result of the workflow callable (summary dict for built-ins).
    """

    func = get(name)
    cfg = settings or Settings()
    ensure_output_dirs(cfg)

    sig = inspect.signature(func)
    result = func(cfg) if len(sig.parameters) >= 1 else func()
    if inspect.iscoroutine(result):
        result = await result
    return result


def run_sync(name: str | WorkflowType, settings: Settings | None = None) -> Any:
    """Synchronous helper around run().

    Args:
        name: Workflow name or enum.
        settings: Optional settings override.

    Returns:
        Workflow result from the async runner.
    """

    return asyncio.run(run(name, settings=settings))


# ---------------------------------------------------------------------------
# Built-in workflow registrations using the class-based factory


@register(WorkflowType.DAILY_ELT)
async def _daily_elt(settings: Settings | None = None) -> dict[str, Any]:
    cfg = settings or Settings()
    flow = build_workflow(WorkflowType.DAILY_ELT, cfg)
    result = await flow.run(PipelineContext())
    return result.summary


@register(WorkflowType.TEST_RUN)
async def _test_run(settings: Settings | None = None) -> dict[str, Any]:
    cfg = settings or Settings()
    flow = build_workflow(WorkflowType.TEST_RUN, cfg)
    result = await flow.run(PipelineContext())
    return result.summary


@register(WorkflowType.TRANSFORM_ONLY)
async def _transform_only(settings: Settings | None = None) -> dict[str, Any]:
    cfg = settings or Settings()
    flow = build_workflow(WorkflowType.TRANSFORM_ONLY, cfg)
    result = await flow.run(PipelineContext())
    return result.summary


@register(WorkflowType.PUBLISH_ONLY)
async def _publish_only(settings: Settings | None = None) -> dict[str, Any]:
    raise ValueError(
        "PUBLISH_ONLY requires cleaned data; please run an ELT workflow first."
    )


_builtins.update(
    {
        WorkflowType.DAILY_ELT: _daily_elt,
        WorkflowType.TEST_RUN: _test_run,
        WorkflowType.TRANSFORM_ONLY: _transform_only,
        WorkflowType.PUBLISH_ONLY: _publish_only,
    }
)


# ---------------------------------------------------------------------------
# Lightweight helper functions (replaces steps.py)


def pull(languages: list[str], settings: Settings | None = None) -> list[RawRecord]:
    """Synchronously fetch raw fact records for the given languages.

    Args:
        languages: Language codes to fetch.
        settings: Optional settings to override defaults.

    Returns:
        Raw records for the requested languages.

    Raises:
        RuntimeError: If called inside an active event loop.
    """

    cfg = settings or Settings()
    fetcher = FactFetcher(
        base_url=cfg.base_url,
        timeout=cfg.request_timeout,
        retries=cfg.request_retries,
        backoff_seconds=cfg.backoff_seconds,
        facts_per_language=cfg.facts_per_language,
    )

    async def _run() -> list[RawRecord]:
        return await fetcher.fetch_all(languages)

    try:
        return asyncio.run(_run())
    except RuntimeError as exc:
        raise RuntimeError(
            "pull() cannot run inside an active event loop; use pull_async() instead."
        ) from exc


async def pull_async(
    languages: list[str], settings: Settings | None = None
) -> list[RawRecord]:
    """Asynchronously fetch raw fact records for the given languages."""

    cfg = settings or Settings()
    fetcher = FactFetcher(
        base_url=cfg.base_url,
        timeout=cfg.request_timeout,
        retries=cfg.request_retries,
        backoff_seconds=cfg.backoff_seconds,
        facts_per_language=cfg.facts_per_language,
    )
    return await fetcher.fetch_all(languages)


def clean_records(
    records: Iterable[RawRecord],
) -> tuple[list[CleanRecord], pd.DataFrame]:
    """Normalize, deduplicate, and enrich raw records."""

    cleaner = FactCleaner()
    return cleaner.normalize(records)


def save_raw(
    data: Iterable[RawRecord],
    filename: str | None = None,
    settings: Settings | None = None,
) -> Path:
    """Persist raw records to JSON staging and return the path."""

    cfg = settings or Settings()
    ensure_output_dirs(cfg)

    target = Path(cfg.raw_data_path)
    if filename:
        target = target.parent / filename

    publisher = FactPublisher(data_path=target)
    return publisher.write_raw_json(data, path=target)


def save_clean(
    data: Iterable[CleanRecord],
    filename: str | None = None,
    settings: Settings | None = None,
) -> Path:
    """Persist cleaned records to JSON and return the path."""

    cfg = settings or Settings()
    ensure_output_dirs(cfg)

    target = Path(cfg.data_path)
    if filename:
        target = target.parent / filename

    publisher = FactPublisher(data_path=target)
    return publisher.write_json(data, path=target)


__all__ = [
    "Workflow",
    "WorkflowResult",
    "WorkflowFactory",
    "ExtractLoadWorkflow",
    "TransformWorkflow",
    "PublishWorkflow",
    "ExtractTransformWorkflow",
    "FullEltWorkflow",
    "build_workflow",
    "register",
    "get",
    "list_workflows",
    "run",
    "run_sync",
    "register_builtins",
    "pull",
    "pull_async",
    "clean_records",
    "save_raw",
    "save_clean",
]
