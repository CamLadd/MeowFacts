"""Application configuration and defaults."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TypedDict

import pandas as pd
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_DATA_PATH = DEFAULT_OUTPUT_DIR / "meowfacts.json"
DEFAULT_RAW_DATA_PATH = DEFAULT_OUTPUT_DIR / "raw_meowfacts.json"
DEFAULT_LOG_PATH = Path("log") / "meowfacts.log"


class WorkflowType(StrEnum):
    """Supported workflow identifiers."""

    DAILY_ELT = "DAILY_ELT"
    TEST_RUN = "TEST_RUN"
    TRANSFORM_ONLY = "TRANSFORM_ONLY"
    PUBLISH_ONLY = "PUBLISH_ONLY"


class FactFields(StrEnum):
    """Canonical field names for fact records."""

    LANGUAGE = "language"
    FACT = "fact"
    HASH = "fact_hash"
    LENGTH = "fact_length"


class ArtifactKey(StrEnum):
    """Named keys for generated artifacts."""

    RAW_JSON = "raw_json"
    JSON = "json"


class RawRecord(TypedDict):
    """Raw record returned by the Meowfacts API."""

    language: str
    fact: str


class CleanRecord(TypedDict):
    """Cleaned and enriched record used downstream."""

    language: str
    fact: str
    fact_length: int


@dataclass
class PipelineContext:
    """Typed state shared between workflow steps."""

    run_id: str | None = None
    raw_records: list[RawRecord] = field(default_factory=list)
    cleaned_records: list[CleanRecord] = field(default_factory=list)
    dataframe: pd.DataFrame | None = None
    output_paths: dict[ArtifactKey, Path] = field(default_factory=dict)


LANGUAGE_LABELS: dict[str, str] = {
    "eng-us": "US English",
    "eng": "US English",
    "esp-es": "Spanish (Spain)",
    "esp-mx": "Spanish (Mexico)",
    "esp": "Spanish",
    "ces-cz": "Czech",
    "ces": "Czech",
    "ger-de": "German",
    "ger": "German",
    "por-br": "Portuguese (Brazil)",
    "por": "Portuguese",
    "rus-ru": "Russian",
    "rus": "Russian",
    "zho-tw": "Chinese (Taiwan)",
    "zho": "Chinese",
    "kor-ko": "Korean",
    "kor": "Korean",
    "ita-it": "Italian",
    "ita": "Italian",
    "ukr-ua": "Ukrainian",
    "ukr": "Ukrainian",
    "ben-in": "Bengali",
    "ben": "Bengali",
    "fil-tl": "Filipino",
    "fil": "Filipino",
    "urd-ud": "Urdu",
    "urd": "Urdu",
    "fra": "French",
    "fra-fr": "French",
}


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables.

    Environment variables are prefixed with ``MEOW_`` (for example,
    ``MEOW_BASE_URL``). Defaults suit local execution.
    """

    model_config = SettingsConfigDict(env_prefix="MEOW_", extra="ignore")

    workflow_name: str | WorkflowType = WorkflowType.DAILY_ELT
    # Empty list means "discover all supported languages dynamically".
    target_languages: list[str] = Field(default_factory=list)

    base_url: str = "https://meowfacts.herokuapp.com/"
    raw_data_path: Path = DEFAULT_RAW_DATA_PATH
    data_path: Path = DEFAULT_DATA_PATH
    log_path: Path = DEFAULT_LOG_PATH
    request_timeout: int = 10
    request_retries: int = 3
    backoff_seconds: float = 1.5
    facts_per_language: int = 10000

    @field_validator("workflow_name", mode="before")
    @classmethod
    def normalize_workflow_name(cls, value: str | WorkflowType) -> str:
        """Allow friendly strings to map to enum members or custom names.

        Args:
            value: WorkflowType enum or string provided via env or caller.

        Returns:
            Normalized workflow name string.
        """

        if isinstance(value, WorkflowType):
            return value.value

        lookup = {
            "DAILY_ELT": WorkflowType.DAILY_ELT.value,
            "FULL_PIPELINE": WorkflowType.DAILY_ELT.value,
            "FETCH_CLEAN": WorkflowType.TEST_RUN.value,
            "TRANSFORM_ONLY": WorkflowType.TRANSFORM_ONLY.value,
            "PUBLISH_VISUALIZE": WorkflowType.PUBLISH_ONLY.value,
            "PUBLISH": WorkflowType.PUBLISH_ONLY.value,
        }

        normalized = str(value).strip()
        lookup_key = normalized.upper()
        return lookup.get(lookup_key, normalized)


def ensure_output_dirs(cfg: Settings) -> None:
    """Create data, report, and log directories if they do not exist.

    Args:
        cfg: Settings instance containing output paths.
    """

    cfg.data_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.raw_data_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.log_path.parent.mkdir(parents=True, exist_ok=True)
