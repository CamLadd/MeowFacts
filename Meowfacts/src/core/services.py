"""Service layer: fetch, clean, publish for Meowfacts."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, cast

import httpx
import pandas as pd
from tabulate import tabulate

from src.core import CleanRecord, FactFields, LANGUAGE_LABELS, RawRecord

RETRYABLE_STATUSES = {429} | set(range(500, 600))
MAX_FACTS_PER_LANGUAGE = 10_000
LANGUAGE_FALLBACKS = {
    "tl-fil": "fil-tl",
    "tl": "fil-tl",
    "ur": "urd-ud",
    "urd-ur": "urd-ud",
}


class FactFetcher:
    """Fetch facts from the Meowfacts API per language."""

    def __init__(
        self,
        base_url: str,
        timeout: int = 10,
        retries: int = 3,
        backoff_seconds: float = 1.5,
        facts_per_language: int = 5,
    ) -> None:
        """Initialize the fetcher.

        Args:
            base_url: Meowfacts API base URL.
            timeout: Request timeout in seconds.
            retries: Number of retry attempts for retryable HTTP status codes.
            backoff_seconds: Base backoff multiplier between retries.
            facts_per_language: Maximum facts to request per language.
        """
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.facts_per_language = max(
            1, min(facts_per_language, MAX_FACTS_PER_LANGUAGE)
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        self.supported_languages: list[str] | None = None

    async def fetch_all(
        self, target_languages: list[str] | None = None
    ) -> list[RawRecord]:
        """Fetch facts for the provided languages or discover them dynamically.

        Args:
            target_languages: Optional explicit language codes to pull. When not
                provided, cached discovery is used; if missing, languages are
                discovered from the API or fall back to ``['eng-us']``.

        Returns:
            All raw fact records fetched across languages.
        """
        languages = target_languages or self.supported_languages
        languages_source = "target" if target_languages else "cached"

        async with httpx.AsyncClient(
            base_url=self.base_url, timeout=self.timeout
        ) as client:
            if not languages:
                self.supported_languages = await self._discover_supported_languages(
                    client
                )
                if self.supported_languages:
                    languages = self.supported_languages
                    languages_source = "discovered"
                    self.logger.debug(
                        "Discovered supported languages from API: %s",
                        ",".join(sorted(languages)),
                    )
                else:
                    languages = ["eng-us"]
                    languages_source = "fallback"
                    self.logger.warning(
                        "Language discovery failed; defaulting to ['eng-us']"
                    )

            languages = sorted(dict.fromkeys(languages))
            if languages_source == "cached" and self.supported_languages:
                languages_source = "cached-discovered"
            self.logger.info(
                "Starting fetch for %d languages (source=%s)",
                len(languages),
                languages_source,
            )
            self.logger.debug("Target languages: %s", ",".join(languages))

            per_language_tasks = [
                self._fetch_language(client, lang_code) for lang_code in languages
            ]
            per_language_results = await asyncio.gather(
                *per_language_tasks, return_exceptions=True
            )

        collected_records: list[RawRecord] = []
        for lang_code, fetch_result in zip(
            languages, per_language_results, strict=False
        ):
            if isinstance(fetch_result, Exception):
                self.logger.error(
                    "Failed to fetch facts for %s: %s", lang_code, fetch_result
                )
                continue
            collected_records.extend(cast(list[RawRecord], fetch_result))

        self._log_fetch_summary(collected_records)
        return collected_records

    async def _fetch_language(
        self, client: httpx.AsyncClient, language_code: str
    ) -> list[RawRecord]:
        """Fetch facts for a single language with fallbacks and retries.

        Args:
            client: Shared HTTP client.
            language_code: Primary language code to attempt first.

        Returns:
            List of raw records for the first successful candidate language; empty
            if all attempts fail.
        """
        candidates = [language_code]
        fallback = LANGUAGE_FALLBACKS.get(language_code)
        if fallback:
            candidates.append(fallback)
        if "eng-us" not in candidates:
            candidates.append("eng-us")

        for candidate in candidates:
            query_params = {"lang": candidate, "count": self.facts_per_language}
            language_label = LANGUAGE_LABELS.get(candidate, candidate)

            self.logger.debug("Fetching facts for language=%s", language_label)
            self.logger.debug("GET params for %s: %s", candidate, query_params)

            for attempt_number in range(1, self.retries + 1):
                try:
                    self.logger.debug(
                        "Attempt %d/%d -> GET %s params=%s",
                        attempt_number,
                        self.retries,
                        self.base_url,
                        query_params,
                    )
                    response = await client.get("", params=query_params)
                    response.raise_for_status()
                    api_payload = response.json()
                    api_data_field = (
                        api_payload.get("data", [])
                        if isinstance(api_payload, dict)
                        else []
                    )
                    api_values = self._coerce_to_list(api_data_field)
                    records_for_language: list[RawRecord] = [
                        {
                            FactFields.LANGUAGE.value: candidate,
                            FactFields.FACT.value: str(fact_value),
                        }
                        for fact_value in api_values
                        if fact_value is not None
                    ]

                    if records_for_language:
                        self.logger.debug(
                            "Fetched %d facts for %s",
                            len(records_for_language),
                            language_label,
                        )
                    else:
                        self.logger.warning("No facts returned for %s", language_label)

                    self.logger.debug(
                        "Fetched %d facts for language=%s",
                        len(records_for_language),
                        candidate,
                    )
                    return records_for_language
                except httpx.HTTPStatusError as exc:
                    status_code = exc.response.status_code
                    if status_code == 400:
                        self.logger.warning(
                            "Attempt %d/%d returned 400 for language=%s",
                            attempt_number,
                            self.retries,
                            candidate,
                        )
                        break  # try next candidate
                    if status_code not in RETRYABLE_STATUSES:
                        self.logger.error(
                            "Non-retryable status=%s for language=%s",
                            status_code,
                            candidate,
                        )
                        break
                    self.logger.warning(
                        "Attempt %d/%d failed for language=%s with status=%s",
                        attempt_number,
                        self.retries,
                        candidate,
                        status_code,
                    )
                except httpx.RequestError as exc:
                    self.logger.warning(
                        "Attempt %d/%d request error for language=%s: %s",
                        attempt_number,
                        self.retries,
                        candidate,
                        exc,
                    )

                await asyncio.sleep(self.backoff_seconds * attempt_number)

        self.logger.error(
            "Failed to fetch facts for language=%s after trying %d candidates",
            language_code,
            len(candidates),
        )
        return []

    async def _discover_supported_languages(
        self, client: httpx.AsyncClient
    ) -> list[str] | None:
        """Discover supported languages from the API's validation error message.

        Args:
            client: HTTP client used for the probe request.

        Returns:
            Sorted list of language codes if discoverable; otherwise ``None``.
        """

        probe_params = {"lang": "__invalid__", "count": 1}
        try:
            response = await client.get("", params=probe_params)
            # Expecting a 400; if 200 just return None and use static list
            if response.status_code != 400:
                return None

            text = response.text
            marker = "valid languages are"
            if marker not in text:
                return None
            after = text.split(marker, 1)[1]
            parts = [p.strip() for p in after.split(",")]
            # Filter out empties and deduplicate
            langs = [p for p in parts if p]
            return sorted(dict.fromkeys(langs)) or None
        except Exception as exc:  # pragma: no cover - network guard
            self.logger.debug("Language discovery probe failed: %s", exc)
            return None

    @staticmethod
    def _coerce_to_list(raw: Any) -> list[Any]:
        """Normalize API "data" payload into a list."""
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            return [raw[key] for key in sorted(raw.keys(), key=str)]
        return []

    def _log_fetch_summary(self, records: list[RawRecord]) -> None:
        """Emit a summary table of fetched counts grouped by language label."""
        # Aggregate by human-friendly label to avoid duplicate rows when multiple codes map to the same language.
        counts_by_label = Counter(
            LANGUAGE_LABELS.get(
                rec[FactFields.LANGUAGE.value], rec[FactFields.LANGUAGE.value]
            )
            for rec in records
        )
        counts_by_code = Counter(rec[FactFields.LANGUAGE.value] for rec in records)
        label_to_codes: dict[str, list[tuple[str, int]]] = {}
        for code, count in counts_by_code.items():
            label = LANGUAGE_LABELS.get(code, code)
            label_to_codes.setdefault(label, []).append((code, count))
        table_rows = [
            [label, counts_by_label[label]] for label in sorted(counts_by_label.keys())
        ]
        summary_table = tabulate(
            table_rows, headers=["Language", "Fetched"], tablefmt="github"
        )
        self.logger.info("Fetch summary by language:\n%s", summary_table)
        doubled_labels = {
            label: codes for label, codes in label_to_codes.items() if len(codes) > 1
        }
        if doubled_labels:
            details = "; ".join(
                f"{label}: "
                + ", ".join(f"{code}={count}" for code, count in sorted(codes))
                for label, codes in sorted(doubled_labels.items())
            )
            self.logger.info(
                "Note: counts include multiple codes per language: %s", details
            )


class FactCleaner:
    """Normalize, deduplicate, and enrich fact records."""

    def normalize(
        self, records: Iterable[RawRecord]
    ) -> tuple[list[CleanRecord], pd.DataFrame]:
        """Clean and deduplicate raw records.

        Args:
            records: Raw records to normalize.

        Returns:
            Tuple of cleaned records and the intermediate DataFrame for inspection.
        """
        frame = pd.DataFrame(list(records))
        if frame.empty:
            return [], pd.DataFrame()

        frame[FactFields.FACT] = frame[FactFields.FACT].astype(str).str.strip()
        frame[FactFields.LANGUAGE] = frame[FactFields.LANGUAGE].astype(str)

        frame = frame[frame[FactFields.FACT].str.len() > 0]
        # Deduplicate per language so different API codes can retain the same text.
        frame[FactFields.HASH] = frame.apply(
            lambda row: self._hash_text(
                f"{row[FactFields.LANGUAGE]}::{row[FactFields.FACT]}"
            ),
            axis=1,
        )
        frame = frame.drop_duplicates(subset=[FactFields.HASH]).reset_index(drop=True)
        frame[FactFields.LENGTH] = frame[FactFields.FACT].apply(len)

        record_dicts = cast(
            list[dict[str, Any]],
            frame[
                [
                    FactFields.LANGUAGE,
                    FactFields.FACT,
                    FactFields.LENGTH,
                ]
            ].to_dict(orient="records"),
        )

        cleaned: list[CleanRecord] = [
            {
                FactFields.LANGUAGE.value: str(item[FactFields.LANGUAGE.value]),
                FactFields.FACT.value: str(item[FactFields.FACT.value]),
                FactFields.LENGTH.value: int(item[FactFields.LENGTH.value]),
            }
            for item in record_dicts
        ]
        return cleaned, frame

    @staticmethod
    def _hash_text(text: str) -> str:
        """Create a deterministic hash for deduplication."""
        return hashlib.sha256(text.encode("utf-8")).hexdigest()


class FactPublisher:
    """Persist cleaned or raw facts to disk."""

    def __init__(self, data_path: Path) -> None:
        """Create a publisher.

        Args:
            data_path: Default output path for cleaned facts.
        """
        self.data_path = data_path
        self.logger = logging.getLogger(self.__class__.__name__)

    def write_json(
        self,
        records: Iterable[Mapping[str, Any]],
        path: Path | None = None,
        dataset_label: str = "cleaned facts",
    ) -> Path:
        """Write records to JSON on disk.

        Args:
            records: Iterable of mapping records to serialize.
            path: Optional path override; defaults to the configured data path.
            dataset_label: Label used in the info log to disambiguate outputs.

        Returns:
            Path to the written JSON file.
        """
        target = path or self.data_path
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as fp:
            json.dump(list(records), fp, indent=2, ensure_ascii=False)
        self.logger.info("Saved %s dataset", dataset_label)
        self.logger.debug("Wrote JSON to %s", target)
        return target

    def write_raw_json(
        self, records: Iterable[Mapping[str, Any]], path: Path | None = None
    ) -> Path:
        """Convenience wrapper to persist raw facts with a clear label."""
        return self.write_json(records, path=path, dataset_label="raw facts")


__all__ = ["FactFetcher", "FactCleaner", "FactPublisher"]
