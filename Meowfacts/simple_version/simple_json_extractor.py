"""Standalone script to pull all available Meowfacts into a JSON file."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Iterable
from pathlib import Path

BASE_URL = "https://meowfacts.herokuapp.com/"


def _coerce_to_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return [raw[key] for key in sorted(raw.keys(), key=str)]
    if raw is None:
        return []
    return [raw]


def discover_languages() -> list[str]:
    """Discover supported languages from the API's validation error text."""

    params = urllib.parse.urlencode({"lang": "__invalid__", "count": 1})
    url = f"{BASE_URL}?{params}"
    text = ""
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            text = resp.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as exc:
        # API returns 400 with the supported language list in the body.
        try:
            text = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            return ["eng-us"]
    except urllib.error.URLError:
        return ["eng-us"]

    marker = "valid languages are"
    if marker not in text:
        return ["eng-us"]

    after = text.split(marker, 1)[1]
    parts = [p.strip() for p in after.split(",")]
    langs = [p for p in parts if p]
    return sorted(dict.fromkeys(langs)) or ["eng-us"]


def fetch_facts(language: str, count: int) -> list[dict[str, str]]:
    params = urllib.parse.urlencode({"lang": language, "count": count})
    url = f"{BASE_URL}?{params}"
    with urllib.request.urlopen(url, timeout=15) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Unexpected status {resp.status} for {url}")
        payload = json.load(resp)

    data_field = payload.get("data", []) if isinstance(payload, dict) else []
    facts: Iterable[Any] = _coerce_to_list(data_field)
    return [
        {"language": language, "fact": str(item)}
        for item in facts
        if item is not None
    ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch maximum Meowfacts for all languages to JSON",
    )
    parser.add_argument(
        "--per-language-count",
        type=int,
        default=10_000,
        help="Facts to request per language (default: 10000)",
    )
    parser.add_argument(
        "--output",
        default="meowfacts_all_languages.json",
        help="Output JSON file (default: meowfacts_all_languages.json)",
    )
    args = parser.parse_args()

    per_language_count = max(1, args.per_language_count)
    languages = discover_languages()
    print(f"Discovered languages: {', '.join(languages)}")

    output_path = Path(__file__).resolve().parent / args.output

    all_facts: list[dict[str, str]] = []
    for lang in languages:
        try:
            facts = fetch_facts(lang, per_language_count)
            print(f"Fetched {len(facts)} facts for {lang}")
            all_facts.extend(facts)
        except Exception as exc:  # pragma: no cover - simple CLI guard
            print(f"Failed to fetch {lang}: {exc}", file=sys.stderr)

    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(all_facts, fp, indent=2, ensure_ascii=False)

    print(
        f"Wrote {len(all_facts)} facts across {len(languages)} languages to {output_path}"
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
