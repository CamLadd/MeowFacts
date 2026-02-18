import json
import tempfile
from pathlib import Path

from src.core.services import FactCleaner, FactFetcher, FactPublisher
from src.core.settings import FactFields
from typing import Any


class TestFactCleaner:
    def test_normalize_dedupes_and_hashes(self) -> None:
        cleaner = FactCleaner()
        raw: Any = [
            {"language": "eng-us", "fact": " cat "},
            {"language": "eng-us", "fact": "cat"},
            {"language": "esp-es", "fact": ""},
        ]

        cleaned, frame = cleaner.normalize(raw)

        assert len(cleaned) == 1
        assert cleaned[0][FactFields.FACT.value] == "cat"
        assert cleaned[0][FactFields.LENGTH.value] == 3
        assert frame is not None and not frame.empty

    def test_normalize_empty(self) -> None:
        cleaner = FactCleaner()

        cleaned, frame = cleaner.normalize([])

        assert cleaned == []
        assert frame.empty


class TestFactFetcher:
    def test_coerce_to_list_handles_dict_and_other(self) -> None:
        fetcher = FactFetcher(base_url="https://example.com")

        assert fetcher._coerce_to_list([1, 2]) == [1, 2]
        assert fetcher._coerce_to_list({"b": 2, "a": 1}) == [1, 2]
        assert fetcher._coerce_to_list("ignored") == []


class TestFactPublisher:
    def test_write_json_creates_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "meowfacts.json"
            publisher = FactPublisher(data_path=target)

            path = publisher.write_json(
                [
                    {
                        FactFields.LANGUAGE.value: "eng-us",
                        FactFields.FACT.value: "cat",
                        FactFields.LENGTH.value: 3,
                    }
                ]
            )

            assert path == target
            assert target.exists()
            content = json.loads(target.read_text(encoding="utf-8"))
            assert content[0][FactFields.FACT.value] == "cat"
            assert FactFields.HASH.value not in content[0]
