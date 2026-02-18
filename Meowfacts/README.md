# Easiest One-Off Pull (standalone)

If you just want a quick JSON of all languages without using the pipeline, use the standalone script in `simple_version` (stdlib only):

```bash
cd simple_version
python simple_json_extractor.py --per-language-count 10000 --output meowfacts_all_languages.json
```

- Discovers all languages exposed by the API and fetches up to the requested count per language.
- Writes the JSON next to the script (in the `simple_version` directory).




# Meowfacts Pipeline

Fetch, clean, and publish cat facts from the Meowfacts API with minimal setup. Languages are discovered dynamically from the API (falls back to `eng-us` if discovery fails).

## Quick Start
1. Create a virtualenv and install deps:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. Run the default daily workflow:
   ```bash
   python main.py
   ```
3. Pick a workflow via env (defaults to `DAILY_ELT`):
   ```bash
   MEOW_WORKFLOW_NAME=TEST_RUN python main.py
   ```

## Built-In Workflows
- `DAILY_ELT`: fetch → clean → publish JSON.
- `TEST_RUN`: fetch → clean (no publish).
- `TRANSFORM_ONLY`: clean from an existing raw JSON file.
- `PUBLISH_ONLY`: publish pre-cleaned data (errors if none loaded).

## Configuration
Env vars use the `MEOW_` prefix. Common options:
- `WORKFLOW_NAME`: which workflow to run.
- `TARGET_LANGUAGES`: comma-separated list (leave empty to auto-discover).
- `FACTS_PER_LANGUAGE`: max facts requested per language (default 10000).
- `BASE_URL`: Meowfacts API base (default `https://meowfacts.herokuapp.com/`).
- `DATA_PATH`: output for cleaned facts (default `output/meowfacts.json`).
- `RAW_DATA_PATH`: output for raw facts (default `output/raw_meowfacts.json`).
- `LOG_PATH`: log file path (default `log/meowfacts.log`).

## Programmatic Use
```python
from src.core import workflows
from src.core.settings import Settings

summary = workflows.run_sync("DAILY_ELT", settings=Settings())
print(summary)
```

To pull raw facts directly:
```python
from src.core import workflows
raw = workflows.pull(["eng-us", "esp-es"])
```

## Outputs
- Cleaned facts: `output/meowfacts.json`
- Raw facts: `output/raw_meowfacts.json`
- Logs: `log/meowfacts.log`

## Testing & Linting
- Tests: `python -m unittest`
- Lint: `.venv\\Scripts\\python.exe -m ruff check .`

## Run Daily (no code changes)

Use the provided runner scripts so you can schedule a single entrypoint (replace `/path/to/Meowfacts` with your clone directory):

- Windows Task Scheduler
   1) Open Task Scheduler → “Create Basic Task…”.
   2) Trigger: Daily at your chosen time.
   3) Action: “Start a program”.
       - Program/script: `C:\\Windows\\System32\\cmd.exe`
       - Add arguments: `/c "C:\\path\\to\\Meowfacts\\scripts\\run_meowfacts.bat"`
       - Start in: `C:\\path\\to\\Meowfacts`
   4) Finish, then in Properties you can check “Run whether user is logged on or not”.

- Cron (Linux/macOS)
   ```cron
   0 6 * * * /path/to/Meowfacts/scripts/run_meowfacts.sh >> /path/to/Meowfacts/log/cron.log 2>&1
   ```

Env vars (optional): set `MEOW_WORKFLOW_NAME`, `MEOW_TARGET_LANGUAGES`, etc., inside the script or in the scheduler definition. The scripts already activate the local `.venv` before running `python main.py`. All paths should be adjusted to your local clone.


