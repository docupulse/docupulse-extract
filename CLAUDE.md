# DocuPulse Extract — Implementation Plan

## Goal

Transform the existing `pipeline.py` into a polished, user-friendly, Docker-ready standalone extraction pipeline that:
- Fetches files batch-wise from Azure Blob Storage
- Processes them with GPU-accelerated Docling
- Writes results to `file_extractions`
- Is fully configurable via env vars AND CLI args
- Is ready for Azure Container Instance deployment via Docker

---

## Current State

- `pipeline.py` — working but with hardcoded `'docling'` strategy name, no page limit, no CLI interface
- `Dockerfile` — CUDA-based, functional
- `run.sh` — Azure ACI deployment script
- `.env.example` / `requirements.txt` / `README.md` — basics in place

---

## Changes

### 1. Make `strategy_name` configurable (remove hardcoded `'docling'`)

**File:** `pipeline.py`

- Add env var `STRATEGY_NAME` (default: `docling`)
- Add CLI arg `--strategy` / `-s`
- Replace every hardcoded `'docling'` string in SQL and metadata with the configured value
- When `STRATEGY_NAME` is empty/unset → fetch ALL pending extractions regardless of strategy (no filter on `strategy_name`)
- Update `FETCH_PENDING`, `INSERT_EXTRACTION`, `SAVE_RESULT` queries to use a parameter instead of literal `'docling'`

### 2. Add `MAX_PAGES` env var — extract only first X pages

**File:** `pipeline.py`

- Add env var `MAX_PAGES` (default: `0` meaning all pages)
- Add CLI arg `--max-pages` / `-p`
- After downloading a file, if `MAX_PAGES > 0` and the file is a PDF:
  - Use Docling's page-range support or pre-trim the PDF before conversion
  - Store the actual pages extracted in the `page_numbers` field (e.g. `[1,2,3]` instead of `["all"]`)
- Update metadata to include `max_pages_setting` so it's traceable

### 3. Add user-friendly CLI with `argparse`

**File:** `pipeline.py`

Add argparse so the user can run:
```bash
python pipeline.py                          # uses env vars / defaults
python pipeline.py --batch-size 10          # override batch size
python pipeline.py --strategy docling       # filter by strategy
python pipeline.py --strategy ""            # no strategy filter (all)
python pipeline.py --max-pages 5            # first 5 pages only
python pipeline.py --workspace <uuid>       # limit to workspace
python pipeline.py --dry-run                # show what would be processed, don't run
python pipeline.py --help                   # full help text
```

- CLI args override env vars (env vars are the fallback defaults)
- Add `--dry-run` flag: queries and prints the files that would be processed, then exits
- Add `--version` flag
- Pretty banner on start showing config summary (batch size, strategy, max pages, workspace filter)

### 4. Improve progress output & logging

**File:** `pipeline.py`

- Add a progress counter per file: `[3/50] Processing file abc123...`
- Add timing per file and total elapsed time at the end
- Add a summary table at the end: succeeded / failed / skipped with file IDs
- Use colored output when running in a terminal (detect `sys.stdout.isatty()`)

### 5. Update `.env.example` with new variables

**File:** `.env.example`

Add:
```env
STRATEGY_NAME=docling
MAX_PAGES=0
```

### 6. Update `Dockerfile` for production readiness

**File:** `Dockerfile`

- Add `LABEL` metadata (maintainer, description, version)
- Add `HEALTHCHECK` instruction
- Pin Python version more explicitly
- Add the new env vars as `ENV` defaults
- Use `ENTRYPOINT` + `CMD` pattern so CLI args can be appended at `docker run`

```dockerfile
ENTRYPOINT ["python3", "pipeline.py"]
CMD []
```

This allows:
```bash
docker run docling-pipeline                              # defaults
docker run docling-pipeline --batch-size 10 --max-pages 5  # with args
```

### 7. Update `run.sh` to pass new env vars

**File:** `run.sh`

- Add `STRATEGY_NAME` and `MAX_PAGES` to the `az container create` environment variables
- Keep backward compatibility (both have safe defaults)

### 8. Add `docker-compose.yml` for local development

**New file:** `docker-compose.yml`

Simple compose file for local Docker testing (without Azure ACI):
```yaml
services:
  pipeline:
    build: .
    env_file: .env
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
```

### 9. Update `README.md`

**File:** `README.md`

- Document all new CLI args
- Document new env vars (`STRATEGY_NAME`, `MAX_PAGES`)
- Add "Local Docker" section (using docker-compose)
- Add "CLI Usage" section with examples
- Update environment variables table

---

## File Change Summary

| File | Action |
|------|--------|
| `pipeline.py` | Major refactor — argparse, configurable strategy, max pages, progress, dry-run |
| `.env.example` | Add `STRATEGY_NAME`, `MAX_PAGES` |
| `Dockerfile` | ENTRYPOINT pattern, labels, new ENV defaults |
| `run.sh` | Pass new env vars to ACI |
| `docker-compose.yml` | **New** — local Docker dev |
| `README.md` | Full rewrite with new features |

---

## Implementation Order

1. `pipeline.py` — all logic changes (strategy, max pages, argparse, progress)
2. `.env.example` — new vars
3. `Dockerfile` — ENTRYPOINT + labels
4. `run.sh` — pass new vars
5. `docker-compose.yml` — new file
6. `README.md` — documentation
