# Docling GPU Pipeline

GPU-accelerated document extraction using [Docling](https://github.com/DS4SD/docling).
One command from your laptop — Azure spins up a GPU, runs the pipeline, tears it down.

## How it works

```
Your laptop                              Azure
───────────                              ─────
./run.sh
  ├─ az acr build ──────────────────►  Builds Docker image in cloud
  ├─ az container create ───────────►  Spins up T4 GPU container
  ├─ az container attach ◄──────────   Streams logs to your terminal
  └─ az container delete ───────────►  Deletes container, billing stops
```

The pipeline inside the container:
1. Queries `file_extractions` for pending jobs (configurable strategy)
2. Auto-discovers files without an extraction yet
3. Downloads each file from Azure Blob Storage
4. Optionally trims PDFs to first N pages
5. Processes with Docling (GPU OCR + table extraction)
6. Writes results to `file_extractions.raw_extracted_data`

**Cost:** ~$0.36/hour on T4. A batch of 50 docs takes a few minutes.

## Setup

```bash
cp .env.example .env
# Fill in DATABASE_URL and AZURE_STORAGE_CONNECTION_STRING
# Fill in RESOURCE_GROUP and ACR_NAME for Azure
```

That's it. Everything else is handled by `run.sh`.

## Run on Azure

```bash
./run.sh
```

First run creates the Azure resource group and container registry automatically.
Subsequent runs reuse them and only rebuild the image if code changed.

## CLI Usage

The pipeline supports both environment variables and CLI arguments. CLI args override env vars.

```bash
python pipeline.py                          # uses env vars / defaults
python pipeline.py --batch-size 10          # override batch size
python pipeline.py --strategy docling       # filter by strategy
python pipeline.py --strategy ""            # no strategy filter (all)
python pipeline.py --max-pages 5            # first 5 pages only
python pipeline.py --workspace <uuid>       # limit to workspace
python pipeline.py --no-gpu                 # run on CPU only
python pipeline.py --dry-run                # show what would be processed, don't run
python pipeline.py --help                   # full help text
python pipeline.py --version                # show version
```

### With Docker

```bash
docker run docling-pipeline                                # defaults
docker run docling-pipeline --batch-size 10 --max-pages 5  # with args
```

## Local Docker

For local development with GPU support, use docker-compose:

```bash
cp .env.example .env
# Fill in your environment variables

docker compose up --build
```

This builds the image locally and runs the pipeline with your `.env` file.

## Files

```
├── pipeline.py          # DB → Blob → Docling → DB (with CLI)
├── Dockerfile           # CUDA 12.1 + Tesseract OCR + Docling
├── docker-compose.yml   # Local Docker dev with GPU
├── requirements.txt     # Python deps
├── .env.example         # Config template
└── run.sh               # Azure ACI one-command deploy
```

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string (same DB as DocuPulse) |
| `AZURE_STORAGE_CONNECTION_STRING` | Yes | — | Azure Blob Storage connection string |
| `RESOURCE_GROUP` | Yes | — | Azure resource group name |
| `ACR_NAME` | Yes | — | Azure Container Registry name (globally unique, lowercase) |
| `BATCH_SIZE` | No | `50` | Max files per run |
| `WORKSPACE_ID` | No | — | Limit to one workspace UUID |
| `STRATEGY_NAME` | No | `docling` | Strategy name filter. Empty string = all strategies |
| `MAX_PAGES` | No | `0` | Extract only first N pages from PDFs. 0 = all pages |
| `USE_GPU` | No | `true` | Set to `false` to run on CPU only |
| `LOG_LEVEL` | No | `INFO` | Python log level |
| `GPU_SKU` | No | `T4` | Azure GPU SKU (T4, V100, or K80) |

## Graceful shutdown

The pipeline can be safely interrupted at any time with `Ctrl+C` (or `docker stop` / `SIGTERM`).

- The current file finishes processing and its result is committed to the DB
- Remaining files stay as `PENDING` and will be picked up on the next run
- A summary of completed work is always printed before exit
- No data corruption or orphaned state

## Checking results

```sql
SELECT f.blob_name, fe.status, fe.extraction_metadata
FROM file_extractions fe
JOIN files f ON f.id = fe.file_id
WHERE fe.strategy_name = 'docling';
```

## Reprocessing failures

```sql
UPDATE file_extractions SET status = 'PENDING'
WHERE strategy_name = 'docling' AND status = 'ERROR';
```

Then `./run.sh` again.
