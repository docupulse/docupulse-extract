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
1. Queries `file_extractions` for pending `docling` jobs
2. Auto-discovers files without a docling extraction yet
3. Downloads each file from Azure Blob Storage
4. Processes with Docling (GPU OCR + table extraction)
5. Writes results to `file_extractions.raw_extracted_data`

**Cost:** ~$0.36/hour on T4. A batch of 50 docs takes a few minutes.

## Setup

```bash
cp .env.example .env
# Fill in DATABASE_URL and AZURE_STORAGE_CONNECTION_STRING
# Fill in RESOURCE_GROUP and ACR_NAME for Azure
```

That's it. Everything else is handled by `run.sh`.

## Run

```bash
./run.sh
```

First run creates the Azure resource group and container registry automatically.
Subsequent runs reuse them and only rebuild the image if code changed.

## Files

```
├── pipeline.py        # DB → Blob → Docling → DB
├── Dockerfile         # CUDA 12.1 + Tesseract OCR + Docling
├── requirements.txt   # Python deps
├── .env.example       # Config template
└── run.sh             # The one command
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes | PostgreSQL connection string (same DB as DocuPulse) |
| `AZURE_STORAGE_CONNECTION_STRING` | Yes | Azure Blob Storage connection string |
| `RESOURCE_GROUP` | Yes | Azure resource group name |
| `ACR_NAME` | Yes | Azure Container Registry name (globally unique, lowercase) |
| `BATCH_SIZE` | No | Max files per run (default: 50) |
| `WORKSPACE_ID` | No | Limit to one workspace UUID |
| `GPU_SKU` | No | T4 (default), V100, or K80 |

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
