"""
Docling GPU Pipeline for DocuPulse

Finds files needing Docling extraction, downloads from Azure Blob Storage,
processes with GPU-accelerated Docling (OCR + table extraction), and writes
results back to the file_extractions table.

Integrates directly with the main DocuPulse database schema:
  - files, folders, user_workspaces, file_extractions
"""

import os
import sys
import json
import hashlib
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import psycopg2
from psycopg2.extras import RealDictCursor
from azure.storage.blob import BlobServiceClient
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.document_converter import PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions, TesseractOcrOptions

# ─── Config ───────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ["DATABASE_URL"]
AZURE_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
WORKSPACE_ID = os.environ.get("WORKSPACE_ID")  # Optional: limit to one workspace

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("docling-pipeline")

# ─── SQL ──────────────────────────────────────────────────────────────────────

# Find files that need Docling processing:
#   - Have a file_extraction with strategy='docling' and status='PENDING'
#   - OR have no docling extraction at all (auto-enqueue)
FETCH_PENDING = """
    SELECT
        f.id                  AS file_id,
        f.blob_name,
        COALESCE(f.original_blob_name, f.blob_name) AS download_blob_name,
        fo.workspace_id,
        (
            SELECT uw.user_id::text
            FROM user_workspaces uw
            WHERE uw.workspace_id = fo.workspace_id
            LIMIT 1
        ) AS user_id,
        fe.id                 AS extraction_id   -- NULL when no extraction row yet
    FROM files f
    JOIN folders fo ON fo.id = f.folder_id
    LEFT JOIN file_extractions fe
        ON fe.file_id = f.id AND fe.strategy_name = 'docling'
    WHERE (fe.id IS NULL OR fe.status = 'PENDING')
      {workspace_filter}
    ORDER BY f.created_at ASC
    LIMIT %s;
"""

INSERT_EXTRACTION = """
    INSERT INTO file_extractions (id, file_id, strategy_name, status, page_numbers, created_at, updated_at)
    VALUES (%s, %s, 'docling', 'PENDING', '["all"]'::jsonb, NOW(), NOW())
    RETURNING id;
"""

SAVE_RESULT = """
    UPDATE file_extractions
    SET status              = 'EXTRACTED',
        raw_extracted_data  = %s::jsonb,
        extraction_metadata = %s::jsonb,
        updated_at          = NOW()
    WHERE id = %s;
"""

MARK_FAILED = """
    UPDATE file_extractions
    SET status              = 'ERROR',
        extraction_metadata = COALESCE(extraction_metadata, '{}'::jsonb) || %s::jsonb,
        updated_at          = NOW()
    WHERE id = %s;
"""


# ─── Helpers ──────────────────────────────────────────────────────────────────

def generate_container_name(user_id: str, workspace_id: str) -> str:
    """Mirror of AzureBlobStorageService.generate_container_name from the main app."""
    combined = f"{user_id}_{workspace_id}"
    short_hash = hashlib.md5(combined.encode()).hexdigest()[:20]
    return f"files{short_hash}".lower()


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def get_blob_service():
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)


def create_converter():
    """Create a Docling converter with OCR + table extraction enabled."""
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.do_table_structure = True
    pipeline_options.ocr_options = TesseractOcrOptions()

    return DocumentConverter(
        allowed_formats=[
            InputFormat.PDF,
            InputFormat.DOCX,
            InputFormat.PPTX,
            InputFormat.XLSX,
            InputFormat.IMAGE,
            InputFormat.HTML,
        ],
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
        },
    )


def download_blob(blob_service, container_name, blob_name, local_path):
    container_client = blob_service.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    with open(local_path, "wb") as f:
        stream = blob_client.download_blob()
        stream.readinto(f)


# ─── Main pipeline ───────────────────────────────────────────────────────────

def run_pipeline():
    log.info("=" * 60)
    log.info("Docling GPU Pipeline — Starting")
    log.info("=" * 60)

    conn = get_db()
    blob_service = get_blob_service()
    converter = create_converter()

    # Build query with optional workspace filter
    workspace_filter = ""
    query_params = [BATCH_SIZE]
    if WORKSPACE_ID:
        workspace_filter = "AND fo.workspace_id = %s"
        query_params = [WORKSPACE_ID, BATCH_SIZE]
        log.info(f"Filtering to workspace: {WORKSPACE_ID}")

    query = FETCH_PENDING.format(workspace_filter=workspace_filter)
    # Fix param order: workspace_id before batch_size (LIMIT is last)
    if WORKSPACE_ID:
        final_params = (WORKSPACE_ID, BATCH_SIZE)
    else:
        final_params = (BATCH_SIZE,)

    with conn.cursor() as cur:
        cur.execute(query, final_params)
        jobs = cur.fetchall()

    if not jobs:
        log.info("No files to process. Exiting.")
        conn.close()
        return

    log.info(f"Found {len(jobs)} file(s) to process.")

    succeeded = 0
    failed = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        for job in jobs:
            file_id = job["file_id"]
            blob_name = job["download_blob_name"]
            user_id = job["user_id"]
            workspace_id = str(job["workspace_id"])
            extraction_id = job["extraction_id"]

            # If no user found for workspace, skip
            if not user_id:
                log.error(f"[{file_id}] No user found for workspace {workspace_id}. Skipping.")
                if extraction_id:
                    with conn.cursor() as cur:
                        cur.execute(MARK_FAILED, (
                            json.dumps({"error": "No user found for workspace"}),
                            extraction_id,
                        ))
                    conn.commit()
                failed += 1
                continue

            # Auto-create extraction record if it doesn't exist
            if extraction_id is None:
                new_id = str(uuid4())
                with conn.cursor() as cur:
                    cur.execute(INSERT_EXTRACTION, (new_id, file_id))
                    extraction_id = cur.fetchone()["id"]
                conn.commit()
                log.info(f"[{file_id}] Created extraction record {extraction_id}")

            container_name = generate_container_name(user_id, workspace_id)
            file_ext = Path(blob_name).suffix or ".pdf"
            local_file = Path(tmpdir) / f"{file_id}{file_ext}"

            try:
                # Download
                log.info(f"[{extraction_id}] Downloading {container_name}/{blob_name}...")
                download_blob(blob_service, container_name, blob_name, str(local_file))
                file_size = local_file.stat().st_size
                log.info(f"[{extraction_id}] Downloaded {file_size:,} bytes")

                # Convert with Docling
                log.info(f"[{extraction_id}] Processing with Docling...")
                result = converter.convert(str(local_file))

                markdown = result.document.export_to_markdown()
                text = (
                    result.document.export_to_text()
                    if hasattr(result.document, "export_to_text")
                    else ""
                )

                num_pages = (
                    len(result.document.pages)
                    if hasattr(result.document, "pages")
                    else None
                )
                num_tables = (
                    len(result.document.tables)
                    if hasattr(result.document, "tables")
                    else 0
                )

                # Build page-level lines for compatibility with existing chunking
                pages = []
                if text:
                    for i, page_text in enumerate(_split_by_pages(text, num_pages), start=1):
                        lines = [
                            {"content": line, "polygon": None}
                            for line in page_text.split("\n")
                            if line.strip()
                        ]
                        if lines:
                            pages.append({"page_number": i, "lines": lines})

                raw_data = {
                    "pages": pages,
                    "content_markdown": markdown,
                    "content_text": text,
                    "num_pages": num_pages,
                    "num_tables": num_tables,
                    "page_numbers": ["all"],
                    "extraction_model": "docling",
                    "extraction_id": str(extraction_id),
                }

                metadata = {
                    "extraction_model": "docling",
                    "processing_method": "docling_gpu",
                    "ocr_used": True,
                    "gpu_accelerated": True,
                    "content_length": len(markdown),
                    "num_pages": num_pages,
                    "num_tables": num_tables,
                }

                with conn.cursor() as cur:
                    cur.execute(SAVE_RESULT, (
                        json.dumps(raw_data),
                        json.dumps(metadata),
                        extraction_id,
                    ))
                conn.commit()

                succeeded += 1
                log.info(f"[{extraction_id}] Done — {len(markdown):,} chars, {num_pages} pages, {num_tables} tables")

            except Exception as e:
                log.error(f"[{extraction_id}] FAILED: {e}", exc_info=True)
                try:
                    with conn.cursor() as cur:
                        cur.execute(MARK_FAILED, (
                            json.dumps({"error": str(e)[:2000]}),
                            extraction_id,
                        ))
                    conn.commit()
                except Exception:
                    log.error(f"[{extraction_id}] Could not mark as failed in DB", exc_info=True)
                failed += 1
            finally:
                # Clean up downloaded file to save disk on large batches
                if local_file.exists():
                    local_file.unlink()

    log.info("=" * 60)
    log.info(f"Pipeline complete: {succeeded} succeeded, {failed} failed out of {len(jobs)}")
    log.info("=" * 60)

    conn.close()


def _split_by_pages(text: str, num_pages: int | None) -> list[str]:
    """Best-effort page split. Falls back to single page if no page info."""
    if not num_pages or num_pages <= 1:
        return [text]
    # Docling sometimes inserts form-feed characters between pages
    parts = text.split("\f")
    if len(parts) >= num_pages:
        return parts[:num_pages]
    return [text]


if __name__ == "__main__":
    run_pipeline()
