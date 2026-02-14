"""
Docling GPU Pipeline for DocuPulse

Finds files needing extraction, downloads from Azure Blob Storage,
processes with GPU-accelerated Docling (OCR + table extraction), and writes
results back to the file_extractions table.

Integrates directly with the main DocuPulse database schema:
  - files, folders, user_workspaces, file_extractions
"""

import argparse
import os
import signal
import sys
import json
import hashlib
import logging
import tempfile
import time
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import RealDictCursor
from azure.storage.blob import BlobServiceClient
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.document_converter import PdfFormatOption
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    ThreadedPdfPipelineOptions,
    EasyOcrOptions,
    AcceleratorOptions,
)

__version__ = "1.0.0"

# ─── Color helpers ────────────────────────────────────────────────────────────

_USE_COLOR = sys.stdout.isatty()


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def _bold(t):   return _c("1", t)
def _green(t):  return _c("32", t)
def _red(t):    return _c("31", t)
def _yellow(t): return _c("33", t)
def _cyan(t):   return _c("36", t)
def _dim(t):    return _c("2", t)

# ─── Graceful shutdown ────────────────────────────────────────────────────────

_shutdown_requested = False


def _request_shutdown(signum, _frame):
    global _shutdown_requested
    _shutdown_requested = True
    name = signal.Signals(signum).name
    print(f"\n  {_yellow('Received ' + name + ' — finishing current file, then stopping...')}")


signal.signal(signal.SIGINT, _request_shutdown)
if hasattr(signal, "SIGTERM"):
    signal.signal(signal.SIGTERM, _request_shutdown)

# ─── SQL ──────────────────────────────────────────────────────────────────────

# Find files that need processing:
#   - Have a file_extraction with the given strategy and status='PENDING'
#   - OR have no extraction row for that strategy yet (auto-enqueue)
# When strategy is empty, match ALL strategies (no filter on strategy_name).
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
        ON fe.file_id = f.id
    WHERE f.status = %s
      AND f.filetype = ANY(%s)
      {workspace_filter}
    ORDER BY f.created_at ASC
    LIMIT %s;
"""

INSERT_EXTRACTION = """
    INSERT INTO file_extractions (id, file_id, strategy_name, status, page_numbers, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s::jsonb, NOW(), NOW())
    RETURNING id;
"""

SAVE_RESULT = """
    UPDATE file_extractions
    SET status              = %s,
        raw_extracted_data  = %s::jsonb,
        extraction_metadata = %s::jsonb,
        updated_at          = NOW()
    WHERE id = %s;
"""

UPDATE_FILE_STATUS = """
    UPDATE files
    SET status     = %s,
        updated_at = NOW()
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
    url = os.environ.get("DATABASE_URL")
    if not url:
        # Build from individual fields (Azure-style)
        host = os.environ["DB_HOST"]
        port = os.environ.get("DB_PORT", "5432")
        name = os.environ["DB_NAME"]
        user = os.environ["DB_USER"]
        password = os.environ["DB_PASSWORD"]
        url = f"postgresql://{user}:{password}@{host}:{port}/{name}?sslmode=require"
    return psycopg2.connect(url, cursor_factory=RealDictCursor)


def get_blob_service():
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        # Build from individual fields
        account = os.environ["AZURE_ACCOUNT_NAME"]
        key = os.environ["AZURE_ACCOUNT_KEY"]
        conn_str = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={account};"
            f"AccountKey={key};"
            f"EndpointSuffix=core.windows.net"
        )
    return BlobServiceClient.from_connection_string(conn_str)


def create_converter(use_gpu: bool = True, ocr_batch: int = 64, layout_batch: int = 64, table_batch: int = 4, lite: bool = False):
    """Create a Docling converter.

    lite=False (default): full pipeline — OCR, table structure, layout analysis
    lite=True:            fast text-only extraction — no OCR, no table structure
    """
    if lite:
        pipeline_options = PdfPipelineOptions(
            do_ocr=False,
            do_table_structure=False,
        )
    else:
        pipeline_options = ThreadedPdfPipelineOptions(
            do_ocr=True,
            do_table_structure=True,
            ocr_options=EasyOcrOptions(),
            ocr_batch_size=ocr_batch,
            layout_batch_size=layout_batch,
            table_batch_size=table_batch,
        )
    pipeline_options.accelerator_options = AcceleratorOptions(
        num_threads=4,
        device="cuda" if use_gpu else "cpu",
    )

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


def trim_pdf_pages(input_path: str, max_pages: int) -> str:
    """Trim a PDF to the first max_pages pages. Returns path to trimmed file."""
    from pypdf import PdfReader, PdfWriter

    reader = PdfReader(input_path)
    if len(reader.pages) <= max_pages:
        return input_path  # No trimming needed

    writer = PdfWriter()
    for i in range(min(max_pages, len(reader.pages))):
        writer.add_page(reader.pages[i])

    trimmed_path = input_path + ".trimmed.pdf"
    with open(trimmed_path, "wb") as f:
        writer.write(f)
    return trimmed_path


def extract_text_lite(file_path: str, max_pages: int = 0):
    """Fast text extraction using pypdf — no GPU, no layout analysis, instant."""
    from pypdf import PdfReader

    reader = PdfReader(file_path)
    total_pages = len(reader.pages)
    pages_to_read = min(max_pages, total_pages) if max_pages > 0 else total_pages

    page_texts = []
    for i in range(pages_to_read):
        raw = reader.pages[i].extract_text() or ""
        page_texts.append(raw.replace("\x00", ""))

    text = "\n\n".join(page_texts)
    pages = []
    for i, page_text in enumerate(page_texts, start=1):
        lines = [
            {"content": line, "polygon": None}
            for line in page_text.split("\n")
            if line.strip()
        ]
        if lines:
            pages.append({"page_number": i, "lines": lines})

    return {
        "text": text,
        "markdown": text,  # plain text as markdown in lite mode
        "pages": pages,
        "num_pages": pages_to_read,
        "num_tables": 0,
    }


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    if minutes < 60:
        return f"{minutes}m {secs:.0f}s"
    hours = int(minutes // 60)
    mins = minutes % 60
    return f"{hours}h {mins}m"


def _split_by_pages(text: str, num_pages: int | None) -> list[str]:
    """Best-effort page split. Falls back to single page if no page info."""
    if not num_pages or num_pages <= 1:
        return [text]
    # Docling sometimes inserts form-feed characters between pages
    parts = text.split("\f")
    if len(parts) >= num_pages:
        return parts[:num_pages]
    return [text]


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="DocuPulse Docling GPU Pipeline — batch document extraction",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--batch-size", "-b", type=int,
        default=int(os.environ.get("BATCH_SIZE", "50")),
        help="Max files per run (default: $BATCH_SIZE or 50)",
    )
    parser.add_argument(
        "--max-file-size", type=int,
        default=int(os.environ.get("MAX_FILE_SIZE_MB", "50")),
        help="Skip files larger than this (MB). Default: $MAX_FILE_SIZE_MB or 50",
    )
    default_filetypes = os.environ.get("FILE_TYPES", "pdf,docx,doc,pptx,xlsx,xls,html,htm,png,jpg,jpeg,tiff,tif")
    parser.add_argument(
        "--file-types", type=str,
        default=default_filetypes,
        help='Comma-separated list of file types to process (default: $FILE_TYPES or common document types)',
    )
    parser.add_argument(
        "--strategy", "-s", type=str,
        default=os.environ.get("STRATEGY_NAME", "docling"),
        help='Strategy name filter (default: $STRATEGY_NAME or "docling"). '
             'Use --strategy "" to fetch all pending extractions.',
    )
    parser.add_argument(
        "--max-pages", "-p", type=int,
        default=int(os.environ.get("MAX_PAGES", "0")),
        help="Extract only first N pages from PDFs (default: $MAX_PAGES or 0 = all)",
    )
    parser.add_argument(
        "--source-status", type=str,
        default=os.environ.get("SOURCE_STATUS", "PENDING"),
        help='Pick up files with this status (default: $SOURCE_STATUS or "PENDING")',
    )
    parser.add_argument(
        "--completion-status", type=str,
        default=os.environ.get("COMPLETION_STATUS", "EXTRACTED"),
        help='Status to set after extraction (default: $COMPLETION_STATUS or "EXTRACTED")',
    )
    parser.add_argument(
        "--workspace", "-w", type=str,
        default=os.environ.get("WORKSPACE_ID", ""),
        help="Limit to a specific workspace UUID (default: $WORKSPACE_ID or all)",
    )
    gpu_default = os.environ.get("USE_GPU", "true").lower() in ("true", "1", "yes")
    parser.add_argument(
        "--gpu", dest="use_gpu",
        action=argparse.BooleanOptionalAction,
        default=gpu_default,
        help="Enable/disable GPU acceleration (default: $USE_GPU or true)",
    )
    parser.add_argument(
        "--ocr-batch", type=int,
        default=int(os.environ.get("OCR_BATCH_SIZE", "64")),
        help="Pages per OCR inference batch (default: $OCR_BATCH_SIZE or 64). Lower for <16GB VRAM.",
    )
    parser.add_argument(
        "--layout-batch", type=int,
        default=int(os.environ.get("LAYOUT_BATCH_SIZE", "64")),
        help="Pages per layout detection batch (default: $LAYOUT_BATCH_SIZE or 64). Lower for <16GB VRAM.",
    )
    parser.add_argument(
        "--table-batch", type=int,
        default=int(os.environ.get("TABLE_BATCH_SIZE", "4")),
        help="Tables per structure inference batch (default: $TABLE_BATCH_SIZE or 4). Memory-intensive, keep low.",
    )
    lite_default = os.environ.get("LITE_MODE", "false").lower() in ("true", "1", "yes")
    parser.add_argument(
        "--lite", dest="lite",
        action=argparse.BooleanOptionalAction,
        default=lite_default,
        help="Fast text-only extraction — skip OCR, table structure, and heavy layout analysis (default: $LITE_MODE or false)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be processed, then exit without running",
    )
    parser.add_argument(
        "--log-level", type=str,
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Log level (default: $LOG_LEVEL or INFO)",
    )
    return parser.parse_args(argv)


# ─── Main pipeline ───────────────────────────────────────────────────────────

def run_pipeline(args):
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # Silence noisy third-party logging
    import warnings
    warnings.filterwarnings("ignore", module="openpyxl")
    logging.getLogger("azure").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("docling").setLevel(logging.ERROR)
    logging.getLogger("openpyxl").setLevel(logging.ERROR)
    log = logging.getLogger("docling-pipeline")

    strategy = args.strategy
    batch_size = args.batch_size
    max_pages = args.max_pages
    use_gpu = args.use_gpu
    workspace_id = args.workspace or None
    source_status = args.source_status
    completion_status = args.completion_status
    file_types = [ft.strip() for ft in args.file_types.split(",")]

    # ── Banner ──
    accel_label = _green("GPU (CUDA)") if use_gpu else _yellow("CPU")
    print()
    print(_bold("=" * 58))
    print(_bold(f"  DocuPulse Docling Pipeline  v{__version__}"))
    print(_bold("=" * 58))
    print()
    mode_label = _yellow("LITE (text-only)") if args.lite else _green("FULL (OCR + tables + layout)")
    print(f"  Mode:          {mode_label}")
    print(f"  Accelerator:   {accel_label}")
    print(f"  Batch size:    {_bold(str(batch_size))}")
    print(f"  Strategy:      {_bold(strategy if strategy else '(all — no filter)')}")
    print(f"  Max pages:     {_bold(str(max_pages) if max_pages > 0 else '0 (all pages)')}")
    print(f"  Max file size: {_bold(str(args.max_file_size) + 'MB')}")
    print(f"  File types:    {_bold(', '.join(file_types))}")
    print(f"  Source status:  {_bold(source_status)}")
    print(f"  Target status:  {_bold(completion_status)}")
    print(f"  Workspace:     {_bold(workspace_id if workspace_id else '(all workspaces)')}")
    print(f"  GPU batching:  {_dim(f'ocr={args.ocr_batch}  layout={args.layout_batch}  table={args.table_batch}')}")
    if args.dry_run:
        print(f"  Mode:          {_yellow('DRY RUN')}")
    print()

    confirm = input(f"  {_bold('Continue? [y/N]')} ").strip().lower()
    if confirm != "y":
        print(_dim("  Aborted."))
        print()
        return

    conn = get_db()

    # ── Build query ──
    query_params = [source_status, file_types]

    if workspace_id:
        workspace_filter = "AND fo.workspace_id = %s"
        query_params.append(workspace_id)
    else:
        workspace_filter = ""

    query_params.append(batch_size)

    query = FETCH_PENDING.format(
        workspace_filter=workspace_filter,
    )

    with conn.cursor() as cur:
        cur.execute(query, tuple(query_params))
        jobs = cur.fetchall()

    if not jobs:
        print(_dim("  No files to process. Exiting."))
        print()
        conn.close()
        return

    print(f"  Found {_bold(str(len(jobs)))} file(s) to process.")
    print()

    # ── Dry run ──
    if args.dry_run:
        print(_yellow("  DRY RUN — files that would be processed:"))
        print()
        for i, job in enumerate(jobs, 1):
            eid = job["extraction_id"] or "(new)"
            print(f"  {i:>3}.  file={job['file_id']}  blob={job['download_blob_name']}  extraction={eid}")
        print()
        print(_dim(f"  {len(jobs)} file(s) would be processed. Exiting."))
        print()
        conn.close()
        return

    # ── Process ──
    blob_service = get_blob_service()
    converter = create_converter(
        use_gpu=use_gpu,
        ocr_batch=args.ocr_batch,
        layout_batch=args.layout_batch,
        table_batch=args.table_batch,
        lite=args.lite,
    )

    page_numbers_json = json.dumps(list(range(1, max_pages + 1))) if max_pages > 0 else '["all"]'

    succeeded = []
    failed = []
    skipped = []
    total_start = time.monotonic()

    with tempfile.TemporaryDirectory() as tmpdir:
        for idx, job in enumerate(jobs, 1):
            # Check for graceful shutdown between files
            if _shutdown_requested:
                remaining = len(jobs) - idx + 1
                print(f"\n  {_yellow('Shutdown requested.')} Stopping with {remaining} file(s) remaining.")
                print(f"  {_dim('Remaining files stay PENDING and will be picked up on next run.')}")
                break

            file_id = job["file_id"]
            blob_name = job["download_blob_name"]
            user_id = job["user_id"]
            workspace_id_job = str(job["workspace_id"])
            extraction_id = job["extraction_id"]

            prefix = f"[{idx}/{len(jobs)}]"
            file_start = time.monotonic()
            file_ext = Path(blob_name).suffix.lower()

            # Skip unsupported file types before downloading
            supported_exts = {".pdf", ".docx", ".pptx", ".xlsx", ".html", ".htm",
                              ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp"}
            if file_ext not in supported_exts:
                print(f"  {prefix} {_yellow('SKIPPED')} — unsupported format: {file_ext or '(none)'}")
                skipped.append(file_id)
                continue

            # If no user found for workspace, skip
            if not user_id:
                log.error(f"{prefix} No user found for workspace {workspace_id_job}. Skipping.")
                if extraction_id:
                    with conn.cursor() as cur:
                        cur.execute(MARK_FAILED, (
                            json.dumps({"error": "No user found for workspace"}),
                            extraction_id,
                        ))
                    conn.commit()
                skipped.append(file_id)
                continue

            # Auto-create extraction record if it doesn't exist
            if extraction_id is None:
                new_id = str(uuid4())
                with conn.cursor() as cur:
                    cur.execute(INSERT_EXTRACTION, (new_id, file_id, strategy or "docling", completion_status, page_numbers_json))
                    extraction_id = cur.fetchone()["id"]
                conn.commit()
                log.info(f"{prefix} Created extraction record {extraction_id}")

            container_name = generate_container_name(user_id, workspace_id_job)
            file_ext = file_ext or ".pdf"
            local_file = Path(tmpdir) / f"{file_id}{file_ext}"

            try:
                # Check blob size before downloading
                max_bytes = args.max_file_size * 1024 * 1024
                blob_client = blob_service.get_container_client(container_name).get_blob_client(blob_name)
                blob_size = blob_client.get_blob_properties().size
                if blob_size > max_bytes:
                    print(f"  {prefix} {_yellow('SKIPPED')} — {blob_size:,} bytes exceeds {args.max_file_size}MB limit")
                    skipped.append(file_id)
                    continue

                # Download
                print(f"  {prefix} {_cyan('Downloading')} {container_name}/{blob_name} ({blob_size:,} bytes)...")
                download_blob(blob_service, container_name, blob_name, str(local_file))

                # Extract content
                if args.lite and file_ext.lower() == ".pdf":
                    # Fast text extraction via pypdf — no GPU needed
                    print(f"  {prefix} {_cyan('Extracting')} text (lite)...")
                    lite_result = extract_text_lite(str(local_file), max_pages)
                    text = lite_result["text"]
                    markdown = lite_result["markdown"]
                    pages = lite_result["pages"]
                    num_pages_found = lite_result["num_pages"]
                    num_tables = 0
                else:
                    # Full Docling extraction
                    convert_path = str(local_file)
                    if max_pages > 0 and file_ext.lower() == ".pdf":
                        log.info(f"{prefix} Trimming PDF to first {max_pages} page(s)...")
                        convert_path = trim_pdf_pages(str(local_file), max_pages)

                    print(f"  {prefix} {_cyan('Processing')} with Docling...")
                    result = converter.convert(convert_path)

                    markdown = result.document.export_to_markdown()
                    text = (
                        result.document.export_to_text()
                        if hasattr(result.document, "export_to_text")
                        else ""
                    )
                    num_pages_found = (
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
                        for i, page_text in enumerate(_split_by_pages(text, num_pages_found), start=1):
                            lines = [
                                {"content": line, "polygon": None}
                                for line in page_text.split("\n")
                                if line.strip()
                            ]
                            if lines:
                                pages.append({"page_number": i, "lines": lines})

                actual_page_numbers = list(range(1, max_pages + 1)) if max_pages > 0 else ["all"]

                # Strip null bytes — PostgreSQL rejects \u0000 in JSON/text
                text = text.replace("\x00", "")
                markdown = markdown.replace("\x00", "")

                raw_data = {
                    "pages": pages,
                    "content_markdown": markdown,
                    "content_text": text,
                    "num_pages": num_pages_found,
                    "num_tables": num_tables,
                    "page_numbers": actual_page_numbers,
                    "extraction_model": strategy or "docling",
                    "extraction_id": str(extraction_id),
                }

                metadata = {
                    "extraction_model": strategy or "docling",
                    "processing_method": "pypdf_lite" if args.lite else ("docling_gpu" if use_gpu else "docling_cpu"),
                    "ocr_used": not args.lite,
                    "gpu_accelerated": use_gpu and not args.lite,
                    "content_length": len(markdown),
                    "num_pages": num_pages_found,
                    "num_tables": num_tables,
                    "max_pages_setting": max_pages,
                }

                with conn.cursor() as cur:
                    cur.execute(SAVE_RESULT, (
                        completion_status,
                        json.dumps(raw_data),
                        json.dumps(metadata),
                        extraction_id,
                    ))
                    cur.execute(UPDATE_FILE_STATUS, (completion_status, file_id))
                conn.commit()

                elapsed = time.monotonic() - file_start
                succeeded.append(file_id)
                print(f"  {prefix} {_green('Done')} — {len(markdown):,} chars, {num_pages_found} pages, {num_tables} tables ({format_duration(elapsed)})")

            except Exception as e:
                conn.rollback()
                elapsed = time.monotonic() - file_start
                print(f"  {prefix} {_red('FAILED')} — {e}")
                try:
                    with conn.cursor() as cur:
                        cur.execute(MARK_FAILED, (
                            json.dumps({"error": str(e)[:2000]}),
                            extraction_id,
                        ))
                    conn.commit()
                except Exception:
                    log.warning(f"{prefix} Could not mark as failed in DB")
                failed.append(file_id)
            finally:
                # Clean up downloaded file to save disk on large batches
                if local_file.exists():
                    local_file.unlink()
                # Clean up trimmed file too
                trimmed = Path(str(local_file) + ".trimmed.pdf")
                if trimmed.exists():
                    trimmed.unlink()

    total_elapsed = time.monotonic() - total_start

    # ── Summary ──
    print()
    print(_bold("=" * 58))
    print(_bold("  Summary"))
    print(_bold("=" * 58))
    print(f"  {_green('Succeeded:')}  {len(succeeded)}")
    print(f"  {_red('Failed:')}     {len(failed)}")
    print(f"  {_yellow('Skipped:')}    {len(skipped)}")
    print(f"  {_dim('Total time:')} {format_duration(total_elapsed)}")
    print()

    if failed:
        print(_red("  Failed files:"))
        for fid in failed:
            print(f"    - {fid}")
        print()

    if skipped:
        print(_yellow("  Skipped files:"))
        for fid in skipped:
            print(f"    - {fid}")
        print()

    conn.close()


if __name__ == "__main__":
    run_pipeline(parse_args())
