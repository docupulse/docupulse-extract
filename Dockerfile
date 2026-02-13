# ── Base: NVIDIA CUDA for GPU acceleration ──
FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

LABEL maintainer="DocuPulse"
LABEL description="GPU-accelerated document extraction pipeline using Docling"
LABEL version="1.0.0"

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV BATCH_SIZE=50
ENV STRATEGY_NAME=docling
ENV MAX_PAGES=0
ENV USE_GPU=true

# ── System deps (Python, Tesseract OCR, poppler for PDF rendering) ──
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 python3-pip python3-venv \
    tesseract-ocr \
    libtesseract-dev \
    poppler-utils \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Python deps (cached layer — only rebuilds when requirements change) ──
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# ── App code ──
COPY pipeline.py .

HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python3 -c "import psycopg2; print('ok')" || exit 1

ENTRYPOINT ["python3", "pipeline.py"]
CMD []
