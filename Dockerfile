# ── Base: NVIDIA CUDA for GPU acceleration ──
FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# ── System deps (Python, Tesseract OCR, poppler for PDF rendering) ──
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv \
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

CMD ["python3", "pipeline.py"]
