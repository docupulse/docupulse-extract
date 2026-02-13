#!/usr/bin/env bash
set -euo pipefail

# ╔════════════════════════════════════════════════════════════════╗
# ║  Docling GPU Pipeline — one command, runs entirely on Azure.   ║
# ║                                                                ║
# ║  What happens:                                                 ║
# ║    1. Builds & pushes Docker image to Azure Container Registry ║
# ║    2. Spins up a GPU container (T4) with your .env vars        ║
# ║    3. Pipeline runs, logs stream to your terminal              ║
# ║    4. Container is deleted → billing stops                     ║
# ║                                                                ║
# ║  Run from your laptop. No SSH, no VM.                          ║
# ╚════════════════════════════════════════════════════════════════╝

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: .env file not found."
    echo "  cp .env.example .env"
    echo "  Then fill in DATABASE_URL and AZURE_STORAGE_CONNECTION_STRING."
    exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

# ── Required vars ─────────────────────────────────────────────
DATABASE_URL="${DATABASE_URL:?Set DATABASE_URL in .env}"
AZURE_STORAGE_CONNECTION_STRING="${AZURE_STORAGE_CONNECTION_STRING:?Set AZURE_STORAGE_CONNECTION_STRING in .env}"
RESOURCE_GROUP="${RESOURCE_GROUP:?Set RESOURCE_GROUP in .env}"
ACR_NAME="${ACR_NAME:?Set ACR_NAME in .env}"

# ── Defaults ──────────────────────────────────────────────────
IMAGE="${ACR_NAME}.azurecr.io/docling-pipeline:latest"
CONTAINER_NAME="docling-job"
BATCH_SIZE="${BATCH_SIZE:-50}"
GPU_SKU="${GPU_SKU:-T4}"
LOCATION="${LOCATION:-eastus}"

# ── Step 1: Ensure Azure resources exist ──────────────────────
echo "1/4  Ensuring resource group & registry exist..."
az group create --name "$RESOURCE_GROUP" --location "$LOCATION" -o none 2>/dev/null || true
az acr create --resource-group "$RESOURCE_GROUP" --name "$ACR_NAME" \
    --sku Basic --admin-enabled true -o none 2>/dev/null || true

# ── Step 2: Build & push image ────────────────────────────────
echo "2/4  Building image in Azure (uses cache if unchanged)..."
az acr build \
    --registry "$ACR_NAME" \
    --image "docling-pipeline:latest" \
    --platform linux/amd64 \
    "$SCRIPT_DIR"

# ── Step 3: Run on GPU ────────────────────────────────────────
echo "3/4  Launching GPU container..."

# Get ACR credentials
ACR_USER=$(az acr credential show --name "$ACR_NAME" --query username -o tsv)
ACR_PASS=$(az acr credential show --name "$ACR_NAME" --query "passwords[0].value" -o tsv)

# Clean up any leftover container
az container delete --resource-group "$RESOURCE_GROUP" \
    --name "$CONTAINER_NAME" --yes 2>/dev/null || true

az container create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$CONTAINER_NAME" \
    --image "$IMAGE" \
    --gpu-count 1 \
    --gpu-sku "$GPU_SKU" \
    --cpu 4 \
    --memory 16 \
    --restart-policy Never \
    --environment-variables \
        DATABASE_URL="$DATABASE_URL" \
        AZURE_STORAGE_CONNECTION_STRING="$AZURE_STORAGE_CONNECTION_STRING" \
        BATCH_SIZE="$BATCH_SIZE" \
        WORKSPACE_ID="${WORKSPACE_ID:-}" \
        LOG_LEVEL="${LOG_LEVEL:-INFO}" \
        STRATEGY_NAME="${STRATEGY_NAME:-docling}" \
        MAX_PAGES="${MAX_PAGES:-0}" \
        USE_GPU="${USE_GPU:-true}" \
    --registry-login-server "${ACR_NAME}.azurecr.io" \
    --registry-username "$ACR_USER" \
    --registry-password "$ACR_PASS" \
    --os-type Linux \
    -o none

echo ""
echo "Container running. Streaming logs..."
echo "════════════════════════════════════════════════════"
echo ""

az container attach \
    --resource-group "$RESOURCE_GROUP" \
    --name "$CONTAINER_NAME"

# ── Step 4: Cleanup ───────────────────────────────────────────
EXIT_CODE=$(az container show \
    --resource-group "$RESOURCE_GROUP" \
    --name "$CONTAINER_NAME" \
    --query "containers[0].instanceView.currentState.exitCode" -o tsv 2>/dev/null || echo "?")

echo ""
echo "4/4  Deleting container (stops billing)..."
az container delete --resource-group "$RESOURCE_GROUP" \
    --name "$CONTAINER_NAME" --yes -o none

echo ""
echo "════════════════════════════════════════════════════"
echo "  Done. Exit code: ${EXIT_CODE}"
echo "  No ongoing costs."
echo "════════════════════════════════════════════════════"
