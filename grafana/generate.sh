#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="$(basename "$(dirname "$SCRIPT_DIR")")"
IMAGE="servicelib-dashboards-${SERVICE_NAME}"
OUT_DIR="$SCRIPT_DIR/dist"

cd "$SCRIPT_DIR"

echo "==> Building Docker image for ${SERVICE_NAME}..."
docker build \
    --build-arg SERVICE_NAME="${SERVICE_NAME}" \
    --build-arg SERVICEGEN_GITHUB_RAW_URL="${SERVICEGEN_GITHUB_RAW_URL:-https://github.com}" \
    -t "$IMAGE" .

mkdir -p "$OUT_DIR"

echo "==> Generating dashboards -> $OUT_DIR"
docker run --rm -v "$OUT_DIR:/output" "$IMAGE"
