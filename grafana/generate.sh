#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="$(basename "$(dirname "$SCRIPT_DIR")")"
IMAGE="servicelib-dashboards-${SERVICE_NAME}"
OUT_DIR="$SCRIPT_DIR/dist"

cd "$SCRIPT_DIR"

echo "==> Building Docker image for ${SERVICE_NAME}..."
set --
if [[ -n "${DEPENDENCY_PROXY_DOCKER_ARGS:-}" ]]; then
    read -r -a dependency_docker_args <<< "${DEPENDENCY_PROXY_DOCKER_ARGS}"
    set -- "${dependency_docker_args[@]}"
fi
docker build \
    "$@" \
    --build-arg SERVICE_NAME="${SERVICE_NAME}" \
    --build-arg DEPENDENCY_DOCKER_REGISTRY="${DEPENDENCY_DOCKER_REGISTRY:-docker.io}" \
    --build-arg DEPENDENCY_GITHUB_RAW_URL="${DEPENDENCY_GITHUB_RAW_URL:-https://github.com}" \
    --build-arg DEPENDENCY_APT_DEBIAN_URL="${DEPENDENCY_APT_DEBIAN_URL:-}" \
    --build-arg DEPENDENCY_APT_DEBIAN_SECURITY_URL="${DEPENDENCY_APT_DEBIAN_SECURITY_URL:-}" \
    -t "$IMAGE" .

mkdir -p "$OUT_DIR"

echo "==> Generating dashboards -> $OUT_DIR"
docker run --rm -v "$OUT_DIR:/output" "$IMAGE"
