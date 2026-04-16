#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$ROOT_DIR/.env}"
LOOKBACK_YEARS="${LOOKBACK_YEARS:-2}"
STATE_DIR="${ROOT_DIR}/output/state"
LOCK_DIR="${STATE_DIR}/.weekly.lock"
LOG_DIR="${ROOT_DIR}/output/logs"
LOG_FILE="${LOG_DIR}/ledger-weekly.log"

mkdir -p "${STATE_DIR}" "${LOG_DIR}"

if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
  echo "Ledger weekly run skipped: lock exists at ${LOCK_DIR}" >> "${LOG_FILE}"
  exit 0
fi

cleanup() {
  rmdir "${LOCK_DIR}" 2>/dev/null || true
}
trap cleanup EXIT

{
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] starting weekly ledger run"
  cd "${ROOT_DIR}"
  uv run ledger --env-file "${ENV_FILE}" --lookback-years "${LOOKBACK_YEARS}"
  uv run ledger-prepare-final-results --env-file "${ENV_FILE}" --run-dir "${ROOT_DIR}/output/latest"
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] finished weekly ledger run"
} >> "${LOG_FILE}" 2>&1
