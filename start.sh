#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_HOST="${FRONTEND_HOST:-127.0.0.1}"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"
RUN_INIT_DB="${RUN_INIT_DB:-true}"
RUN_GOOGLE_AUTH="${RUN_GOOGLE_AUTH:-false}"

backend_pid=""
frontend_pid=""

print_usage() {
  cat <<EOF
Usage: bash start.sh [options]

Starts the local Weekly Product Review Pulse backend and frontend together.

Options:
  --skip-init-db   Skip database initialization before startup.
  --auth-google    Run Google MCP auth before starting services.
  --help           Show this help text.

Environment overrides:
  BACKEND_HOST     Default: 127.0.0.1
  BACKEND_PORT     Default: 8000
  FRONTEND_HOST    Default: 127.0.0.1
  FRONTEND_PORT    Default: 3000
  RUN_INIT_DB      Default: true
  RUN_GOOGLE_AUTH  Default: false
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-init-db)
      RUN_INIT_DB="false"
      shift
      ;;
    --auth-google)
      RUN_GOOGLE_AUTH="true"
      shift
      ;;
    --help|-h)
      print_usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      print_usage >&2
      exit 1
      ;;
  esac
done

cleanup() {
  local exit_code=$?

  if [[ -n "${frontend_pid}" ]] && kill -0 "${frontend_pid}" 2>/dev/null; then
    kill "${frontend_pid}" 2>/dev/null || true
    wait "${frontend_pid}" 2>/dev/null || true
  fi

  if [[ -n "${backend_pid}" ]] && kill -0 "${backend_pid}" 2>/dev/null; then
    kill "${backend_pid}" 2>/dev/null || true
    wait "${backend_pid}" 2>/dev/null || true
  fi

  exit "${exit_code}"
}

trap cleanup EXIT INT TERM

find_python() {
  if [[ -x "${ROOT_DIR}/.venv/Scripts/python.exe" ]]; then
    printf '%s\n' "${ROOT_DIR}/.venv/Scripts/python.exe"
    return 0
  fi
  if [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
    printf '%s\n' "${ROOT_DIR}/.venv/bin/python"
    return 0
  fi
  return 1
}

find_npm() {
  if command -v npm >/dev/null 2>&1; then
    command -v npm
    return 0
  fi
  if command -v npm.cmd >/dev/null 2>&1; then
    command -v npm.cmd
    return 0
  fi
  return 1
}

PYTHON_BIN="$(find_python || true)"
NPM_BIN="$(find_npm || true)"

if [[ -z "${PYTHON_BIN}" ]]; then
  echo "Error: could not find the project virtualenv Python." >&2
  echo "Expected one of:" >&2
  echo "  ${ROOT_DIR}/.venv/Scripts/python.exe" >&2
  echo "  ${ROOT_DIR}/.venv/bin/python" >&2
  exit 1
fi

if [[ -z "${NPM_BIN}" ]]; then
  echo "Error: npm is required but was not found on PATH." >&2
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  echo "Error: ${ROOT_DIR}/.env does not exist." >&2
  exit 1
fi

if [[ ! -f "${ROOT_DIR}/frontend/.env.local" ]]; then
  echo "Error: ${ROOT_DIR}/frontend/.env.local does not exist." >&2
  exit 1
fi

if [[ "${RUN_GOOGLE_AUTH}" == "true" ]]; then
  echo "Loading Google MCP auth environment from .env..."
  while IFS= read -r line; do
    case "${line}" in
      GOOGLE_CLIENT_ID=*|GOOGLE_CLIENT_SECRET=*|GOOGLE_MCP_PROFILE=*)
        export "${line}"
        ;;
    esac
  done < "${ROOT_DIR}/.env"

  echo "Running Google MCP auth..."
  (
    cd "${ROOT_DIR}"
    npx --yes @a-bonus/google-docs-mcp auth
  )
fi

if [[ "${RUN_INIT_DB}" == "true" ]]; then
  echo "Initializing database..."
  (
    cd "${ROOT_DIR}"
    "${PYTHON_BIN}" -m agent init-db
  )
fi

if [[ ! -d "${ROOT_DIR}/frontend/node_modules" ]]; then
  echo "Installing frontend dependencies..."
  (
    cd "${ROOT_DIR}/frontend"
    "${NPM_BIN}" install
  )
fi

echo "Starting backend on http://${BACKEND_HOST}:${BACKEND_PORT}"
(
  cd "${ROOT_DIR}"
  exec "${PYTHON_BIN}" -m agent serve --host "${BACKEND_HOST}" --port "${BACKEND_PORT}"
) &
backend_pid=$!

echo "Starting frontend on http://${FRONTEND_HOST}:${FRONTEND_PORT}"
(
  cd "${ROOT_DIR}/frontend"
  exec "${NPM_BIN}" run dev -- --hostname "${FRONTEND_HOST}" --port "${FRONTEND_PORT}"
) &
frontend_pid=$!

echo
echo "Services are starting..."
echo "Backend:  http://${BACKEND_HOST}:${BACKEND_PORT}"
echo "Frontend: http://${FRONTEND_HOST}:${FRONTEND_PORT}"
echo "Press Ctrl+C to stop both."
echo

wait -n "${backend_pid}" "${frontend_pid}"
