#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [[ -n "${ENV_FILE:-}" ]]; then
  SELECTED_ENV_FILE="$ENV_FILE"
elif [[ -f ".env" ]]; then
  SELECTED_ENV_FILE=".env"
else
  SELECTED_ENV_FILE=".env.postgres.example"
fi

if [[ -f "$SELECTED_ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$SELECTED_ENV_FILE"
  set +a
  echo "Loaded environment from $SELECTED_ENV_FILE"
else
  echo "Warning: env file not found ($SELECTED_ENV_FILE). Running without sourced env vars."
fi

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  echo "Starting PostgreSQL container..."
  docker compose --env-file "$SELECTED_ENV_FILE" up -d postgres
else
  echo "Docker Compose not found. Skipping PostgreSQL startup."
fi

echo "Downloading Go modules..."
go mod download

if [[ $# -eq 0 ]]; then
  echo "No CLI flags provided. Using recommended defaults."
  set -- -pages-per-span 2 -cards-per-page 5 -workers 1 -timeout-sec 90
fi

echo "Running scraper..."
go run . "$@"
