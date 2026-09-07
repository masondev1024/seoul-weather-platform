#!/bin/zsh
# Opt-in macOS startup wrapper for the personal Weather runtime.
#
# The default is a no-op preflight.  A launchd plist may pass --start, but the
# wrapper still requires WEATHER_STARTUP_AUTOSTART=enabled.  This prevents an
# accidentally copied plist from starting Docker services before the operator
# has reviewed the local target.  It never changes Airflow DAG pause state and
# never issues a collection/backfill command.

set -euo pipefail

SCRIPT_DIR="${0:A:h}"
DEFAULT_ROOT="${SCRIPT_DIR:h}"
ROOT="$DEFAULT_ROOT"
ENV_FILE="${DEFAULT_ROOT}/weather-platform.prod.env"
START=false

while (( $# > 0 )); do
  case "$1" in
    --repo-root)
      (( $# >= 2 )) || { print -u2 "weather_startup_error=missing_repo_root"; exit 2; }
      ROOT="$2"
      shift 2
      ;;
    --env-file)
      (( $# >= 2 )) || { print -u2 "weather_startup_error=missing_env_file"; exit 2; }
      ENV_FILE="$2"
      shift 2
      ;;
    --start)
      START=true
      shift
      ;;
    *)
      print -u2 "weather_startup_error=unknown_argument"
      exit 2
      ;;
  esac
done

ROOT="${ROOT:A}"
ENV_FILE="${ENV_FILE:A}"
PYTHON="${ROOT}/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="$(command -v python3 2>/dev/null || true)"
fi
if [[ -z "$PYTHON" || ! -x "$PYTHON" ]]; then
  print -u2 "weather_startup_error=python_unavailable"
  exit 0
fi
PREFLIGHT=(
  "$PYTHON"
  "${ROOT}/tools/weather_startup_preflight.py"
  --repo-root "${ROOT}"
  --env-file "${ENV_FILE}"
)
COMPOSE=(
  docker compose
  --env-file "${ENV_FILE}"
  -f "${ROOT}/docker-compose.yml"
  -f "${ROOT}/docker-compose.local.yml"
)

# Docker Desktop may still be starting when launchd runs this process.  Keep
# this bounded; launchd's KeepAlive can retry the wrapper later.
for attempt in {1..24}; do
  if /usr/bin/env docker info --format '{{.ServerVersion}}' >/dev/null 2>&1; then
    break
  fi
  if (( attempt == 24 )); then
    print -u2 "weather_startup_error=docker_unavailable"
    exit 0
  fi
  sleep 5
done

if ! "${PREFLIGHT[@]}" --configuration-only >/dev/null; then
  print -u2 "weather_startup_error=compose_configuration_invalid"
  exit 0
fi

if [[ "$START" != true || "${WEATHER_STARTUP_AUTOSTART:-disabled}" != "enabled" ]]; then
  print "weather_startup_status=preflight_only"
  exit 0
fi

# Starting the core Compose stack is the only mutation this wrapper owns.  It
# deliberately does not use --build or --force-recreate; image build and
# destructive reconciliation require a separate, human-approved operation.
if ! "${COMPOSE[@]}" up -d --no-build >/dev/null; then
  print -u2 "weather_startup_error=compose_start_failed"
  exit 0
fi

for attempt in {1..30}; do
  if "${PREFLIGHT[@]}" >/dev/null; then
    print "weather_startup_status=ready"
    exit 0
  fi
  sleep 5
done

print -u2 "weather_startup_error=runtime_not_ready"
exit 0
