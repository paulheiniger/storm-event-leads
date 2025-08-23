#!/usr/bin/env bash
set -Eeuo pipefail

# Minimal wrapper: choose a Python and pass args through verbatim.
PYTHON_BIN="${PYTHON_BIN:-/opt/homebrew/Caskroom/miniconda/base/envs/storm-leads/bin/python}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  # Fallbacks if the conda python isn’t on PATH
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    echo "❌ No python found. Set PYTHON_BIN or install Python." >&2
    exit 127
  fi
fi

# Helpful debug
if [[ " $* " == *" --debug "* ]]; then
  echo "▶ Using Python: $PYTHON_BIN ($("$PYTHON_BIN" -c 'import sys; print(sys.executable)'))"
  "$PYTHON_BIN" --version
  echo "▶ Running exporter:"
  printf '   %q ' "$PYTHON_BIN" "bin/export_skiptrace_targets.py" "$@"
  echo
fi

mkdir -p exports logs

# Exec so the exit code is propagated and no extra shell interferes
exec "$PYTHON_BIN" "bin/export_skiptrace_targets.py" "$@"