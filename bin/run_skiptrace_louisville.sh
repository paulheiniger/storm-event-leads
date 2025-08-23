#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<USAGE
Usage: $0 --state KY --center "-85.7585,38.2527" [options]
  --state STATE
  --center "lon,lat"
  --radius-km N        (default: 40)
  --dist-m N           (default: 200)
  --target N           (default: 1000)
  --hail-table NAME    (default: hail_cluster_boundaries_\${state})
  --addr-table NAME    (default: addresses)
  --outfile PATH       (default: exports/…)
  --include-multiunits (keep APT/UNIT/STE/#)
  --no-index           (skip index creation)
  --source TEXT        (default: storm-leads)
  --list-name TEXT     (label for vendor job)
  --webhook-url URL    (override \$BATCHDATA_WEBHOOK_URL)
  --base-url URL       (override \$BATCHDATA_BASE_URL; default vendor base)
  --no-submit          (export only; skip vendor submit)
USAGE
  exit 1
}

# -------- Defaults --------
STATE=""
CENTER=""
RADIUS_KM="40"
DIST_M="200"
TARGET="1000"
HAIL_TABLE=""
ADDR_TABLE=""
OUTFILE=""
INCLUDE_MULTIUNITS=0
CREATE_INDEXES=1
SOURCE_TAG="storm-leads"
LIST_NAME=""
WEBHOOK_URL="${BATCHDATA_WEBHOOK_URL:-}"
BASE_URL="${BATCHDATA_BASE_URL:-https://api.batchdata.com/api/v1}"
DO_SUBMIT=1

EXPORT_SCRIPT="bin/export_skiptrace_targets.sh"
SUBMIT_SCRIPT="bin/submit_skiptrace_batchdata.py"

# -------- Parse args --------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --state) STATE="$2"; shift 2 ;;
    --center) CENTER="$2"; shift 2 ;;
    --radius-km) RADIUS_KM="$2"; shift 2 ;;
    --dist-m) DIST_M="$2"; shift 2 ;;
    --target) TARGET="$2"; shift 2 ;;
    --hail-table) HAIL_TABLE="$2"; shift 2 ;;
    --addr-table) ADDR_TABLE="$2"; shift 2 ;;
    --outfile) OUTFILE="$2"; shift 2 ;;
    --include-multiunits) INCLUDE_MULTIUNITS=1; shift ;;
    --no-index) CREATE_INDEXES=0; shift ;;
    --source) SOURCE_TAG="$2"; shift 2 ;;
    --list-name) LIST_NAME="$2"; shift 2 ;;
    --webhook-url) WEBHOOK_URL="$2"; shift 2 ;;
    --base-url) BASE_URL="$2"; shift 2 ;;
    --no-submit) DO_SUBMIT=0; shift ;;
    -h|--help) usage ;;
    *) echo "Unknown arg: $1"; usage ;;
  esac
done

# -------- Preflight --------
: "${DATABASE_URL:?Set DATABASE_URL, e.g. export DATABASE_URL=postgresql://user:pass@host:5432/db}"
: "${BATCHDATA_API_KEY:?Set BATCHDATA_API_KEY for vendor submit}"

[[ -z "$STATE" || -z "$CENTER" ]] && usage
command -v psql >/dev/null || { echo "psql not found"; exit 2; }
command -v python >/dev/null || { echo "python not found"; exit 2; }
[[ -x "$EXPORT_SCRIPT" ]] || { echo "$EXPORT_SCRIPT not executable"; exit 2; }
[[ -f "$SUBMIT_SCRIPT" ]] || { echo "$SUBMIT_SCRIPT missing"; exit 2; }

echo "▶ Orchestrator starting…"
echo "   STATE=$STATE  CENTER=$CENTER  RADIUS_KM=$RADIUS_KM  DIST_M=$DIST_M  TARGET=$TARGET"
echo "   LIST_NAME=${LIST_NAME:-<none>}  SOURCE_TAG=$SOURCE_TAG"
echo

mkdir -p exports logs
STAMP=$(date +%Y%m%d-%H%M%S)
RUN_LOG="logs/run_${STAMP}.log"
echo "▶ Running export → logs in $RUN_LOG"

echo "▶ Running export → logs in $RUN_LOG"
set +o pipefail
bin/export_skiptrace_targets.sh \
  --state "$STATE" \
  --center "$CENTER" \
  --radius-km "$RADIUS_KM" \
  --dist-m "$DIST_M" \
  --target "$TARGET" \
  --source "$SOURCE_TAG" 2>&1 | tee "$RUN_LOG"
export_rc=${PIPESTATUS[0]}
set -o pipefail

if [[ $export_rc -ne 0 ]]; then
  echo "❌ Export script failed (rc=$export_rc). See $RUN_LOG"
  exit $export_rc
fi

# Determine outfile: prefer ::OUTFILE:: marker; fallback to “Writing to '…'”
OUTFILE_DET="$(grep -E '^::OUTFILE:: ' "$RUN_LOG" | tail -n1 | awk '{print $2}')"
if [[ -z "$OUTFILE_DET" ]]; then
  OUTFILE_DET="$(grep -o "Writing to '[^']\\+\\.csv'" "$RUN_LOG" | tail -n1 | sed -E "s/.*'([^']+)'.*/\\1/")"
fi
if [[ -z "$OUTFILE_DET" ]]; then
  echo "❌ Export script returned success but outfile not found in log."
  echo "   Check $RUN_LOG"
  exit 3
fi

# Validate outfile exists and non-empty (has header + ≥1 row)
if [[ ! -f "$OUTFILE_DET" ]]; then
  echo "❌ Outfile not found: $OUTFILE_DET"
  exit 4
fi
LINES=$(wc -l < "$OUTFILE_DET" | tr -d ' ')
if [[ ${LINES:-0} -le 1 ]]; then
  echo "❌ Outfile has no rows (lines=$LINES): $OUTFILE_DET"
  exit 5
fi

echo "▶ Export OK → $OUTFILE_DET  (lines=$LINES)"

# Find run_id we just logged
RUN_ID="$(
  psql "$DATABASE_URL" -tA -v ON_ERROR_STOP=1 \
    -v p="$OUTFILE_DET" \
    -c "SELECT run_id FROM public.skiptrace_runs WHERE output_path = :'p' ORDER BY created_at DESC LIMIT 1;"
)"
RUN_ID="${RUN_ID//$'\n'/}"
if [[ -z "$RUN_ID" ]]; then
  echo "❌ Could not find run_id for $OUTFILE_DET in skiptrace_runs"
  exit 6
fi
echo "▶ Logged run_id=$RUN_ID"

# Submit to vendor unless suppressed
if [[ $DO_SUBMIT -eq 0 ]]; then
  echo "ℹ️  --no-submit set; skipping vendor submission."
  exit 0
fi

: "${WEBHOOK_URL:?Pass --webhook-url or set BATCHDATA_WEBHOOK_URL}"

echo "▶ Submitting run_id=$RUN_ID to vendor…"
python "$SUBMIT_SCRIPT" \
  --run-id "$RUN_ID" \
  --webhook-url "$WEBHOOK_URL" \
  --list-name "${LIST_NAME:-$STATE $CENTER $RADIUS_KM km}" \
  --source "$SOURCE_TAG" \
  --base-url "$BASE_URL"