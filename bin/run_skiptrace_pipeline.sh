#!/usr/bin/env bash
set -Eeuo pipefail

# =========================================
# run_skiptrace_pipeline.sh
# =========================================
# Orchestrates:
#   1) (optional) hail clustering
#   2) export/curate residential targets
#   3) record a run row in DB (handled by export script)
#   4) submit to BatchData async skip-trace (JSON with base64 file)
#
# REQS:
#   - env: DATABASE_URL, BATCHDATA_API_KEY
#   - optional env: BATCHDATA_WEBHOOK_URL (or pass --webhook)
#
# EXAMPLE:
#   bin/run_skiptrace_pipeline.sh \
#     --state KY \
#     --center "-85.7585,38.2527" \
#     --radius-km 40 \
#     --dist-m 200 \
#     --target 1000 \
#     --list-name "KY Louisville 40km" \
#     --source "storm-leads" \
#     --webhook "$BATCHDATA_WEBHOOK_URL"
#
# With recluster step (if needed):
#   bin/run_skiptrace_pipeline.sh ... \
#     --recluster \
#     --source-table swdi_nx3hail_ky_20240101_20250801 \
#     --dest-table   hail_cluster_boundaries_ky_20240101_20250801 \
#     --eps 0.03 --min-samples 6
#
# =========================================

# -------- Defaults --------
STATE=""
CENTER=""
RADIUS_KM="40"
DIST_M="200"
TARGET="1000"
LIST_NAME="Skiptrace Run"
SOURCE_TAG="storm-leads"
WEBHOOK_URL="${BATCHDATA_WEBHOOK_URL:-}"
BATCHDATA_BASE_URL="${BATCHDATA_BASE_URL:-https://api.batchdata.com/api/v1}"

# Optional pre-cluster flags
DO_RECLUSTER="0"
SRC_TABLE=""
DEST_TABLE=""
CL_EPS="0.03"
CL_MIN_SAMPLES="6"

usage() {
  cat <<USAGE
Usage: $0 --state KY --center "lon,lat" [options]

Required:
  --state STATE             e.g., KY
  --center "lon,lat"        WGS84 center for export

Export options:
  --radius-km N             default ${RADIUS_KM}
  --dist-m N                default ${DIST_M}
  --target N                default ${TARGET}
  --list-name STR           default "${LIST_NAME}"
  --source STR              default "${SOURCE_TAG}"
  --webhook URL             override \$BATCHDATA_WEBHOOK_URL
  --batchdata-base URL      default ${BATCHDATA_BASE_URL}

Optional pre-cluster step:
  --recluster
  --source-table NAME       e.g., swdi_nx3hail_ky_20240101_20250801
  --dest-table NAME         e.g., hail_cluster_boundaries_ky_20240101_20250801
  --eps 0.03
  --min-samples 6

Notes:
- Requires bin/export_skiptrace_targets.sh and bin/submit_skiptrace_batchdata.py in PATH.
- Submits JSON with base64 fileUrl per BatchData docs.

USAGE
  exit 1
}

# -------- Parse args --------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --state) STATE="$2"; shift 2 ;;
    --center) CENTER="$2"; shift 2 ;;
    --radius-km) RADIUS_KM="$2"; shift 2 ;;
    --dist-m) DIST_M="$2"; shift 2 ;;
    --target) TARGET="$2"; shift 2 ;;
    --list-name) LIST_NAME="$2"; shift 2 ;;
    --source) SOURCE_TAG="$2"; shift 2 ;;
    --webhook) WEBHOOK_URL="$2"; shift 2 ;;
    --batchdata-base) BATCHDATA_BASE_URL="$2"; shift 2 ;;

    --recluster) DO_RECLUSTER="1"; shift 1 ;;
    --source-table) SRC_TABLE="$2"; shift 2 ;;
    --dest-table) DEST_TABLE="$2"; shift 2 ;;
    --eps) CL_EPS="$2"; shift 2 ;;
    --min-samples) CL_MIN_SAMPLES="$2"; shift 2 ;;

    -h|--help) usage ;;
    *) echo "Unknown arg: $1"; usage ;;
  esac
done

# -------- Preflight --------
: "${DATABASE_URL:?Set DATABASE_URL=postgresql://user:pass@host:5432/dbname}"
: "${BATCHDATA_API_KEY:?Set BATCHDATA_API_KEY=<token>}"
[[ -z "$STATE" || -z "$CENTER" ]] && usage
if [[ -z "$WEBHOOK_URL" ]]; then
  echo "❌ Missing webhook URL. Pass --webhook or set BATCHDATA_WEBHOOK_URL."
  exit 2
fi

IFS=',' read -r LON LAT <<<"$(echo "$CENTER" | tr -d ' ')"
if [[ -z "$LON" || -z "$LAT" ]]; then
  echo "❌ Invalid --center; expected \"lon,lat\""
  exit 2
fi

echo "▶ STATE=$STATE  CENTER=$LON,$LAT  RADIUS=${RADIUS_KM}km  DIST=${DIST_M}m  TARGET=${TARGET}"
echo "▶ LIST_NAME=$LIST_NAME  SOURCE=$SOURCE_TAG"
echo "▶ WEBHOOK=$WEBHOOK_URL"
echo "▶ API_BASE=$BATCHDATA_BASE_URL"
echo

# -------- 1) Optional: recluster hail --------
if [[ "$DO_RECLUSTER" == "1" ]]; then
  if [[ -z "$SRC_TABLE" || -z "$DEST_TABLE" ]]; then
    echo "❌ --recluster requires --source-table and --dest-table"
    exit 2
  fi
  echo "🌀 Reclustering hail → $DEST_TABLE  (src=$SRC_TABLE  eps=$CL_EPS  min_samples=$CL_MIN_SAMPLES)"
  python cluster/cluster_hail.py \
    --source-table "$SRC_TABLE" \
    --dest-table   "$DEST_TABLE" \
    --eps "$CL_EPS" \
    --min-samples "$CL_MIN_SAMPLES"
  echo
fi

# -------- 2) Export targets (and log run row) --------
echo "📦 Exporting skiptrace targets…"
# NOTE: export script itself inserts a row into public.skiptrace_runs
bin/export_skiptrace_targets.sh \
  --state "$STATE" \
  --center "$LON,$LAT" \
  --radius-km "$RADIUS_KM" \
  --dist-m "$DIST_M" \
  --target "$TARGET"

# Capture latest run row for this state+most recent
read -r RUN_ID OUTFILE <<<"$(psql "$DATABASE_URL" -tA -c \
  "SELECT run_id, output_path
   FROM public.skiptrace_runs
   WHERE state = '$STATE'
   ORDER BY created_at DESC
   LIMIT 1")"

RUN_ID="${RUN_ID:-}"
OUTFILE="${OUTFILE:-}"

if [[ -z "$RUN_ID" || -z "$OUTFILE" ]]; then
  echo "❌ Could not find run row or output file after export."
  exit 3
fi
if [[ ! -f "$OUTFILE" ]]; then
  echo "❌ Output CSV not found on disk: $OUTFILE"
  exit 3
fi

LINES=$(( $(wc -l < "$OUTFILE" 2>/dev/null || echo 0) ))
ROWS=$(( LINES>0 ? LINES-1 : 0 ))
echo "🧾 run_id=$RUN_ID   file=$OUTFILE   rows=$ROWS"
echo

# -------- 3) Submit to BatchData async skip-trace --------
echo "🚀 Submitting to BatchData (JSON + base64 fileUrl)…"
# Base64 encode (no -b on macOS; strip newlines)
B64=$(base64 "$OUTFILE" | tr -d '\n')

REQ_JSON=$(python - <<PY
import json, os
print(json.dumps({
  "fileUrl": f"data:text/csv;base64,${B64}",
  "options": {"webhook": {"url": os.environ["WB_URL"]}},
  "listName": os.environ["WB_LIST"],
  "source": os.environ["WB_SRC"],
}, separators=(",",":")))
PY
)

# Send JSON request
HTTP_CODE=$(curl -sS -o /tmp/bd_resp.json -w "%{http_code}" \
  -X POST "${BATCHDATA_BASE_URL%/}/property/skip-trace/async" \
  -H "Authorization: Bearer ${BATCHDATA_API_KEY}" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  --data "$REQ_JSON" \
  --env WB_URL="$WEBHOOK_URL" \
  --env WB_LIST="$LIST_NAME" \
  --env WB_SRC="$SOURCE_TAG" \
)

echo "HTTP $HTTP_CODE"
RESP="$(cat /tmp/bd_resp.json || true)"
echo "Response: ${RESP:0:2000}"
echo

# Basic success check
if [[ "$HTTP_CODE" != 2* && "$HTTP_CODE" != 3* ]]; then
  echo "❌ Submission failed. See response above."
  exit 4
fi

# Extract job id if present
JOB_ID=$(python - <<'PY'
import json,sys
try:
  d=json.load(open("/tmp/bd_resp.json"))
except:
  d=None
jid=None
if isinstance(d,dict):
  # try common fields
  for k in ("id","jobId","job_id"):
    if isinstance(d.get(k), (str,int)):
      jid=str(d[k]); break
  if not jid and isinstance(d.get("data"), dict):
    for k in ("id","jobId","job_id"):
      v=d["data"].get(k)
      if isinstance(v,(str,int)): jid=str(v); break
print(jid or "")
PY
)

# Update DB with job id + api_base + webhook
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<SQL
UPDATE public.skiptrace_runs
SET batch_job_id = COALESCE(NULLIF('${JOB_ID}',''), batch_job_id),
    webhook_url  = COALESCE(NULLIF('${WEBHOOK_URL}',''), webhook_url),
    submitted_at = COALESCE(submitted_at, now()),
    api_base     = COALESCE(NULLIF('${BATCHDATA_BASE_URL}',''), api_base)
WHERE run_id = ${RUN_ID};
SQL

echo "✅ Submitted. run_id=${RUN_ID}  job_id=${JOB_ID:-<none>}"
echo "   File: ${OUTFILE}  (${ROWS} rows)"