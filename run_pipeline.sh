#!/opt/homebrew/bin/bash
set -euo pipefail

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
START=2024-01-01
END=2025-08-01
CHUNK_DAYS=14

DATASET=nx3hail
HAIL_EPS=0.1
HAIL_MIN_SAMPLES=5
ADDR_BUFFER=0.02
ADDR_EPS=0.001
ADDR_MIN_SAMPLES=10

STATES=(GA IN OH KY)

declare -A BBOX
BBOX[GA]="-85.61,30.36,-80.84,35.00"
BBOX[IN]="-88.10,37.70,-84.79,41.76"
BBOX[OH]="-84.82,38.40,-80.52,41.98"
BBOX[KY]="-89.57,36.49,-81.97,39.15"

# ─── PRECHECKS ──────────────────────────────────────────────────────────────────
if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "❌ DATABASE_URL not set"; exit 1
fi
command -v psql >/dev/null || { echo "❌ psql required"; exit 1; }

iso_nodash(){ tr -d '-' <<<"$1"; }
sql(){ psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -Atqc "$1"; }

chunk_loop(){
  python - <<PY
from datetime import date, timedelta
s=date.fromisoformat("$START"); e=date.fromisoformat("$END"); d=$CHUNK_DAYS
cur=s
while cur<e:
  nxt=min(cur+timedelta(days=d), e)
  print(cur, nxt)
  cur=nxt
PY
}

# ─── MAIN LOOP ──────────────────────────────────────────────────────────────────
for STATE in "${STATES[@]}"; do
  echo; echo "===== ▶ $STATE ====="; echo
  BB=${BBOX[$STATE]}
  echo "Using bbox: $BB"

  CHUNK_TABLES=()

  while read -r S E; do
    [[ -z "$S" || -z "$E" ]] && continue
    SNO=$(iso_nodash "$S"); ENO=$(iso_nodash "$E")
    TBL="swdi_${DATASET}_${SNO}_${ENO}"

    echo "Fetching $DATASET $S → $E (table: $TBL)…"
    if python ingest/fetch_and_load_swdi.py --start "$S" --end "$E" --bbox="$BB" --datasets "$DATASET"; then
      # Verify table exists and has rows
      EXISTS=$(sql "SELECT to_regclass('public.${TBL}') IS NOT NULL")
      if [[ "$EXISTS" == "t" ]]; then
        CNT=$(sql "SELECT COUNT(*) FROM public.${TBL}")
        if (( CNT > 0 )); then
          CHUNK_TABLES+=("$TBL")
        else
          echo "⚠️ $TBL exists but empty; skipping."
        fi
      else
        echo "⚠️ $TBL missing; skipping."
      fi
    else
      echo "⚠️ Fetch failed for $S → $E; skipping."
    fi
  done < <(chunk_loop)

  if (( ${#CHUNK_TABLES[@]} == 0 )); then
    echo "❌ No SWDI tables fetched for $STATE; skipping."
    continue
  fi

  STARTNO=$(iso_nodash "$START"); ENDNO=$(iso_nodash "$END")
  UNION_VIEW="swdi_${DATASET}_${STATE}_${STARTNO}_${ENDNO}"

  # Build union over verified tables only
  UNION_SQL="CREATE OR REPLACE VIEW public.\"${UNION_VIEW}\" AS "
  for i in "${!CHUNK_TABLES[@]}"; do
    T="${CHUNK_TABLES[$i]}"
    SEP=$([[ $i -gt 0 ]] && echo " UNION ALL " || echo "")
    UNION_SQL+="${SEP}SELECT * FROM public.\"${T}\""
  done
  UNION_SQL+=";"
  sql "$UNION_SQL"

  # Hail clustering → dated table + stable alias
  HAIL_OUT="hail_cluster_boundaries_${STATE}_${STARTNO}_${ENDNO}"
  python cluster/cluster_hail.py \
    --source-table "${UNION_VIEW}" \
    --dest-table   "${HAIL_OUT}" \
    --eps          "${HAIL_EPS}" \
    --min-samples  "${HAIL_MIN_SAMPLES}"

  sql "CREATE OR REPLACE VIEW public.hail_cluster_boundaries_${STATE} AS SELECT * FROM public.\"${HAIL_OUT}\";"

  # Address clustering → dated table + stable alias
  ADDR_OUT="address_clusters_${STATE}_${STARTNO}_${ENDNO}"
  python cluster/cluster_addresses.py \
    --hail-cluster-table "hail_cluster_boundaries_${STATE}" \
    --address-table      "addresses" \
    --dest-table         "${ADDR_OUT}" \
    --buffer             "${ADDR_BUFFER}" \
    --eps                "${ADDR_EPS}" \
    --min-samples        "${ADDR_MIN_SAMPLES}"

  sql "CREATE OR REPLACE VIEW public.address_clusters_${STATE} AS SELECT * FROM public.\"${ADDR_OUT}\";"

  # Export
  python ingest/query_addresses.py \
    --storm-table   "hail_cluster_boundaries_${STATE}" \
    --address-table "addresses" \
    --output        "addresses_in_event_${STATE}_${STARTNO}_${ENDNO}.geojson"

  echo "✅ Done $STATE"
done

echo "🎉 All states done!"