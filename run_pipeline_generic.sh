#!/opt/homebrew/bin/bash
set -Eeuo pipefail

# ===== Config =====
START=${START:-2024-01-01}
END=${END:-2025-08-01}
DATASET=${DATASET:-nx3hail}

# Hail clustering
HAIL_EPS=${HAIL_EPS:-0.03}
HAIL_MIN_SAMPLES=${HAIL_MIN_SAMPLES:-6}

# Address clustering
ADDR_BUFFER=${ADDR_BUFFER:-0.01}
ADDR_EPS=${ADDR_EPS:-0.0006}
ADDR_MIN_SAMPLES=${ADDR_MIN_SAMPLES:-8}

# States to run
STATES=("GA" "IN" "OH" "KY")

# BBoxes (minLon,minLat,maxLon,maxLat)
declare -A BBOX
BBOX[GA]="-85.61,30.36,-80.84,35.00"
BBOX[IN]="-88.10,37.70,-84.79,41.76"
BBOX[OH]="-84.82,38.40,-80.52,41.98"
BBOX[KY]="-89.57,36.49,-81.97,39.15"

# ===== Flags =====
FORCE=${FORCE:-0}       # set FORCE=1 for a clean run
if [[ "${1:-}" == "--force" ]]; then FORCE=1; shift; fi

# ===== Preflight =====
: "${DATABASE_URL:?Set DATABASE_URL first, e.g. export DATABASE_URL=postgresql://user:pass@host:5432/storm_leads}"
mkdir -p maps

# Helpers
exists_table_or_view () {
  local name="$1"
  psql "$DATABASE_URL" -tA -c "SELECT to_regclass('public.${name}')" | grep -q .
}
drop_view () {
  local v="$1"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "DROP VIEW IF EXISTS public.${v} CASCADE;" >/dev/null
}
drop_table () {
  local t="$1"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "DROP TABLE IF EXISTS public.${t} CASCADE;" >/dev/null
}

# Build ~30-day windows
readarray -t WINDOWS < <(
python - <<PY
from datetime import date,timedelta
s=date.fromisoformat("$START"); e=date.fromisoformat("$END")
d=s
out=[]
while d<e:
    nx=min(d+timedelta(days=30), e)
    out.append(f"{d.isoformat()}:{nx.isoformat()}")
    d=nx
print("\\n".join(out))
PY
)

STARTNS="${START//-/}"
ENDNS="${END//-/}"

# One-time-safe index normalization (rename any generic idx onto state tables)
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT i.indexname AS old_idx, i.tablename AS tbl
    FROM pg_indexes i
    WHERE i.schemaname='public'
      AND i.indexname ~ '^idx_swdi_nx3hail_[0-9]{8}_[0-9]{8}_geometry$'
      AND i.tablename ~ '^swdi_nx3hail_(ga|in|oh|ky)_[0-9]{8}_[0-9]{8}$'
  LOOP
    EXECUTE format('ALTER INDEX public.%I RENAME TO %I',
                   r.old_idx,
                   'idx_'||r.tbl||'_geometry');
  END LOOP;
END $$;
SQL

# ===== Force clean (drops EVERYTHING for the selected states & range) =====
if [[ "$FORCE" == "1" ]]; then
  echo "🧹 Force clean enabled — dropping old views/tables/indexes/maps for ${STATES[*]} ${START}→${END}"

  # Drop consolidated + cluster + address artifacts
  for S in "${STATES[@]}"; do
    drop_view "swdi_nx3hail_${S,,}_${STARTNS}_${ENDNS}" || true
    drop_view "hail_cluster_boundaries_${S,,}" || true
    drop_table "hail_cluster_boundaries_${S,,}_${STARTNS}_${ENDNS}" || true
    drop_table "address_clusters_${S,,}_${STARTNS}_${ENDNS}" || true
  done

  # Drop per-state swdi window tables and generic ones
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<SQL || true
DO \$\$
DECLARE r record;
BEGIN
  -- state-scoped swdi tables
  FOR r IN
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='public'
      AND table_name ~ '^swdi_nx3hail_(ga|in|oh|ky)_[0-9]{8}_[0-9]{8}$'
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE;', r.table_name);
  END LOOP;

  -- generic swdi tables (any windows)
  FOR r IN
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='public'
      AND table_name ~ '^swdi_nx3hail_[0-9]{8}_[0-9]{8}$'
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE;', r.table_name);
  END LOOP;
END \$\$;
SQL

  # Nuke maps for this range
  rm -f "maps/"*"_clusters_${STARTNS}_${ENDNS}.html" || true
fi

# ===== Main loop =====
for STATE in "${STATES[@]}"; do
  echo
  echo "================= $STATE ================="
  echo "Date range: $START → $END  | dataset=${DATASET}  | bbox=${BBOX[$STATE]}"
  echo

  CONS_VIEW="swdi_nx3hail_${STATE,,}_${STARTNS}_${ENDNS}"
  drop_view "$CONS_VIEW" || true

  # 1) Fetch each 30-day window, rename to state, fix index name
  for W in "${WINDOWS[@]}"; do
    S="${W%%:*}"
    E="${W##*:}"
    Sns="${S//-/}"; Ens="${E//-/}"
    SRC="swdi_nx3hail_${Sns}_${Ens}"             # generic name the fetcher uses
    DST="swdi_nx3hail_${STATE,,}_${Sns}_${Ens}"  # per-state table

    if exists_table_or_view "$DST"; then
      if [[ "$FORCE" == "1" ]]; then
        drop_table "\"$DST\"" || true
      else
        echo "↷ exists: $DST (skipping fetch)"
        continue
      fi
    fi

    # pre-drop generic to avoid dependency errors
    drop_table "\"$SRC\"" || true

    echo "Fetching ${DATASET} $S → $E (bbox=${BBOX[$STATE]}) → $DST"
    python ingest/fetch_and_load_swdi.py \
      --start "$S" --end "$E" \
      --bbox="${BBOX[$STATE]}" \
      --datasets "$DATASET" || true

    # Rename generic table (if created) -> state-scoped; then rename its index
    if exists_table_or_view "$SRC"; then
      psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
        "ALTER TABLE public.\"$SRC\" RENAME TO \"$DST\";" || true

      psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<SQL || true
DO \$\$
DECLARE
  old_idx text := 'idx_' || '$SRC' || '_geometry';
  new_idx text := 'idx_' || '$DST' || '_geometry';
BEGIN
  IF EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname=old_idx) THEN
    EXECUTE format('ALTER INDEX public.%I RENAME TO %I', old_idx, new_idx);
  END IF;
END \$\$;
SQL
    fi
  done

  # 2) Consolidated view per state
  echo "Building view $CONS_VIEW ..."
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<SQL
DO \$\$
DECLARE q text;
BEGIN
  SELECT
    'CREATE VIEW public.${CONS_VIEW} AS ' ||
    string_agg(format('SELECT * FROM public.%I', table_name), ' UNION ALL ' ORDER BY table_name)
  INTO q
  FROM information_schema.tables
  WHERE table_schema='public'
    AND table_name ~ '^swdi_nx3hail_${STATE,,}_[0-9]{8}_[0-9]{8}\$';

  IF q IS NULL THEN
    RAISE NOTICE 'No swdi_nx3hail_${STATE,,}_* tables found for ${STATE}; skipping state.';
    RETURN;
  END IF;
  q := q || ';';
  EXECUTE q;
END \$\$;
SQL

  if ! exists_table_or_view "$CONS_VIEW"; then
    echo "❌ No consolidated SWDI view for $STATE — skipping."
    continue
  fi

  # 3) Hail clustering (always rebuild if FORCE)
  HAIL_OUT="hail_cluster_boundaries_${STATE,,}_${STARTNS}_${ENDNS}"
  if [[ "$FORCE" == "1" ]]; then drop_table "$HAIL_OUT" || true; fi
  if exists_table_or_view "$HAIL_OUT"; then
    echo "↷ exists: $HAIL_OUT (skipping hail clustering)"
  else
    echo "Clustering hail → $HAIL_OUT ..."
    python cluster/cluster_hail.py \
      --source-table "$CONS_VIEW" \
      --dest-table   "$HAIL_OUT" \
      --eps "$HAIL_EPS" --min-samples "$HAIL_MIN_SAMPLES"
  fi

  drop_view "hail_cluster_boundaries_${STATE,,}" || true
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
    "CREATE VIEW public.hail_cluster_boundaries_${STATE,,} AS SELECT * FROM public.${HAIL_OUT};"

  # 4) Address clusters (always rebuild if FORCE)
  ADDR_OUT="address_clusters_${STATE,,}_${STARTNS}_${ENDNS}"
  if [[ "$FORCE" == "1" ]]; then drop_table "$ADDR_OUT" || true; fi
  if exists_table_or_view "$ADDR_OUT"; then
    echo "↷ exists: $ADDR_OUT (skipping address clustering)"
  else
    echo "Clustering addresses → $ADDR_OUT ..."
    python cluster/cluster_addresses.py \
      --hail-cluster-table "$HAIL_OUT" \
      --address-table      addresses \
      --dest-table         "$ADDR_OUT" \
      --buffer "$ADDR_BUFFER" --eps "$ADDR_EPS" --min-samples "$ADDR_MIN_SAMPLES"
  fi

  # 5) Map export (overwrite if FORCE)
  OUT_HTML="maps/${STATE,,}_clusters_${STARTNS}_${ENDNS}.html"
  if [[ "$FORCE" == "1" && -f "$OUT_HTML" ]]; then rm -f "$OUT_HTML"; fi
  if [[ -f "$OUT_HTML" ]]; then
    echo "↷ exists: $OUT_HTML (skipping map render)"
  else
    echo "Rendering map → $OUT_HTML ..."
    python visualization/plot_clusters_map.py \
      --hail-cluster-table "$HAIL_OUT" \
      --addr-cluster-table "$ADDR_OUT" \
      --out "$OUT_HTML" || echo "⚠️ map render failed (continuing)"
  fi

  echo "✅ Done $STATE"
done

echo
echo "🎉 All states done!"