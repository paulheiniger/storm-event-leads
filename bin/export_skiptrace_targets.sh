#!/usr/bin/env bash
set -Eeuo pipefail

# -------- Defaults (override via flags) --------
STATE=""
CENTER=""                 # "lon,lat" (required)
RADIUS_KM="40"            # ~25 miles
DIST_M="200"              # address must be within X meters of hail polygon
TARGET="1000"             # max rows to export
HAIL_TABLE=""             # default: hail_cluster_boundaries_${STATE,,}
ADDR_TABLE="addresses"
OUTFILE=""                # default under exports/
INCLUDE_MULTIUNITS="0"    # 0=exclude APT/UNIT/STE/#  | 1=allow multi-unit addresses
CREATE_INDEXES="1"        # 1=create helpful indexes first
SOURCE="clusters"         # reserved for future (clusters vs points)

usage() {
  cat <<USAGE
Usage: $0 --state KY --center "-85.7585,38.2527" [options]
  --state STATE
  --center "lon,lat"
  --radius-km N        (default: ${RADIUS_KM})
  --dist-m N           (default: ${DIST_M})
  --target N           (default: ${TARGET})
  --hail-table NAME    (default: hail_cluster_boundaries_\${state})
  --addr-table NAME    (default: ${ADDR_TABLE})
  --outfile PATH       (default: exports/…)
  --include-multiunits (keep APT/UNIT/STE/#)
  --no-index           (skip index creation)
  --source clusters    (reserved; default: clusters)
USAGE
  exit 1
}

# -------- Parse Args --------
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
    --include-multiunits) INCLUDE_MULTIUNITS="1"; shift 1 ;;
    --no-index) CREATE_INDEXES="0"; shift 1 ;;
    --source) SOURCE="$2"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "Unknown arg: $1"; usage ;;
  esac
done

# -------- Preflight --------
: "${DATABASE_URL:?Set DATABASE_URL first, e.g. export DATABASE_URL=postgresql://user:pass@host:5432/storm_leads}"
[[ -z "$STATE" || -z "$CENTER" ]] && usage

IFS=',' read -r LON LAT <<<"$(echo "$CENTER" | tr -d ' ')"
[[ -z "$LON" || -z "$LAT" ]] && { echo "Invalid --center; use \"lon,lat\""; exit 2; }

RADIUS_M=$(python - <<PY
print(int(round(float("$RADIUS_KM")*1000)))
PY
)

LOWSTATE=$(echo "$STATE" | tr '[:upper:]' '[:lower:]')
[[ -z "$HAIL_TABLE" ]] && HAIL_TABLE="hail_cluster_boundaries_${LOWSTATE}"

mkdir -p exports
STAMP=$(date +%Y%m%d-%H%M%S)
[[ -z "$OUTFILE" ]] && OUTFILE="exports/skiptrace_${STATE}_${STAMP}_${RADIUS_KM}km_${DIST_M}m.csv"

# temp names + index names (short, collision-resistant)
SUFFIX=$(LC_ALL=C tr -dc 'A-F0-9' </dev/urandom | head -c6)
HAIL_TMP="__hail_src_${SUFFIX}"
ADDR_TMP="__addr_src_${SUFFIX}"
EXPORT_TMP="__export_${SUFFIX}"
EXPORT_OUT="__export_out_${SUFFIX}"
HAIL_IDX="idx_${HAIL_TMP}_geom"
ADDR_IDX="idx_${ADDR_TMP}_geom"

echo "▶ STATE=$STATE  CENTER=$LON,$LAT  RADIUS=${RADIUS_KM}km  DIST=${DIST_M}m  TARGET=${TARGET}"
echo "▶ SOURCE=${SOURCE}"
echo "▶ HAIL_TABLE=$HAIL_TABLE  ADDR_TABLE=$ADDR_TABLE"
echo "▶ OUTFILE=$OUTFILE"
echo

# -------- Optional: helpful global index on addresses --------
if [[ "$CREATE_INDEXES" == "1" ]]; then
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
CREATE INDEX IF NOT EXISTS idx_addresses_geom
  ON public.addresses USING GIST (geom);
SQL
fi

# -------- Main SQL --------
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -v LON="$LON" -v LAT="$LAT" -v RADIUS_M="$RADIUS_M" \
  -v HAIL_TABLE="$HAIL_TABLE" -v ADDR_TABLE="$ADDR_TABLE" \
  -v STATE="$STATE" -v INCLUDE_MULTIUNITS="$INCLUDE_MULTIUNITS" \
  -v DIST_M="$DIST_M" -v TARGET="$TARGET" -v SOURCE="$SOURCE" \
  -v OUTFILE="$OUTFILE" \
  -v HAIL_TMP="$HAIL_TMP" -v ADDR_TMP="$ADDR_TMP" \
  -v EXPORT_TMP="$EXPORT_TMP" -v EXPORT_OUT="$EXPORT_OUT" \
  -v HAIL_IDX="$HAIL_IDX" -v ADDR_IDX="$ADDR_IDX" <<'SQL'
\set ON_ERROR_STOP on
\pset tuples_only on

-- Clean any leftovers for this run's suffix
DROP TABLE IF EXISTS :"HAIL_TMP" CASCADE;
DROP TABLE IF EXISTS :"ADDR_TMP" CASCADE;
DROP TABLE IF EXISTS :"EXPORT_TMP" CASCADE;
DROP TABLE IF EXISTS :"EXPORT_OUT" CASCADE;

-- Detect hail geometry column
SELECT CASE
  WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name=lower(:'HAIL_TABLE') AND column_name='geometry'
  ) THEN 'geometry'
  WHEN EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name=lower(:'HAIL_TABLE') AND column_name='geom'
  ) THEN 'geom'
  ELSE ''
END AS geocol \gset

\if :'geocol' = ''
  \echo ❌ Could not find geometry column on :HAIL_TABLE (looked for geometry/geom)
  \quit 3
\endif

-- Build hail temp (alias to 'geom', include times if present)
CREATE TEMP TABLE :"HAIL_TMP" AS
WITH params AS (
  SELECT ST_Transform(
           ST_Buffer(
             ST_Transform(ST_SetSRID(ST_MakePoint(:LON, :LAT), 4326), 3857),
             :RADIUS_M
           ),
           4326
         ) AS target_area
)
SELECT
  h.cluster_id,
  h.:"geocol" AS geom,
  -- carry time columns if they exist (NULL if not present)
  (SELECT CASE WHEN EXISTS (
     SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name=lower(:'HAIL_TABLE') AND column_name='start_time'
   ) THEN h.start_time ELSE NULL END) AS start_time,
  (SELECT CASE WHEN EXISTS (
     SELECT 1 FROM information_schema.columns
     WHERE table_schema='public' AND table_name=lower(:'HAIL_TABLE') AND column_name='end_time'
   ) THEN h.end_time ELSE NULL END)   AS end_time
FROM public.:"HAIL_TABLE" h, params p
WHERE ST_Intersects(h.:"geocol", p.target_area);

-- Addresses temp (pre-dedup + filters)
CREATE TEMP TABLE :"ADDR_TMP" AS
WITH params AS (
  SELECT ST_Transform(
           ST_Buffer(
             ST_Transform(ST_SetSRID(ST_MakePoint(:LON, :LAT), 4326), 3857),
             :RADIUS_M
           ),
           4326
         ) AS target_area
)
SELECT DISTINCT ON (lower(coalesce(a.address,'')), coalesce(a.zip,''))
  a.id, a.address, a.street, a.city, a.state, a.zip, a.geom
FROM public.:"ADDR_TABLE" a, params p
WHERE a.state = :'STATE'
  AND a.address IS NOT NULL
  AND a.address !~* 'PO[[:space:]]*BOX'
  AND (
    :'INCLUDE_MULTIUNITS'::int = 1
    OR a.address !~* '(APT|UNIT|STE|SUITE|#[[:space:]]*[0-9]+)'
  )
  AND ST_Intersects(a.geom, p.target_area)
ORDER BY lower(coalesce(a.address,'')), coalesce(a.zip,''), a.id;

-- Temp indexes on geoms (using psql identifier vars)
CREATE INDEX IF NOT EXISTS :"HAIL_IDX" ON :"HAIL_TMP" USING GIST (geom);
CREATE INDEX IF NOT EXISTS :"ADDR_IDX" ON :"ADDR_TMP" USING GIST (geom);
ANALYZE :"HAIL_TMP";
ANALYZE :"ADDR_TMP";

-- Export candidates with distance + recency
CREATE TEMP TABLE :"EXPORT_TMP" AS
SELECT
  a.id,
  a.address,
  a.street,
  a.city,
  a.state,
  a.zip,
  ST_X(a.geom) AS lon,
  ST_Y(a.geom) AS lat,
  h.cluster_id,
  COALESCE(h.end_time, h.start_time) AS storm_time,
  ROUND(ST_Distance(a.geom::geography, h.geom::geography)::numeric, 1) AS distance_m
FROM :"ADDR_TMP" a
JOIN :"HAIL_TMP" h
  ON ST_DWithin(a.geom::geography, h.geom::geography, :DIST_M);

-- Final pre-limited set (favor recent storms, then closer, then id)
CREATE TEMP TABLE :"EXPORT_OUT" AS
SELECT *
FROM :"EXPORT_TMP"
ORDER BY storm_time DESC NULLS LAST, distance_m ASC, id
LIMIT :TARGET;

-- Persist request metadata for traceability
CREATE TABLE IF NOT EXISTS public.skiptrace_requests(
  request_id     bigserial PRIMARY KEY,
  created_at     timestamptz DEFAULT now(),
  state          text NOT NULL,
  center_lon     double precision NOT NULL,
  center_lat     double precision NOT NULL,
  radius_km      double precision NOT NULL,
  dist_m         double precision NOT NULL,
  target_n       integer NOT NULL,
  hail_table     text NOT NULL,
  addr_table     text NOT NULL,
  source         text NOT NULL,
  out_path       text NOT NULL
);

INSERT INTO public.skiptrace_requests
(state, center_lon, center_lat, radius_km, dist_m, target_n, hail_table, addr_table, source, out_path)
VALUES ( :'STATE', :LON, :LAT, (:RADIUS_M/1000.0), :DIST_M, :TARGET, :'HAIL_TABLE', :'ADDR_TABLE', :'SOURCE', :'OUTFILE');

\echo Writing to :'OUTFILE'
\copy public.:"EXPORT_OUT" TO :'OUTFILE' CSV HEADER
SQL

echo
echo "✅ Wrote $(wc -l < "$OUTFILE" | tr -d ' ') lines to $OUTFILE"

# ---- Log this export in DB ---------------------------------------------------
# Count rows (minus header)
LINES=$( (wc -l < "$OUTFILE" 2>/dev/null) || echo 0 )
EXPORTED_ROWS=$(( LINES > 0 ? LINES - 1 : 0 ))

# Create table if needed and add 'exported_rows' if missing
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
CREATE TABLE IF NOT EXISTS public.skiptrace_runs (
  run_id           bigserial PRIMARY KEY,
  created_at       timestamptz NOT NULL DEFAULT now(),
  state            text        NOT NULL,
  center_lon       double precision NOT NULL,
  center_lat       double precision NOT NULL,
  radius_km        double precision NOT NULL,
  dist_m           integer     NOT NULL,
  target           integer,
  output_path      text        NOT NULL,
  source           text,
  hail_table       text,
  addr_table       text,
  hail_eps         double precision,
  hail_min_samples integer,
  addr_buffer      double precision,
  addr_eps         double precision,
  addr_min_samples integer,
  start_date       date,
  end_date         date,
  bbox             text,
  notes            text
);
ALTER TABLE public.skiptrace_runs
  ADD COLUMN IF NOT EXISTS exported_rows integer;
COMMIT;
SQL

# Insert row and capture run_id
RUN_ID=$(
  psql "$DATABASE_URL" -tA \
    -v st="$STATE" -v lon="$LON" -v lat="$LAT" \
    -v rkm="$RADIUS_KM" -v dm="$DIST_M" \
    -v tgt="${TARGET:-}" -v out="$OUTFILE" \
    -v src="${SOURCE:-clusters}" \
    -v hail="$HAIL_TABLE" -v addr="$ADDR_TABLE" \
    -v rows="$EXPORTED_ROWS" <<'SQL'
WITH ins AS (
  INSERT INTO public.skiptrace_runs
    (state, center_lon, center_lat, radius_km, dist_m, target,
     output_path, source, hail_table, addr_table, exported_rows)
  VALUES
    ( :'st'
    , :'lon'::float8
    , :'lat'::float8
    , :'rkm'::float8
    , :'dm'::int
    , NULLIF(:'tgt','')::int
    , :'out'
    , :'src'
    , :'hail'
    , :'addr'
    , NULLIF(:'rows','')::int
    )
  RETURNING run_id
)
SELECT run_id FROM ins;
SQL
)
RUN_ID="${RUN_ID//$'\n'/}"
echo "🧾 Logged skiptrace run_id=${RUN_ID:-?}  (${EXPORTED_ROWS} rows) → $OUTFILE"