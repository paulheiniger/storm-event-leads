#!/opt/homebrew/bin/bash
set -Eeuo pipefail
IFS=$'\n\t'

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
START=2024-01-01
END=2025-08-01

DATASET=nx3hail
HAIL_EPS=0.1
HAIL_MIN_SAMPLES=5
ADDR_BUFFER=0.02
ADDR_EPS=0.001
ADDR_MIN_SAMPLES=10

# Return a single-token bbox for a state (minLon,minLat,maxLon,maxLat)
bbox_for() {
  case "$1" in
    GA) echo "-85.61,30.36,-80.84,35.00" ;;
    IN) echo "-88.10,37.70,-84.79,41.76" ;;
    OH) echo "-84.82,38.40,-80.52,41.98" ;;
    KY) echo "-89.57,36.49,-81.97,39.15" ;;
    *)  echo ""; return 1 ;;
  esac
}

# ─── LOOP OVER STATES ───────────────────────────────────────────────────────────
for STATE in GA IN OH KY; do
  echo
  echo "======================================================"
  echo "  ▶ Running pipeline for $STATE"
  echo "======================================================"
  echo

  BB="$(bbox_for "$STATE")"
  if [[ -z "$BB" ]]; then
    echo "ERROR: no bbox configured for $STATE" >&2
    exit 1
  fi
  echo "Using bbox: $BB"

  # 1) Fetch & load SWDI hail shapefile
  python ingest/fetch_and_load_swdi.py \
    --start="$START" --end="$END" \
    --bbox="$BB" \
    --datasets="$DATASET"

  SWDI_TABLE="swdi_${DATASET}_${START//-/}${END//-/}"

  # 2) Cluster hail points into polygons
  python cluster/cluster_hail.py \
    --source-table "$SWDI_TABLE" \
    --dest-table "hail_cluster_boundaries_$STATE" \
    --eps "$HAIL_EPS" \
    --min-samples "$HAIL_MIN_SAMPLES"

  # 3) Cluster addresses around hail clusters
  python cluster/cluster_addresses.py \
    --hail-cluster-table "hail_cluster_boundaries_$STATE" \
    --address-table "addresses" \
    --dest-table "address_clusters_$STATE" \
    --buffer "$ADDR_BUFFER" \
    --eps "$ADDR_EPS" \
    --min-samples "$ADDR_MIN_SAMPLES"

  # 4) (Optional) kick off skip-trace batch
  # python ingest/fetch_skip_trace_async.py --batch 100

  # 5) Export addresses in event to GeoJSON
  python ingest/query_addresses.py \
    --storm-table   "hail_cluster_boundaries_$STATE" \
    --address-table "addresses" \
    --output        "addresses_in_event_${STATE}.geojson"

  echo "✅ Pipeline complete for $STATE — wrote addresses_in_event_${STATE}.geojson"
done

echo
echo "🎉 All states done!"