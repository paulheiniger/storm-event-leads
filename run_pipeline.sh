#!/usr/bin/env bash
set -euo pipefail

# ─── CONFIG ─────────────────────────────────────────────────────────────────────

# 1-year window
START=2024-01-01
END=2024-12-31

# Datasets and clustering params
DATASET=nx3hail
HAIL_EPS=0.1
HAIL_MIN_SAMPLES=5
ADDR_BUFFER=0.02
ADDR_EPS=0.001
ADDR_MIN_SAMPLES=10

# Ensure your DATABASE_URL is set:
#   export DATABASE_URL=postgresql://user:pass@host:port/dbname

# Per‐state bounding-boxes (minLon,minLat,maxLon,maxLat)
declare -A BBOX
BBOX[GA]="-85.61,30.36,-80.84,35.00"
BBOX[IN]="-88.10,37.70,-84.79,41.76"
BBOX[OH]="-84.82,38.40,-80.52,41.98"
BBOX[KY]="-89.57,36.49,-81.97,39.15"

# ─── LOOP OVER STATES ────────────────────────────────────────────────────────────

for STATE in GA IN OH KY; do
  echo
  echo "======================================================"
  echo "  ▶ Running pipeline for $STATE"
  echo "======================================================"
  echo

  BB=${BBOX[$STATE]}

  # 1) Fetch & load SWDI hail shapefile
  python ingest/fetch_and_load_swdi.py \
    --start "$START" --end "$END" \
    --bbox "$BB" \
    --datasets $DATASET

  SWDI_TABLE=swdi_${DATASET}_${START//-/}${END//-/}

  # 2) Cluster hail points into polygons
  python cluster/cluster_hail.py \
    --source-table $SWDI_TABLE \
    --dest-table hail_cluster_boundaries_$STATE \
    --eps $HAIL_EPS \
    --min-samples $HAIL_MIN_SAMPLES

  # 3) Cluster addresses around hail clusters
  python cluster/cluster_addresses.py \
    --hail-cluster-table hail_cluster_boundaries_$STATE \
    --address-table      addresses \
    --dest-table         address_clusters_$STATE \
    --buffer             $ADDR_BUFFER \
    --eps                $ADDR_EPS \
    --min-samples        $ADDR_MIN_SAMPLES

  # 4) Export addresses‐in‐event to GeoJSON
  python ingest/query_addresses.py \
    --storm-table   hail_cluster_boundaries_$STATE \
    --address-table addresses \
    --output        addresses_in_event_${STATE}.geojson

  echo "✅ Pipeline complete for $STATE — generated addresses_in_event_${STATE}.geojson"
done

echo
echo "🎉 All states done!"