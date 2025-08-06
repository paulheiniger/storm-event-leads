# Storm Event Leads Pipeline

This repository contains the scaffolding for a storm event lead generation tool:

1. Ingest storm events from NOAA
2. Store events in Postgres+PostGIS
3. Plot event boundaries on a map
4. Lookup addresses within boundaries
5. Pull property data
6. Skip-trace via BulkData API

Follow the README in each module for usage.

## Pipeline Overview

```mermaid
flowchart TD
  subgraph Ingestion_and_Clustering
    A1["fetch_swdi_shapefile.py
(dataset=nx3hail
start=2025-07-01
end=2025-07-28
bbox=-85.05,33.40,-83.55,34.35)"]
    A1 --> A2[PostGIS: swdi_nx3hail_20250701_20250728]

    B1["cluster_hail.py
source=swdi_nx3hail_20250701_20250728
dest=hail_cluster_boundaries_atlanta
eps=0.1
min_samples=5"]
    B1 --> B2[PostGIS: hail_cluster_boundaries_atlanta]

    C1["ingest_openaddresses.py
folders=us_south,us_west,us_midwest,us_northeast
table=addresses"]
    C1 --> C2[PostGIS: addresses]

    D1["cluster_addresses.py
hail_cluster_table=hail_cluster_boundaries_atlanta
address_table=addresses
dest=address_clusters_atlanta
buffer=0.02
eps=0.001
min_samples=10"]
    D1 --> D2[PostGIS: address_clusters_atlanta]
  end

  subgraph Enrichment
    E1["fetch_property_data.py
address_table=addresses
property_table=properties
owner_table=owners
filter=ST_Intersects(geom,…)
batch_size=100"]
    E1 --> E2[PostGIS: properties & owners]

    F1["fetch_skip_trace.py
address_table=addresses
skiptrace_table=skip_traces
filter=ST_Intersects(geom,…)
batch_size=100"]
    F1 --> F2[PostGIS: skip_traces]
  end

  subgraph Visualization
    G1["plot_data.py / dashboards
reads hail & address clusters
and enrichment tables"]
    G1 --> G2[Interactive Maps & Reports]
  end