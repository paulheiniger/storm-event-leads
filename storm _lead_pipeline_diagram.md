```mermaid
---
config:
  theme: default
  look: classic
  layout: elk
---
flowchart TD
 subgraph Ingestion["Ingestion"]
        A2["Build Point GeoDataFrames"]
        A1["Fetch SWDI CSVs/Shapefiles"]
        A3["Compute Convex Hulls - storm event boundaries"]
        A4["PostGIS: storm_event_boundaries"]
        B2["Parse & Normalize Addresses"]
        B1["Collect OA GeoJSON files"]
        B3["Filter & Centroid non-Points"]
        B4["PostGIS: addresses"]
  end
 subgraph Clustering["Clustering"]
        C2["DBSCAN on hail points"]
        C1["Read swdi_nx3hail table"]
        C3["Compute hail clusters hulls"]
        C4["PostGIS: hail_cluster_boundaries"]
        C5["Read hail_cluster_boundaries & addresses"]
        C6["Buffer centroids & DBSCAN addr points"]
        C7["Compute address cluster hulls"]
        C8["PostGIS: address_clusters"]
  end
 subgraph Enrichment["Enrichment"]
        D2["BatchData API: property search"]
        D1["address_clusters"]
        D3["PostGIS: properties"]
        D4["BatchData API: skip trace async"]
        D5["PostGIS: skip_traces"]
  end
 subgraph Visualization_Outreach["Visualization_Outreach"]
        E2["plot_most_recent_storms.py"]
        E1["Query storm_event_boundaries"]
        E3["plot_event_boundaries.py"]
        E4["plot_hail_clusters.py"]
        E5["plot_address_clusters.py"]
        F1["Generate maps & reports"]
        G1["AI-driven call scheduler - fetch appointments & owners"]
  end
    A1 --> A2
    A2 --> A3
    A3 -- to_postgis --> A4
    B1 --> B2
    B2 --> B3
    B3 -- to_postgis --> B4
    C1 --> C2
    C2 --> C3
    C3 -- to_postgis --> C4
    C4 --> C5 & E4
    C5 --> C6
    C6 --> C7
    C7 -- to_postgis --> C8
    D1 --> D2
    D2 --> D3
    D3 --> D4
    D4 --> D5
    E1 --> E2 & E3
    C8 --> E5
    E2 --> F1
    E3 --> F1
    E4 --> F1
    E5 --> F1
    F1 --> G1
    style Ingestion fill:#f9f,stroke:#333,stroke-width:2px
    style Clustering fill:#9ff,stroke:#333,stroke-width:2px
    style Enrichment fill:#ff9,stroke:#333,stroke-width:2px
    style Visualization_Outreach fill:#9f9,stroke:#333,stroke-width:2px
