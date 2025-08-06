#!/usr/bin/env python3
import os
import sys
import geopandas as gpd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DB_URL         = os.getenv("DATABASE_URL")
CLUSTER_TABLE  = "address_clusters_jefferson"  # ← change to your actual table name
ADDRESS_TABLE  = "addresses"

if not DB_URL:
    print("Error: set DATABASE_URL in your environment")
    sys.exit(1)

engine = create_engine(DB_URL)

# ─── LOAD DATA ──────────────────────────────────────────────────────────────────
try:
    clusters = gpd.read_postgis(
        f"SELECT hail_cluster_id, addr_cluster_id, num_addresses, geom FROM {CLUSTER_TABLE}",
        engine, geom_col="geom"
    )
except Exception as e:
    print(f"Could not load clusters from {CLUSTER_TABLE}: {e}")
    sys.exit(1)

try:
    homes = gpd.read_postgis(
        f"SELECT id, geom FROM {ADDRESS_TABLE}",
        engine, geom_col="geom"
    )
except Exception as e:
    print(f"Could not load addresses from {ADDRESS_TABLE}: {e}")
    sys.exit(1)

# ─── PLOT ───────────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 12))

# Plot clustered hulls
clusters.plot(
    ax=ax,
    column="hail_cluster_id",
    cmap="tab20",
    alpha=0.3,
    edgecolor="black",
    linewidth=1,
    legend=True,
    legend_kwds={"title": "Hail Cluster ID"}
)

# Plot home points
homes.plot(
    ax=ax,
    marker="o",
    markersize=2,
    color="red",
    label="Homes"
)

ax.set_title("Address Clusters & Homes", fontsize=16)
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
ax.legend()

plt.tight_layout()
plt.show()