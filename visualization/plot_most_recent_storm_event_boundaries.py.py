#!/usr/bin/env python3
import os
import sys
from datetime import datetime, timedelta

import geopandas as gpd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DB_URL     = os.getenv("DATABASE_URL")
TABLE      = "storm_event_boundaries"
LOOKBACK_DAYS = 30
MAX_EVENTS    = 20

if not DB_URL:
    print("Error: please set DATABASE_URL in your environment")
    sys.exit(1)

engine = create_engine(DB_URL)

# ─── STEP 1: List columns ────────────────────────────────────────────────────────
cols = engine.execute(
    text("SELECT column_name FROM information_schema.columns "
         "WHERE table_schema = 'public' AND table_name = :tbl"),
    {"tbl": TABLE}
).fetchall()
colnames = [c[0] for c in cols]
print("\nColumns in", TABLE, ":\n", colnames)

# ─── STEP 2: Show a sample ──────────────────────────────────────────────────────
print("\nSample rows (first 5):")
sample = gpd.read_postgis(
    f"SELECT * FROM {TABLE} LIMIT 5",
    engine,
    geom_col="geom",
    crs="EPSG:4326"
)
print(sample)

# ─── STEP 3: Pull & plot most recent ────────────────────────────────────────────
# Here we assume your timestamp column is called BEGIN_DATE_TIME
# (adjust if it’s named differently; eg. 'begin', 'begin_date', etc.)
ts_col = "BEGIN_DATE_TIME" if "BEGIN_DATE_TIME" in colnames else None
if not ts_col:
    print("\nNo 'BEGIN_DATE_TIME' column found—please pick the right timestamp field from above.")
    sys.exit(0)

since = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).isoformat()
sql = f"""
    SELECT *,
           {ts_col}::timestamp AS event_time
      FROM {TABLE}
     WHERE {ts_col} >= :since
     ORDER BY {ts_col} DESC
     LIMIT :limit
"""
recent = gpd.read_postgis(
    text(sql),
    engine,
    params={"since": since, "limit": MAX_EVENTS},
    geom_col="geom",
    crs="EPSG:4326"
)

if recent.empty:
    print(f"\nNo events in the last {LOOKBACK_DAYS} days.")
    sys.exit(0)

print("\nMost recent events:")
print(recent[[ts_col, "event_time"]])

# Plot boundaries
fig, ax = plt.subplots(1, 1, figsize=(10, 10))
recent.plot(
    ax=ax,
    column="event_time",
    cmap="viridis",
    edgecolor="black",
    alpha=0.5,
    legend=True,
    legend_kwds={"title": ts_col}
)
for _, row in recent.iterrows():
    c = row.geom.centroid
    label = row.event_time.strftime("%m-%d %H:%M")
    ax.text(c.x, c.y, label, fontsize=8, ha="center")
ax.set_title(f"Storm Boundaries in Last {LOOKBACK_DAYS} Days")
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")
plt.tight_layout()
plt.show()