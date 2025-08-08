#!/usr/bin/env python3
import os
import sys
import argparse
import json
import geopandas as gpd
import pandas as pd
from shapely.geometry import mapping
from sqlalchemy import create_engine, text
import folium

def get_geom_col(engine, table_name: str) -> str:
    q = text("""
        SELECT lower(column_name) AS column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
    """)
    with engine.connect() as conn:
        cols = pd.read_sql(q, conn, params={"t": table_name.lower()})["column_name"].tolist()
    for c in ("geom", "geometry"):
        if c in cols:
            return c
    raise RuntimeError(f"No geometry column ('geom' or 'geometry') on {table_name}")

def hex_color(n: int) -> str:
    # deterministic but varied-ish
    n = (n * 2654435761) & 0xFFFFFF
    return f"#{n:06x}"

def main():
    ap = argparse.ArgumentParser(description="Plot hail & address clusters to an interactive HTML map")
    ap.add_argument("--hail-cluster-table", required=True)
    ap.add_argument("--addr-cluster-table", required=True)
    ap.add_argument("--out", default="clusters_map.html")
    args = ap.parse_args()

    dburl = os.getenv("DATABASE_URL")
    if not dburl:
        sys.exit("DATABASE_URL not set")

    engine = create_engine(dburl)

    hail_col = get_geom_col(engine, args.hail_cluster_table)
    addr_col = get_geom_col(engine, args.addr_cluster_table)

    hail_sql = text(f"""
        SELECT cluster_id, {hail_col} AS geom
        FROM {args.hail_cluster_table}
    """)
    hail = gpd.read_postgis(hail_sql, engine, geom_col="geom")
    if hail.empty:
        sys.exit(f"No rows in {args.hail_cluster_table}")

    addr_sql = text(f"""
        SELECT hail_cluster_id, addr_cluster_id, num_addresses, {addr_col} AS geom
        FROM {args.addr_cluster_table}
    """)
    addr = gpd.read_postgis(addr_sql, engine, geom_col="geom")
    if addr.empty:
        print(f"[warn] No rows in {args.addr_cluster_table} (nothing to draw for address clusters)")

    # Center the map
    minx, miny, maxx, maxy = hail.total_bounds
    center = [(miny + maxy) / 2.0, (minx + maxx) / 2.0]

    m = folium.Map(location=center, zoom_start=7, control_scale=True, tiles="cartodbpositron")

    # Hail cluster polygons
    def hail_style(feat):
        cid = feat["properties"].get("cluster_id", 0)
        return {
            "color": hex_color(cid),
            "weight": 2,
            "fill": True,
            "fillOpacity": 0.15,
        }

    hail_gj = folium.GeoJson(
        data=json.loads(hail.to_json()),
        name="Hail Cluster Boundaries",
        style_function=hail_style,
        tooltip=folium.GeoJsonTooltip(fields=["cluster_id"], aliases=["Hail cluster:"]),
    )
    hail_gj.add_to(m)

    # Address cluster hulls (if any)
    if not addr.empty:
        def addr_style(feat):
            hid = feat["properties"].get("hail_cluster_id", 0)
            return {
                "color": hex_color(hid ^ 0x55AA55),  # different but related
                "weight": 2,
                "fill": True,
                "fillOpacity": 0.35,
            }

        addr_gj = folium.GeoJson(
            data=json.loads(addr.to_json()),
            name="Address Cluster Hulls",
            style_function=addr_style,
            tooltip=folium.GeoJsonTooltip(
                fields=["hail_cluster_id", "addr_cluster_id", "num_addresses"],
                aliases=["Hail cluster:", "Addr cluster:", "# addresses:"],
            ),
        )
        addr_gj.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    m.save(args.out)
    print(f"[ok] Wrote {args.out}")

if __name__ == "__main__":
    main()