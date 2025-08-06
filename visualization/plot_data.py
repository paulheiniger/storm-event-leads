#!/usr/bin/env python3
"""
plot_data.py

Query PostGIS and plot cluster boundaries and hail points color-coded by severity (sevprob)
and sized by maxsize, with filtering options.
Usage:
  python visualization/plot_data.py \
    --cluster-table hail_cluster_boundaries_atlanta \
    --point-table nx3hail_atlanta \
    [--start-date 2025-07-01] \
    [--end-date 2025-07-28] \
    [--bbox "-85.05,33.40,-83.55,34.35"] \
    [--output map.html]

Environment:
  DATABASE_URL: PostgreSQL connection string, e.g. postgresql://user:pass@host:port/db
"""
import os
import sys
import argparse
import geopandas as gpd
import folium
from folium.plugins import MarkerCluster, MeasureControl, Fullscreen, MiniMap
from branca.colormap import linear
from sqlalchemy import create_engine, text


def build_where_clause(start_date=None, end_date=None, bbox=None):
    clauses = []
    if start_date:
        clauses.append(f"event_date >= '{start_date}'")
    if end_date:
        clauses.append(f"event_date <= '{end_date}'")
    if bbox:
        minLon, minLat, maxLon, maxLat = map(float, bbox.split(','))
        clauses.append(
            f"ST_Intersects(geometry, ST_MakeEnvelope({minLon}, {minLat}, {maxLon}, {maxLat}, 4326))"
        )
    return ' AND '.join(clauses) if clauses else 'TRUE'


def load_gdf(table, database_url, where='TRUE'):
    engine = create_engine(database_url)
    sql = f"SELECT * FROM {table} WHERE {where};"
    print(f"Executing: {sql}")
    return gpd.read_postgis(text(sql), engine, geom_col='geometry')


def plot_map(clusters, points, output_html):
    # Map center
    if clusters.empty and points.empty:
        print("No data to plot.")
        return
    geom_union = (clusters.geometry.unary_union if not clusters.empty else points.geometry.unary_union)
    center = geom_union.centroid
    m = folium.Map(location=[center.y, center.x], zoom_start=9, control_scale=True)

    # UI Controls
    m.add_child(Fullscreen())
    m.add_child(MeasureControl())
    m.add_child(MiniMap(toggle_display=True))

    # Prepare colormap for cluster IDs
    if 'cluster_id' in clusters.columns and not clusters.empty:
        cmin, cmax = clusters['cluster_id'].min(), clusters['cluster_id'].max()
        colormap = linear.Set1_09.scale(cmin, cmax)
        colormap.caption = 'Cluster ID'
        colormap.add_to(m)
    else:
        colormap = None

    # Plot cluster boundaries
    if not clusters.empty:
        def style_fn(feature):
            cid = feature['properties'].get('cluster_id')
            return {
                'color': colormap(cid) if colormap else 'blue',
                'weight': 2,
                'fillOpacity': 0.2
            }
        folium.GeoJson(
            clusters,
            name='Cluster Boundaries',
            style_function=style_fn,
            tooltip=folium.GeoJsonTooltip(fields=['cluster_id', 'num_points'])
        ).add_to(m)

    # Plot hail points with clustering, severity color & size by maxsize
    if not points.empty:
        # Severity colormap
        if 'sevprob' in points.columns:
            vmin, vmax = points['sevprob'].min(), points['sevprob'].max()
            severity_cmap = linear.YlOrRd_09.scale(vmin, vmax)
            severity_cmap.caption = 'SevProb'
            severity_cmap.add_to(m)
        else:
            severity_cmap = None

        marker_cluster = MarkerCluster(name='Hail Points').add_to(m)
        # Determine size scaling factor
        if 'maxsize' in points.columns:
            maxsize_vals = points['maxsize'].dropna()
            if not maxsize_vals.empty:
                # Scale such that typical maxsize ~1.0 yields radius ~5
                scale = 5.0 / maxsize_vals.max()
            else:
                scale = 1.0
        else:
            scale = 1.0

        for _, row in points.iterrows():
            lon, lat = row.geometry.x, row.geometry.y
            sev = row.get('sevprob') if 'sevprob' in row else None
            size = row.get('maxsize') * scale if 'maxsize' in row and row.get('maxsize') else 3
            color = severity_cmap(sev) if severity_cmap and sev is not None else 'black'
            popup = f"SevProb: {sev}<br>MaxSize: {row.get('maxsize')}"
            folium.CircleMarker(
                location=[lat, lon],
                radius=size,
                color=color,
                fill=True,
                fill_opacity=0.7,
                popup=popup
            ).add_to(marker_cluster)

    folium.LayerControl(collapsed=False).add_to(m)
    m.save(output_html)
    print(f"Map saved to {output_html}")


def main():
    parser = argparse.ArgumentParser(description='Plot cluster boundaries and hail points')
    parser.add_argument('--cluster-table', required=True,
                        help='PostGIS table with cluster boundaries')
    parser.add_argument('--point-table', required=True,
                        help='PostGIS table or view with hail points')
    parser.add_argument('--start-date', help='Filter by event_date >= YYYY-MM-DD')
    parser.add_argument('--end-date', help='Filter by event_date <= YYYY-MM-DD')
    parser.add_argument('--bbox', help='Bounding box minLon,minLat,maxLon,maxLat')
    parser.add_argument('--output', default='map.html', help='Output HTML file')
    args = parser.parse_args()

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        sys.exit('Error: set DATABASE_URL env var')

    where = build_where_clause(args.start_date, args.end_date, args.bbox)
    clusters = load_gdf(args.cluster_table, database_url, where)
    points   = load_gdf(args.point_table, database_url, where)
    plot_map(clusters, points, args.output)

if __name__ == '__main__':
    main()