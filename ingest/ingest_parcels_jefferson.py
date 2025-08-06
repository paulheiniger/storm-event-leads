#!/usr/bin/env python3
"""
ingest_parcels_jefferson.py

Fetch Jefferson County, KY parcels (LOJIC / Louisville Metro) via ArcGIS REST,
normalize fields, and write to PostGIS `parcels` and `owners` tables.

Assumptions:
- DATABASE_URL env var set to PostGIS connection string.
- Target PostGIS schema has `parcels` and `owners` as per canonical schema.
- Feature service URL may need to be updated if the county changes URL.

Usage:
    python ingest_parcels_jefferson.py [--bbox minlon,minlat,maxlon,maxlat]

Example:
    python ingest_parcels_jefferson.py --bbox=-85.90,38.00,-85.40,38.40
"""
import os
import sys
import argparse
import logging
from urllib.parse import urlencode

import requests
import geopandas as gpd
from sqlalchemy import create_engine, text
from shapely.geometry import shape

# --- CONFIGURATION ---
# Replace with the up-to-date Jefferson County parcel feature service if changed.
FEATURE_SERVICE_URL = (
    "https://services3.arcgis.com/2Z7xj6M2ZJzLqTzQ/ArcGIS/rest/services/Parcels/FeatureServer/0/query"
)
# You may need to discover the real endpoint via LOJIC/ArcGIS Hub if this is outdated.

# Fields mapping: service-specific field names -> canonical
FIELD_MAP = {
    # Example mapping; adjust based on actual service field names
    "PARCEL_ID": "parcel_id",          # replace with actual field like "PARID"
    "OWNER_NAME": "owner_name",        # e.g., "OWNER1"
    "MAILING_ADDR": "mailing_address", # e.g., "MAILING_ADDRESS"
    "ASSESSED_VALUE": "assessed_value" # e.g., "TOT_ASSD_VAL"
}

# --- logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger()

# --- helpers ---
def fetch_parcels(bbox=None):
    """
    Query the ArcGIS REST feature service and return a GeoDataFrame.
    """
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "outSR": "4326",
        "geometryPrecision": 6,
        "resultRecordCount": 1000  # initial page size, will paginate
    }
    if bbox:
        # ArcGIS expects envelope: xmin,ymin,xmax,ymax
        params.update({
            "geometry": bbox,
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects"
        })

    logger.info(f"Fetching parcels with params: {params}")
    resp = requests.get(FEATURE_SERVICE_URL, params=params, timeout=60)
    resp.raise_for_status()
    gdf = gpd.read_file(resp.text)
    return gdf


def normalize_fields(gdf):
    """
    Normalize the service-specific field names into canonical columns.
    """
    # Lowercase copy for safe access
    cols = {c.upper(): c for c in gdf.columns}

    def safe_get(src, names):
        for n in names:
            if n in cols:
                return gdf[cols[n]]
        return None

    # Build canonical columns with fallbacks
    parcel_id = safe_get(gdf, ["PARCEL_ID", "PARID", "PARCELID"])  # adapt
    owner_name = safe_get(gdf, ["OWNER_NAME", "OWNER1", "OWNERNM"])
    mailing_address = safe_get(gdf, ["MAILING_ADDR", "MAILING_ADDRESS", "MAIL_ADDR"])
    assessed = safe_get(gdf, ["ASSESSED_VALUE", "TOT_ASSD_VAL", "ASSESSED"])

    out = gdf.copy()
    out["parcel_id"] = parcel_id.astype(str).str.strip() if parcel_id is not None else None
    out["owner_name"] = owner_name.str.title().str.strip() if owner_name is not None else None
    out["mailing_address"] = mailing_address.str.strip() if mailing_address is not None else None
    # Try numeric conversion
    out["assessed_value"] = pd.to_numeric(assessed, errors="coerce") if assessed is not None else None
    out["county"] = "Jefferson"

    # Keep raw JSON snapshot
    out["raw"] = out.apply(lambda r: r.to_json(), axis=1)
    return out[["parcel_id", "county", "owner_name", "mailing_address", "assessed_value", "geometry", "raw"]]


def upsert_parcels_and_owners(parcels_gdf, engine):
    """
    Upsert parcels into parcels table and populate owners.
    """
    with engine.begin() as conn:
        for _, row in parcels_gdf.iterrows():
            parcel_id = row["parcel_id"]
            if not parcel_id:
                continue  # skip bad
            # Upsert parcel
            upsert_parcel_sql = text("""
            INSERT INTO parcels (parcel_id, county, owner_name, mailing_address, assessed_value, geom, raw)
            VALUES (:parcel_id, :county, :owner_name, :mailing_address, :assessed_value, ST_SetSRID(ST_GeomFromGeoJSON(:geom_geojson),4326), :raw)
            ON CONFLICT (parcel_id) DO UPDATE
              SET owner_name = EXCLUDED.owner_name,
                  mailing_address = EXCLUDED.mailing_address,
                  assessed_value = EXCLUDED.assessed_value,
                  geom = EXCLUDED.geom,
                  raw = EXCLUDED.raw
            RETURNING id
            """)
            geom_geojson = row["geometry"].__geo_interface__  # shapely geometry
            parcel_res = conn.execute(upsert_parcel_sql, {
                "parcel_id": parcel_id,
                "county": row["county"],
                "owner_name": row["owner_name"],
                "mailing_address": row["mailing_address"],
                "assessed_value": row["assessed_value"],
                "geom_geojson": json.dumps(geom_geojson),
                "raw": row["raw"]
            })
            parcel_db_id = parcel_res.fetchone()[0]

            # Upsert owner (simple dedupe on parcel -> owner_name)
            if row["owner_name"]:
                owner_sql = text("""
                INSERT INTO owners (parcel_id, name, mailing_address, metadata)
                VALUES (:parcel_id, :name, :mailing_address, :meta)
                ON CONFLICT (parcel_id, name) DO NOTHING
                """)
                conn.execute(owner_sql, {
                    "parcel_id": parcel_db_id,
                    "name": row["owner_name"],
                    "mailing_address": row["mailing_address"],
                    "meta": {"source": "jefferson_arcgis", "raw_owner": row["owner_name"]}
                })


def main():
    import json
    import pandas as pd

    parser = argparse.ArgumentParser(description="Ingest Jefferson County parcel + owner data")
    parser.add_argument('--bbox', help="Envelope bbox minlon,minlat,maxlon,maxlat to limit fetch", default=None)
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)
    engine = create_engine(db_url)

    logger.info("Fetching parcels...")
    try:
        raw = fetch_parcels(bbox=args.bbox)
    except Exception as e:
        logger.error(f"Failed to fetch parcels: {e}")
        sys.exit(1)
    if raw.empty:
        logger.warning("No parcel features returned.")
        return

    logger.info(f"Raw parcels retrieved: {len(raw)}")
    norm = normalize_fields(raw)

    # Re-wrap normalized into GeoDataFrame
    parcels_gdf = gpd.GeoDataFrame(norm, geometry="geom", crs="EPSG:4326")

    logger.info("Upserting into PostGIS...")
    upsert_parcels_and_owners(parcels_gdf, engine)
    logger.info("Finished ingesting parcels and owners.")


if __name__ == "__main__":
    main()