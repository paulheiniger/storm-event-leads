#!/usr/bin/env python3
"""
fetch_property_data.py

Fetch property and owner details for a set of addresses using the BatchData Async Property Search API,
then load into PostGIS property and owner tables.

Usage:
  python ingest/fetch_property_data.py \
    --address-table addresses \
    --property-table properties \
    --owner-table owners \
    --filter "ST_Intersects(geom, ST_MakeEnvelope(-85.05,33.40,-83.55,34.35,4326))" \
    --batch-size 100

Environment:
  DATABASE_URL        PostgreSQL connection string
  BATCHDATA_API_URL   Base URL for BatchData API (e.g. https://api.batchdata.com/v1)
  BATCHDATA_API_KEY   Bearer token API key
"""
import os
import sys
import time
import json
import argparse
import requests
import geopandas as gpd
from sqlalchemy import create_engine, text


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]


def submit_search(batch, api_url, api_key):
    endpoint = f"{api_url}/property-search-async"
    payload = {
        "addresses": [
            {
                "line1": f"{a['number']} {a['street']}",
                "city": a['city'],
                "state": a['state'],
                "postalCode": a['postal_code'],
                "externalId": str(a['id'])
            } for a in batch
        ]
    }
    headers = {
        'Authorization': f"Bearer {api_key}",
        'Content-Type': 'application/json'
    }
    resp = requests.post(endpoint, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()['searchId']


def poll_search(search_id, api_url, api_key, interval=5, timeout=300):
    status_url = f"{api_url}/property-search-async/{search_id}/status"
    headers = {'Authorization': f"Bearer {api_key}"}
    elapsed = 0
    while elapsed < timeout:
        resp = requests.get(status_url, headers=headers)
        resp.raise_for_status()
        status = resp.json().get('status')
        if status == 'Completed':
            return
        if status in ('Failed', 'Error'):
            raise RuntimeError(f"Search {search_id} failed: {status}")
        time.sleep(interval)
        elapsed += interval
    raise TimeoutError(f"Search {search_id} did not complete within {timeout}s")


def fetch_results(search_id, api_url, api_key):
    results_url = f"{api_url}/property-search-async/{search_id}/results"
    headers = {'Authorization': f"Bearer {api_key}"}
    resp = requests.get(results_url, headers=headers)
    resp.raise_for_status()
    return resp.json().get('results', [])


def main():
    parser = argparse.ArgumentParser(
        description="Fetch property data via BatchData Async API")
    parser.add_argument('--address-table', required=True)
    parser.add_argument('--property-table', required=True)
    parser.add_argument('--owner-table', required=True)
    parser.add_argument('--filter', default='TRUE')
    parser.add_argument('--batch-size', type=int, default=100)
    args = parser.parse_args()

    DATABASE_URL = os.getenv('DATABASE_URL')
    API_URL = os.getenv('BATCHDATA_API_URL')
    API_KEY = os.getenv('BATCHDATA_API_KEY')
    if not (DATABASE_URL and API_URL and API_KEY):
        sys.exit('Error: DATABASE_URL, BATCHDATA_API_URL, and BATCHDATA_API_KEY must be set')

    engine = create_engine(DATABASE_URL)

    # 1) Query addresses
    addr_sql = text(
        f"SELECT id, number, street, city, state, postcode AS postal_code, geom "
        f"FROM {args.address_table} WHERE {args.filter};"
    )
    addrs = gpd.read_postgis(addr_sql, engine, geom_col='geom')
    if addrs.empty:
        print('No addresses matching filter; exiting.')
        return
    print(f"Found {len(addrs)} addresses to look up.")

    # 2) Batch, submit, poll, fetch
    records_prop = []
    records_owner = []
    for batch in chunked(addrs.to_dict('records'), args.batch_size):
        sid = submit_search(batch, API_URL, API_KEY)
        print(f"Submitted search {sid} for {len(batch)} addresses.")
        poll_search(sid, API_URL, API_KEY)
        print(f"Search {sid} completed; fetching results.")
        results = fetch_results(sid, API_URL, API_KEY)

        # 3) Process results
        for rec in results:
            ext_id = int(rec.get('externalId'))
            prop = rec.get('property', {})
            records_prop.append({
                'address_id':     ext_id,
                'parcel_id':      prop.get('parcelId'),
                'sq_ft':          prop.get('sqFt'),
                'year_built':     prop.get('yearBuilt'),
                'assessed_value': prop.get('assessedValue'),
                'metadata':       json.dumps(rec)
            })
            for owner in rec.get('owners', []):
                records_owner.append({
                    'address_id':      ext_id,
                    'name':            owner.get('name'),
                    'phone':           owner.get('phone'),
                    'email':           owner.get('email'),
                    'mailing_address': owner.get('mailingAddress'),
                    'metadata':        json.dumps(owner)
                })

    # 4) Bulk insert into PostGIS
    import pandas as pd
    df_prop = pd.DataFrame(records_prop)
    df_owner = pd.DataFrame(records_owner)
    with engine.begin() as conn:
        if not df_prop.empty:
            print(f"Loading {len(df_prop)} properties into {args.property_table}...")
            df_prop.to_sql(args.property_table, conn, if_exists='replace', index=False)
        if not df_owner.empty:
            print(f"Loading {len(df_owner)} owners into {args.owner_table}...")
            df_owner.to_sql(args.owner_table, conn, if_exists='replace', index=False)

    print('Property and owner data ingest complete.')


if __name__ == '__main__':
    main()