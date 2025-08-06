#!/usr/bin/env python3
"""
fetch_skip_trace.py

Fetch skip-trace data for addresses using the BatchData Async Property Skip Trace API,
then load into PostGIS skip_trace table.

Usage:
  python ingest/fetch_skip_trace.py \
    --address-table addresses \
    --skiptrace-table skip_traces \
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
import pandas as pd
from sqlalchemy import create_engine, text


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]


def submit_skip_trace(batch, api_url, api_key):
    endpoint = f"{api_url}/property-skip-trace-async"
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


def poll_skip_trace(search_id, api_url, api_key, interval=5, timeout=300):
    status_url = f"{api_url}/property-skip-trace-async/{search_id}/status"
    headers = {'Authorization': f"Bearer {api_key}"}
    elapsed = 0
    while elapsed < timeout:
        resp = requests.get(status_url, headers=headers)
        resp.raise_for_status()
        status = resp.json().get('status')
        if status == 'Completed':
            return
        if status in ('Failed', 'Error'):
            raise RuntimeError(f"Skip-trace {search_id} failed: {status}")
        time.sleep(interval)
        elapsed += interval
    raise TimeoutError(f"Skip-trace {search_id} did not complete in {timeout}s")


def fetch_skip_trace_results(search_id, api_url, api_key):
    results_url = f"{api_url}/property-skip-trace-async/{search_id}/results"
    headers = {'Authorization': f"Bearer {api_key}"}
    resp = requests.get(results_url, headers=headers)
    resp.raise_for_status()
    return resp.json().get('results', [])


def main():
    parser = argparse.ArgumentParser(description="Fetch skip-trace data via BatchData Async API")
    parser.add_argument('--address-table', required=True,
                        help='PostGIS table with addresses to skip-trace')
    parser.add_argument('--skiptrace-table', required=True,
                        help='PostGIS table for skip-trace results')
    parser.add_argument('--filter', default='TRUE',
                        help='SQL WHERE clause to filter addresses')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Addresses per API request')
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
        print('No addresses to skip-trace; exiting.')
        return
    print(f"Found {len(addrs)} addresses for skip-trace.")

    records = []
    # 2) Submit batches and fetch results
    for batch in chunked(addrs.to_dict('records'), args.batch_size):
        sid = submit_skip_trace(batch, API_URL, API_KEY)
        print(f"Submitted skip-trace job {sid} for {len(batch)} addresses.")
        poll_skip_trace(sid, API_URL, API_KEY)
        print(f"Job {sid} completed; fetching results.")
        results = fetch_skip_trace_results(sid, API_URL, API_KEY)
        # 3) Process each result
        for rec in results:
            ext_id = int(rec.get('externalId'))
            trace = rec.get('skipTrace', {})
            records.append({
                'address_id': ext_id,
                'skiptrace_data': json.dumps(trace),
                'metadata': json.dumps(rec)
            })

    # 4) Bulk-load to PostGIS
    df = pd.DataFrame(records)
    with engine.begin() as conn:
        if not df.empty:
            print(f"Loading {len(df)} skip-trace records into {args.skiptrace_table}...")
            df.to_sql(args.skiptrace_table, conn, if_exists='replace', index=False)
    print('Skip-trace ingest complete.')

if __name__ == '__main__':
    main()