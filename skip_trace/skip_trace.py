"""
Perform skip-trace using BulkData API for owner data enrichment.
"""
import os
import requests
import pandas as pd

API_KEY = os.getenv('BULKDATA_API_KEY')
API_URL = 'https://api.bulkdata.com/skiptrace'


def skip_trace(properties: pd.DataFrame):
    resp = requests.post(
        API_URL,
        json={'records': properties.to_dict(orient='records')},
        headers={'Authorization': f'Bearer {API_KEY}'}
    )
    resp.raise_for_status()
    return pd.DataFrame(resp.json()['results'])

if __name__ == '__main__':
    props = pd.read_csv('property_data.csv')
    owners = skip_trace(props)
    owners.to_csv('owner_data.csv', index=False)
