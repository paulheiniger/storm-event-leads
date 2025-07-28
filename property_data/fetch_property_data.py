"""
Fetch property details from a property data API (e.g., ATTOM) given a list of addresses.
"""
import os
import requests
import pandas as pd

API_KEY = os.getenv('PROPERTY_API_KEY')
API_URL = 'https://api.example.com/property'


def fetch_properties(addresses: pd.DataFrame):
    props = []
    for idx, row in addresses.iterrows():
        params = { 'address': row['address'], 'apikey': API_KEY }
        r = requests.get(API_URL, params=params)
        r.raise_for_status()
        props.append(r.json())
    return pd.DataFrame(props)

if __name__ == '__main__':
    addrs = pd.read_json('addresses_in_event.geojson')
    df = fetch_properties(addrs)
    df.to_csv('property_data.csv', index=False)
