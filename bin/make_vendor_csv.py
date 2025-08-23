#!/usr/bin/env python3
import sys, os
import pandas as pd

if len(sys.argv) < 3:
    print("Usage: bin/make_vendor_csv.py <in.csv> <out.csv>", file=sys.stderr)
    sys.exit(2)

inp, outp = sys.argv[1], sys.argv[2]
if not os.path.isfile(inp):
    print(f"Input CSV not found: {inp}", file=sys.stderr)
    sys.exit(1)

df = pd.read_csv(inp)

# Try to assemble vendor columns
# Prefer an existing 'address' column; otherwise fall back to 'street'
if 'address' in df.columns and df['address'].notna().any():
    addr = df['address'].fillna('')
elif 'street' in df.columns and df['street'].notna().any():
    addr = df['street'].fillna('')
else:
    # last-resort: try to synthesize from parts we might have
    parts = []
    for col in ['street','address1','addr1','line1']:
        if col in df.columns:
            parts.append(df[col].fillna(''))
            break
    if parts:
        addr = parts[0]
    else:
        addr = pd.Series(['']*len(df))

city = df['city'].fillna('') if 'city' in df.columns else ''
state = df['state'].fillna('') if 'state' in df.columns else ''
zipc  = df['zip'].fillna('')   if 'zip'   in df.columns else (df['postal_code'].fillna('') if 'postal_code' in df.columns else '')

out = pd.DataFrame({
    'address': addr,
    'city': city,
    'state': state,
    'zip': zipc,
})

# Filter empty rows
mask = (~out['address'].astype(str).str.strip().eq('')) & (~out['city'].astype(str).str.strip().eq('')) & (~out['state'].astype(str).str.strip().eq(''))
out = out.loc[mask].copy()

out.to_csv(outp, index=False)
print(f"Wrote {len(out)} rows → {outp}")