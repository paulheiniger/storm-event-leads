#!/usr/bin/env bash
set -euo pipefail

# ----------------------------------------------------------------------------
# Database initialization script for Storm Event Leads Pipeline
# Requires: psql CLI and $DATABASE_URL env var pointing to your Postgres database
# ----------------------------------------------------------------------------

# Ensure PostGIS extension and create schema
psql "$DATABASE_URL" << 'EOSQL'
-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Table: storm_events
CREATE TABLE IF NOT EXISTS storm_events (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_date DATE NOT NULL,
    geom GEOMETRY(Polygon,4326) NOT NULL,
    metadata JSONB
);
CREATE INDEX IF NOT EXISTS idx_storm_events_geom ON storm_events USING GIST (geom);

-- Table: addresses
CREATE TABLE IF NOT EXISTS addresses (
    id SERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    geom GEOMETRY(Point,4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_addresses_geom ON addresses USING GIST (geom);

-- Table: properties
CREATE TABLE IF NOT EXISTS properties (
    id SERIAL PRIMARY KEY,
    storm_event_id INTEGER REFERENCES storm_events (id) ON DELETE CASCADE,
    address_id INTEGER REFERENCES addresses (id) ON DELETE CASCADE,
    parcel_id TEXT,
    sq_ft INTEGER,
    year_built INTEGER,
    assessed_value NUMERIC,
    metadata JSONB
);

-- Table: owners
CREATE TABLE IF NOT EXISTS owners (
    id SERIAL PRIMARY KEY,
    property_id INTEGER REFERENCES properties (id) ON DELETE CASCADE,
    name TEXT,
    phone TEXT,
    email TEXT,
    mailing_address TEXT,
    metadata JSONB
);

-- Table: calls (for later AI-driven calls)
CREATE TABLE IF NOT EXISTS calls (
    id SERIAL PRIMARY KEY,
    owner_id INTEGER REFERENCES owners (id),
    storm_event_id INTEGER REFERENCES storm_events (id),
    call_time TIMESTAMP,
    result TEXT,
    notes TEXT
);

-- Table: appointments
CREATE TABLE IF NOT EXISTS appointments (
    id SERIAL PRIMARY KEY,
    call_id INTEGER REFERENCES calls (id),
    owner_id INTEGER REFERENCES owners (id),
    scheduled_time TIMESTAMP,
    status TEXT
);
EOSQL

echo "Database initialized successfully."
