#!/usr/bin/env python3
"""
orchestrate_pipeline.py

One-button pipeline for hail-storm lead gen.

What it does (per state):
  1) Fetch SWDI hail points in date chunks (to avoid 500s) and load to PostGIS.
  2) UNION the successfully-loaded chunk tables into one combined *view*.
  3) Cluster hail points into polygon hulls (one table per run window).
  4) Create a stable alias view (hail_cluster_boundaries_<state>).
  5) Cluster addresses around hail clusters (DBSCAN) into hulls.
  6) Export an interactive Folium map of hail hulls + address clusters.

Idempotent:
- Skips steps if outputs already exist (unless --force).
- Only unions tables that actually exist.
- Logs each step to pipeline_run_log.

Requirements:
- DATABASE_URL env var (e.g., postgresql://user:pass@host:5432/dbname)
- Your existing scripts:
    ingest/fetch_and_load_swdi.py
    cluster/cluster_hail.py
    cluster/cluster_addresses.py
    visualization/plot_clusters_map.py
"""

import argparse
import datetime as dt
import os
import sys
import subprocess
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ── Simple state bounding boxes (minLon,minLat,maxLon,maxLat).
# Extend as needed; these four are the ones you’re working right now.
STATE_BBOX = {
    "GA": "-85.61,30.36,-80.84,35.00",
    "IN": "-88.10,37.70,-84.79,41.76",
    "OH": "-84.82,38.40,-80.52,41.98",
    "KY": "-89.57,36.49,-81.97,39.15",
}

# Defaults / tuning knobs
DEFAULT_DATASET = "nx3hail"
DEFAULT_CHUNK_DAYS = 45
DEFAULT_HAIL_EPS = 0.1
DEFAULT_HAIL_MIN_SAMPLES = 5
DEFAULT_ADDR_BUFFER = 0.02
DEFAULT_ADDR_EPS = 0.001
DEFAULT_ADDR_MIN_SAMPLES = 10


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def run(cmd: List[str], cwd: Path = None) -> None:
    """Run a subprocess and stream output; raise if non-zero exit."""
    print("→", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(cwd) if cwd else None)


def split_dates(start: dt.date, end: dt.date, span_days: int) -> List[Tuple[dt.date, dt.date]]:
    """Split [start, end) into non-overlapping chunks of at most span_days."""
    out = []
    cur = start
    while cur < end:
        nxt = min(cur + dt.timedelta(days=span_days), end)
        out.append((cur, nxt))
        cur = nxt
    return out


def ensure_dir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True) if p.suffix else p.mkdir(parents=True, exist_ok=True)


def engine_or_die() -> Engine:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL not set.", file=sys.stderr)
        sys.exit(2)
    return create_engine(dsn)


def table_exists(engine: Engine, name: str) -> bool:
    """Check for a table or view in the public schema."""
    sql = text("""
        SELECT EXISTS (
          SELECT 1 FROM information_schema.tables
          WHERE table_schema='public' AND table_name=:t
        )
        OR EXISTS (
          SELECT 1 FROM information_schema.views
          WHERE table_schema='public' AND table_name=:t
        ) AS ok
    """)
    with engine.begin() as con:
        return bool(con.execute(sql, {"t": name.lower()}).scalar())


def log_step(engine: Engine, state: str, step: str, status: str, note: str = ""):
    with engine.begin() as con:
        con.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_run_log (
              id SERIAL PRIMARY KEY,
              ts TIMESTAMP DEFAULT NOW(),
              state TEXT NOT NULL,
              step  TEXT NOT NULL,
              status TEXT NOT NULL,
              note TEXT
            );
        """))
        con.execute(text("""
            INSERT INTO pipeline_run_log(state, step, status, note)
            VALUES(:state, :step, :status, :note)
        """), {"state": state, "step": step, "status": status, "note": note[:2000]})


def list_existing(engine: Engine, names: List[str]) -> List[str]:
    """Return subset of names that exist as table or view (simple & robust)."""
    return [nm for nm in names if table_exists(engine, nm)]


# ──────────────────────────────────────────────────────────────────────
# Main orchestration
# ──────────────────────────────────────────────────────────────────────

def orchestrate_state(
    state: str,
    start: dt.date,
    end: dt.date,
    dataset: str,
    chunk_days: int,
    repo_root: Path,
    out_dir: Path,
    force: bool,
    hail_eps: float,
    hail_min_samples: int,
    addr_buffer: float,
    addr_eps: float,
    addr_min_samples: int,
):
    engine = engine_or_die()

    if state not in STATE_BBOX:
        raise SystemExit(f"No bbox configured for state={state}. Add to STATE_BBOX.")

    bbox = STATE_BBOX[state]
    print(f"\n================= {state} =================")
    print(f"Date range: {start} → {end}  | dataset={dataset}  | bbox={bbox}")

    # 1) Fetch SWDI chunks
    chunk_ranges = split_dates(start, end, chunk_days)

    # Expected chunk table names *by our fetcher’s naming convention*:
    def chunk_table(s: dt.date, e: dt.date) -> str:
        return f"swdi_{dataset}_{s:%Y%m%d}_{e:%Y%m%d}"

    fetched_tables = []

    for s, e in chunk_ranges:
        tbl = chunk_table(s, e)
        if table_exists(engine, tbl) and not force:
            print(f"↷ exists: {tbl} (skipping fetch)")
            fetched_tables.append(tbl)
            continue

        print(f"Fetching {dataset} {s} → {e} (table: {tbl})")
        try:
            run([
                sys.executable, str(repo_root / "ingest" / "fetch_and_load_swdi.py"),
                "--start", s.isoformat(),
                "--end", e.isoformat(),
                f"--bbox={bbox}",
                "--datasets", dataset,
            ], cwd=repo_root)
        except subprocess.CalledProcessError as err:
            log_step(engine, state, "fetch", "FAIL", f"{s}→{e}: {err}")
            print(f"⚠️  Fetch failed for {s}→{e}; continuing.")
            continue

        # Confirm the table now exists
        if table_exists(engine, tbl):
            fetched_tables.append(tbl)
            log_step(engine, state, "fetch", "OK", f"{s}→{e}: {tbl}")
        else:
            log_step(engine, state, "fetch", "MISS", f"{s}→{e}: expected {tbl}")

    if not fetched_tables:
        print(f"❌ No SWDI tables fetched for {state}; skipping rest.")
        return

    # 2) Create a combined view for this window/state (UNION ALL only existing tables)
    combined_view = f"swdi_{dataset}_{state.lower()}_{start:%Y%m%d}_{end:%Y%m%d}"

    # Deduplicate & ensure only existing are unioned
    fetched_tables = list(dict.fromkeys(list_existing(engine, fetched_tables)))

    union_sql = " UNION ALL ".join([f"SELECT * FROM public.{t}" for t in fetched_tables])

    with engine.begin() as con:
        con.execute(text(f"DROP VIEW IF EXISTS public.{combined_view};"))
        con.execute(text(f"CREATE VIEW public.{combined_view} AS {union_sql};"))
    log_step(engine, state, "combine_swdi", "OK", f"{combined_view} from {len(fetched_tables)} tables")
    print(f"Combined SWDI view: {combined_view} (from {len(fetched_tables)} chunk tables)")

    # 3) Cluster hail points → polygons
    hail_out_table = f"hail_cluster_boundaries_{state.lower()}_{start:%Y%m%d}_{end:%Y%m%d}"
    if table_exists(engine, hail_out_table) and not force:
        print(f"↷ exists: {hail_out_table} (skipping cluster_hail)")
    else:
        run([
            sys.executable, str(repo_root / "cluster" / "cluster_hail.py"),
            "--source-table", combined_view,
            "--dest-table", hail_out_table,
            "--eps", str(hail_eps),
            "--min-samples", str(hail_min_samples),
        ], cwd=repo_root)
        log_step(engine, state, "cluster_hail", "OK", hail_out_table)

    # 3a) Create/refresh a stable alias view
    stable_hail_view = f"hail_cluster_boundaries_{state.lower()}"
    with engine.begin() as con:
        con.execute(text(f"""
            CREATE OR REPLACE VIEW public.{stable_hail_view}
            AS SELECT * FROM public.{hail_out_table};
        """))
    log_step(engine, state, "hail_view", "OK", f"{stable_hail_view} -> {hail_out_table}")

    # 4) Cluster addresses around hail clusters
    addr_out_table = f"address_clusters_{state.lower()}_{start:%Y%m%d}_{end:%Y%m%d}"
    if table_exists(engine, addr_out_table) and not force:
        print(f"↷ exists: {addr_out_table} (skipping cluster_addresses)")
    else:
        run([
            sys.executable, str(repo_root / "cluster" / "cluster_addresses.py"),
            "--hail-cluster-table", hail_out_table,          # use the dated table, not the alias
            "--address-table", "addresses",
            "--dest-table", addr_out_table,
            "--buffer", str(addr_buffer),
            "--eps", str(addr_eps),
            "--min-samples", str(addr_min_samples),
        ], cwd=repo_root)
        log_step(engine, state, "cluster_addresses", "OK", addr_out_table)

    # 5) Map export
    out_map = out_dir / f"{state.lower()}_clusters_{start:%Y%m%d}_{end:%Y%m%d}.html"
    ensure_dir(out_map)
    run([
        sys.executable, str(repo_root / "visualization" / "plot_clusters_map.py"),
        "--hail-cluster-table", hail_out_table,
        "--addr-cluster-table", addr_out_table,
        "--out", str(out_map),
    ], cwd=repo_root)
    log_step(engine, state, "map_export", "OK", str(out_map))
    print(f"✅ Map written: {out_map}")


def main():
    parser = argparse.ArgumentParser(description="Run hail → address-cluster pipeline per state")
    parser.add_argument("--states", required=True,
                        help="Comma-separated states, e.g. GA,IN,OH,KY")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD (exclusive end OK)")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="SWDI dataset (default: nx3hail)")
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS,
                        help="SWDI fetch chunk size in days (default: 45)")
    parser.add_argument("--out-dir", default="maps", help="Directory for HTML map outputs")
    parser.add_argument("--force", action="store_true", help="Re-run steps even if outputs exist")

    # Clustering params
    parser.add_argument("--hail-eps", type=float, default=DEFAULT_HAIL_EPS)
    parser.add_argument("--hail-min-samples", type=int, default=DEFAULT_HAIL_MIN_SAMPLES)
    parser.add_argument("--addr-buffer", type=float, default=DEFAULT_ADDR_BUFFER)
    parser.add_argument("--addr-eps", type=float, default=DEFAULT_ADDR_EPS)
    parser.add_argument("--addr-min-samples", type=int, default=DEFAULT_ADDR_MIN_SAMPLES)

    args = parser.parse_args()

    try:
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
    except ValueError:
        raise SystemExit("start/end must be YYYY-MM-DD")

    if start >= end:
        raise SystemExit("start must be < end")

    repo_root = Path(__file__).resolve().parent
    out_dir = (repo_root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]
    if not states:
        raise SystemExit("No states provided.")

    # Preflight DATABASE_URL
    _ = engine_or_die()

    for st in states:
        try:
            orchestrate_state(
                st, start, end,
                dataset=args.dataset,
                chunk_days=args.chunk_days,
                repo_root=repo_root,
                out_dir=out_dir,
                force=args.force,
                hail_eps=args.hail_eps,
                hail_min_samples=args.hail_min_samples,
                addr_buffer=args.addr_buffer,
                addr_eps=args.addr_eps,
                addr_min_samples=args.addr_min_samples,
            )
        except subprocess.CalledProcessError as e:
            print(f"❌ Error for {st}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"❌ Error for {st}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()