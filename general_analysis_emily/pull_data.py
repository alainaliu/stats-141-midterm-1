"""Cache the Polymarket course data locally before the access window closes (Oct 29, 2026).

Resumable: files already on disk are skipped. Run from the project root:
    .venv/bin/python general_analysis_emily/pull_data.py
"""
import os, time, datetime as dt, concurrent.futures as cf
from pathlib import Path
import pandas as pd, requests
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
CACHE = Path(os.environ.get("PM_CACHE", Path.home() / "polymarket_cache"))   # outside iCloud: ~3 GB
BASE = os.environ["PM_BASE_URL"]                                            # trade-file host, from .env
AUTH = (os.environ["PM_USER"], os.environ["PM_PASSWORD"])
engine = create_engine(os.environ["JUNE_DB_URL"], connect_args={"sslmode": "require"})


def pull_catalog():
    """markets, events, resolutions -> parquet. Skips tables already saved."""
    queries = {
        "events": "SELECT slug, title, ticker, start_date, end_date, neg_risk FROM events",
        "resolutions": "SELECT market_slug, resolved_yes, price_at_close, resolved_at, uma_resolution_status FROM resolutions",
        "markets": """SELECT slug, title, event_slug, condition_id, start_date, end_date, neg_risk,
                             outcomes::text AS outcomes, volume_clob, synced_at FROM markets""",
    }
    for name, sql in queries.items():
        out = CACHE / f"{name}.parquet"
        if out.exists():
            continue
        t = time.time()
        chunks = pd.read_sql(sql, engine, chunksize=200_000)
        pd.concat(chunks, ignore_index=True).to_parquet(out, index=False)
        print(f"{name}: saved in {time.time() - t:.0f} s", flush=True)


def pull_trades(first=dt.date(2025, 5, 2), last=dt.date(2026, 10, 9)):
    """One parquet per trade date. Downloads to .part then renames, so a cut-off file is never kept."""
    folder = CACHE / "wallet_trades"
    folder.mkdir(parents=True, exist_ok=True)
    days = [first + dt.timedelta(i) for i in range((last - first).days + 1)]
    todo = [d for d in days if not (folder / f"trades-{d}.parquet").exists()]
    print(f"trade files: {len(days) - len(todo)} cached, {len(todo)} to download", flush=True)

    def get(d):
        dest = folder / f"trades-{d}.parquet"
        part = dest.with_suffix(".part")
        for attempt in range(4):
            try:
                with requests.get(f"{BASE}/wallet_trades/trades-{d}.parquet", auth=AUTH, stream=True, timeout=120) as r:
                    r.raise_for_status()
                    with open(part, "wb") as f:
                        for block in r.iter_content(1 << 20):
                            f.write(block)
                part.rename(dest)
                return d, None
            except Exception as e:
                time.sleep(5 * (attempt + 1))
                err = e
        return d, err

    with cf.ThreadPoolExecutor(8) as ex:
        for i, (d, err) in enumerate(ex.map(get, todo), 1):
            if err:
                print(f"FAILED {d}: {err}", flush=True)
            elif i % 50 == 0:
                print(f"  {i}/{len(todo)} downloaded", flush=True)


if __name__ == "__main__":
    CACHE.mkdir(parents=True, exist_ok=True)
    pull_catalog()
    pull_trades()
    print("done", flush=True)
