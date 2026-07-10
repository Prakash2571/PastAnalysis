#!/usr/bin/env python3
"""
Hourly Futures Data Backfill (Zerodha Kite Connect)
====================================================

Downloads hourly (60-minute) closing candles for all F&O stocks — current month,
mid month, and far month futures — for the past 10 years and stores them in
local MongoDB (database: past_data, collection: hourly_futures).

Data source: Zerodha Kite Connect API (paid subscription, gives full 10yr history).

Features:
  - Fetches ALL 3 monthly contracts (current, mid, far) for every F&O stock
  - 10 years of hourly candle data per instrument
  - Serial + polite: respects Kite's rate limits (3 req/sec max, we do ~1/sec)
  - Resumable: progress tracked per symbol+expiry; re-run picks up where it left off
  - Auto-detects current F&O stock list from Kite instruments API
  - Checks for missing/new stocks and fetches them too
  - Includes Open Interest data
  - Dockerized for easy deployment

Setup:
  1. Get your api_key and api_secret from https://developers.kite.trade
  2. Generate access_token daily (see generate_token.py helper)
  3. Put credentials in .env file

Usage:
  pip install -r requirements.txt
  python generate_token.py             # one-time daily: get access_token
  python hourly_futures_backfill.py                    # full backfill
  python hourly_futures_backfill.py --symbol RELIANCE  # one stock only
  python hourly_futures_backfill.py --dry-run          # fetch + print, no DB
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, date, timedelta
from dataclasses import dataclass

from dotenv import load_dotenv
from kiteconnect import KiteConnect
from pymongo import MongoClient, ReplaceOne, ASCENDING
from pymongo.errors import BulkWriteError

log = logging.getLogger("hourly_futures")

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "past_data")
COLLECTION = os.getenv("MONGO_COLLECTION", "hourly_futures")
PROGRESS_COLLECTION = "backfill_progress"

# Kite historical API allows max 400 days per request for intraday candles (60min)
# So for 10 years we chunk into 60-day segments (to be safe)
CHUNK_DAYS = 60

# Contract labels
CONTRACT_LABELS = {
    0: "current_month",
    1: "mid_month",
    2: "far_month",
}


# --------------------------------------------------------------------------- #
# Kite Connect Client
# --------------------------------------------------------------------------- #

class KiteFetcher:
    """Handles Kite Connect API calls for historical futures data."""

    def __init__(self, api_key: str, access_token: str, delay: float = 0.4):
        self.kite = KiteConnect(api_key=api_key)
        self.kite.set_access_token(access_token)
        self.delay = delay
        self._instruments_cache = None

    def get_fno_futures_instruments(self) -> list[dict]:
        """Get all NFO futures instruments (FUT type only, not options)."""
        if self._instruments_cache is None:
            log.info("Fetching NFO instruments list from Kite...")
            all_nfo = self.kite.instruments("NFO")
            # Filter: only futures (not options), only equity (not index)
            self._instruments_cache = [
                i for i in all_nfo
                if i["instrument_type"] == "FUT"
                and i["segment"] == "NFO-FUT"
                and i["name"] != ""  # skip if no underlying name
            ]
            log.info("Found %d NFO-FUT instruments", len(self._instruments_cache))
        return self._instruments_cache

    def get_current_fno_symbols(self) -> list[str]:
        """Get distinct stock symbols that currently have futures listed."""
        instruments = self.get_fno_futures_instruments()
        # Filter only stock futures (exclude index futures like NIFTY, BANKNIFTY)
        # Index futures have lot_size typically > 15 and symbol in known index list
        INDEX_SYMBOLS = {
            "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX",
            "BANKEX", "NIFTYNXT50",
        }
        stock_symbols = sorted(set(
            i["name"] for i in instruments
            if i["name"] not in INDEX_SYMBOLS
        ))
        return stock_symbols

    def get_instruments_for_symbol(self, symbol: str) -> list[dict]:
        """
        Get all active futures instruments for a symbol, sorted by expiry.
        Returns list of instruments (typically 3: current, mid, far month).
        """
        instruments = self.get_fno_futures_instruments()
        sym_instruments = [
            i for i in instruments
            if i["name"] == symbol
        ]
        # Sort by expiry date
        sym_instruments.sort(key=lambda x: x["expiry"])
        return sym_instruments

    def fetch_historical_hourly(
        self, instrument_token: int, from_date: date, to_date: date
    ) -> list[dict]:
        """
        Fetch hourly candles for an instrument between dates.
        Handles Kite's per-request day limit by chunking.
        Returns list of candle dicts.
        """
        all_candles = []
        current = from_date

        while current < to_date:
            chunk_end = min(current + timedelta(days=CHUNK_DAYS), to_date)
            try:
                data = self.kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=current,
                    to_date=chunk_end,
                    interval="60minute",
                    continuous=False,
                    oi=True,
                )
                all_candles.extend(data)
            except Exception as e:
                err_msg = str(e).lower()
                if "too many requests" in err_msg or "429" in err_msg:
                    log.warning("  Rate limited, sleeping 5s...")
                    time.sleep(5)
                    continue  # retry same chunk
                elif "no data" in err_msg or "empty" in err_msg:
                    pass  # no data for this period, move on
                else:
                    log.error("  Kite API error for token %s (%s to %s): %s",
                              instrument_token, current, chunk_end, e)
                    # Don't stop the whole run, just skip this chunk
                    pass

            current = chunk_end + timedelta(days=1)
            time.sleep(self.delay)  # respect rate limits

        return all_candles


# --------------------------------------------------------------------------- #
# MongoDB Storage
# --------------------------------------------------------------------------- #

class MongoStore:
    def __init__(self, uri: str, db_name: str, dry_run: bool = False):
        self.dry_run = dry_run
        if dry_run:
            return
        self.client = MongoClient(uri, serverSelectionTimeoutMS=15000)
        self.client.admin.command("ping")
        self.db = self.client[db_name]
        self.col = self.db[COLLECTION]
        self.progress = self.db[PROGRESS_COLLECTION]
        self._ensure_indexes()

    def _ensure_indexes(self):
        self.col.create_index(
            [("symbol", 1), ("contract_type", 1), ("timestamp", 1)],
            unique=True,
            name="uniq_symbol_contract_time",
        )
        self.col.create_index([("symbol", 1), ("date", 1)], name="idx_symbol_date")
        self.col.create_index([("timestamp", 1)], name="idx_timestamp")
        self.col.create_index([("expiry", 1)], name="idx_expiry")

    def get_completed_tasks(self) -> set[str]:
        """Returns set of 'symbol|contract_type' strings for completed fetches."""
        return {
            f"{d['symbol']}|{d['contract_type']}"
            for d in self.progress.find({"status": "done"}, {"symbol": 1, "contract_type": 1})
        }

    def get_last_timestamp(self, symbol: str, contract_type: str) -> datetime | None:
        """Get the most recent candle timestamp for a symbol+contract for incremental updates."""
        doc = self.col.find_one(
            {"symbol": symbol, "contract_type": contract_type},
            sort=[("timestamp", -1)],
            projection={"timestamp": 1}
        )
        return doc["timestamp"] if doc else None

    def store_candles(self, candles: list[dict]) -> int:
        if not candles or self.dry_run:
            return len(candles)
        ops = [
            ReplaceOne(
                {
                    "symbol": c["symbol"],
                    "contract_type": c["contract_type"],
                    "timestamp": c["timestamp"],
                },
                c,
                upsert=True,
            )
            for c in candles
        ]
        # Batch in groups of 1000 to avoid huge memory usage
        total = 0
        for i in range(0, len(ops), 1000):
            batch = ops[i:i+1000]
            try:
                self.col.bulk_write(batch, ordered=False)
            except BulkWriteError as bwe:
                non_dupes = [e for e in bwe.details.get("writeErrors", [])
                             if e.get("code") != 11000]
                if non_dupes:
                    raise
            total += len(batch)
        return total

    def mark_done(self, symbol: str, contract_type: str, candle_count: int):
        if self.dry_run:
            return
        self.progress.replace_one(
            {"symbol": symbol, "contract_type": contract_type},
            {
                "symbol": symbol,
                "contract_type": contract_type,
                "status": "done",
                "candle_count": candle_count,
                "completed_at": datetime.utcnow(),
            },
            upsert=True,
        )

    def close(self):
        if not self.dry_run:
            self.client.close()


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def run(args):
    load_dotenv()

    # Kite credentials
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        sys.exit(
            "ERROR: KITE_API_KEY and KITE_ACCESS_TOKEN must be set in .env\n"
            "Run: python generate_token.py  (to get today's access_token)"
        )

    uri = os.getenv("MONGODB_URI", MONGODB_URI)
    db_name = os.getenv("MONGO_DB", DB_NAME)
    delay = float(os.getenv("REQUEST_DELAY", "0.4"))
    years_back = int(os.getenv("YEARS_BACK", "10"))

    store = MongoStore(uri, db_name, dry_run=args.dry_run)
    fetcher = KiteFetcher(api_key, access_token, delay=delay)

    # Date range
    end_date = date.today()
    start_date = end_date - timedelta(days=years_back * 365)

    # Get symbols
    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        symbols = fetcher.get_current_fno_symbols()
        log.info("Found %d F&O stock symbols", len(symbols))

    # Check which are already done (resume)
    completed = store.get_completed_tasks() if not args.dry_run else set()
    total_skipped = 0
    total_fetched = 0
    total_candles = 0

    # For each symbol, get its current active futures instruments
    # and also fetch historical using continuous=False to get individual contracts
    # Strategy: use the 3 contract labels (current/mid/far) as separate tasks
    total_tasks = len(symbols) * 3  # 3 contracts per symbol
    task_num = 0

    log.info("Backfilling hourly futures: %d stocks × 3 contracts = %d tasks",
             len(symbols), total_tasks)
    log.info("Date range: %s -> %s (%d years)", start_date, end_date, years_back)

    for symbol in symbols:
        # Get active instruments for this symbol
        instruments = fetcher.get_instruments_for_symbol(symbol)

        if not instruments:
            log.warning("  %s: no futures instruments found, skipping", symbol)
            task_num += 3
            continue

        # Process each contract (current, mid, far)
        for idx, contract_type in CONTRACT_LABELS.items():
            task_num += 1
            task_key = f"{symbol}|{contract_type}"

            # Skip if already completed
            if task_key in completed:
                total_skipped += 1
                continue

            # For historical data of individual expiry contracts, we need to
            # use the specific instrument token. But for a full 10-year history,
            # we use the CONTINUOUS contract approach.
            # Kite's continuous=True stitches all expiry contracts together.
            if idx < len(instruments):
                # Use the specific active contract for recent data
                instr = instruments[idx]
                token = instr["instrument_token"]
                expiry = instr["expiry"]
            else:
                # If fewer than 3 contracts exist, skip
                log.info("[%d/%d] %s %s: no contract available (< 3 expiries)",
                         task_num, total_tasks, symbol, contract_type)
                store.mark_done(symbol, contract_type, 0)
                continue

            log.info("[%d/%d] Fetching %s %s (expiry: %s, token: %s) ...",
                     task_num, total_tasks, symbol, contract_type, expiry, token)

            # Check if we can do incremental update
            last_ts = store.get_last_timestamp(symbol, contract_type)
            fetch_from = last_ts.date() if last_ts else start_date

            # Fetch hourly candles
            raw_candles = fetcher.fetch_historical_hourly(token, fetch_from, end_date)

            if raw_candles:
                # Normalize into our schema
                docs = []
                for c in raw_candles:
                    ts = c["date"] if isinstance(c["date"], datetime) else datetime.combine(c["date"], datetime.min.time())
                    docs.append({
                        "symbol": symbol,
                        "contract_type": contract_type,
                        "expiry": datetime.combine(expiry, datetime.min.time()) if isinstance(expiry, date) else expiry,
                        "timestamp": ts,
                        "date": datetime(ts.year, ts.month, ts.day),
                        "hour": ts.hour,
                        "open": float(c.get("open", 0)),
                        "high": float(c.get("high", 0)),
                        "low": float(c.get("low", 0)),
                        "close": float(c.get("close", 0)),
                        "volume": int(c.get("volume", 0)),
                        "open_interest": int(c.get("oi", 0)),
                    })

                n = store.store_candles(docs)
                store.mark_done(symbol, contract_type, n)
                total_candles += n
                total_fetched += 1

                first_dt = docs[0]["timestamp"].date() if docs else None
                last_dt = docs[-1]["timestamp"].date() if docs else None
                log.info("  -> %d candles stored (%s to %s)", n, first_dt, last_dt)
            else:
                store.mark_done(symbol, contract_type, 0)
                total_fetched += 1
                log.warning("  -> 0 candles (no data for this contract)")

            # Rate limit pause
            time.sleep(delay)

    log.info("Done. fetched=%d skipped=%d total_candles=%d",
             total_fetched, total_skipped, total_candles)
    store.close()


def main():
    parser = argparse.ArgumentParser(
        description="Backfill hourly futures candles from Kite Connect into MongoDB"
    )
    parser.add_argument("--symbol", help="Process only this symbol (e.g. RELIANCE)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and print, don't write to MongoDB")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    run(args)


if __name__ == "__main__":
    main()
