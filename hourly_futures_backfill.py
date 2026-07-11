#!/usr/bin/env python3
"""
Hybrid Futures Data Backfill (Zerodha Kite Connect)
====================================================

For every F&O stock, downloads TWO things into local MongoDB (past_data):

  1. DAILY continuous history (10 years) — the front-month rolled series,
     using Kite's continuous=True with interval="day". This is the long-term
     dataset (Kite does NOT support continuous intraday, so daily is the only
     way to get a full 10-year stitched series).

  2. HOURLY candles for the 3 active contracts (current / mid / far month) —
     each contract's ~3-month life, market-hours only (9:15-3:30 IST), using
     continuous=False with interval="60minute". This is the recent detailed
     dataset.

Both are stored in the same collection, distinguished by a `timeframe` field
("day" or "hour").

Features:
  - Serial + rate-limited (respects Kite's ~3 req/sec limit)
  - Resumable: progress tracked per symbol+contract+timeframe
  - Auto-detects current F&O stock list from Kite instruments API
  - Includes Open Interest in every candle

Usage:
  pip install -r requirements.txt
  python generate_token.py                             # daily: get access_token
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

from dotenv import load_dotenv
from kiteconnect import KiteConnect
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import BulkWriteError

log = logging.getLogger("futures_backfill")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "past_data")
COLLECTION = os.getenv("MONGO_COLLECTION", "hourly_futures")
PROGRESS_COLLECTION = "backfill_progress"

# Kite per-request limits: intraday 60min ~ 400 days, daily ~ 2000 days.
CHUNK_DAYS_INTRADAY = 60
CHUNK_DAYS_DAILY = 1800

# Valid market-hours candle start times (IST): 9,10,11,12,13,14,15
VALID_MARKET_HOURS = {9, 10, 11, 12, 13, 14, 15}

# Active contract positions (sorted by expiry)
CONTRACT_LABELS = {
    0: "current_month",
    1: "mid_month",
    2: "far_month",
}

# Index symbols to exclude (we want stock futures only)
INDEX_SYMBOLS = {
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX",
    "BANKEX", "NIFTYNXT50",
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
            self._instruments_cache = [
                i for i in all_nfo
                if i["instrument_type"] == "FUT"
                and i["segment"] == "NFO-FUT"
                and i["name"] != ""
            ]
            log.info("Found %d NFO-FUT instruments", len(self._instruments_cache))
        return self._instruments_cache

    def get_current_fno_symbols(self) -> list[str]:
        """Distinct stock symbols that currently have futures (excludes indices)."""
        instruments = self.get_fno_futures_instruments()
        return sorted(set(
            i["name"] for i in instruments if i["name"] not in INDEX_SYMBOLS
        ))

    def get_instruments_for_symbol(self, symbol: str) -> list[dict]:
        """All active futures instruments for a symbol, sorted by expiry."""
        instruments = self.get_fno_futures_instruments()
        sym = [i for i in instruments if i["name"] == symbol]
        sym.sort(key=lambda x: x["expiry"])
        return sym

    def fetch_candles(self, token, from_date, to_date, interval,
                      continuous, chunk_days) -> list[dict]:
        """Generic chunked historical fetch. Returns list of candle dicts."""
        all_candles = []
        current = from_date
        while current < to_date:
            chunk_end = min(current + timedelta(days=chunk_days), to_date)
            try:
                data = self.kite.historical_data(
                    instrument_token=token,
                    from_date=current,
                    to_date=chunk_end,
                    interval=interval,
                    continuous=continuous,
                    oi=True,
                )
                all_candles.extend(data)
            except Exception as e:
                err = str(e).lower()
                if "too many requests" in err or "429" in err:
                    log.warning("  Rate limited, sleeping 5s...")
                    time.sleep(5)
                    continue
                elif "no data" in err or "empty" in err or "no historical" in err:
                    pass
                else:
                    log.error("  Kite error token %s (%s to %s): %s",
                              token, current, chunk_end, e)
            current = chunk_end + timedelta(days=1)
            time.sleep(self.delay)
        return all_candles



# --------------------------------------------------------------------------- #
# Normalization
# --------------------------------------------------------------------------- #

def normalize(raw, symbol, contract_type, timeframe, expiry, market_hours_only):
    """Convert Kite candles into our MongoDB schema."""
    docs = []
    for c in raw:
        d = c["date"]
        ts = d if isinstance(d, datetime) else datetime.combine(d, datetime.min.time())
        # drop tzinfo for consistent storage
        ts = ts.replace(tzinfo=None)

        if market_hours_only and ts.hour not in VALID_MARKET_HOURS:
            continue

        exp = expiry
        if isinstance(exp, date) and not isinstance(exp, datetime):
            exp = datetime.combine(exp, datetime.min.time())

        doc = {
            "symbol": symbol,
            "contract_type": contract_type,
            "timeframe": timeframe,
            "expiry": exp,
            "timestamp": ts,
            "date": datetime(ts.year, ts.month, ts.day),
            "open": float(c.get("open", 0)),
            "high": float(c.get("high", 0)),
            "low": float(c.get("low", 0)),
            "close": float(c.get("close", 0)),
            "volume": int(c.get("volume", 0)),
            "open_interest": int(c.get("oi", 0)),
        }
        if timeframe == "hour":
            doc["hour"] = ts.hour
            doc["candle_number"] = sorted(VALID_MARKET_HOURS).index(ts.hour) + 1
        docs.append(doc)
    return docs



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
            [("symbol", 1), ("contract_type", 1), ("timeframe", 1), ("timestamp", 1)],
            unique=True, name="uniq_symbol_contract_tf_time",
        )
        self.col.create_index([("symbol", 1), ("timeframe", 1), ("date", 1)],
                              name="idx_symbol_tf_date")
        self.col.create_index([("timestamp", 1)], name="idx_timestamp")

    def get_completed_tasks(self) -> set[str]:
        return {
            f"{d['symbol']}|{d['contract_type']}|{d['timeframe']}"
            for d in self.progress.find(
                {"status": "done"},
                {"symbol": 1, "contract_type": 1, "timeframe": 1})
        }

    def get_last_timestamp(self, symbol, contract_type, timeframe):
        doc = self.col.find_one(
            {"symbol": symbol, "contract_type": contract_type, "timeframe": timeframe},
            sort=[("timestamp", -1)], projection={"timestamp": 1})
        return doc["timestamp"] if doc else None


    def store_candles(self, candles: list[dict]) -> int:
        if not candles or self.dry_run:
            return len(candles)
        ops = [
            ReplaceOne(
                {"symbol": c["symbol"], "contract_type": c["contract_type"],
                 "timeframe": c["timeframe"], "timestamp": c["timestamp"]},
                c, upsert=True)
            for c in candles
        ]
        total = 0
        for i in range(0, len(ops), 1000):
            batch = ops[i:i+1000]
            try:
                self.col.bulk_write(batch, ordered=False)
            except BulkWriteError as bwe:
                nd = [e for e in bwe.details.get("writeErrors", [])
                      if e.get("code") != 11000]
                if nd:
                    raise
            total += len(batch)
        return total

    def mark_done(self, symbol, contract_type, timeframe, n):
        if self.dry_run:
            return
        self.progress.replace_one(
            {"symbol": symbol, "contract_type": contract_type, "timeframe": timeframe},
            {"symbol": symbol, "contract_type": contract_type, "timeframe": timeframe,
             "status": "done", "candle_count": n, "completed_at": datetime.now()},
            upsert=True)

    def close(self):
        if not self.dry_run:
            self.client.close()



# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def run(args):
    load_dotenv()
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

    end_date = date.today()
    start_date = end_date - timedelta(days=years_back * 365)

    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        symbols = fetcher.get_current_fno_symbols()
        log.info("Found %d F&O stock symbols", len(symbols))

    completed = store.get_completed_tasks() if not args.dry_run else set()
    total_skipped = total_fetched = total_candles = 0
    total_tasks = len(symbols) * 4  # 1 daily + 3 hourly per symbol
    task_num = 0

    log.info("HYBRID backfill: %d stocks x (1 daily 10yr + 3 hourly) = %d tasks",
             len(symbols), total_tasks)
    log.info("Date range: %s -> %s (%d years)", start_date, end_date, years_back)


    for symbol in symbols:
        instruments = fetcher.get_instruments_for_symbol(symbol)
        if not instruments:
            log.warning("  %s: no futures instruments, skipping", symbol)
            task_num += 4
            continue
        front_token = instruments[0]["instrument_token"]

        # ---- Task 1: DAILY continuous, full 10 years ----
        task_num += 1
        if f"{symbol}|continuous|day" in completed:
            total_skipped += 1
        else:
            log.info("[%d/%d] %s DAILY continuous (10yr) ...", task_num, total_tasks, symbol)
            last_ts = store.get_last_timestamp(symbol, "continuous", "day")
            frm = last_ts.date() if last_ts else start_date
            raw = fetcher.fetch_candles(front_token, frm, end_date, "day", True, CHUNK_DAYS_DAILY)
            docs = normalize(raw, symbol, "continuous", "day", None, market_hours_only=False)
            n = store.store_candles(docs)
            store.mark_done(symbol, "continuous", "day", n)
            total_candles += n
            total_fetched += 1
            if docs:
                log.info("  -> %d daily candles (%s to %s)", n,
                         docs[0]["date"].date(), docs[-1]["date"].date())
            else:
                log.warning("  -> 0 daily candles")
            time.sleep(delay)

        # ---- Tasks 2-4: HOURLY per contract (current / mid / far) ----
        for idx, contract_type in CONTRACT_LABELS.items():
            task_num += 1
            if f"{symbol}|{contract_type}|hour" in completed:
                total_skipped += 1
                continue
            if idx >= len(instruments):
                store.mark_done(symbol, contract_type, "hour", 0)
                continue
            instr = instruments[idx]
            token = instr["instrument_token"]
            expiry = instr["expiry"]
            log.info("[%d/%d] %s %s HOURLY (expiry %s) ...",
                     task_num, total_tasks, symbol, contract_type, expiry)
            last_ts = store.get_last_timestamp(symbol, contract_type, "hour")
            frm = last_ts.date() if last_ts else start_date
            raw = fetcher.fetch_candles(token, frm, end_date, "60minute", False, CHUNK_DAYS_INTRADAY)
            docs = normalize(raw, symbol, contract_type, "hour", expiry, market_hours_only=True)
            n = store.store_candles(docs)
            store.mark_done(symbol, contract_type, "hour", n)
            total_candles += n
            total_fetched += 1
            if docs:
                log.info("  -> %d hourly candles (%s to %s)", n,
                         docs[0]["date"].date(), docs[-1]["date"].date())
            else:
                log.warning("  -> 0 hourly candles")
            time.sleep(delay)

    log.info("Done. fetched=%d skipped=%d total_candles=%d",
             total_fetched, total_skipped, total_candles)
    store.close()



def main():
    parser = argparse.ArgumentParser(
        description="Hybrid futures backfill (10yr daily + hourly) from Kite into MongoDB"
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
