#!/usr/bin/env python3
"""
Hourly Futures Data Backfill
=============================

Downloads hourly closing candles for all F&O stocks (current month + mid month
futures) from TradingView and stores them in local MongoDB.

Data source: TradingView WebSocket (free, anonymous, no API key needed).
Limit: ~3.5 years of hourly data per symbol (6,077 candles max). This is the
maximum available from any free source for NSE futures hourly candles.

Features:
  - Fetches BOTH current-month (SYMBOL1!) and mid-month (SYMBOL2!) futures
  - Serial + polite: one symbol at a time with delay to avoid being blocked
  - Resumable: progress tracked per symbol; if interrupted, re-run picks up
  - Auto-detects current F&O stock list from NSE bhavcopy
  - Checks for missing/new stocks and fetches them too
  - Dockerized for easy deployment

Output: mongodb://localhost:27017 -> database "past_data"
  - Collection: hourly_futures (one doc per symbol per contract per hour)
  - Collection: backfill_progress (tracks which symbols are done)

Usage:
  pip install -r requirements.txt
  python hourly_futures_backfill.py                    # full backfill
  python hourly_futures_backfill.py --symbol RELIANCE  # one stock only
  python hourly_futures_backfill.py --dry-run          # fetch + print, no DB
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import random
import re
import string
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta

import requests
import websocket
from dotenv import load_dotenv
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

# TradingView WebSocket
TV_WS_URL = "wss://data.tradingview.com/socket.io/websocket"
TV_ORIGIN = "https://www.tradingview.com"

# How many bars to request per batch (max useful is ~6100)
BARS_PER_REQUEST = 300
BARS_PER_PAGE = 2000
MAX_PAGES = 20  # pagination attempts

# Contract types
CONTRACT_TYPES = {
    "1!": "current_month",
    "2!": "mid_month",
}

# UDiFF bhavcopy URL for fetching current FNO stock list
UDIFF_URL = (
    "https://nsearchives.nseindia.com/content/fo/"
    "BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
}


# --------------------------------------------------------------------------- #
# TradingView WebSocket Client
# --------------------------------------------------------------------------- #

class TradingViewFetcher:
    """Fetches hourly candle data from TradingView WebSocket."""

    def __init__(self, delay: float = 2.0):
        self.delay = delay

    def _gen_chart(self) -> str:
        return "cs_" + "".join(random.choices(string.ascii_lowercase, k=12))

    def _send(self, ws, msg: str):
        ws.send(f"~m~{len(msg)}~m~{msg}")

    def _recv_all(self, ws, timeout: float = 3.0, max_reads: int = 20) -> str:
        data = ""
        for _ in range(max_reads):
            try:
                ws.settimeout(timeout)
                data += ws.recv()
            except (websocket.WebSocketTimeoutException, Exception):
                break
        return data

    def fetch_hourly(self, symbol: str, contract_suffix: str) -> list[dict]:
        """
        Fetch all available hourly candles for a TradingView symbol.
        Returns list of dicts with: timestamp, open, high, low, close, volume.
        """
        tv_symbol = f"NSE:{symbol}{contract_suffix}"
        contract_type = CONTRACT_TYPES[contract_suffix]

        try:
            ws = websocket.create_connection(
                TV_WS_URL,
                headers={"Origin": TV_ORIGIN},
                timeout=20,
            )
        except Exception as e:
            log.error("WebSocket connection failed for %s: %s", tv_symbol, e)
            return []

        try:
            ws.recv()  # initial message
            chart = self._gen_chart()

            # Auth + session
            self._send(ws, json.dumps({"m": "set_auth_token", "p": ["unauthorized_user_token"]}))
            self._send(ws, json.dumps({"m": "chart_create_session", "p": [chart, ""]}))

            # Resolve symbol + create series
            sym_config = json.dumps({"symbol": tv_symbol, "adjustment": "splits"})
            self._send(ws, json.dumps({
                "m": "resolve_symbol",
                "p": [chart, "sds_sym_1", f"={sym_config}"]
            }))
            self._send(ws, json.dumps({
                "m": "create_series",
                "p": [chart, "sds_1", "s1", "sds_sym_1", "60", BARS_PER_REQUEST, ""]
            }))

            time.sleep(4)
            all_data = self._recv_all(ws, timeout=3, max_reads=25)

            # Check for errors (symbol not found, etc.)
            if "symbol_error" in all_data or "invalid_symbol" in all_data:
                log.warning("  %s: symbol not found on TradingView", tv_symbol)
                ws.close()
                return []

            # Paginate to get all historical data
            for page in range(MAX_PAGES):
                self._send(ws, json.dumps({
                    "m": "request_more_data",
                    "p": [chart, "sds_1", BARS_PER_PAGE]
                }))
                time.sleep(3)
                chunk = self._recv_all(ws, timeout=2, max_reads=15)
                if not chunk or "timescale_update" not in chunk:
                    break
                all_data += chunk

            ws.close()
        except Exception as e:
            log.error("WebSocket error for %s: %s", tv_symbol, e)
            try:
                ws.close()
            except:
                pass
            return []

        # Parse candle data from response
        # Format: "v":[timestamp, open, high, low, close, volume]
        matches = re.findall(
            r'"v":\[(\d+\.?\d*),([^,]+),([^,]+),([^,]+),([^,]+),([^\]]+)\]',
            all_data
        )

        candles = []
        seen_ts = set()
        for m in matches:
            ts = float(m[0])
            if ts in seen_ts:
                continue
            seen_ts.add(ts)

            dt = datetime.utcfromtimestamp(ts)
            candles.append({
                "symbol": symbol,
                "contract_type": contract_type,
                "timestamp": dt,
                "hour": dt.hour,
                "date": datetime(dt.year, dt.month, dt.day),
                "open": round(float(m[1]), 2),
                "high": round(float(m[2]), 2),
                "low": round(float(m[3]), 2),
                "close": round(float(m[4]), 2),
                "volume": int(float(m[5])),
            })

        candles.sort(key=lambda x: x["timestamp"])
        return candles


# --------------------------------------------------------------------------- #
# FNO Stock List
# --------------------------------------------------------------------------- #

def get_current_fno_stocks() -> list[str]:
    """Get the current F&O stock list from the latest NSE bhavcopy."""
    from datetime import date

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    # Try last 10 weekdays
    today = date.today()
    for i in range(10):
        dt = today - timedelta(days=i)
        if dt.weekday() >= 5:
            continue
        url = UDIFF_URL.format(yyyymmdd=dt.strftime("%Y%m%d"))
        try:
            r = session.get(url, timeout=15)
            if r.status_code != 200:
                continue
            with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                name = zf.namelist()[0]
                with zf.open(name) as fp:
                    txt = fp.read().decode("utf-8")
            symbols = set()
            for row in csv.DictReader(io.StringIO(txt)):
                if (row.get("FinInstrmTp") or "").strip() == "STF":
                    symbols.add(row["TckrSymb"].strip())
            if symbols:
                log.info("Got %d F&O stocks from bhavcopy %s", len(symbols), dt)
                return sorted(symbols)
        except (zipfile.BadZipFile, Exception):
            continue

    log.error("Could not fetch current F&O stock list from NSE")
    return []


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

    def get_completed_symbols(self) -> dict[str, dict]:
        """Returns {symbol: {contract_type: True/False, ...}} for completed fetches."""
        completed = {}
        for doc in self.progress.find({"status": "done"}):
            sym = doc["symbol"]
            ct = doc["contract_type"]
            if sym not in completed:
                completed[sym] = {}
            completed[sym][ct] = True
        return completed

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
        try:
            self.col.bulk_write(ops, ordered=False)
        except BulkWriteError as bwe:
            non_dupes = [e for e in bwe.details.get("writeErrors", [])
                         if e.get("code") != 11000]
            if non_dupes:
                raise
        return len(candles)

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
    uri = os.getenv("MONGODB_URI", MONGODB_URI)
    db_name = os.getenv("MONGO_DB", DB_NAME)
    delay = float(os.getenv("REQUEST_DELAY", "3.0"))

    store = MongoStore(uri, db_name, dry_run=args.dry_run)
    fetcher = TradingViewFetcher(delay=delay)

    # Get symbols
    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        symbols = get_current_fno_stocks()
        if not symbols:
            sys.exit("ERROR: Could not get F&O stock list. Try --symbol RELIANCE")

    # Check which are already done (resume)
    completed = store.get_completed_symbols() if not args.dry_run else {}
    total_skipped = 0
    total_fetched = 0
    total_candles = 0

    total_tasks = len(symbols) * len(CONTRACT_TYPES)
    task_num = 0

    log.info("Backfilling hourly futures: %d stocks × %d contracts = %d tasks",
             len(symbols), len(CONTRACT_TYPES), total_tasks)

    for symbol in symbols:
        for suffix, contract_type in CONTRACT_TYPES.items():
            task_num += 1

            # Skip if already completed
            if symbol in completed and completed[symbol].get(contract_type):
                total_skipped += 1
                continue

            log.info("[%d/%d] Fetching %s %s ...", task_num, total_tasks, symbol, contract_type)

            candles = fetcher.fetch_hourly(symbol, suffix)

            if candles:
                n = store.store_candles(candles)
                store.mark_done(symbol, contract_type, n)
                total_candles += n
                total_fetched += 1

                first_dt = candles[0]["timestamp"].date()
                last_dt = candles[-1]["timestamp"].date()
                log.info("  -> %d candles stored (%s to %s)", n, first_dt, last_dt)
            else:
                # Mark as done with 0 candles (symbol might not have futures on TV)
                store.mark_done(symbol, contract_type, 0)
                total_fetched += 1
                log.warning("  -> 0 candles (symbol may not be available on TradingView)")

            # Be polite
            time.sleep(delay)

    log.info("Done. fetched=%d skipped=%d total_candles=%d",
             total_fetched, total_skipped, total_candles)
    store.close()


def main():
    parser = argparse.ArgumentParser(
        description="Backfill hourly futures candles from TradingView into MongoDB"
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
