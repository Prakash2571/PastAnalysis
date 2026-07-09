#!/usr/bin/env python3
"""
Calendar Spread Analysis for NSE Stock Futures (FUTSTK).

For every symbol and trading date this script looks at the running futures
contracts, picks the two nearest expiries -- the "current month" (near) and the
"mid month" (next) -- and computes the calendar spread on the CLOSE price:

        spread = close(mid_month) - close(current_month)

It then aggregates, per symbol, over the past N years (default 7) or from the
first date the symbol appears in F&O:

    * max_spread        -> largest spread observed
    * min_spread        -> smallest (most negative) spread observed
    * mean_spread       -> average spread
    * mean_deviation    -> mean absolute deviation from the mean spread
                           ( (1/n) * sum(|spread_i - mean_spread|) )
    * max_abs_spread    -> largest spread in absolute terms

Only symbols that are CURRENTLY in the NSE F&O list are analysed. The current
list is fetched from NSE's market-lots file; if NSE is unreachable it falls
back to the symbols present on the most recent trading date already in the DB.

Results are written back to the local MongoDB:
    * <db>.spread_daily    -> one document per (symbol, trading_date)
    * <db>.spread_summary  -> one document per symbol with the statistics

Everything runs server-side via the MongoDB aggregation framework, so it stays
fast even on ~1M documents.
"""

import argparse
import csv
import io
import os
from datetime import datetime, timedelta
from urllib.request import Request, urlopen

from pymongo import ASCENDING, DESCENDING, MongoClient

# NSE publishes the current F&O market lots (and therefore the current F&O
# universe) in this CSV. The "Derivatives on Individual Securities" section
# lists every stock currently in F&O.
NSE_MKTLOTS_URLS = [
    "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv",
    "https://www1.nseindia.com/content/fo/fo_mktlots.csv",
]
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,text/plain,*/*",
}


def years_ago(n_years: int) -> datetime:
    """Return a UTC datetime n_years in the past (approx, leap-safe)."""
    return datetime.utcnow() - timedelta(days=int(round(365.25 * n_years)))


def parse_fno_symbols_csv(text: str) -> set:
    """
    Parse NSE's market-lots CSV and return the set of stock symbols that are
    currently in F&O (the "Derivatives on Individual Securities" section only,
    so indices like NIFTY/BANKNIFTY are excluded).
    """
    symbols = set()
    in_stock_section = False
    reader = csv.reader(io.StringIO(text))
    for row in reader:
        if not row:
            continue
        first = row[0].strip().upper()
        # The stock section begins after this marker row.
        if first.startswith("DERIVATIVES ON INDIVIDUAL"):
            in_stock_section = True
            continue
        if not in_stock_section:
            continue
        if len(row) < 2:
            continue
        symbol = row[1].strip().upper()
        # Skip the header ("Symbol") and any blank entries.
        if symbol and symbol != "SYMBOL":
            symbols.add(symbol)
    return symbols


def fetch_fno_symbols_from_nse() -> set:
    """Download and parse the current F&O stock universe from NSE."""
    last_err = None
    for url in NSE_MKTLOTS_URLS:
        try:
            req = Request(url, headers=NSE_HEADERS)
            with urlopen(req, timeout=30) as resp:
                text = resp.read().decode("utf-8", errors="replace")
            symbols = parse_fno_symbols_csv(text)
            if symbols:
                return symbols
        except Exception as exc:  # noqa: BLE001 - fall back to next url / DB
            last_err = exc
    raise RuntimeError(f"Could not fetch F&O list from NSE: {last_err}")


def fetch_fno_symbols_from_db(db, source: str, instrument: str) -> set:
    """
    Fallback: derive the "currently in F&O" universe from the data itself --
    every symbol that traded on the most recent trading date in the collection.
    """
    latest = list(
        db[source]
        .find({"instrument": instrument}, {"trading_date": 1})
        .sort("trading_date", DESCENDING)
        .limit(1)
    )
    if not latest:
        return set()
    latest_date = latest[0]["trading_date"]
    symbols = db[source].distinct(
        "symbol", {"instrument": instrument, "trading_date": latest_date}
    )
    return {s.strip().upper() for s in symbols if s}


def resolve_fno_symbols(db, source: str, instrument: str, fno_source: str) -> set:
    """Get the current F&O symbol universe according to the chosen strategy."""
    if fno_source in ("nse", "auto"):
        try:
            symbols = fetch_fno_symbols_from_nse()
            print(f"      fetched {len(symbols)} F&O symbols from NSE.")
            return symbols
        except Exception as exc:  # noqa: BLE001
            if fno_source == "nse":
                raise
            print(f"      NSE fetch failed ({exc}); falling back to DB latest date.")
    symbols = fetch_fno_symbols_from_db(db, source, instrument)
    print(f"      derived {len(symbols)} F&O symbols from DB's latest trading date.")
    return symbols


def build_daily_pipeline(cutoff: datetime, instrument: str, daily_coll: str, symbols):
    """
    Aggregation that produces one document per (symbol, trading_date) with the
    near/mid month contracts and the close-price calendar spread, written to
    `daily_coll` via $out.
    """
    return [
        # 1. Only stock futures, within the time window, and only symbols that
        #    are currently in the F&O list.
        {
            "$match": {
                "instrument": instrument,
                "trading_date": {"$gte": cutoff},
                "symbol": {"$in": sorted(symbols)},
            }
        },

        # 2. Sort so expiries are ascending within each (symbol, trading_date).
        {"$sort": {"symbol": ASCENDING, "trading_date": ASCENDING, "expiry": ASCENDING}},

        # 3. Collect the running contracts for each symbol/day.
        {
            "$group": {
                "_id": {"symbol": "$symbol", "trading_date": "$trading_date"},
                "contracts": {
                    "$push": {
                        "expiry": "$expiry",
                        # Use close; fall back to settle_price only if close is missing.
                        "close": {"$ifNull": ["$close", "$settle_price"]},
                    }
                },
            }
        },

        # 4. Need at least two expiries to form a calendar spread.
        {"$match": {"contracts.1": {"$exists": True}}},

        # 5. Grab the two nearest expiries: current (0) and mid (1) month.
        {
            "$project": {
                "_id": 0,
                "symbol": "$_id.symbol",
                "trading_date": "$_id.trading_date",
                "near": {"$arrayElemAt": ["$contracts", 0]},
                "mid": {"$arrayElemAt": ["$contracts", 1]},
            }
        },

        # 6. Drop rows where either close price is missing.
        {
            "$match": {
                "near.close": {"$ne": None},
                "mid.close": {"$ne": None},
            }
        },

        # 7. Compute the spread on close prices.
        {
            "$project": {
                "symbol": 1,
                "trading_date": 1,
                "near_expiry": "$near.expiry",
                "mid_expiry": "$mid.expiry",
                "near_close": "$near.close",
                "mid_close": "$mid.close",
                "spread": {"$subtract": ["$mid.close", "$near.close"]},
            }
        },

        # 8. Persist the daily spread series.
        {"$out": daily_coll},
    ]


def build_summary_pipeline(summary_coll: str):
    """
    Aggregation over the daily spread collection to produce per-symbol stats,
    including the mean deviation, written to `summary_coll` via $out.
    """
    return [
        {
            "$group": {
                "_id": "$symbol",
                "spreads": {"$push": "$spread"},
                "mean_spread": {"$avg": "$spread"},
                "max_spread": {"$max": "$spread"},
                "min_spread": {"$min": "$spread"},
                "observations": {"$sum": 1},
                "first_date": {"$min": "$trading_date"},
                "last_date": {"$max": "$trading_date"},
            }
        },
        # Mean absolute deviation from the mean, and max absolute spread.
        {
            "$addFields": {
                "mean_deviation": {
                    "$avg": {
                        "$map": {
                            "input": "$spreads",
                            "as": "s",
                            "in": {"$abs": {"$subtract": ["$$s", "$mean_spread"]}},
                        }
                    }
                },
                "max_abs_spread": {
                    "$max": {
                        "$map": {
                            "input": "$spreads",
                            "as": "s",
                            "in": {"$abs": "$$s"},
                        }
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "symbol": "$_id",
                "observations": 1,
                "first_date": 1,
                "last_date": 1,
                "mean_spread": {"$round": ["$mean_spread", 4]},
                "max_spread": {"$round": ["$max_spread", 4]},
                "min_spread": {"$round": ["$min_spread", 4]},
                "max_abs_spread": {"$round": ["$max_abs_spread", 4]},
                "mean_deviation": {"$round": ["$mean_deviation", 4]},
            }
        },
        {"$sort": {"symbol": ASCENDING}},
        {"$out": summary_coll},
    ]


def main():
    parser = argparse.ArgumentParser(
        description="Calendar spread analysis for NSE stock futures (close price)."
    )
    parser.add_argument(
        "--uri",
        default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB connection URI (default: mongodb://localhost:27017)",
    )
    parser.add_argument("--db", default="nse_fno", help="Database name (default: nse_fno)")
    parser.add_argument(
        "--source", default="stock_futures", help="Source collection (default: stock_futures)"
    )
    parser.add_argument(
        "--daily-collection",
        default="spread_daily",
        help="Output collection for the daily spread series (default: spread_daily)",
    )
    parser.add_argument(
        "--summary-collection",
        default="spread_summary",
        help="Output collection for the per-symbol stats (default: spread_summary)",
    )
    parser.add_argument(
        "--instrument", default="FUTSTK", help="Instrument type to analyse (default: FUTSTK)"
    )
    parser.add_argument(
        "--years", type=int, default=7, help="Look-back window in years (default: 7)"
    )
    parser.add_argument(
        "--all-history",
        action="store_true",
        help="Ignore the --years window and use every available date.",
    )
    parser.add_argument(
        "--fno-source",
        choices=["auto", "nse", "db"],
        default="auto",
        help=(
            "How to determine the current F&O universe: 'nse' (fetch from NSE), "
            "'db' (symbols on the latest trading date in the DB), or 'auto' "
            "(NSE with DB fallback). Default: auto."
        ),
    )
    args = parser.parse_args()

    cutoff = datetime(1970, 1, 1) if args.all_history else years_ago(args.years)

    client = MongoClient(args.uri)
    db = client[args.db]

    window = "all history" if args.all_history else f"last {args.years} years (since {cutoff.date()})"
    print(f"Connected to {args.uri} -> db '{args.db}'")
    print(f"Source: {args.source} | instrument: {args.instrument} | window: {window}")
    print("Spread definition: close(mid_month) - close(current_month)\n")

    # --- Stage 0: resolve the CURRENT F&O universe ------------------------
    print(f"[0/4] Resolving current F&O symbols (source: {args.fno_source}) ...")
    fno_symbols = resolve_fno_symbols(db, args.source, args.instrument, args.fno_source)
    if not fno_symbols:
        raise SystemExit(
            "No current F&O symbols could be determined. "
            "Try --fno-source db, or check connectivity to NSE."
        )
    # Keep only symbols that actually exist in the source collection.
    present = set(db[args.source].distinct("symbol", {"instrument": args.instrument}))
    present_upper = {s.strip().upper(): s for s in present if s}
    matched = {present_upper[s] for s in fno_symbols if s in present_upper}
    print(
        f"      {len(matched)} of {len(fno_symbols)} F&O symbols found in "
        f"{args.source} and will be analysed.\n"
    )

    # --- Stage 1: daily spread series -------------------------------------
    print(f"[1/4] Building daily spreads -> {args.db}.{args.daily_collection} ...")
    db[args.source].aggregate(
        build_daily_pipeline(cutoff, args.instrument, args.daily_collection, matched),
        allowDiskUse=True,
    )
    daily_count = db[args.daily_collection].count_documents({})
    print(f"      done: {daily_count:,} daily spread documents.")

    # Helpful indexes on the daily series.
    db[args.daily_collection].create_index([("symbol", ASCENDING), ("trading_date", ASCENDING)])

    # --- Stage 2: per-symbol summary --------------------------------------
    print(f"[2/4] Building per-symbol summary -> {args.db}.{args.summary_collection} ...")
    db[args.daily_collection].aggregate(
        build_summary_pipeline(args.summary_collection),
        allowDiskUse=True,
    )
    db[args.summary_collection].create_index([("symbol", ASCENDING)], unique=True)
    symbol_count = db[args.summary_collection].count_documents({})
    print(f"      done: {symbol_count:,} symbols summarised.")

    # --- Stage 3: preview --------------------------------------------------
    print("\n[3/4] Sample summary (top 10 by max_abs_spread):")
    header = f"{'SYMBOL':<14}{'OBS':>6}{'MEAN':>12}{'MAX':>12}{'MIN':>12}{'MEAN_DEV':>12}"
    print(header)
    print("-" * len(header))
    for doc in (
        db[args.summary_collection]
        .find({}, {"_id": 0})
        .sort("max_abs_spread", -1)
        .limit(10)
    ):
        print(
            f"{doc['symbol']:<14}"
            f"{doc['observations']:>6}"
            f"{doc['mean_spread']:>12.2f}"
            f"{doc['max_spread']:>12.2f}"
            f"{doc['min_spread']:>12.2f}"
            f"{doc['mean_deviation']:>12.2f}"
        )

    print("\nAll results stored in MongoDB. Done.")
    client.close()


if __name__ == "__main__":
    main()
