#!/usr/bin/env python3
"""
Calendar Spread Analysis for NSE Stock Futures (FUTSTK).

For every symbol and trading date this script looks at the running futures
contracts, picks the two nearest expiries -- the "current month" (near) and the
"mid month" (next) -- and computes the calendar spread on the CLOSE price:

        spread = close(mid_month) - close(current_month)

It then aggregates, per symbol, over the past N years (default 7) or from the
first date the symbol appears in F&O:

    * mean_spread       -> average spread
    * std_dev_spread    -> population standard deviation of the spread
    * max_spread        -> largest spread observed
    * min_spread        -> smallest (most negative) spread observed
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

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne

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


def build_summary_pipeline(summary_coll: str, gap_threshold_days: int):
    """
    Aggregation over the daily spread collection to produce a normalised,
    per-symbol summary written to `summary_coll` via $out.

    Spread is always mid_month_close - current_month_close. Per symbol it
    computes mean_spread, std_dev_spread (population standard deviation),
    max_spread, min_spread, plus mean_deviation and GAP DETECTION.

    Gap detection surfaces stocks that were in F&O, dropped out, and later
    rejoined: the daily series has no documents while the stock is out of F&O,
    so consecutive trading dates are far apart. Any gap larger than
    `gap_threshold_days` calendar days (normal weekend/holiday breaks are only
    a few days) is flagged.
    """
    ms_per_day = 86400000
    return [
        # Sort so the pushed arrays below are in chronological order.
        {"$sort": {"symbol": ASCENDING, "trading_date": ASCENDING}},
        {
            "$group": {
                "_id": "$symbol",
                "spreads": {"$push": "$spread"},
                "dates": {"$push": "$trading_date"},
                "mean_spread": {"$avg": "$spread"},
                "std_dev_spread": {"$stdDevPop": "$spread"},
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
                # Gaps (in calendar days) between each pair of consecutive
                # trading dates. date_gaps[k] = dates[k+1] - dates[k].
                "date_gaps": {
                    "$map": {
                        "input": {"$range": [1, {"$size": "$dates"}]},
                        "as": "i",
                        "in": {
                            "$divide": [
                                {
                                    "$subtract": [
                                        {"$arrayElemAt": ["$dates", "$$i"]},
                                        {
                                            "$arrayElemAt": [
                                                "$dates",
                                                {"$subtract": ["$$i", 1]},
                                            ]
                                        },
                                    ]
                                },
                                ms_per_day,
                            ]
                        },
                    }
                },
            }
        },
        {
            "$addFields": {
                "calendar_days_span": {
                    "$round": [
                        {
                            "$divide": [
                                {"$subtract": ["$last_date", "$first_date"]},
                                ms_per_day,
                            ]
                        },
                        0,
                    ]
                },
                "largest_gap_days": {"$ifNull": [{"$round": [{"$max": "$date_gaps"}, 0]}, 0]},
                # Number of gaps longer than the threshold.
                "gap_count": {
                    "$size": {
                        "$filter": {
                            "input": "$date_gaps",
                            "as": "g",
                            "cond": {"$gt": ["$$g", gap_threshold_days]},
                        }
                    }
                },
                # Index (into date_gaps) of the single largest gap, or -1.
                "_max_gap_idx": {
                    "$indexOfArray": ["$date_gaps", {"$max": "$date_gaps"}]
                },
            }
        },
        {
            "$addFields": {
                "has_gap": {"$gt": ["$gap_count", 0]},
                # The last trading date before the largest gap ...
                "largest_gap_start": {
                    "$cond": [
                        {"$gte": ["$_max_gap_idx", 0]},
                        {"$arrayElemAt": ["$dates", "$_max_gap_idx"]},
                        None,
                    ]
                },
                # ... and the first trading date after it (when it resumed).
                "largest_gap_end": {
                    "$cond": [
                        {"$gte": ["$_max_gap_idx", 0]},
                        {
                            "$arrayElemAt": [
                                "$dates",
                                {"$add": ["$_max_gap_idx", 1]},
                            ]
                        },
                        None,
                    ]
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                # --- identity ---
                "symbol": "$_id",
                "instrument": {"$literal": "FUTSTK"},
                "spread_definition": {
                    "$literal": "mid_month_close - current_month_close"
                },
                # --- coverage ---
                "observations": 1,
                "first_date": 1,
                "last_date": 1,
                "calendar_days_span": 1,
                # --- core spread statistics (mid - current month) ---
                "mean_spread": {"$round": ["$mean_spread", 4]},
                "std_dev_spread": {"$round": ["$std_dev_spread", 4]},
                "max_spread": {"$round": ["$max_spread", 4]},
                "min_spread": {"$round": ["$min_spread", 4]},
                # --- extra dispersion measures ---
                "mean_deviation": {"$round": ["$mean_deviation", 4]},
                "max_abs_spread": {"$round": ["$max_abs_spread", 4]},
                # --- gap detection (left & rejoined F&O) ---
                "has_gap": 1,
                "gap_count": 1,
                "largest_gap_days": 1,
                "largest_gap_start": 1,
                "largest_gap_end": 1,
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
    parser.add_argument(
        "--gap-threshold-days",
        type=int,
        default=10,
        help=(
            "A break longer than this many calendar days between consecutive "
            "trading dates is treated as a gap (e.g. left and rejoined F&O). "
            "Normal weekend/holiday breaks are only a few days. Default: 10."
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
    # NOTE: this is an EXISTENCE check across ALL dates, not the 7-year window.
    # Each matched symbol is later analysed from its own earliest available
    # date, so symbols with < 7 years of history are NOT dropped here.
    present = set(db[args.source].distinct("symbol", {"instrument": args.instrument}))
    present_upper = {s.strip().upper(): s for s in present if s}
    matched = {present_upper[s] for s in fno_symbols if s in present_upper}
    missing = sorted(s for s in fno_symbols if s not in present_upper)
    print(
        f"      {len(matched)} of {len(fno_symbols)} F&O symbols found in "
        f"{args.source} and will be analysed."
    )

    # Diagnostic: plainly list the current F&O symbols that are NOT present in
    # the data (no name guessing / no rename assumptions). Persist the full
    # list so it can be reviewed in MongoDB.
    db["spread_unmatched_fno"].drop()
    if missing:
        print(
            f"      {len(missing)} current F&O symbols are NOT available in "
            f"{args.source} (so they can't be analysed):"
        )
        # Print in compact rows for readability.
        for i in range(0, len(missing), 6):
            print("        " + "  ".join(missing[i : i + 6]))
        db["spread_unmatched_fno"].insert_many(
            [{"symbol": s, "reason": "not_in_source"} for s in missing]
        )
        print(f"      (full list saved to {args.db}.spread_unmatched_fno)")
    else:
        print(f"      all {len(fno_symbols)} current F&O symbols are available in {args.source}.")
    print()

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
        build_summary_pipeline(args.summary_collection, args.gap_threshold_days),
        allowDiskUse=True,
    )
    db[args.summary_collection].create_index([("symbol", ASCENDING)], unique=True)
    symbol_count = db[args.summary_collection].count_documents({})
    print(f"      done: {symbol_count:,} symbols summarised.")

    # Enrich each summary with the TRUE earliest date the symbol's futures data
    # is available (ignoring the analysis window), so you can see exactly "from
    # where" each stock's history begins -- and whether the 7-year cap is
    # truncating older data that exists.
    print("      annotating each symbol with its data-availability start ...")
    avail = db[args.source].aggregate(
        [
            {"$match": {"instrument": args.instrument, "symbol": {"$in": sorted(matched)}}},
            {
                "$group": {
                    "_id": "$symbol",
                    "data_available_from": {"$min": "$trading_date"},
                    "data_available_to": {"$max": "$trading_date"},
                }
            },
        ],
        allowDiskUse=True,
    )
    ops = []
    for a in avail:
        available_from = a["data_available_from"]
        ops.append(
            UpdateOne(
                {"symbol": a["_id"]},
                {
                    "$set": {
                        "data_available_from": available_from,
                        "data_available_to": a["data_available_to"],
                        # True if older-than-window data exists that was excluded.
                        "history_truncated_by_window": (
                            not args.all_history and available_from < cutoff
                        ),
                    }
                },
            )
        )
    if ops:
        db[args.summary_collection].bulk_write(ops)
    truncated = db[args.summary_collection].count_documents(
        {"history_truncated_by_window": True}
    )
    if truncated:
        print(
            f"      note: {truncated} symbols have futures data older than the "
            f"{args.years}-year window (analysed from {cutoff.date()}). "
            "Use --all-history to include everything."
        )

    # --- Stage 3: preview --------------------------------------------------
    print("\n[3/4] Sample summary (top 10 by max_abs_spread) | spread = mid - current month:")
    header = (
        f"{'SYMBOL':<12}{'OBS':>6}{'AVAIL_FROM':>12}{'MEAN':>10}"
        f"{'STD_DEV':>10}{'MAX':>10}{'MIN':>10}"
    )
    print(header)
    print("-" * len(header))

    def _d(val):
        return val.date().isoformat() if hasattr(val, "date") else str(val)

    for doc in (
        db[args.summary_collection]
        .find({}, {"_id": 0})
        .sort("max_abs_spread", -1)
        .limit(10)
    ):
        print(
            f"{doc['symbol']:<12}"
            f"{doc['observations']:>6}"
            f"{_d(doc.get('data_available_from')):>12}"
            f"{doc['mean_spread']:>10.2f}"
            f"{doc['std_dev_spread']:>10.2f}"
            f"{doc['max_spread']:>10.2f}"
            f"{doc['min_spread']:>10.2f}"
        )

    # --- Stage 4: gap report ----------------------------------------------
    gapped = list(
        db[args.summary_collection]
        .find({"has_gap": True}, {"_id": 0})
        .sort("largest_gap_days", -1)
    )
    print(
        f"\n[4/4] Symbols with data gaps > {args.gap_threshold_days} days "
        f"(likely left & rejoined F&O): {len(gapped)}"
    )
    if gapped:
        header = (
            f"{'SYMBOL':<14}{'GAPS':>5}{'MAX_GAP_DAYS':>14}"
            f"{'GAP_FROM':>13}{'GAP_TO':>13}{'OBS':>7}"
        )
        print(header)
        print("-" * len(header))

        def _d(val):
            return val.date().isoformat() if hasattr(val, "date") else str(val)

        for doc in gapped:
            print(
                f"{doc['symbol']:<14}"
                f"{doc['gap_count']:>5}"
                f"{int(doc['largest_gap_days']):>14}"
                f"{_d(doc.get('largest_gap_start')):>13}"
                f"{_d(doc.get('largest_gap_end')):>13}"
                f"{doc['observations']:>7}"
            )
        print(
            "\nTip: for these symbols the pre-gap and post-gap data are merged "
            "in the stats. Review them if you need a continuous series."
        )
    else:
        print("      None. Every current F&O symbol has a continuous history.")

    print("\nAll results stored in MongoDB. Done.")
    client.close()


if __name__ == "__main__":
    main()
