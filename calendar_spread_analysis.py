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

Results are written back to the local MongoDB:
    * <db>.spread_daily    -> one document per (symbol, trading_date)
    * <db>.spread_summary  -> one document per symbol with the statistics

Everything runs server-side via the MongoDB aggregation framework, so it stays
fast even on ~1M documents.
"""

import argparse
import os
from datetime import datetime, timedelta

from pymongo import ASCENDING, MongoClient


def years_ago(n_years: int) -> datetime:
    """Return a UTC datetime n_years in the past (approx, leap-safe)."""
    return datetime.utcnow() - timedelta(days=int(round(365.25 * n_years)))


def build_daily_pipeline(cutoff: datetime, instrument: str, daily_coll: str):
    """
    Aggregation that produces one document per (symbol, trading_date) with the
    near/mid month contracts and the close-price calendar spread, written to
    `daily_coll` via $out.
    """
    return [
        # 1. Only stock futures within the requested time window.
        {"$match": {"instrument": instrument, "trading_date": {"$gte": cutoff}}},

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
    args = parser.parse_args()

    cutoff = datetime(1970, 1, 1) if args.all_history else years_ago(args.years)

    client = MongoClient(args.uri)
    db = client[args.db]

    window = "all history" if args.all_history else f"last {args.years} years (since {cutoff.date()})"
    print(f"Connected to {args.uri} -> db '{args.db}'")
    print(f"Source: {args.source} | instrument: {args.instrument} | window: {window}")
    print("Spread definition: close(mid_month) - close(current_month)\n")

    # --- Stage 1: daily spread series -------------------------------------
    print(f"[1/3] Building daily spreads -> {args.db}.{args.daily_collection} ...")
    db[args.source].aggregate(
        build_daily_pipeline(cutoff, args.instrument, args.daily_collection),
        allowDiskUse=True,
    )
    daily_count = db[args.daily_collection].count_documents({})
    print(f"      done: {daily_count:,} daily spread documents.")

    # Helpful indexes on the daily series.
    db[args.daily_collection].create_index([("symbol", ASCENDING), ("trading_date", ASCENDING)])

    # --- Stage 2: per-symbol summary --------------------------------------
    print(f"[2/3] Building per-symbol summary -> {args.db}.{args.summary_collection} ...")
    db[args.daily_collection].aggregate(
        build_summary_pipeline(args.summary_collection),
        allowDiskUse=True,
    )
    db[args.summary_collection].create_index([("symbol", ASCENDING)], unique=True)
    symbol_count = db[args.summary_collection].count_documents({})
    print(f"      done: {symbol_count:,} symbols summarised.")

    # --- Stage 3: preview --------------------------------------------------
    print("\n[3/3] Sample summary (top 10 by max_abs_spread):")
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
