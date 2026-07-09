#!/usr/bin/env python3
"""
Backfill missing F&O stock futures into `stock_futures`.

Some symbols in the current NSE F&O list are not present in the local
`stock_futures` collection (they are listed in `spread_unmatched_fno`, produced
by calendar_spread_analysis.py). This script:

    1. Reads the missing symbols from `spread_unmatched_fno`.
    2. For each one, downloads its historical stock-futures (FUTSTK) data from
       NSE for the past N years (default 7) -- or from whenever it first became
       available in that window.
    3. Inserts the data into `stock_futures` in the SAME schema as the existing
       documents (idempotent upsert on symbol+instrument+expiry+trading_date).
    4. Reports, per symbol, the date from which data is available and how many
       rows were added, and prunes symbols that were successfully backfilled
       from `spread_unmatched_fno`.

Data is fetched via the `nselib` library, which handles NSE's session cookies
and request chunking. Because NSE blocks datacenter IPs, run this on your own
machine / residential connection.

Requires Python 3.10+ (a transitive dependency uses 3.10 syntax).

Field mapping (nselib future_price_volume_data -> stock_futures):
    TIMESTAMP        -> trading_date (date at 00:00)
    INSTRUMENT       -> instrument ("FUTSTK")
    SYMBOL           -> symbol
    EXPIRY_DT        -> expiry (date at 00:00)
    OPENING_PRICE    -> open
    TRADE_HIGH_PRICE -> high
    TRADE_LOW_PRICE  -> low
    CLOSING_PRICE    -> close
    SETTLE_PRICE     -> settle_price
    TOT_TRADED_QTY / MARKET_LOT -> contracts
    TOT_TRADED_VAL   -> value_lakh (rupees / 1e5)
    OPEN_INT         -> open_interest
    CHANGE_IN_OI     -> change_in_oi
    (source_format is set to "nse_api" so backfilled rows are identifiable)
"""

import argparse
import os
import time
from datetime import datetime, timedelta

from pymongo import ASCENDING, MongoClient, UpdateOne

DATE_FMT_OUT = "%d-%m-%Y"  # nselib expects dd-mm-YYYY
# NSE stock futures (FUTSTK) were introduced in Nov 2001; use a safe floor.
INCEPTION_FLOOR = datetime(2001, 11, 1)


def years_ago(n_years: int) -> datetime:
    return datetime.utcnow() - timedelta(days=int(round(365.25 * n_years)))


def to_number(val):
    """Coerce a possibly-comma-formatted / string value to float, else None.

    Treats NaN (pandas' empty-cell value) and blanks as None so downstream
    int()/division never sees a NaN.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            f = float(val)
        except (TypeError, ValueError):
            return None
        return None if f != f else f  # f != f is True only for NaN
    s = str(val).strip().replace(",", "")
    if s in ("", "-", "NA", "na", "nan", "NaN", "None"):
        return None
    try:
        f = float(s)
    except ValueError:
        return None
    return None if f != f else f


def to_date(val):
    """Parse NSE date strings ('08-Jul-2019' etc.) to a midnight datetime."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return datetime(val.year, val.month, val.day)
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y %H:%M:%S"):
        try:
            d = datetime.strptime(s, fmt)
            return datetime(d.year, d.month, d.day)
        except ValueError:
            continue
    # Last resort: pandas parser.
    try:
        import pandas as pd

        d = pd.to_datetime(s, dayfirst=True)
        return datetime(d.year, d.month, d.day)
    except Exception:  # noqa: BLE001
        return None


def row_to_doc(row):
    """Map one nselib record (dict) to the stock_futures schema."""
    trading_date = to_date(row.get("TIMESTAMP"))
    expiry = to_date(row.get("EXPIRY_DT"))
    close = to_number(row.get("CLOSING_PRICE"))
    if trading_date is None or expiry is None or close is None:
        return None  # unusable for spread analysis

    qty = to_number(row.get("TOT_TRADED_QTY"))
    lot = to_number(row.get("MARKET_LOT"))
    contracts = int(round(qty / lot)) if (qty and lot) else None

    val = to_number(row.get("TOT_TRADED_VAL"))
    value_lakh = round(val / 1e5, 2) if val is not None else None

    oi = to_number(row.get("OPEN_INT"))
    chg_oi = to_number(row.get("CHANGE_IN_OI"))

    return {
        "trading_date": trading_date,
        "symbol": str(row.get("SYMBOL", "")).strip().upper(),
        "instrument": (str(row.get("INSTRUMENT", "FUTSTK")).strip().upper() or "FUTSTK"),
        "expiry": expiry,
        "open": to_number(row.get("OPENING_PRICE")),
        "high": to_number(row.get("TRADE_HIGH_PRICE")),
        "low": to_number(row.get("TRADE_LOW_PRICE")),
        "close": close,
        "settle_price": to_number(row.get("SETTLE_PRICE")),
        "contracts": contracts,
        "value_lakh": value_lakh,
        "open_interest": int(oi) if oi is not None else None,
        "change_in_oi": int(chg_oi) if chg_oi is not None else None,
        "source_format": "nse_api",
    }


def fetch_symbol(symbol, instrument, from_date, to_date_str, retries=3, sleep=1.0):
    """Fetch futures data for one symbol via nselib, with simple retries."""
    from nselib import derivatives

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            df = derivatives.future_price_volume_data(
                symbol=symbol,
                instrument=instrument,
                from_date=from_date,
                to_date=to_date_str,
            )
            return df.to_dict("records") if df is not None and not df.empty else []
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < retries:
                time.sleep(sleep * attempt)
    raise RuntimeError(f"fetch failed for {symbol}: {last_err}")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill missing current-F&O stock futures into stock_futures."
    )
    parser.add_argument(
        "--uri",
        default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB connection URI (default: mongodb://localhost:27017)",
    )
    parser.add_argument("--db", default="nse_fno", help="Database name (default: nse_fno)")
    parser.add_argument(
        "--source", default="stock_futures", help="Target collection (default: stock_futures)"
    )
    parser.add_argument(
        "--unmatched-collection",
        default="spread_unmatched_fno",
        help="Collection listing missing symbols (default: spread_unmatched_fno)",
    )
    parser.add_argument(
        "--instrument", default="FUTSTK", help="Instrument type (default: FUTSTK)"
    )
    parser.add_argument(
        "--years", type=int, default=7, help="Look-back window in years (default: 7)"
    )
    parser.add_argument(
        "--all-history",
        action="store_true",
        help="Fetch from F&O inception (~2001) instead of the --years window.",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols to backfill (overrides the unmatched collection).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.8,
        help="Seconds to pause between symbols (be polite to NSE). Default: 0.8",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and report, but do not write to stock_futures.",
    )
    args = parser.parse_args()

    from_dt = INCEPTION_FLOOR if args.all_history else years_ago(args.years)
    from_date = from_dt.strftime(DATE_FMT_OUT)
    to_date_str = datetime.utcnow().strftime(DATE_FMT_OUT)

    client = MongoClient(args.uri)
    db = client[args.db]

    # Resolve which symbols to backfill.
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = sorted(
            {
                str(d["symbol"]).strip().upper()
                for d in db[args.unmatched_collection].find({}, {"symbol": 1})
                if d.get("symbol")
            }
        )

    print(f"Connected to {args.uri} -> db '{args.db}'")
    print(f"Target: {args.source} | instrument: {args.instrument}")
    print(f"Fetch window: {from_date} to {to_date_str}")
    print(f"Symbols to backfill: {len(symbols)}")
    if not symbols:
        print("Nothing to do (no missing symbols). Exiting.")
        return
    print()

    # Index to make idempotent upserts fast on a large collection.
    if not args.dry_run:
        db[args.source].create_index(
            [
                ("symbol", ASCENDING),
                ("instrument", ASCENDING),
                ("expiry", ASCENDING),
                ("trading_date", ASCENDING),
            ],
            name="backfill_upsert_key",
        )

    header = f"{'SYMBOL':<14}{'AVAIL_FROM':>13}{'AVAIL_TO':>13}{'ROWS':>8}{'STATUS':>12}"
    print(header)
    print("-" * len(header))

    total_rows = 0
    backfilled, empty, failed = [], [], []

    for sym in symbols:
        try:
            records = fetch_symbol(
                sym, args.instrument, from_date, to_date_str, sleep=args.sleep
            )
        except Exception as exc:  # noqa: BLE001
            failed.append(sym)
            print(f"{sym:<14}{'-':>13}{'-':>13}{0:>8}{'FAILED':>12}   {exc}")
            time.sleep(args.sleep)
            continue

        docs = [d for d in (row_to_doc(r) for r in records) if d and d["symbol"] == sym]
        if not docs:
            empty.append(sym)
            db[args.unmatched_collection].update_one(
                {"symbol": sym}, {"$set": {"reason": "no_data_on_nse"}}
            )
            print(f"{sym:<14}{'-':>13}{'-':>13}{0:>8}{'NO DATA':>12}")
            time.sleep(args.sleep)
            continue

        dates = [d["trading_date"] for d in docs]
        avail_from, avail_to = min(dates), max(dates)

        if args.dry_run:
            status = "DRY-RUN"
        else:
            ops = [
                UpdateOne(
                    {
                        "symbol": d["symbol"],
                        "instrument": d["instrument"],
                        "expiry": d["expiry"],
                        "trading_date": d["trading_date"],
                    },
                    {"$set": d},
                    upsert=True,
                )
                for d in docs
            ]
            db[args.source].bulk_write(ops, ordered=False)
            # Successfully backfilled -> drop from the unmatched list.
            db[args.unmatched_collection].delete_one({"symbol": sym})
            status = "INSERTED"

        total_rows += len(docs)
        backfilled.append(sym)
        print(
            f"{sym:<14}{avail_from.date().isoformat():>13}"
            f"{avail_to.date().isoformat():>13}{len(docs):>8}{status:>12}"
        )
        time.sleep(args.sleep)

    print("\n=== Summary ===")
    print(f"  backfilled : {len(backfilled)} symbols, {total_rows:,} rows")
    print(f"  no data    : {len(empty)} symbols {empty if empty else ''}")
    print(f"  failed     : {len(failed)} symbols {failed if failed else ''}")
    if not args.dry_run and backfilled:
        print(
            "\nDone. Re-run calendar_spread_analysis.py to include these symbols "
            "in the spread analytics."
        )
    client.close()


if __name__ == "__main__":
    main()
