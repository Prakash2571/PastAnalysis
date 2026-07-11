# Futures Data Backfill (Hybrid: 10yr Daily + Hourly) — Zerodha Kite

For every F&O stock, downloads two datasets into your local MongoDB (`past_data`):

| Dataset | Timeframe | History | How |
|---|---|---|---|
| **Daily continuous** | `day` | **10 years** | Kite `continuous=True` (front-month rolled series) |
| **Hourly, 3 contracts** | `hour` | ~3 months each | current / mid / far month, market hours only |

**Why hybrid?** Kite's API does **not** support `continuous=True` for intraday
intervals — that only works with `day`. So a full 10-year *hourly* stitched series
is not possible through Kite. This tool gets the maximum available: 10 years of
**daily** continuous data + **hourly** detail for the 3 currently-active contracts.

Both live in the same collection (`hourly_futures`), separated by a `timeframe` field.

## Setup and run (locally on Windows)

```powershell
git clone https://github.com/Prakash2571/PastAnalysis.git
cd PastAnalysis
pip install -r requirements.txt

# 1. Configure Kite credentials
copy .env.example .env
notepad .env          # paste KITE_API_KEY and KITE_API_SECRET

# 2. Generate today's access token (login in browser, paste request_token)
python generate_token.py

# 3. Run
python hourly_futures_backfill.py
```

Data goes straight into `mongodb://localhost:27017` → `past_data.hourly_futures`.

## Resume

If you stop (Ctrl+C) or the token expires overnight, just regenerate and re-run —
it skips completed tasks and continues:

```powershell
python generate_token.py     # if token expired
python hourly_futures_backfill.py
```

## Data model

| Field | Description |
|---|---|
| `symbol` | Stock symbol (e.g. RELIANCE) |
| `timeframe` | `day` (10yr continuous) or `hour` (recent, 3 contracts) |
| `contract_type` | `continuous` (daily) / `current_month` / `mid_month` / `far_month` |
| `expiry` | Contract expiry (null for daily continuous) |
| `timestamp` | Candle timestamp |
| `date` | Trading date |
| `hour` | Hour of candle (hourly only) |
| `candle_number` | 1-7 within the day (hourly only) |
| `open` / `high` / `low` / `close` | OHLC |
| `volume` | Volume |
| `open_interest` | Open interest |

## Example queries (mongosh)

```javascript
use past_data

// 10-year daily history for RELIANCE
db.hourly_futures.find({ symbol: "RELIANCE", timeframe: "day" }).sort({ date: 1 })

// Recent hourly current-month candles for RELIANCE
db.hourly_futures.find({ symbol: "RELIANCE", timeframe: "hour", contract_type: "current_month" })

// How many candles per timeframe
db.hourly_futures.aggregate([{ $group: { _id: "$timeframe", n: { $sum: 1 } } }])
```

## Notes on full 10-year hourly

Kite cannot provide 10 years of *hourly* futures data (continuous intraday isn't
supported, and it only lists currently-active contract tokens). For true 10-year
hourly, you'd need a specialised data vendor (e.g. TrueData, GlobalDatafeeds).
This tool captures the maximum Kite offers: 10yr daily + recent hourly.
