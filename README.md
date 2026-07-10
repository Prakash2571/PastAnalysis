# Hourly Futures Data Backfill (Zerodha Kite Connect)

Downloads **hourly (60-minute) closing candles** for all F&O stocks — **current month,
mid month, AND far month futures** — for the past **10 years** and stores them in
local MongoDB (`past_data.hourly_futures`).

## Data source

**Zerodha Kite Connect API** (₹500/month subscription — gives full 10-year intraday history
with open interest for all NSE futures contracts).

## What it fetches

- All **~210 F&O stocks** × **3 contracts** (current/mid/far month) = ~630 tasks
- **10 years** of hourly candle data per contract
- ~6 candles/day × 250 days/yr × 10 yrs = ~15,000 candles per contract
- Total: **~9.5 million documents** with OHLC + volume + open interest

## Features

- All 3 monthly contracts (current, mid, far) for every stock
- Full 10-year history (max available from Zerodha)
- Open Interest included in every candle
- Serial + rate-limited: respects Kite's API limits (~3 req/sec)
- **Resumable**: if you stop midway (`Ctrl-C`, SSH drops, token expires), re-run picks up exactly where it left off
- Auto-detects current F&O stock list from Kite instruments API
- Checks for missing/new stocks and fetches them too
- Dockerized with bundled MongoDB

## Quick start (serial commands)

```bash
git clone https://github.com/Prakash2571/PastAnalysis.git
cd PastAnalysis

bash setup.sh          # install Docker, Compose, screen, make (once)
newgrp docker

# 1. Configure your Kite credentials
cp .env.example .env
nano .env              # paste KITE_API_KEY and KITE_API_SECRET

# 2. Generate today's access token (must do this daily before running)
pip install -r requirements.txt
python generate_token.py

# 3. Build and run
make build
make run

# 4. Monitor
make logs              # follow live log
make monitor           # DB snapshot
make watch             # auto-refresh every 30s
```

## Daily access token

Kite tokens expire every day at ~6 AM IST. Before each run (or re-run after overnight expiry):

```bash
python generate_token.py
```

This opens the Kite login URL, you authenticate, paste back the request_token, and it
saves the new access_token to `.env`. The backfill then continues from where it left off.

## When done — export to your local machine

```bash
make export            # -> hourly_dump_YYYYMMDD.archive.gz
```

From your laptop:
```bash
scp -i vivek.pem ubuntu@54.236.5.36:~/PastAnalysis/hourly_dump_*.archive.gz .
& "C:\Program Files\MongoDB\Tools\100\bin\mongorestore.exe" --archive=hourly_dump_YYYYMMDD.archive.gz --gzip
```

Data lands in `mongodb://localhost:27017` → database **`past_data`** → collection **`hourly_futures`**.

## Data model

Each document is one hourly candle for one futures contract:

| Field | Description |
|---|---|
| `symbol` | Stock symbol (e.g. RELIANCE) |
| `contract_type` | `current_month`, `mid_month`, or `far_month` |
| `expiry` | Contract expiry date |
| `timestamp` | Candle timestamp |
| `date` | Trading date |
| `hour` | Hour of the candle (9, 10, 11, 12, 13, 14, 15) |
| `open` | Open price |
| `high` | High price |
| `low` | Low price |
| `close` | Close price |
| `volume` | Volume |
| `open_interest` | Open interest |

## All `make` commands

| Command | Description |
|---|---|
| `make setup` | Install Docker+Compose+screen+make, add swap |
| `make build` | Build the backfill image |
| `make run` | Start MongoDB + backfill in detached screen |
| `make attach` | Attach to live screen session |
| `make logs` | Follow the backfill log |
| `make monitor` | One-time progress snapshot |
| `make watch` | Auto-refresh progress every 30s |
| `make export` | Dump dataset to portable .archive.gz |
| `make stop` | Stop MongoDB (data kept) |
| `make down` | Remove containers (data kept) |
| `make purge` | DANGER: delete everything |

## Time estimate

- ~630 tasks × ~60 chunks per task × 0.4s delay ≈ **4-6 hours** for full 10-year backfill
- But it's resumable: if token expires after ~6 hours, just regenerate and `make run` again
- Typical daily session: can backfill ~200-300 symbols before token expires

## Ports

- **Inbound**: only SSH (22). Nothing else.
- **Outbound**: HTTPS (443) to `api.kite.trade` + `27017` to local MongoDB (inside Docker).
