# Hourly Futures Data Backfill

Downloads **hourly closing candles** for all F&O stocks — both **current month** and
**mid month (next month) futures** — and stores them in local MongoDB (`past_data`).

## Data source

**TradingView WebSocket** (free, anonymous, no API key needed).

- ~3.5 years of hourly data per symbol (maximum available free for NSE futures)
- 210 current F&O stocks × 2 contracts (current + mid month) = ~420 fetch tasks
- ~6,000 hourly candles per symbol → total ~2.5 million documents

## Features

- Fetches **current-month** (`SYMBOL1!`) and **mid-month** (`SYMBOL2!`) futures
- **Serial + polite**: one symbol at a time with 3s delay between requests
- **Resumable**: progress tracked per symbol; if interrupted, `make run` picks up where it left off
- **Auto-detects** current F&O stock list from NSE bhavcopy — fetches any missing/new stocks too
- Dockerized with MongoDB bundled — no external DB needed

## Quick start (serial commands)

```bash
git clone https://github.com/Prakash2571/PastAnalysis.git
cd PastAnalysis

bash setup.sh          # install Docker, Compose, screen, make (once)
newgrp docker

make build             # build the image
make run               # start MongoDB + launch backfill in screen session

make logs              # follow live log
make monitor           # DB snapshot: candles, symbols done, date range
make watch             # auto-refresh every 30s
```

## When done — export to your local machine

```bash
make export            # -> hourly_dump_YYYYMMDD.archive.gz
```

From your laptop:
```bash
scp -i vivek.pem ubuntu@<ip>:~/PastAnalysis/hourly_dump_*.archive.gz .
mongorestore --archive=hourly_dump_*.archive.gz --gzip
```

Data lands in `mongodb://localhost:27017` → database **`past_data`** → collection **`hourly_futures`**.

## Data model

Each document is one hourly candle for one futures contract:

| Field | Description |
|---|---|
| `symbol` | Stock symbol (e.g. RELIANCE) |
| `contract_type` | `current_month` or `mid_month` |
| `timestamp` | Candle timestamp (UTC) |
| `date` | Trading date |
| `hour` | Hour of the candle |
| `open` | Open price |
| `high` | High price |
| `low` | Low price |
| `close` | Close price |
| `volume` | Volume |

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

- ~420 tasks × (10s fetch + 3s delay) ≈ **~1.5-2 hours**
- Resumable: if it stops, just `make run` again

## Important note

**10 years of hourly futures data is not available from any free source.** TradingView's
free tier provides ~3.5 years maximum. For longer history, you'd need a paid data
provider (e.g., Zerodha Kite API at ~₹2000/yr which gives full 10yr intraday data).
This script fetches the maximum available for free.
