# trading-dashboard

A real-time intraday trading dashboard built with **Streamlit**, showing per-stock and per-expiry PnL, net exposure, traded value, and lot positions.

Designed for futures traders monitoring multiple symbols and expiries simultaneously.

---

## Screenshots

> Dark-themed dashboard with expandable rows per stock/expiry.

---

## Features

- **Per-stock expand / collapse** — click the `▶` arrow to expand any stock and view its individual expiry rows
- **Expand all / Collapse all** button for quick overview
- **Net PnL** computed from carry PnL + day PnL − expenses
- **Net Exposure** and **Traded Value** per expiry and rolled up to stock level
- **KPI strip** — total Net Exposure, Traded Value, Net PnL at a glance
- **Auto-refresh** every 10 seconds (configurable)
- **Dummy data mode** for testing without live feeds
- **Live data ready** — plug in MongoDB (trades) + Redis (LTP) with minimal changes

---

## Tech Stack

| Layer       | Technology                        |
|-------------|-----------------------------------|
| UI          | Streamlit                         |
| Data store  | MongoDB (trades), Redis (LTP)     |
| Compute     | Pure Python PnL engine            |
| Styling     | Custom HTML/CSS inside Streamlit  |

---

## Project Structure

```
trading-dashboard/
│
├── trading_dashboard.py          # v1 — Expand all only (original)
├── trading_dashboard_up.py       # v2 — Per-stock expand/collapse (updated)
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/trading-dashboard.git
cd trading-dashboard
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run with dummy data

```bash
streamlit run trading_dashboard_up.py
```

Open your browser at `http://localhost:8501`

---

## Live Data Setup

### MongoDB (trades)

Set these environment variables before running:

```bash
export MONGO_URI="mongodb://localhost:27017/"
export MONGO_DB="dropcopy"
export MONGO_COLL="trades"
```

Then in `trading_dashboard_up.py`, replace inside `load_data()`:

```python
# Comment this:
return DUMMY_DATA

# Uncomment this:
return load_data_from_mongo()
```

### Redis (LTP)

```bash
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
export LTP_HASH_KEY="last_price"
```

Redis is expected to hold a hash `last_price` with `{ "SYMBOL": "price" }` entries.

---

## Configuration

All config is at the top of the script via environment variables:

| Variable          | Default                        | Description                        |
|-------------------|--------------------------------|------------------------------------|
| `MONGO_URI`       | `mongodb://localhost:27017/`   | MongoDB connection string          |
| `MONGO_DB`        | `dropcopy`                     | MongoDB database name              |
| `MONGO_COLL`      | `trades`                       | MongoDB collection name            |
| `REDIS_HOST`      | `localhost`                    | Redis host                         |
| `REDIS_PORT`      | `6379`                         | Redis port                         |
| `LTP_HASH_KEY`    | `last_price`                   | Redis hash key for LTP prices      |
| `EXPENSE_PER_CR`  | `10000`                        | Brokerage/expense per crore traded |
| `REFRESH_SECONDS` | `10`                           | Dashboard auto-refresh interval    |

---

## PnL Calculation

```
Carry PnL  =  qty_overnight  × (LTP − prev_close)
Day PnL    =  qty_today_buy  × (LTP − buy_avg)
           +  qty_today_sell × (sell_avg − LTP)
Expenses   =  (traded_value / 1Cr) × EXPENSE_PER_CR
Net PnL    =  Carry PnL + Day PnL − Expenses
```

---

## Changelog

### v2 — `trading_dashboard_up.py`
- Added per-stock `▶` / `▼` expand/collapse toggle
- Fixed ghost row duplication bug on last stock
- Used `on_click` callback pattern to avoid mid-loop rerun issues
- Cleaner HTML table rendering with shared CSS

### v1 — `trading_dashboard.py`
- Initial version
- Single "Expand all / Collapse all" button
- Full position book with KPI strip

---

## Requirements

```
streamlit
pymongo
pandas
numpy
redis
```

---

## License

MIT
