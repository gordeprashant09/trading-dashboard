"""
trading_dashboard.py
====================
Streamlit Trading Dashboard — PnL viewer per stock / expiry

Run:
    pip install streamlit pymongo pandas
    streamlit run trading_dashboard.py

Data flow (current):  DUMMY DATA  →  PnL Engine  →  Dashboard
Data flow (live):     MongoDB / Redis  →  PnL Engine  →  Dashboard

To connect live data:
    1. Set MONGO_URI, MONGO_DB, MONGO_COLL at the top
    2. Replace load_data() with load_data_from_mongo()
    3. Replace get_ltp() with get_ltp_from_redis()
"""

from __future__ import annotations

import os
import time
import math
from datetime import datetime, date
from typing import Optional

import pandas as pd
import numpy as np
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Trading Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ============================================================
# CONFIG  — change these when going live
# ============================================================
MONGO_URI        = os.getenv("MONGO_URI",        "mongodb://localhost:27017/")
MONGO_DB         = os.getenv("MONGO_DB",         "dropcopy")
MONGO_COLL_TRADES = os.getenv("MONGO_COLL",      "trades")

REDIS_HOST       = os.getenv("REDIS_HOST",       "localhost")
REDIS_PORT       = int(os.getenv("REDIS_PORT",   "6379"))
LTP_HASH_KEY     = os.getenv("LTP_HASH_KEY",     "last_price")

EXPENSE_PER_CR   = float(os.getenv("EXPENSE_PER_CR", "0"))
REFRESH_SECONDS  = int(os.getenv("REFRESH_SECONDS",  "10"))

# ============================================================
# DUMMY DATA
# ============================================================
# Shape: list of dicts, one per stock.
# Each stock has a list of expiries.
# When you connect MongoDB, load_data() should return the same shape.
#
# Fields per expiry:
#   qty_overnight   : signed prev-EOD qty  (+buy / -sell)
#   prev_close      : previous day closing / bhav price
#   qty_today_buy   : today's absolute buy qty
#   qty_today_sell  : today's absolute sell qty
#   buy_avg         : today's avg buy price  (0 if no buys)
#   sell_avg        : today's avg sell price (0 if no sells)
#   ltp             : last traded price  (live from Redis / hardcoded here)
#   mtd             : month-to-date realized PnL
#   lot_size        : contract lot size

DUMMY_DATA = [
    {
        "sym": "IDEA", "book": "prop", "lot_size": 7000,
        "expiries": [
            {"label": "IDEA20260529", "qty_overnight": 35000,  "prev_close": 9.70,
             "qty_today_buy": 14000, "qty_today_sell": 7000,
             "buy_avg": 9.85,  "sell_avg": 10.20, "ltp": 10.45, "mtd": 12400},
            {"label": "IDEA20260626", "qty_overnight": -7000,  "prev_close": 9.90,
             "qty_today_buy": 0,     "qty_today_sell": 14000,
             "buy_avg": 0,     "sell_avg": 9.75,  "ltp": 10.45, "mtd": -3200},
        ],
    },
    {
        "sym": "HDFC", "book": "prop", "lot_size": 550,
        "expiries": [
            {"label": "HDFC20260529", "qty_overnight": 2200,   "prev_close": 1810,
             "qty_today_buy": 1650,  "qty_today_sell": 550,
             "buy_avg": 1820, "sell_avg": 1865, "ltp": 1882, "mtd": 48000},
            {"label": "HDFC20260626", "qty_overnight": -550,   "prev_close": 1825,
             "qty_today_buy": 0,     "qty_today_sell": 550,
             "buy_avg": 0,    "sell_avg": 1840, "ltp": 1882, "mtd": -8200},
        ],
    },
    {
        "sym": "RELIANCE", "book": "client", "lot_size": 250,
        "expiries": [
            {"label": "RELIANCE20260529", "qty_overnight": 1000, "prev_close": 2895,
             "qty_today_buy": 750,  "qty_today_sell": 250,
             "buy_avg": 2910, "sell_avg": 2960, "ltp": 2975, "mtd": 62000},
            {"label": "RELIANCE20260626", "qty_overnight": 500,  "prev_close": 2905,
             "qty_today_buy": 500,  "qty_today_sell": 0,
             "buy_avg": 2940, "sell_avg": 0,    "ltp": 2975, "mtd": 0},
        ],
    },
    {
        "sym": "NIFTY", "book": "client", "lot_size": 75,
        "expiries": [
            {"label": "NIFTY20260515", "qty_overnight": -450,  "prev_close": 24150,
             "qty_today_buy": 0,    "qty_today_sell": 150,
             "buy_avg": 0,     "sell_avg": 24350, "ltp": 24280, "mtd": 32000},
            {"label": "NIFTY20260529", "qty_overnight": 225,   "prev_close": 24100,
             "qty_today_buy": 150,  "qty_today_sell": 0,
             "buy_avg": 24150, "sell_avg": 0,     "ltp": 24280, "mtd": 18500},
        ],
    },
]

# ============================================================
# DATA LOADER
# ============================================================
# Currently returns dummy data.
# When ready, swap load_data() body with load_data_from_mongo().

def load_data(book_filter: str = "all") -> list[dict]:
    """
    Returns position data.
    LIVE MODE:  reads from Redis key dashboard:positions:latest (written by dashboard_worker.py)
    DUMMY MODE: falls back to DUMMY_DATA if Redis is unavailable or key is empty.
    """
    data = load_data_from_redis()
    if not data:
        data = DUMMY_DATA

    if book_filter != "all":
        data = [s for s in data if s.get("book") == book_filter]
    return data


def load_data_from_redis() -> list[dict]:
    """
    Read positions published by dashboard_worker.py from Redis.
    Key: dashboard:positions:latest  → JSON { as_of, positions: [...] }
    Returns empty list on any error so dashboard falls back to dummy data.
    """
    try:
        import redis as _redis
        import json as _json
        r = _redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=0,
            decode_responses=True, socket_timeout=1.0
        )
        raw = r.get("dashboard:positions:latest")
        if not raw:
            return []
        payload = _json.loads(raw)
        positions = payload.get("positions", [])
        st.session_state["data_as_of"]  = payload.get("as_of", "")
        st.session_state["data_source"] = payload.get("source", "redis")
        st.session_state["log_date"]    = payload.get("log_date", "")
        return positions
    except Exception:
        return []


def load_data_from_mongo() -> list[dict]:
    """
    ============================================================
    LIVE DATA LOADER — connect to MongoDB drop copy collection.

    Expected MongoDB document shape (one doc per trade):
    {
        "symbol"        : "IDEA20260529",
        "buy_sell"      : "B",           # "B" or "S"
        "quantity"      : 7000,
        "price"         : 9.85,
        "trade_time"    : <timestamp>,
        "trader_id"     : 1234,
        "stored_at"     : <datetime>
    }

    This function aggregates raw trades into the per-expiry
    position + avg price format needed by the PnL engine.
    ============================================================
    """
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]
    coll   = db[MONGO_COLL_TRADES]

    today_str = date.today().strftime("%Y-%m-%d")

    # Aggregate: group by symbol + side, sum qty, compute weighted avg price
    pipeline = [
        {"$match": {
            "stored_at": {"$gte": datetime.combine(date.today(), __import__('datetime').time.min)}
        }},
        {"$group": {
            "_id": {"symbol": "$symbol", "side": "$buy_sell"},
            "total_qty":   {"$sum": "$quantity"},
            "total_value": {"$sum": {"$multiply": ["$quantity", "$price"]}},
        }},
    ]
    rows = list(coll.aggregate(pipeline))

    # Build per-symbol dict
    sym_map: dict[str, dict] = {}
    for row in rows:
        sym    = row["_id"]["symbol"]
        side   = row["_id"]["side"]   # "B" or "S"
        qty    = row["total_qty"]
        avg    = row["total_value"] / qty if qty else 0

        if sym not in sym_map:
            sym_map[sym] = {"qty_today_buy": 0, "buy_avg": 0,
                            "qty_today_sell": 0, "sell_avg": 0}
        if side == "B":
            sym_map[sym]["qty_today_buy"] = qty
            sym_map[sym]["buy_avg"]       = avg
        else:
            sym_map[sym]["qty_today_sell"] = qty
            sym_map[sym]["sell_avg"]       = avg

    # Attach LTP from Redis
    ltp_map = get_ltp_from_redis()

    # TODO: attach prev_close from your EOD file / prev_positions collection
    # TODO: group expiries under stocks (use parse_symbol() from risk_lib_fast)
    # For now returns a flat list — wire grouping as needed
    result = []
    for sym, vals in sym_map.items():
        result.append({
            "sym":      sym,
            "book":     "prop",      # TODO: derive from trader metadata
            "lot_size": 1,           # TODO: derive from NSE master
            "expiries": [{
                "label":           sym,
                "qty_overnight":   0,    # TODO: load from prev EOD file
                "prev_close":      0,    # TODO: load from prev EOD file
                "qty_today_buy":   vals["qty_today_buy"],
                "qty_today_sell":  vals["qty_today_sell"],
                "buy_avg":         vals["buy_avg"],
                "sell_avg":        vals["sell_avg"],
                "ltp":             ltp_map.get(sym, vals["buy_avg"] or vals["sell_avg"]),
                "mtd":             0,    # TODO: load from MTD store
            }]
        })
    return result


def get_ltp_from_redis() -> dict[str, float]:
    """
    Fetch LTP from Redis hash (same key used by risk_worker.py).
    Returns: { "NIFTY": 24280.0, "IDEA": 10.45, ... }
    """
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                        decode_responses=True, socket_timeout=1.0)
        raw = r.hgetall(LTP_HASH_KEY) or {}
        return {k: float(v) for k, v in raw.items() if v}
    except Exception:
        return {}


# ============================================================
# PNL ENGINE
# Mirrors logic from risk_worker.py:
#   carry_pnl  ≈ compute_carry_pnl_from_prev()
#   day_pnl    ≈ compute_day_pnl_from_trades()
#   net_pnl    = carry + day - expenses
# ============================================================

def calc_expiry_pnl(e: dict, lot_size: int) -> dict:
    """Calculate all PnL fields for one expiry row."""
    net_today  = e["qty_today_buy"] - e["qty_today_sell"]
    open_qty   = e["qty_overnight"] + net_today
    lots       = open_qty / lot_size if lot_size > 0 else None

    # Carry PnL: prev EOD position marked to today's LTP
    carry = e["qty_overnight"] * (e["ltp"] - e["prev_close"])

    # Day PnL: today's trades marked to LTP
    day_buy  = e["qty_today_buy"]  * (e["ltp"] - e["buy_avg"])  if e["qty_today_buy"]  > 0 else 0
    day_sell = e["qty_today_sell"] * (e["sell_avg"] - e["ltp"]) if e["qty_today_sell"] > 0 else 0
    day      = day_buy + day_sell

    # Expenses
    traded_val = (e["qty_today_buy"]  * (e["buy_avg"]  or e["ltp"])) + \
                 (e["qty_today_sell"] * (e["sell_avg"] or e["ltp"]))
    expenses   = (traded_val / 1e7) * EXPENSE_PER_CR

    net     = carry + day - expenses
    net_exp = open_qty * e["ltp"]

    return {
        "label":      e["label"],
        "ltp":        e["ltp"],
        "lots":       lots,
        "open_qty":   open_qty,
        "net_exp":    net_exp,
        "traded_val": traded_val,
        "carry":      carry,
        "day":        day,
        "net":        net,
        "mtd":        e.get("mtd", 0),
    }


def build_table(data: list[dict]) -> tuple[pd.DataFrame, dict]:
    """
    Build flat DataFrame for display + summary KPIs dict.
    Returns (df, kpis)
    """
    rows = []
    kpis = {"net_exp": 0.0, "gross_exp": 0.0, "carry": 0.0, "day": 0.0, "net": 0.0}

    for st in data:
        sym      = st["sym"]
        lot_size = st["lot_size"]

        exp_rows = []
        for e in st["expiries"]:
            r = calc_expiry_pnl(e, lot_size)
            exp_rows.append(r)
            for k in ["net_exp", "carry", "day", "net"]:
                kpis[k] += r[k]

        # gross_exp = absolute sum of each expiry net_exp
        kpis["gross_exp"] += sum(abs(x["net_exp"]) for x in exp_rows)

        # Stock aggregate row — lots = total open qty / lot_size (valid: same lot_size per stock)
        total_open_qty = sum(x["open_qty"] for x in exp_rows)
        agg = {
            "sym":        sym,
            "label":      sym,
            "lot_size":   lot_size,
            "is_stock":   True,
            "lots":       total_open_qty / lot_size if lot_size > 0 else None,
            "open_qty":   total_open_qty,
            "net_exp":    sum(x["net_exp"]     for x in exp_rows),
            "traded_val": sum(x["traded_val"]  for x in exp_rows),
            "carry":      sum(x["carry"]       for x in exp_rows),
            "day":        sum(x["day"]         for x in exp_rows),
            "net":        sum(x["net"]         for x in exp_rows),
            "mtd":        sum(x["mtd"]         for x in exp_rows),
            "ltp":        None,
        }
        rows.append(agg)

        for r in exp_rows:
            rows.append({**r, "sym": sym, "is_stock": False})

    df = pd.DataFrame(rows)
    return df, kpis


# ============================================================
# FORMATTERS
# ============================================================

def fmt_inr(n: Optional[float], show_sign: bool = False) -> str:
    """Format number in Indian notation (L / Cr)."""
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    n = float(n)
    a = abs(n)
    sign = "+" if (show_sign and n >= 0) else ""
    if a >= 1e7:
        return f"{sign}{n/1e7:.2f} Cr"
    if a >= 1e5:
        return f"{sign}{n/1e5:.2f} L"
    return f"{sign}{n:,.0f}"


def fmt_lots(v: Optional[float]) -> str:
    if v is None:
        return "—"
    r = round(v, 1)
    return f"+{r}" if r > 0 else str(r)


def color_val(v: Optional[float]) -> str:
    """Return green / red / grey CSS color string."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "grey"
    if abs(v) < 1:
        return "grey"
    return "green" if v > 0 else "red"


# ============================================================
# CUSTOM CSS
# ============================================================
st.html("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap');

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1rem !important; padding-bottom: 0.5rem !important; }

[data-testid="stMetricValue"] {
    font-size: 1.3rem !important; font-weight: 600;
    font-family: 'JetBrains Mono', monospace !important;
}
[data-testid="stMetricLabel"] {
    font-size: 10px !important; text-transform: uppercase;
    letter-spacing: .08em; color: #555c6e !important;
}
[data-testid="stMetric"] {
    background: #13151a;
    border: 1px solid #1e2230;
    border-radius: 6px;
    padding: 10px 14px !important;
}

/* ── Unified position table ── */
.dash-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    table-layout: fixed;
}
.dash-table colgroup col:nth-child(1) { width: 5%; }
.dash-table colgroup col:nth-child(2) { width: 22%; }
.dash-table colgroup col:nth-child(3) { width: 8%; }
.dash-table colgroup col:nth-child(4) { width: 13%; }
.dash-table colgroup col:nth-child(5) { width: 13%; }
.dash-table colgroup col:nth-child(6) { width: 12%; }
.dash-table colgroup col:nth-child(7) { width: 14%; }
.dash-table colgroup col:nth-child(8) { width: 13%; }

.dash-table th {
    text-align: right;
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 10px; font-weight: 600;
    color: #555c6e; text-transform: uppercase; letter-spacing: .08em;
    padding: 8px 12px 8px 6px;
    border-bottom: 1px solid #1e2230;
    border-top: 1px solid #1e2230;
    background: #0f1117;
    white-space: nowrap;
}
.dash-table th.left { text-align: left; padding-left: 10px; }

.dash-table td {
    padding: 7px 12px 7px 6px;
    text-align: right;
    border-bottom: 1px solid #181b22;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.dash-table td.left  { text-align: left; padding-left: 10px; }
.dash-table td.btn-cell { text-align: center; padding: 0; }

/* stock parent row */
.dash-table tr.stock { background: #1a1d26; }
.dash-table tr.stock td { font-weight: 600; font-size: 12.5px; }
.dash-table tr.stock:hover { background: #1e2230; }

/* expiry child row */
.dash-table tr.expiry { background: #13151c; }
.dash-table tr.expiry td { font-size: 11px; color: #7a8294; }
.dash-table tr.expiry td.left { padding-left: 28px; }
.dash-table tr.expiry:hover { background: #171923; }

/* inline toggle button */
.tog-btn {
    background: #1a1d26; border: 1px solid #252936;
    color: #555c6e; border-radius: 3px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px; line-height: 1; padding: 3px 6px;
    cursor: pointer; transition: color .15s, border-color .15s;
}
.tog-btn:hover { color: #c0c6d4; border-color: #363b4a; }

/* colour classes */
.pos { color: #2eca8a; }
.neg { color: #f05252; }
.zer { color: #454c5e; }

/* symbol + lot badge */
.sym-name { color: #d4d8e8; }
.lot-badge {
    font-size: 9px; color: #454c5e;
    background: #1e2230; border-radius: 3px;
    padding: 1px 5px; margin-left: 6px;
    vertical-align: middle; font-weight: 400;
}

/* expiry label + ltp */
.exp-label { color: #6b7385; }
.ltp-lbl {
    font-size: 9px; color: #454c5e;
    margin-left: 6px; font-weight: 400;
}

/* section header */
.section-hdr {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 10px; color: #454c5e;
    text-transform: uppercase; letter-spacing: .1em;
    margin: 0 0 6px 2px;
}
</style>
""")


# ============================================================
# HELPERS FOR TABLE HTML
# ============================================================

def pnl_td(v: Optional[float], show_sign: bool = True) -> str:
    if v is None:
        return '<td class="zer">—</td>'
    cls = "pos" if v > 1 else ("neg" if v < -1 else "zer")
    s   = fmt_inr(v, show_sign=show_sign)
    return f'<td class="{cls}">{s}</td>'


def pill(open_qty: float) -> str:
    if open_qty > 0:
        return '<span class="pill-L">Long</span>'
    if open_qty < 0:
        return '<span class="pill-S">Short</span>'
    return '<span class="pill-F">Flat</span>'


def render_table_html(df: pd.DataFrame, expand_all: bool = True, expanded_syms: set = None) -> str:
    if expanded_syms is None: expanded_syms = set()
    """Build full table HTML from dataframe."""
    html = """
    <table class="dash-table">
    <thead><tr>
      <th class="left" style="width:28%">Symbol / Expiry</th>
      <th style="width:8%">Lots</th>
      <th style="width:13%">Net Exp.</th>
      <th style="width:13%">Traded Val</th>
      <th style="width:12%">Carry PnL</th>
      <th style="width:13%">Day PnL</th>
      <th style="width:13%">Net PnL</th>
    </tr></thead>
    <tbody>
    """

    for _, row in df.iterrows():
        if row["is_stock"]:
            sym      = row["label"]
            lot_size = int(row.get("lot_size", 1))
            lots_v   = row["lots"]
            if lots_v is None:
                lots_td = '<td class="zer">—</td>'
            else:
                r   = round(float(lots_v), 1)
                cls = "pos" if r > 0 else ("neg" if r < 0 else "zer")
                val = f"+{r}" if r > 0 else str(r)
                lots_td = f'<td class="{cls}">{val}</td>'
            html += f"""
            <tr class="stock">
              <td class="left">{sym} <span style="font-size:9px;color:#565c6e;margin-left:4px;font-weight:400">lot {lot_size:,}</span></td>
              {lots_td}
              {pnl_td(row["net_exp"],    show_sign=False)}
              {pnl_td(row["traded_val"], show_sign=False)}
              {pnl_td(row["carry"])}
              {pnl_td(row["day"])}
              {pnl_td(row["net"])}
            </tr>"""
        else:
            sym_of_row = row.get("sym", "")
            if not expand_all and sym_of_row not in expanded_syms:
                continue
            ltp_str = f"{row['ltp']:,.2f}" if row["ltp"] else ""
            lots_v  = row["lots"]
            if lots_v is None:
                lots_html = '<td class="zer">—</td>'
            else:
                cls = "pos" if lots_v > 0 else ("neg" if lots_v < 0 else "zer")
                lots_html = f'<td class="{cls}">{fmt_lots(lots_v)}</td>'

            html += f"""
            <tr class="expiry">
              <td class="left">{row["label"]}
                <span class="ltp-lbl">LTP {ltp_str}</span>
              </td>
              {lots_html}
              {pnl_td(row["net_exp"],    show_sign=False)}
              {pnl_td(row["traded_val"], show_sign=False)}
              {pnl_td(row["carry"])}
              {pnl_td(row["day"])}
              {pnl_td(row["net"])}
            </tr>"""

    html += "</tbody></table>"
    return html


# ============================================================
# MAIN APP
# ============================================================

def build_position_table_html(data: list[dict], expand_all: bool, expanded_syms: set) -> str:
    """
    Render the entire position book as ONE unified <table> so every
    column is pixel-perfect aligned regardless of row type.
    Toggle buttons are rendered as plain HTML <button> elements that
    call a JS helper which sets a hidden Streamlit text_input, then
    triggers a rerun via the native Streamlit component bridge.
    Because we cannot call st.rerun() from JS, we use a lightweight
    query-param trick: clicking a button appends ?tog=SYM to the URL
    which Streamlit reads on the next run.
    """
    COLS = """
    <colgroup>
      <col style="width:5%">
      <col style="width:22%">
      <col style="width:8%">
      <col style="width:13%">
      <col style="width:13%">
      <col style="width:12%">
      <col style="width:14%">
      <col style="width:13%">
    </colgroup>"""

    html = f"""
    <table class="dash-table">
    {COLS}
    <thead><tr>
      <th class="left"></th>
      <th class="left">Symbol / Expiry</th>
      <th>Lots</th>
      <th>Net Exp.</th>
      <th>Traded Val</th>
      <th>Carry PnL</th>
      <th>Day PnL</th>
      <th>Net PnL</th>
    </tr></thead>
    <tbody>
    """

    for item in data:
        sym      = item["sym"]
        lot_size = item["lot_size"]
        is_open  = expand_all or (sym in expanded_syms)

        exp_calcs  = [calc_expiry_pnl(e, lot_size) for e in item["expiries"]]
        total_oq   = sum(x["open_qty"] for x in exp_calcs)
        stock_lots = total_oq / lot_size if lot_size > 0 else None
        s_net_exp  = sum(x["net_exp"]    for x in exp_calcs)
        s_tval     = sum(x["traded_val"] for x in exp_calcs)
        s_carry    = sum(x["carry"]      for x in exp_calcs)
        s_day      = sum(x["day"]        for x in exp_calcs)
        s_net      = sum(x["net"]        for x in exp_calcs)

        # lots cell for stock row
        if stock_lots is None:
            lots_td = '<td class="zer">—</td>'
        else:
            r2  = round(float(stock_lots), 1)
            cls = "pos" if r2 > 0 else ("neg" if r2 < 0 else "zer")
            val = f"+{r2}" if r2 > 0 else str(r2)
            lots_td = f'<td class="{cls}">{val}</td>'

        arrow = "▾" if is_open else "▸"
        # toggle via query param
        toggle_href = f"?tog={sym}"

        html += f"""
        <tr class="stock">
          <td class="btn-cell">
            <a href="{toggle_href}" style="text-decoration:none;">
              <span class="tog-btn">{arrow}</span>
            </a>
          </td>
          <td class="left">
            <span class="sym-name">{sym}</span>
            <span class="lot-badge">lot {lot_size:,}</span>
          </td>
          {lots_td}
          {pnl_td(s_net_exp,  show_sign=False)}
          {pnl_td(s_tval,     show_sign=False)}
          {pnl_td(s_carry)}
          {pnl_td(s_day)}
          {pnl_td(s_net)}
        </tr>"""

        if is_open:
            for ec in exp_calcs:
                ltp_str = f"{ec['ltp']:,.2f}" if ec.get("ltp") else ""
                lv = ec["lots"]
                if lv is None:
                    lh = '<td class="zer">—</td>'
                else:
                    cls = "pos" if lv > 0 else ("neg" if lv < 0 else "zer")
                    lh = f'<td class="{cls}">{fmt_lots(lv)}</td>'

                html += f"""
                <tr class="expiry">
                  <td></td>
                  <td class="left">
                    <span class="exp-label">{ec["label"]}</span>
                    <span class="ltp-lbl">LTP {ltp_str}</span>
                  </td>
                  {lh}
                  {pnl_td(ec["net_exp"],    show_sign=False)}
                  {pnl_td(ec["traded_val"], show_sign=False)}
                  {pnl_td(ec["carry"])}
                  {pnl_td(ec["day"])}
                  {pnl_td(ec["net"])}
                </tr>"""

    html += "</tbody></table>"
    return html


def main():
    # ── Session state ────────────────────────────────────────
    if "expand_all" not in st.session_state:
        st.session_state.expand_all = False
    if "expanded_syms" not in st.session_state:
        st.session_state.expanded_syms = set()

    # Auto-refresh every 10 seconds — preserves expand/collapse state
    st_autorefresh(interval=5000, key="dashboard_refresh")

    # Load data FIRST so session state is set before rendering
    data = load_data()
    df, kpis = build_table(data)

    # Handle toggle via query params (set by the HTML anchor links)
    qp = st.query_params
    tog_sym = qp.get("tog", None)
    if tog_sym:
        if tog_sym == "__all__":
            st.session_state.expand_all = not st.session_state.expand_all
            if not st.session_state.expand_all:
                st.session_state.expanded_syms = set()
        else:
            if tog_sym in st.session_state.expanded_syms:
                st.session_state.expanded_syms.discard(tog_sym)
            else:
                st.session_state.expanded_syms.add(tog_sym)
        st.query_params.clear()
        st.rerun()

    # ── Top bar ──────────────────────────────────────────────
    col_title, col_time, col_btn = st.columns([3, 1, 1])

    with col_title:
        st.html(
            "<div style='font-family:IBM Plex Sans,sans-serif;"
            "font-size:17px;font-weight:600;color:#c8cdd8;"
            "letter-spacing:.02em;padding-top:6px'>"
            "📊 Trading Dashboard</div>"
        )

    with col_time:
        source   = st.session_state.get("data_source", "dummy")
        as_of    = st.session_state.get("data_as_of", "")
        src_html = "<span style='color:#2eca8a'>● LIVE</span>" if source == "log_file" else "<span style='color:#e8a825'>● DUMMY DATA</span>"

        # Use log_date from worker (extracted from log filename e.g. 20260509)
        log_date = st.session_state.get("log_date", "")
        try:
            if log_date:
                trade_date_fmt = datetime.strptime(log_date, "%Y-%m-%d").strftime("%d %b %Y")
            else:
                trade_date_fmt = datetime.now().strftime("%d %b %Y")
        except Exception:
            trade_date_fmt = log_date or datetime.now().strftime("%d %b %Y")

        st.html(
            f"<div style='padding-top:10px;font-size:10px;"
            f"font-family:JetBrains Mono,monospace;color:#454c5e;"
            f"text-align:right'>"
            f"<span style='color:#6b7385;font-size:11px;font-weight:600'>{trade_date_fmt}</span><br>"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}<br>"
            f"{src_html}</div>"
        )

    # Style the expand/collapse button to match dark theme
    st.html("""<style>
    div[data-testid="stButton"] > button {
        background: #1a1d26 !important;
        border: 1px solid #252936 !important;
        color: #7a8294 !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 10px !important;
        padding: 5px 12px !important;
        border-radius: 4px !important;
        height: auto !important;
        min-height: 0 !important;
        letter-spacing: .04em !important;
        margin-top: 8px;
    }
    div[data-testid="stButton"] > button:hover {
        background: #1e2230 !important;
        border-color: #363b4a !important;
        color: #c0c6d4 !important;
    }
    </style>""")

    with col_btn:
        lbl = "▾ Collapse all" if st.session_state.expand_all else "▸ Expand all"
        if st.button(lbl, key="expand_all_btn", use_container_width=True):
            st.session_state.expand_all = not st.session_state.expand_all
            if not st.session_state.expand_all:
                st.session_state.expanded_syms = set()
            st.rerun()

    # ── KPI strip ────────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Net Exposure",   fmt_inr(kpis["net_exp"]))
    k2.metric("Gross Exposure", fmt_inr(kpis["gross_exp"]))
    k3.metric("Carry PnL",      fmt_inr(kpis["carry"], show_sign=True))
    k4.metric("Day PnL",        fmt_inr(kpis["day"],   show_sign=True))
    k5.metric("Net PnL",        fmt_inr(kpis["net"],   show_sign=True))

    st.html("<div style='margin:10px 0 6px'></div>")

    # ── Section label ────────────────────────────────────────
    st.html("<div class='section-hdr'>Position Book — Intraday</div>")

    # ── Single unified position table ───────────────────────
    table_html = build_position_table_html(
        data,
        expand_all    = st.session_state.expand_all,
        expanded_syms = st.session_state.expanded_syms,
    )
    st.html(table_html)




if __name__ == "__main__":
    main()
