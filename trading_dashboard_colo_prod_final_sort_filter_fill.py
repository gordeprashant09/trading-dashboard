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
import io
import csv
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
    page_title="Prod Trading Dashboard",
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
REDIS_DB         = int(os.getenv("REDIS_DB",      "1"))
LTP_HASH_KEY     = os.getenv("LTP_HASH_KEY",     "last_price")

EXPENSE_PER_CR   = float(os.getenv("EXPENSE_PER_CR", "1906"))

# ── Signal Redis config ───────────────────────────────────────
# Keys: obstrategy:signal:latest:<SYMBOL>
# Fields: real_signal, final_signal
SIGNAL_REDIS_HOST = os.getenv("SIGNAL_REDIS_HOST", "127.0.0.1")
SIGNAL_REDIS_PORT = int(os.getenv("SIGNAL_REDIS_PORT", "6379"))
SIGNAL_REDIS_DB   = int(os.getenv("SIGNAL_REDIS_DB",   "0"))
SIGNAL_KEY_PREFIX = "obstrategy:signal:latest:"
REFRESH_SECONDS  = int(os.getenv("REFRESH_SECONDS",  "10"))

# ── Snapshot config (mirrors dashboard_worker.py) ────────────
SSH_HOST             = os.getenv("SSH_HOST",             "192.168.71.200")
SSH_PORT             = int(os.getenv("SSH_PORT",         "22"))
SSH_USER             = os.getenv("SSH_USER",             "Data_colo")
SSH_PASS             = os.getenv("SSH_PASS",             "Datacolo@2026")
REMOTE_DASHBOARD_DIR = os.getenv("REMOTE_DASHBOARD_DIR", "/data/Dashboard")
SNAPSHOT_SUBDIR      = os.getenv("SNAPSHOT_SUBDIR",      "snapshots")

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

def merge_signals_with_positions(positions: list[dict]) -> list[dict]:
    """
    Merge signal-only symbols into positions list.
    - Symbols with fills     → keep existing position data
    - Symbols with signals   → add empty row with signal + LTP, PnL/lots = 0
    - Shows ALL signals including signal = 0 (flat)
    """
    try:
        import redis as _redis
        r = _redis.Redis(
            host=SIGNAL_REDIS_HOST, port=SIGNAL_REDIS_PORT,
            db=SIGNAL_REDIS_DB, decode_responses=True, socket_timeout=2.0
        )
        keys = r.keys(f"{SIGNAL_KEY_PREFIX}*")
        signal_syms = {k.replace(SIGNAL_KEY_PREFIX, "").upper() for k in keys}
    except Exception:
        return positions

    filled_syms = {p["sym"].upper() for p in positions}
    ltp_map     = get_ltp_from_redis()
    try:
        import redis as _r2
        r2 = _r2.Redis(host=REDIS_HOST, port=REDIS_PORT, db=2, decode_responses=True, socket_timeout=2.0)
    except Exception:
        r2 = None
    extra       = []

    for sym in sorted(signal_syms):
        if sym in filled_syms:
            continue
        ltp = ltp_map.get(sym, 0.0)
        try:
            sig_data = r.hgetall(f"{SIGNAL_KEY_PREFIX}{sym}")
            sym_token = sig_data.get("token_id", "")
        except Exception:
            sym_token = ""
        lot_size = 1
        if r2:
            try:
                val = r2.hget(f"fo:stock_spot:{sym}", "lot_size")
                if val:
                    lot_size = int(float(val))
            except Exception:
                pass
        extra.append({
            "sym":      sym,
            "lot_size": lot_size,
            "book":     "signal",
            "expiries": [{
                "label":          sym,
                "token":          sym_token,
                "qty_overnight":  0,
                "qty_today_buy":  0,
                "qty_today_sell": 0,
                "buy_avg":        0.0,
                "sell_avg":       0.0,
                "prev_close":     ltp,
                "ltp":            ltp,
                "carry_lots":     0,
            }]
        })

    return positions + extra


def load_data(book_filter: str = "all") -> list[dict]:
    """
    Returns position data.
    LIVE MODE:  reads from Redis key dashboard:positions:latest2 (written by dashboard_worker.py)
    DUMMY MODE: falls back to DUMMY_DATA if Redis is unavailable or key is empty.
    """
    data = load_data_from_redis()
    if not data:
        data = DUMMY_DATA

    # Merge signal-only symbols — show all symbols with signals even without fills
    data = merge_signals_with_positions(data)

    if book_filter != "all":
        data = [s for s in data if s.get("book") == book_filter]
    return data


def load_data_from_redis() -> list[dict]:
    """
    Read positions published by dashboard_worker.py from Redis.
    Key: dashboard:positions:latest2  → JSON { as_of, positions: [...] }
    Returns empty list on any error so dashboard falls back to dummy data.
    """
    try:
        import redis as _redis
        import json as _json
        r = _redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
            decode_responses=True, socket_timeout=1.0
        )
        raw = r.get("dashboard:positions:latest2")
        if not raw:
            return []
        payload = _json.loads(raw)
        positions = payload.get("positions", [])
        st.session_state["data_as_of"]  = payload.get("as_of", "")
        # Only mark as LIVE if positions are actually non-empty
        st.session_state["data_source"] = payload.get("source", "redis") if positions else "dummy"
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
    Fetch LTP from Redis:
      db=0 — index prices (NIFTY, BANKNIFTY, SENSEX etc) from fo_realtime_feeder
      db=2 — stock prices (IDEA, HDFC, RELIANCE etc) from stock_realtime_feeder
    Merges both. db=2 stock prices take priority for stocks.
    """
    result = {}
    try:
        import redis
        # db=0 — index LTP
        r0 = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0,
                         decode_responses=True, socket_timeout=1.0)
        raw0 = r0.hgetall(LTP_HASH_KEY) or {}
        result.update({k: float(v) for k, v in raw0.items() if v})
    except Exception:
        pass
    try:
        import redis
        # db=2 — stock LTP
        r2 = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=2,
                         decode_responses=True, socket_timeout=1.0)
        raw2 = r2.hgetall(LTP_HASH_KEY) or {}
        result.update({k: float(v) for k, v in raw2.items() if v})
    except Exception:
        pass
    return result


@st.cache_data(ttl=60)
def fetch_median_slippage_bps():
    """Weighted avg slippage: S(slip_bps x traded_val) / S(traded_val) from CSV fills."""
    try:
        import paramiko, csv as _csv
        from datetime import date
        trade_date = date.today().strftime("%Y%m%d")
        remote_path = f"/data/Dashboard/snapshots/slippage_log_{trade_date}.csv"
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect("192.168.71.200", port=22, username="Data_colo", password="Datacolo@2026", timeout=5)
        sftp = client.open_sftp()
        try:
            with sftp.open(remote_path, "r") as f:
                reader = _csv.DictReader(f.read().decode("utf-8").splitlines())
                total_sv, total_v = 0.0, 0.0
                for row in reader:
                    try:
                        slip = float(row.get("slip_bps",   ""))
                        tval = float(row.get("traded_val", ""))
                        if tval > 0:
                            total_sv += slip * tval
                            total_v  += tval
                    except: continue
        finally:
            sftp.close(); client.close()
        return total_sv / total_v if total_v > 0 else None
    except Exception:
        return None


@st.cache_data(ttl=60)
def fetch_avg_t1_minutes():
    """
    Avg T1 = mean(fill_time - window_start) in minutes across all fills.
    Uses window_start and time_ist columns from slippage log CSV.
    """
    try:
        import paramiko
        from datetime import date, datetime as dt
        trade_date = date.today().strftime("%Y%m%d")
        remote_path = f"/data/Dashboard/snapshots/slippage_log_{trade_date}.csv"
        import csv as _csv
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect("192.168.71.200", port=22, username="Data_colo", password="Datacolo@2026", timeout=5)
        sftp = client.open_sftp()
        try:
            with sftp.open(remote_path, "r") as f:
                reader = _csv.DictReader(f.read().decode("utf-8").splitlines())
                t1_list = []
                for row in reader:
                    try:
                        fill_t = dt.strptime(row["time_ist"].strip(),    "%H:%M:%S")
                        win_t  = dt.strptime(row["window_start"].strip(), "%H:%M:%S")
                        t1_sec = (fill_t - win_t).total_seconds()
                        if t1_sec >= 0:
                            t1_list.append(t1_sec / 60.0)
                    except: continue
        finally:
            sftp.close(); client.close()
        return sum(t1_list) / len(t1_list) if t1_list else None
    except Exception:
        return None


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

    # Day PnL: split into REALIZED (squared) + UNREALIZED (open leg)
    # Case 1: pure intraday — match buy vs sell
    matched_qty  = min(e["qty_today_buy"], e["qty_today_sell"])
    open_buy_qty = e["qty_today_buy"]  - matched_qty
    open_sel_qty = e["qty_today_sell"] - matched_qty

    realized    = matched_qty  * (e["sell_avg"] - e["buy_avg"]) if matched_qty  > 0 else 0
    unreal_buy  = open_buy_qty * (e["ltp"] - e["buy_avg"])      if open_buy_qty > 0 else 0
    unreal_sell = open_sel_qty * (e["sell_avg"] - e["ltp"])     if open_sel_qty > 0 else 0

    # Case 2: overnight squareoff — excess sells close overnight long (or vice versa)
    # e.g. overnight=+2625, sell_today=2625 → fully squared off overnight position
    # FIX: also remove carry for the closed portion to avoid double counting
    qty_on = e["qty_overnight"]
    if qty_on > 0 and open_sel_qty > 0:
        # Overnight long closed by today's sells
        close_qty    = min(qty_on, open_sel_qty)
        # Realized = sell_avg - prev_close (LOCKED, LTP-independent)
        realized    += close_qty * (e["sell_avg"] - e["prev_close"])
        # Remove carry for closed qty (avoid double count with realized)
        carry       -= close_qty * (e["ltp"] - e["prev_close"])
        unreal_sell  = 0
    elif qty_on < 0 and open_buy_qty > 0:
        # Overnight short closed by today's buys
        close_qty    = min(abs(qty_on), open_buy_qty)
        # Realized = prev_close - buy_avg (LOCKED)
        realized    += close_qty * (e["prev_close"] - e["buy_avg"])
        # Remove carry for closed qty
        carry       += close_qty * (e["ltp"] - e["prev_close"])
        unreal_buy   = 0

    day         = realized + unreal_buy + unreal_sell

    # Expenses — split buy/sell side costs
    buy_val    = e["qty_today_buy"]  * (e["buy_avg"]  or e["ltp"])
    sell_val   = e["qty_today_sell"] * (e["sell_avg"] or e["ltp"])
    traded_val = buy_val + sell_val
    buy_cost   = (buy_val  / 1e7) * 1018
    sell_cost  = (sell_val / 1e7) * 5818
    expenses   = buy_cost + sell_cost

    net     = carry + day - expenses
    net_exp = open_qty * e["ltp"]

    # PnL% — based on direction of open position
    # Only calculated when relevant avg price exists (intraday fills)
    # Shows — for overnight-only positions (no fills today)
    pnl_pct = None
    b_avg = e.get("buy_avg", 0.0)
    s_avg = e.get("sell_avg", 0.0)
    ltp   = e["ltp"]
    if lots is not None:
        if lots > 0 and b_avg and b_avg > 0:
            # Long position — need buy_avg from today's fills
            pnl_pct = (ltp - b_avg) / b_avg * 100
        elif lots < 0 and s_avg and s_avg > 0 and ltp and ltp > 0:
            # Short position — need sell_avg from today's fills
            pnl_pct = (s_avg - ltp) / ltp * 100
        # else: overnight only (no fills) → pnl_pct stays None → shows —

    carry_lots   = e["qty_overnight"] / lot_size if lot_size > 0 else 0.0
    carry_exp_cr = (e["qty_overnight"] * e["ltp"]) / 1e7  # in Crores

    # Slippage — from worker payload or recompute
    slippage = e.get("slippage", None)
    if slippage is None:
        net_today   = e["qty_today_buy"] - e["qty_today_sell"]
        buy_avg_fb  = e.get("buy_avg",  0) or 0
        sell_avg_fb = e.get("sell_avg", 0) or 0
        mid_p       = e["ltp"] if e["ltp"] and e["ltp"] > 0 else None
        if mid_p:
            if net_today > 0 and buy_avg_fb:
                slippage = (buy_avg_fb  - mid_p) / mid_p
            elif net_today < 0 and sell_avg_fb:
                slippage = (sell_avg_fb - mid_p) / mid_p

    return {
        "label":        e["label"],
        "token":        e.get("token", ""),
        "ltp":          e["ltp"],
        "buy_avg":      e.get("buy_avg",  0.0),
        "sell_avg":     e.get("sell_avg", 0.0),
        "carry_lots":   carry_lots,
        "carry_exp_cr": carry_exp_cr,
        "lots":         lots,
        "open_qty":     open_qty,
        "net_exp":      net_exp,
        "traded_val":   traded_val,
        "cost":         expenses,
        "cost_pct":     round((expenses / traded_val * 100), 4) if traded_val else 0.0,
        "carry":        carry,
        "day":          day,
        "realized":     realized,
        "unrealized":   unreal_buy + unreal_sell,
        "net":          net,
        "pnl_pct":      pnl_pct,
        "mtd":          e.get("mtd", 0),
        "slippage":     slippage,
        "inr_slippage": e.get("inr_slippage", None),
    }


def get_signal_map() -> dict[str, dict]:
    """
    Fetch real_signal and final_signal for all symbols from Redis.
    Key pattern: obstrategy:signal:latest:<SYMBOL>
    Returns { "RELIANCE": {"real_signal": "BUY", "final_signal": "BUY"}, ... }
    """
    result = {}
    try:
        import redis as _redis
        r = _redis.Redis(
            host=SIGNAL_REDIS_HOST, port=SIGNAL_REDIS_PORT,
            db=SIGNAL_REDIS_DB, decode_responses=True, socket_timeout=2.0
        )
        keys = r.keys(f"{SIGNAL_KEY_PREFIX}*")
        for key in keys:
            sym = key.replace(SIGNAL_KEY_PREFIX, "").upper()
            vals = r.hmget(key, "real_signal", "final_signal")
            result[sym] = {
                "real_signal":  vals[0] or "—",
                "final_signal": vals[1] or "—",
            }
    except Exception:
        pass  # Redis unavailable — show — for all signals
    return result


def signal_td(val: str) -> str:
    """Render signal cell: BUY=green, SELL=red, else grey."""
    if not val or val == "—":
        return '<td class="zer">—</td>'
    v = str(val).upper()
    if "BUY" in v:
        cls = "pos"
    elif "SELL" in v:
        cls = "neg"
    else:
        cls = "zer"
    return f'<td class="{cls}" style="font-size:11px;font-weight:600">{val}</td>'


def mismatch_td(final_sig, lots) -> str:
    try:
        fs = float(str(final_sig)) if str(final_sig) not in ("None", "—", "", "0") else 0.0
    except (ValueError, TypeError):
        fs = 0.0
    try:
        lt = float(lots) if lots is not None else 0.0
    except (ValueError, TypeError):
        lt = 0.0
    # Red dot if lots != final_sig exactly
    mismatch = fs != lt
    if mismatch:
        return '<td style="text-align:center;color:#ff4444;font-size:14px">&#9679;</td>'
    return '<td></td>'


def build_table(data: list[dict]) -> tuple[pd.DataFrame, dict]:
    """
    Build flat DataFrame for display + summary KPIs dict.
    Returns (df, kpis)
    """
    rows = []
    kpis = {"net_exp": 0.0, "gross_exp": 0.0, "carry": 0.0, "day": 0.0, "net": 0.0, "expenses": 0.0, "slippages": []}

    for st in data:
        sym      = st["sym"]
        lot_size = st["lot_size"]

        exp_rows = []
        for e in st["expiries"]:
            r = calc_expiry_pnl(e, lot_size)
            exp_rows.append(r)
            for k in ["net_exp", "carry", "day", "net"]:
                kpis[k] += r[k]
            kpis["expenses"] += r.get("cost", 0.0)
            if r.get("slippage") is not None:
                kpis["slippages"].append(r["slippage"])

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
    min-width: 1800px;
    border-collapse: collapse;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    table-layout: fixed;
}

.dash-table th {
    text-align: right;
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 9px; font-weight: 600;
    color: #555c6e; text-transform: uppercase; letter-spacing: .06em;
    padding: 6px 6px 6px 2px;
    border-bottom: 1px solid #1e2230;
    border-top: 1px solid #1e2230;
    background: #0f1117;
    white-space: nowrap;
}
.dash-table th.left { text-align: left; padding-left: 8px; }

.dash-table td {
    padding: 6px 6px 6px 2px;
    text-align: right;
    border-bottom: 1px solid #181b22;
    white-space: nowrap;
}
.dash-table td.left  { text-align: left; padding-left: 8px; white-space: nowrap; }
.dash-table td.btn-cell { text-align: center; padding: 0; }

/* stock parent row */
.dash-table tr.stock { background: #1a1d26; }
.dash-table tr.stock td { font-weight: 600; font-size: 11px; }
.dash-table tr.stock:hover { background: #1e2230; }

/* expiry child row */
.dash-table tr.expiry { background: #13151c; }
.dash-table tr.expiry td { font-size: 10.5px; color: #7a8294; }
.dash-table tr.expiry td.left { padding-left: 20px; }
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
.sym-name { color: #d4d8e8; font-size: 12px; font-weight: 700; }
.lot-badge {
    font-size: 9px; color: #555c6e;
    background: #1e2230; border-radius: 3px;
    padding: 1px 4px; margin-left: 5px;
    vertical-align: middle; font-weight: 400;
    white-space: nowrap;
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


def neutral_td(v: Optional[float], show_sign: bool = False, fmt: str = "inr") -> str:
    """Neutral grey cell — no green/red coloring."""
    if v is None:
        return '<td class="zer">—</td>'
    if fmt == "inr":
        s = fmt_inr(v, show_sign=show_sign)
    else:
        s = str(v)
    return f'<td style="color:#7a8294">{s}</td>'


def pct_td(v: Optional[float]) -> str:
    """PnL% cell with green/red coloring."""
    if v is None:
        return '<td class="zer">—</td>'
    cls = "pos" if v > 0 else ("neg" if v < 0 else "zer")
    sign = "+" if v > 0 else ""
    return f'<td class="{cls}">{sign}{v:.2f}%</td>'

def slip_td(v, open_qty=0):
    """Slippage in basis points. Direction-aware colouring."""
    if v is None:
        return '<td class="zer">—</td>'
    bps  = v * 10000
    sign = "+" if bps > 0 else ""
    if abs(bps) < 0.5:
        cls = "zer"
    elif open_qty > 0:
        cls = "pos" if bps < 0 else "neg"
    elif open_qty < 0:
        cls = "pos" if bps > 0 else "neg"
    else:
        cls = "zer"
    return f'<td class="{cls}" title="{v:.8f}">{sign}{bps:.2f}</td>'



def render_table_html(df: pd.DataFrame, expand_all: bool = True, expanded_syms: set = None) -> str:
    if expanded_syms is None: expanded_syms = set()
    """Build full table HTML from dataframe."""
    html = """
    <table class="dash-table">
    <colgroup>
      <col style="width:3%">
      <col style="width:14%">
      <col style="width:7%">
      <col style="width:7%">
      <col style="width:7%">
      <col style="width:6%">
      <col style="width:8%">
      <col style="width:8%">
      <col style="width:8%">
      <col style="width:8%">
      <col style="width:8%">
      <col style="width:5%">
      <col style="width:5%">
    </colgroup>
    <thead><tr>
      <th class="left"></th>
      <th class="left">Symbol / Expiry</th>
      <th>LTP</th>
      <th>Buy Avg</th>
      <th>Sell Avg</th>
      <th>Lots</th>
      <th>Net Exp.</th>
      <th>Traded Val</th>
      <th>Carry PnL</th>
      <th>Realized</th>
      <th>Unrealized</th>
      <th>Day PnL</th>
      <th>Net PnL</th>
      <th>PnL%</th>
      <th>Slippage (bp)</th>
      <th>INR Slip (bp)</th>
      <th style="color:#565c6e">Last Fill</th>
    </tr></thead>
    <tbody>
    """

    for _, row in df.iterrows():
        if row["is_stock"]:
            sym      = row["label"]
            lot_size = int(row.get("lot_size", 1))

            # Lots — neutral grey
            lots_v = row["lots"]
            if lots_v is None:
                lots_td = '<td class="zer">—</td>'
            else:
                r   = round(float(lots_v), 1)
                val = f"+{r}" if r > 0 else str(r)
                lots_td = f'<td style="color:#7a8294">{val}</td>'

            # Stock PnL% = day_pnl / traded_val * 100
            tval = row.get("traded_val", 0)
            day  = row.get("day", 0)
            if tval and tval != 0:
                stock_pct = (day / abs(tval)) * 100
            else:
                stock_pct = None

            html += f"""
            <tr class="stock">
              <td class="btn-cell"></td>
              <td class="left" style="white-space:nowrap">{sym} <span style="font-size:9px;color:#565c6e;margin-left:4px;font-weight:400">lot {lot_size:,}</span></td>
              <td class="zer">—</td>
              <td class="zer">—</td>
              <td class="zer">—</td>
              {lots_td}
              {neutral_td(row["net_exp"])}
              {neutral_td(row["traded_val"])}
              {pnl_td(row["carry"])}
              {pnl_td(row["day"])}
              {pnl_td(row["net"])}
              {pct_td(stock_pct)}
            </tr>"""
        else:
            sym_of_row = row.get("sym", "")
            if not expand_all and sym_of_row not in expanded_syms:
                continue

            ltp      = row.get("ltp", 0)
            b_avg    = row.get("buy_avg", 0)
            s_avg    = row.get("sell_avg", 0)
            pnl_pct  = row.get("pnl_pct", None)

            ltp_str  = f"{ltp:,.2f}"   if ltp   else "—"
            b_str    = f"{b_avg:,.2f}" if b_avg  else "—"
            s_str    = f"{s_avg:,.2f}" if s_avg  else "—"

            # Lots — neutral grey
            lots_v = row["lots"]
            if lots_v is None:
                lots_html = '<td class="zer">—</td>'
            else:
                val = fmt_lots(lots_v)
                lots_html = f'<td style="color:#7a8294">{val}</td>'

            html += f"""
            <tr class="expiry">
              <td></td>
              <td class="left" style="white-space:nowrap">{row["label"]}</td>
              <td style="color:#c0c6d4;text-align:right;padding-right:12px">{ltp_str}</td>
              <td style="color:#7a8294;text-align:right;padding-right:12px">{b_str}</td>
              <td style="color:#7a8294;text-align:right;padding-right:12px">{s_str}</td>
              {lots_html}
              {neutral_td(row["net_exp"])}
              {neutral_td(row["traded_val"])}
              {pnl_td(row["carry"])}
              {pnl_td(row["day"])}
              {pnl_td(row["net"])}
              {pct_td(pnl_pct)}
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
      <col style="width:22px">   <!-- toggle btn -->
      <col style="width:130px">  <!-- Symbol / Expiry -->
      <col style="width:55px">   <!-- Token -->
      <col style="width:45px">   <!-- Real Sig -->
      <col style="width:45px">   <!-- Final Sig -->
      <col style="width:75px">   <!-- LTP -->
      <col style="width:80px">   <!-- Buy Avg -->
      <col style="width:80px">   <!-- Sell Avg -->
      <col style="width:55px">   <!-- Carry Lots -->
      <col style="width:70px">   <!-- Carry Avg -->
      <col style="width:55px">   <!-- Lots -->
      <col style="width:20px">   <!-- mismatch -->
      <col style="width:70px">   <!-- Carry Exp.(Cr) -->
      <col style="width:70px">   <!-- Net Exp.(Cr) -->
      <col style="width:80px">   <!-- Traded Val(Cr) -->
      <col style="width:50px">   <!-- Cost -->
      <col style="width:60px">   <!-- Cost%(Bips) -->
      <col style="width:60px">   <!-- Carry PnL -->
      <col style="width:80px">   <!-- Realized -->
      <col style="width:80px">   <!-- Unrealized -->
      <col style="width:75px">   <!-- Day PnL -->
      <col style="width:75px">   <!-- Net PnL -->
      <col style="width:55px">   <!-- PnL% -->
      <col style="width:75px">   <!-- Slippage -->
      <col style="width:75px">   <!-- INR Slip -->
      <col style="width:65px">   <!-- Last Fill -->
    </colgroup>"""

    html = f"""
    <table class="dash-table">
    {COLS}
    <thead><tr>
      <th class="left"></th>
      <th class="left" style="cursor:pointer"><a href="__HREF_sym__" style="text-decoration:none;color:inherit">Symbol / Expiry <span style="color:__SC_sym__;font-size:11px;font-weight:bold">__SA_sym__</span></a></th>
      <th>Token</th>
      <th>Real Sig</th>
      <th>Final Sig</th>
      <th style="cursor:pointer"><a href="__HREF_ltp__" style="text-decoration:none;color:inherit">LTP <span style="color:__SC_ltp__;font-size:11px;font-weight:bold">__SA_ltp__</span></a></th>
      <th>Buy Avg</th>
      <th>Sell Avg</th>
      <th>Carry Lots</th>
      <th style="color:#565c6e">Carry Avg</th>
      <th style="cursor:pointer"><a href="__HREF_lots__" style="text-decoration:none;color:inherit">Lots <span style="color:__SC_lots__;font-size:11px;font-weight:bold">__SA_lots__</span></a></th>
      <th title="Signal/Lots mismatch">⚡</th>
      <th>Carry Exp.(Cr)</th>
      <th style="cursor:pointer"><a href="__HREF_netexp__" style="text-decoration:none;color:inherit">Net Exp.(Cr) <span style="color:__SC_netexp__;font-size:11px;font-weight:bold">__SA_netexp__</span></a></th>
      <th style="cursor:pointer"><a href="__HREF_tv__" style="text-decoration:none;color:inherit">Traded Val(Cr) <span style="color:__SC_tv__;font-size:11px;font-weight:bold">__SA_tv__</span></a></th>
      <th>Cost</th>
      <th>Cost%(Bips)</th>
      <th style="cursor:pointer"><a href="__HREF_carry__" style="text-decoration:none;color:inherit">Carry PnL <span style="color:__SC_carry__;font-size:11px;font-weight:bold">__SA_carry__</span></a></th>
      <th style="color:#2eca8a;cursor:pointer"><a href="__HREF_real__" style="text-decoration:none;color:inherit">Realized <span style="color:__SC_real__;font-size:11px;font-weight:bold">__SA_real__</span></a></th>
      <th style="color:#e8a825;cursor:pointer"><a href="__HREF_unreal__" style="text-decoration:none;color:inherit">Unrealized <span style="color:__SC_unreal__;font-size:11px;font-weight:bold">__SA_unreal__</span></a></th>
      <th style="cursor:pointer"><a href="__HREF_day__" style="text-decoration:none;color:inherit">Day PnL <span style="color:__SC_day__;font-size:11px;font-weight:bold">__SA_day__</span></a></th>
      <th style="cursor:pointer"><a href="__HREF_net__" style="text-decoration:none;color:inherit">Net PnL <span style="color:__SC_net__;font-size:11px;font-weight:bold">__SA_net__</span></a></th>
      <th>PnL%</th>
      <th>Slippage (bp)</th>
      <th>INR Slip (bp)</th>
      <th style="color:#565c6e">Last Fill</th>
    </tr></thead>
    <tbody>
    """

    # Fetch signals once for all symbols
    signal_map = get_signal_map()

    for item in data:
        sym      = item["sym"]
        lot_size = item["lot_size"]
        is_open  = expand_all or (sym in expanded_syms)

        # Signal for this symbol
        sig       = signal_map.get(sym.upper(), {})
        real_sig  = sig.get("real_signal",  "—")
        final_sig = sig.get("final_signal", "—")

        exp_calcs  = [calc_expiry_pnl(e, lot_size) for e in item["expiries"]]
        total_oq   = sum(x["open_qty"] for x in exp_calcs)
        # Weighted avg slippage for stock row (weight = traded_val)
        _slip_pairs = [(x["slippage"], x["traded_val"])
                       for x in exp_calcs
                       if x.get("slippage") is not None and x.get("traded_val", 0) > 0]
        if _slip_pairs:
            _total_w   = sum(w for _, w in _slip_pairs)
            stock_slip = sum(s * w for s, w in _slip_pairs) / _total_w if _total_w else None
        else:
            stock_slip = None

        # INR-weighted slippage for stock row
        _inr_pairs = [(x["inr_slippage"], x["traded_val"])
                      for x in exp_calcs
                      if x.get("inr_slippage") is not None and x.get("traded_val", 0) > 0]
        if _inr_pairs:
            _inr_w        = sum(w for _, w in _inr_pairs)
            stock_inr_slip = sum(s * w for s, w in _inr_pairs) / _inr_w if _inr_w else None
        else:
            stock_inr_slip = None
        stock_open_qty = total_oq
        stock_lots = total_oq / lot_size if lot_size > 0 else None
        s_net_exp    = sum(x["net_exp"]    for x in exp_calcs)
        s_tval       = sum(x["traded_val"] for x in exp_calcs)
        s_carry      = sum(x["carry"]      for x in exp_calcs)
        s_day        = sum(x["day"]        for x in exp_calcs)
        s_net        = sum(x["net"]        for x in exp_calcs)
        s_realized   = sum(x.get("realized",   0) for x in exp_calcs)
        s_unrealized = sum(x.get("unrealized", 0) for x in exp_calcs)

        # Stock lots — neutral grey
        if stock_lots is None:
            lots_td = '<td class="zer">—</td>'
        else:
            r2  = round(float(stock_lots), 1)
            val = f"+{r2}" if r2 > 0 else str(r2)
            lots_td = f'<td style="color:#7a8294">{val}</td>'

        # Stock PnL% = day_pnl / traded_val * 100
        if s_tval and s_tval != 0:
            stock_pct = (s_day / abs(s_tval)) * 100
            pct_cls   = "pos" if stock_pct > 0 else ("neg" if stock_pct < 0 else "zer")
            pct_sign  = "+" if stock_pct > 0 else ""
            stock_pct_td = f'<td class="{pct_cls}">{pct_sign}{stock_pct:.2f}%</td>'
        else:
            stock_pct_td = '<td class="zer">—</td>'

        # Latest expiry token — sort expiries by label to get latest
        latest_token = ""
        latest_ltp   = ""
        latest_b_avg = ""
        latest_s_avg = ""
        if item["expiries"]:
            latest_exp   = sorted(item["expiries"], key=lambda x: x["label"])[-1]
            latest_token = latest_exp.get("token", "")
            # Get latest expiry calc
            latest_ec    = next((ec for ec in exp_calcs if ec["label"] == latest_exp["label"]), None)
            if latest_ec:
                latest_ltp   = f"{latest_ec['ltp']:,.2f}"   if latest_ec.get("ltp")      else "—"
                latest_b_avg = f"{latest_ec['buy_avg']:,.2f}" if latest_ec.get("buy_avg") else "—"
                latest_s_avg = f"{latest_ec['sell_avg']:,.2f}" if latest_ec.get("sell_avg") else "—"

        # Carry Lots aggregate
        s_carry_lots   = sum(x["carry_lots"]   for x in exp_calcs)
        s_carry_exp_cr = sum(x["carry_exp_cr"] for x in exp_calcs)

        # Carry Lots td
        cl = round(float(s_carry_lots), 1)
        carry_lots_td = f'<td style="color:#7a8294">{("+" if cl > 0 else "") + str(cl)}</td>' if cl != 0 else '<td class="zer">0.0</td>'
        # Carry Avg = prev_close from latest expiry
        _carry_prev = 0.0
        if item["expiries"]:
            _le = sorted(item["expiries"], key=lambda x: x["label"])[-1]
            _carry_prev = _le.get("prev_close", 0.0) or 0.0
        carry_avg_td = f'<td style="color:#565c6e;font-size:10px">{f"{_carry_prev:,.2f}" if _carry_prev else "—"}</td>'

        # Net Exp in Cr
        net_exp_cr = s_net_exp / 1e7
        net_exp_cr_str = f"{net_exp_cr:+.2f}" if net_exp_cr != 0 else "0"

        # Cost Bips = cost_pct × 100
        s_cost     = sum(x["cost"]     for x in exp_calcs)
        s_cost_pct = sum(x["cost_pct"] for x in exp_calcs if x["traded_val"])
        cost_bips  = round(s_cost_pct * 100, 2) if s_tval else None

        # Last fill time across all expiries for this stock
        _stock_last_fill = ""
        for _e in item["expiries"]:
            _t = _e.get("last_fill_time", "")
            if _t and _t > _stock_last_fill:
                _stock_last_fill = _t

        arrow = "▾" if is_open else "▸"
        toggle_href = f"?tog={sym}"

        html += f"""
        <tr class="stock">
          <td class="btn-cell">
            <a href="{toggle_href}" style="text-decoration:none;">
              <span class="tog-btn">{arrow}</span>
            </a>
          </td>
          <td class="left" style="white-space:nowrap">
            <span class="sym-name">{sym}</span>
            <span class="lot-badge">lot {lot_size:,}</span>
          </td>
          <td style="color:#565c6e;text-align:right;padding-right:8px;font-size:11px">{latest_token}</td>
          {signal_td(real_sig)}
          {signal_td(final_sig)}
          <td style="color:#c0c6d4;text-align:right;padding-right:12px">{latest_ltp}</td>
          <td style="color:#7a8294;text-align:right;padding-right:12px">{latest_b_avg}</td>
          <td style="color:#7a8294;text-align:right;padding-right:12px">{latest_s_avg}</td>
          {carry_lots_td}
          {carry_avg_td}
          {lots_td}
          {mismatch_td(final_sig, stock_lots)}
          <td style="color:#7a8294;text-align:right;padding-right:12px">{s_carry_exp_cr:+.2f}</td>
          <td style="color:#7a8294;text-align:right;padding-right:12px">{net_exp_cr_str}</td>
          <td style="color:#7a8294;text-align:right;padding-right:12px">{s_tval/1e7:+.2f}</td>
          <td style="color:#7a8294;text-align:right;padding-right:12px">{fmt_inr(s_cost)}</td>
          <td style="color:#7a8294;text-align:right;padding-right:12px;font-size:10px">{f"{cost_bips:.2f}" if cost_bips is not None else "—"}</td>
          {pnl_td(s_carry)}
          {pnl_td(s_realized)}
          {pnl_td(s_unrealized)}
          {pnl_td(s_day)}
          {pnl_td(s_net)}
          {stock_pct_td}
          {slip_td(stock_slip, stock_open_qty)}
          {slip_td(stock_inr_slip, stock_open_qty)}
          <td style="color:#565c6e;font-size:10px;text-align:right;padding-right:8px">{_stock_last_fill or "—"}</td>
        </tr>"""

        if is_open:
            for ec in exp_calcs:
                ltp   = ec.get("ltp",      0)
                b_avg = ec.get("buy_avg",  0)
                s_avg = ec.get("sell_avg", 0)
                pnl_pct = ec.get("pnl_pct", None)

                ltp_str = f"{ltp:,.2f}"   if ltp   else "—"
                b_str   = f"{b_avg:,.2f}" if b_avg  else "—"
                s_str   = f"{s_avg:,.2f}" if s_avg  else "—"

                # Lots — neutral grey
                lv = ec["lots"]
                if lv is None:
                    lh = '<td class="zer">—</td>'
                else:
                    lh = f'<td style="color:#7a8294">{fmt_lots(lv)}</td>'

                # PnL%
                if pnl_pct is not None:
                    pct_cls  = "pos" if pnl_pct > 0 else ("neg" if pnl_pct < 0 else "zer")
                    pct_sign = "+" if pnl_pct > 0 else ""
                    pnl_pct_td = f'<td class="{pct_cls}">{pct_sign}{pnl_pct:.2f}%</td>'
                else:
                    pnl_pct_td = '<td class="zer">—</td>'

                # Carry Lots
                ecl = round(float(ec["carry_lots"]), 1)
                carry_lots_td_exp = f'<td style="color:#7a8294">{("+" if ecl > 0 else "") + str(ecl)}</td>' if ecl != 0 else '<td class="zer">0.0</td>'
                _ec_prev = ec.get("prev_close", 0.0) or 0.0
                carry_avg_td_exp = f'<td style="color:#565c6e;font-size:10px">{f"{_ec_prev:,.2f}" if _ec_prev else "—"}</td>'

                # Net Exp in Cr
                ec_net_exp_cr = ec["net_exp"] / 1e7
                ec_net_exp_cr_str = f"{ec_net_exp_cr:+.2f}" if ec_net_exp_cr != 0 else "0"

                # Carry Exp in Cr
                ec_carry_exp_cr = ec["carry_exp_cr"]

                # Cost Bips
                ec_cost_bips = round(ec["cost_pct"] * 100, 2) if ec["traded_val"] else None

                html += f"""
                <tr class="expiry">
                  <td></td>
                  <td class="left" style="white-space:nowrap">
                    <span class="exp-label">{ec["label"]}</span>
                  </td>
                  <td class="zer">—</td>
                  {signal_td(real_sig)}
                  {signal_td(final_sig)}
                  <td style="color:#c0c6d4;text-align:right;padding-right:12px">{ltp_str}</td>
                  <td style="color:#7a8294;text-align:right;padding-right:12px">{b_str}</td>
                  <td style="color:#7a8294;text-align:right;padding-right:12px">{s_str}</td>
                  {carry_lots_td_exp}
                  {carry_avg_td_exp}
                  {lh}
                  {mismatch_td(final_sig, ec.get("lots"))}
                  <td style="color:#7a8294;text-align:right;padding-right:12px">{ec_carry_exp_cr:+.2f}</td>
                  <td style="color:#7a8294;text-align:right;padding-right:12px">{ec_net_exp_cr_str}</td>
                  <td style="color:#7a8294;text-align:right;padding-right:12px">{ec["traded_val"]/1e7:+.2f}</td>
                  <td style="color:#7a8294;text-align:right;padding-right:12px">{fmt_inr(ec["cost"])}</td>
                  <td style="color:#7a8294;text-align:right;padding-right:12px;font-size:10px">{f"{ec_cost_bips:.2f}" if ec_cost_bips is not None else "—"}</td>
                  {pnl_td(ec["carry"])}
                  {pnl_td(ec.get("realized", 0))}
                  {pnl_td(ec.get("unrealized", 0))}
                  {pnl_td(ec["day"])}
                  {pnl_td(ec["net"])}
                  {pnl_pct_td}
                  {slip_td(ec.get("slippage"), ec.get("open_qty", 0))}
                  {slip_td(ec.get("inr_slippage"), ec.get("open_qty", 0))}
                  <td style="color:#565c6e;font-size:10px;text-align:right;padding-right:8px">{ec.get("last_fill_time") or "—"}</td>
                </tr>"""

    html += "</tbody></table>"
    return html


# ============================================================
# SNAPSHOT — save current dashboard data to dated CSV on colo
# ============================================================

def _calc_pnl_snap(e: dict, lot_size: int) -> dict:
    """PnL engine — one row per expiry, mirrors worker logic."""
    qty_buy  = e["qty_today_buy"]
    qty_sell = e["qty_today_sell"]
    qty_on   = e["qty_overnight"]
    ltp      = e["ltp"]
    b_avg    = e["buy_avg"]
    s_avg    = e["sell_avg"]

    open_qty  = qty_on + (qty_buy - qty_sell)
    lots      = round(open_qty / lot_size, 2) if lot_size > 0 else None
    carry     = qty_on * (ltp - e["prev_close"])
    day       = (qty_buy  * (ltp - b_avg)  if qty_buy  > 0 else 0.0) + \
                (qty_sell * (s_avg - ltp)  if qty_sell > 0 else 0.0)
    tval      = (qty_buy  * (b_avg  or ltp)) + (qty_sell * (s_avg or ltp))
    expenses  = (tval / 1e7) * EXPENSE_PER_CR
    net       = carry + day - expenses

    return {
        "open_qty":   open_qty,
        "lots":       lots,
        "net_exp":    round(open_qty * ltp, 2),
        "traded_val": round(tval,           2),
        "carry_pnl":  round(carry,          2),
        "day_pnl":    round(day,            2),
        "expenses":   round(expenses,       2),
        "net_pnl":    round(net,            2),
    }


def build_snapshot_csv(data: list[dict], as_of: str, log_date: str) -> bytes:
    """Return UTF-8 CSV bytes for the current dashboard data."""
    buf = io.StringIO()
    fieldnames = [
        "snapshot_time", "trade_date", "sym", "lot_size",
        "expiry_label", "ltp", "qty_overnight", "prev_close",
        "qty_today_buy", "buy_avg", "qty_today_sell", "sell_avg",
        "open_qty", "lots", "net_exp", "traded_val",
        "carry_pnl", "day_pnl", "expenses", "net_pnl", "stock_net_pnl",
    ]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()

    for stock in data:
        sym      = stock["sym"]
        lot_size = stock["lot_size"]
        pnls     = [_calc_pnl_snap(e, lot_size) for e in stock["expiries"]]
        stock_net = round(sum(p["net_pnl"] for p in pnls), 2)

        for e, p in zip(stock["expiries"], pnls):
            writer.writerow({
                "snapshot_time":  as_of,
                "trade_date":     log_date,
                "sym":            sym,
                "lot_size":       lot_size,
                "expiry_label":   e["label"],
                "ltp":            e["ltp"],
                "qty_overnight":  e["qty_overnight"],
                "prev_close":     e["prev_close"],
                "qty_today_buy":  e["qty_today_buy"],
                "buy_avg":        round(e["buy_avg"],  4),
                "qty_today_sell": e["qty_today_sell"],
                "sell_avg":       round(e["sell_avg"], 4),
                "open_qty":       p["open_qty"],
                "lots":           p["lots"],
                "net_exp":        p["net_exp"],
                "traded_val":     p["traded_val"],
                "carry_pnl":      p["carry_pnl"],
                "day_pnl":        p["day_pnl"],
                "expenses":       p["expenses"],
                "net_pnl":        p["net_pnl"],
                "stock_net_pnl":  stock_net,
            })

    return buf.getvalue().encode("utf-8")


def save_snapshot_now(data: list[dict], log_date: str) -> tuple[bool, str]:
    """
    Build CSV and upload to colo server via SFTP.
    Returns (success: bool, message: str).
    Filename: dashboard_snapshot_YYYYMMDD.csv
    """
    try:
        import paramiko as _pm
    except ImportError:
        return False, "paramiko not installed — run: pip install paramiko"

    as_of = datetime.now().isoformat(timespec="seconds")

    # Normalise date tag → YYYYMMDD
    try:
        date_tag = datetime.strptime(log_date, "%Y-%m-%d").strftime("%Y%m%d") \
                   if "-" in log_date else log_date[:8]
    except Exception:
        date_tag = date.today().strftime("%Y%m%d")

    remote_dir  = f"{REMOTE_DASHBOARD_DIR}/{SNAPSHOT_SUBDIR}"
    remote_path = f"{remote_dir}/dashboard_snapshot_{date_tag}.csv"

    csv_bytes = build_snapshot_csv(data, as_of, log_date)
    if not csv_bytes:
        return False, "No data rows — nothing to save."

    try:
        client = _pm.SSHClient()
        client.set_missing_host_key_policy(_pm.AutoAddPolicy())
        client.connect(SSH_HOST, port=SSH_PORT,
                       username=SSH_USER, password=SSH_PASS, timeout=15)

        # Ensure remote directory exists
        client.exec_command(f"mkdir -p {remote_dir}")
        import time as _t; _t.sleep(0.4)

        sftp = client.open_sftp()
        with sftp.open(remote_path, "wb") as f:
            f.write(csv_bytes)
        sftp.close()
        client.close()

        row_count = csv_bytes.decode().count("\n") - 1   # subtract header
        return True, f"Saved → {SSH_HOST}:{remote_path}  ({row_count} rows)"

    except Exception as exc:
        return False, f"SFTP failed: {exc}"


def main():
    # ── Session state ────────────────────────────────────────
    if "expand_all" not in st.session_state:
        st.session_state.expand_all = False
    if "expanded_syms" not in st.session_state:
        st.session_state.expanded_syms = set()
    if "snap_msg" not in st.session_state:
        st.session_state.snap_msg = ""   # last snapshot status message
    if "snap_ok" not in st.session_state:
        st.session_state.snap_ok  = None  # True / False / None
    # Day High — resets each new trading day
    from datetime import datetime as _dt
    today_str = _dt.now().strftime("%Y%m%d")
    if "day_high_date" not in st.session_state or st.session_state.day_high_date != today_str:
        st.session_state.day_high_pnl  = 0.0
        st.session_state.day_high_date = today_str

    # Auto-refresh every 10 seconds — preserves expand/collapse state
    st_autorefresh(interval=5000, key="dashboard_refresh")

    # Load data FIRST so session state is set before rendering
    data = load_data()
    df, kpis = build_table(data)

    # Handle toggle via query params (set by the HTML anchor links)
    qp = st.query_params

    # Handle sort via query params (?sort=col&asc=0 or asc=1)
    # State lives in URL — survives autorefresh
    sort_col = qp.get("sort", None)
    if sort_col:
        # asc=1 means ascending was requested by the link
        asc_param = qp.get("asc", "0")
        st.session_state["sort_col"] = sort_col
        st.session_state["sort_asc"] = (asc_param == "1")
        st.query_params.clear()
        st.rerun()

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
    col_title, col_time, col_btn, col_snap = st.columns([3, 1, 1, 1])

    with col_title:
        st.html(
            "<div style='font-family:IBM Plex Sans,sans-serif;"
            "font-size:17px;font-weight:600;color:#c8cdd8;"
            "letter-spacing:.02em;padding-top:6px'>"
            "📊 Prod Trading Dashboard</div>"
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

    with col_snap:
        if st.button("💾 Snapshot", key="snap_btn", use_container_width=True):
            log_date = st.session_state.get("log_date", "") or \
                       date.today().strftime("%Y-%m-%d")
            with st.spinner("Saving…"):
                ok, msg = save_snapshot_now(data, log_date)
            st.session_state.snap_ok  = ok
            st.session_state.snap_msg = msg

    # ── Snapshot status message (shown below top bar) ────────
    if st.session_state.snap_msg:
        if st.session_state.snap_ok:
            st.success(f"✅ {st.session_state.snap_msg}", icon=None)
        else:
            st.error(f"❌ {st.session_state.snap_msg}", icon=None)

    # ── KPI strip ────────────────────────────────────────────
    k1, k2, k3, k4, k4b, k5, k6, k7, k8 = st.columns(9)
    k1.metric("Net Exposure",   fmt_inr(kpis["net_exp"]))
    k2.metric("Gross Exposure", fmt_inr(kpis["gross_exp"]))
    k3.metric("Carry PnL",      fmt_inr(kpis["carry"], show_sign=True))
    k4.metric("Day PnL",        fmt_inr(kpis["day"],   show_sign=True))
    cur_net = kpis["net"]
    # Persist Day High in Redis — only update when LIVE data (not DUMMY)
    from datetime import date as _date
    _today = _date.today().strftime("%Y%m%d")
    _dh_key = f"dashboard:day_high:{_today}"
    _is_live = st.session_state.get("data_source", "dummy") == "log_file"
    try:
        import redis as _redis_dh
        _r = _redis_dh.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True, socket_timeout=2.0)
        _stored = _r.get(_dh_key)
        _dh = float(_stored) if _stored else 0.0
        if _is_live and cur_net > _dh:
            _dh = cur_net
            _r.set(_dh_key, str(_dh), ex=86400)
    except Exception as _e:
        _dh = getattr(st.session_state, "day_high_pnl", 0.0)
        if _is_live and cur_net > _dh:
            _dh = cur_net
    st.session_state.day_high_pnl = _dh
    k4b.metric("Day High", fmt_inr(_dh, show_sign=True))
    k5.metric("Expenses",       fmt_inr(-kpis["expenses"], show_sign=True))
    k6.metric("Net PnL",        fmt_inr(kpis["net"],   show_sign=True))
    slips = kpis.get("slippages", [])
    wtd_slip = fetch_median_slippage_bps()
    if wtd_slip is None and slips:
        import statistics as _stats
        wtd_slip = _stats.median(slips) * 10000
    if wtd_slip is not None:
        sign = "+" if wtd_slip > 0 else ""
        k7.metric("Wtd Slip (bp)", f"{sign}{wtd_slip:.2f} bp")
    else:
        k7.metric("Wtd Slip (bp)", "—")
    avg_t1 = fetch_avg_t1_minutes()
    if avg_t1 is not None:
        k8.metric("Avg T1 (sec)", f"{avg_t1 * 60:.0f} sec")
    else:
        k8.metric("Avg T1 (sec)", "—")

    st.html("<div style='margin:10px 0 6px'></div>")

    # ── Section label ────────────────────────────────────────
    st.html("<div class='section-hdr'>Position Book — Intraday</div>")

    # ── Single unified position table ───────────────────────
    # ── Filter + Reset sort button ───────────────────────────────────────
    _rc1, _rc2, _rc3 = st.columns([1, 1, 5])
    with _rc1:
        _filter_sym = st.text_input("Symbol Filter", key="pos_filter",
                                    placeholder="🔍 Filter symbol...",
                                    label_visibility="collapsed")
    with _rc2:
        if st.button("↺ Reset Sort", key="reset_sort"):
            st.session_state.pop("sort_col", None)
            st.session_state.pop("sort_asc", None)
            st.rerun()
    if _filter_sym.strip():
        data = [d for d in data if _filter_sym.strip().upper() in d["sym"].upper()]

    # Apply sort from session state (set by clicking column headers)
    _active_sort_col = st.session_state.get("sort_col", None)
    _active_sort_asc = st.session_state.get("sort_asc", False)

    _SORT_KEY_MAP = {
        "sym":    lambda item, ec: item["sym"],
        "net":    lambda item, ec: sum(x["net"]        for x in ec),
        "day":    lambda item, ec: sum(x["day"]        for x in ec),
        "carry":  lambda item, ec: sum(x["carry"]      for x in ec),
        "lots":   lambda item, ec: sum(x["open_qty"]   for x in ec),
        "tv":     lambda item, ec: sum(x["traded_val"] for x in ec),
        "real":   lambda item, ec: sum(x.get("realized",0) for x in ec),
        "unreal": lambda item, ec: sum(x.get("unrealized",0) for x in ec),
        "netexp": lambda item, ec: sum(x["net_exp"]    for x in ec),
    }
    if _active_sort_col and _active_sort_col in _SORT_KEY_MAP:
        _fn = _SORT_KEY_MAP[_active_sort_col]
        def _sort_key_with_zeros(item):
            ec  = [calc_expiry_pnl(e, item["lot_size"]) for e in item["expiries"]]
            val = _fn(item, ec)
            tv  = sum(x["traded_val"] for x in ec)
            oq  = sum(abs(x["open_qty"]) for x in ec)
            # Push no-activity rows (no trades, no open position) to bottom
            has_activity = tv > 0 or oq > 0
            if isinstance(val, str):
                return (1, val)  # symbol sort — no zero penalty
            return (0 if has_activity else 1, -val if not _active_sort_asc else val)
        data = sorted(data, key=_sort_key_with_zeros)

    # Replace sort arrow placeholders with actual state
    _SORT_COLS = ["sym","ltp","lots","netexp","tv","carry","real","unreal","day","net"]
    def _sa(col):
        if _active_sort_col != col: return "⇅"
        return "↑" if _active_sort_asc else "↓"

    def _href(col):
        # Next click toggles direction if same col, else desc
        if _active_sort_col == col:
            next_asc = 0 if _active_sort_asc else 1
        else:
            next_asc = 0
        return f"?sort={col}&asc={next_asc}"
    def _sc(col): return "#2eca8a" if (_active_sort_col==col and not _active_sort_asc) else ("#ff9f43" if (_active_sort_col==col and _active_sort_asc) else "#3a4055")

    table_html = build_position_table_html(
        data,
        expand_all    = st.session_state.expand_all,
        expanded_syms = st.session_state.expanded_syms,
    )
    # Apply sort arrow + href placeholders
    for _c in _SORT_COLS:
        table_html = table_html.replace(f"__SA_{_c}__",   _sa(_c))
        table_html = table_html.replace(f"__SC_{_c}__",   _sc(_c))
        table_html = table_html.replace(f"__HREF_{_c}__", _href(_c))

    st.html(f"""
    <div style="overflow-x:auto; width:100%; transform:rotateX(180deg); -webkit-transform:rotateX(180deg);">
        <div style="transform:rotateX(180deg); -webkit-transform:rotateX(180deg);">
            {table_html}
        </div>
    </div>
    """)




if __name__ == "__main__":
    main()
