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
import logging
import io
import csv
import time
import math
import html as _html
from datetime import datetime, date
from typing import Optional

import pandas as pd
import numpy as np
import streamlit as st
from streamlit_autorefresh import st_autorefresh

log = logging.getLogger(__name__)

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
        st.session_state["eod_date"]    = payload.get("eod_date", "")
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
        client.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS, timeout=5)
        sftp = client.open_sftp()
        try:
            with sftp.open(remote_path, "r") as f:
                reader = _csv.DictReader(f.read().decode("utf-8", errors="replace").splitlines())
                total_sv, total_v = 0.0, 0.0
                for row in reader:
                    try:
                        slip = float(row.get("slip_bps",   ""))
                        tval = float(row.get("traded_val", ""))
                        if tval > 0 and abs(slip) <= 500:
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
    Avg T1 = mean(t1_sec) from slippage_log CSV.

    t1_sec is the real order-to-fill latency written by the worker from
    NSE timestamp1 (order-sent epoch ns) and logtime (fill receipt IST).

    Falls back to window_slot arithmetic for older CSV files that predate
    the t1_sec column.

    Returns seconds / 60 so the caller can do avg_t1 * 60 to display seconds.
    Returns None if the CSV is missing or has no usable rows.
    """
    try:
        import paramiko
        import csv as _csv
        from datetime import date, datetime as dt

        trade_date  = date.today().strftime("%Y%m%d")
        remote_path = f"/data/Dashboard/snapshots/slippage_log_{trade_date}.csv"

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS, timeout=5)
        sftp = client.open_sftp()
        raw = []
        parse_errors = 0
        try:
            with sftp.open(remote_path, "r") as f:
                raw = f.read().decode("utf-8", errors="replace").splitlines()
        finally:
            sftp.close()
            client.close()

        if not raw:
            log.warning("fetch_avg_t1: empty CSV at %s", remote_path)
            return None

        reader  = _csv.DictReader(raw)
        cols    = reader.fieldnames or []
        t1_vals = []
        has_t1_col = "t1_sec" in (cols or [])

        for row in reader:
            try:
                t1_sec = None

                if has_t1_col:
                    # ── Primary: real exchange→strategy latency from algo log ──
                    raw_t1 = row.get("t1_sec", "").strip()
                    if raw_t1 and raw_t1.lower() not in ("", "none", "null"):
                        t1_sec = float(raw_t1)

                if t1_sec is None:
                    # ── Fallback: window_slot arithmetic (older CSV files) ─────
                    time_ist_raw = row.get("time_ist", "").strip()
                    slot_raw     = str(row.get("window_slot", "")).strip().zfill(4)
                    if (time_ist_raw and slot_raw and
                            slot_raw != "0000" and len(slot_raw) == 4 and slot_raw.isdigit()):
                        fill_t = dt.strptime(time_ist_raw, "%H:%M:%S")
                        slot_t = dt.strptime(f"{slot_raw[:2]}:{slot_raw[2:]}", "%H:%M")
                        t1_sec = (fill_t - slot_t).total_seconds()

                # Clamp: algo log T1 typically 0.001–2 s; > 30 s = outlier/error
                if t1_sec is not None and 0.001 <= t1_sec <= 30:
                    t1_vals.append(t1_sec)

            except Exception:
                parse_errors += 1
                continue

        if parse_errors:
            log.warning("fetch_avg_t1: %d rows failed to parse (cols=%s)", parse_errors, cols)

        if not t1_vals:
            log.warning("fetch_avg_t1: no usable rows — has_t1_col=%s, raw_lines=%d",
                        has_t1_col, len(raw))
            return None

        avg_sec = sum(t1_vals) / len(t1_vals)
        log.info("fetch_avg_t1: %d fills, avg=%.3f s, min=%.3f s, max=%.3f s (t1_col=%s)",
                 len(t1_vals), avg_sec, min(t1_vals), max(t1_vals), has_t1_col)
        return avg_sec / 60.0   # caller does × 60 to display seconds

    except Exception as exc:
        log.error("fetch_avg_t1_minutes failed: %s", exc)
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

    # ── C PNL logic ──────────────────────────────────────────────────────
    # Split into two parts:
    #   carry_locked : PnL earned up to & including previous day's close.
    #                  Frozen value from yesterday's EOD snapshot CSV.
    #                  Shows 0 if no snapshot available (first day / new position).
    #   carry_today  : Today's live carry = qty_overnight * (ltp - prev_close).
    #                  = 0 pre-market (ltp == 0), then floats with LTP once market opens.
    #
    # The "carry" field shown in the dashboard column is carry_today only —
    # reflecting today's movement from yesterday's close.  carry_locked is
    # preserved separately and shown as the previous day's dated carry row.
    # carry_locked is historical — NOT shown in C PNL column
    # C PNL only shows today's carry = qty × (ltp - prev_close)
    carry_locked = 0.0

    # ── Time gate: carry PnL only live from 9:10 AM IST ──────────────────────
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI
    _now         = _dt.now(tz=_ZI("Asia/Kolkata"))
    _market_gate = _now.replace(hour=9, minute=10, second=0, microsecond=0)

    if _now < _market_gate:
        carry = 0.0
    else:
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
    #
    # FIX (2026-06-23): original logic only triggered when open_sel_qty > 0
    # (i.e. sells exceed buys). But FIFO accounting means sells FIRST close
    # the overnight long before opening new shorts — even when buy==sell today
    # (e.g. INFY: overnight 400L, sold 400 at 09:30, rebought 400 at 09:35).
    # In that case open_sel_qty=0 but the overnight was still fully closed.
    # We must use qty_today_sell directly to determine overnight closure,
    # not the residual open_sel_qty after intraday matching.
    qty_on = e["qty_overnight"]
    if qty_on > 0 and e["qty_today_sell"] > 0:
        close_qty    = min(qty_on, e["qty_today_sell"])
        realized    += close_qty * (e["sell_avg"] - e["prev_close"])
        carry       -= close_qty * (e["ltp"] - e["prev_close"])
        # Sells used to close overnight are no longer available to match
        # against intraday buys — so those intraday buys become open/unrealized
        freed_buys   = min(close_qty, matched_qty)   # buys freed up by reclassifying sells
        unreal_buy  += freed_buys * (e["ltp"] - e["buy_avg"]) if freed_buys > 0 and e["buy_avg"] else 0
        realized    -= freed_buys * (e["sell_avg"] - e["buy_avg"]) if freed_buys > 0 and e["buy_avg"] else 0
        excess_sell  = max(0, open_sel_qty - close_qty)
        unreal_sell  = excess_sell * (e["sell_avg"] - e["ltp"]) if excess_sell > 0 else 0
    elif qty_on < 0 and e["qty_today_buy"] > 0:
        close_qty    = min(abs(qty_on), e["qty_today_buy"])
        realized    += close_qty * (e["prev_close"] - e["buy_avg"])
        carry       += close_qty * (e["ltp"] - e["prev_close"])
        freed_sells  = min(close_qty, matched_qty)
        unreal_sell += freed_sells * (e["sell_avg"] - e["ltp"]) if freed_sells > 0 and e["sell_avg"] else 0
        realized    -= freed_sells * (e["sell_avg"] - e["buy_avg"]) if freed_sells > 0 and e["sell_avg"] else 0
        excess_buy   = max(0, open_buy_qty - close_qty)
        unreal_buy   = excess_buy * (e["ltp"] - e["buy_avg"]) if excess_buy > 0 else 0

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
        "prev_close":   e.get("prev_close", 0.0),
        "qty_overnight": e.get("qty_overnight", 0.0),
        "ltp":          e["ltp"],
        "buy_avg":      e.get("buy_avg",  0.0),
        "sell_avg":     e.get("sell_avg", 0.0),
        "carry_lots":   carry_lots,
        "carry_exp_cr": carry_exp_cr,
        "lots":         lots,
        "open_qty":     open_qty,
        "net_exp":      net_exp,
        "traded_val":   traded_val,
        "buy_tv":       buy_val,
        "sell_tv":      sell_val,
        "cost":         expenses,
        "cost_pct":     round((expenses / traded_val * 100), 4) if traded_val else 0.0,
        "carry":              carry,        # today's carry: 0 pre-market, live once LTP feeds
        "carry_locked":       carry_locked, # yesterday's carry: frozen at previous close
        "day":                day,
        "realized":           realized,
        "unrealized":         unreal_buy + unreal_sell,
        "net":                net,
        "pnl_pct":            pnl_pct,
        "mtd":                e.get("mtd", 0),
        "slippage":           slippage,
        "inr_slippage":       e.get("inr_slippage", None),
        "inr_slip_traded_val": e.get("inr_slip_traded_val", None),
        "eod_date":           e.get("eod_date", ""),
        "fin_sig_lots":       e.get("fin_sig_lots", None),
    }


def fetch_otr_per_symbol(positions: list | None = None) -> dict:
    """OTR per symbol: orders_sent / fills from today's strategy log."""
    import re as _re
    try:
        import paramiko as _pm
        from datetime import date as _date
        trade_date = _date.today().strftime("%Y%m%d")
        client = _pm.SSHClient()
        client.set_missing_host_key_policy(_pm.AutoAddPolicy())
        client.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS, timeout=5)
        _, out, _ = client.exec_command(
            f"ls /data/logs/*algo_1_{trade_date}.log 2>/dev/null | head -1")
        log_path = out.read().decode().strip()
        if not log_path:
            _, out, _ = client.exec_command(
                "ls -t /data/logs/*algo_1_*.log 2>/dev/null | head -1")
            log_path = out.read().decode().strip()
        if not log_path:
            client.close(); return {}
        _, out, _ = client.exec_command(
            f"grep -E 'send_order::EXECUTION_STRATEGY_LIVE|emit_trade_fill::FTRD' {log_path}")
        lines = out.read().decode('utf-8', errors='replace').splitlines()
        client.close()

        # ── Build tok2sym from live positions — always 100% dynamic ──
        # Primary: positions data has sym + token for every symbol in the book.
        # No Redis EOD dependency, no hardcoding — new symbols just work.
        tok2sym = {}
        if positions:
            for _stock in positions:
                _sym = _stock.get("sym", "")
                for _e in _stock.get("expiries", []):
                    _tok = str(_e.get("token", "")).strip()
                    if _tok and _sym:
                        tok2sym[_tok] = _sym
        # Fallback: Redis EOD key (only if positions not passed)
        if not tok2sym:
            import redis as _rd, json as _json
            r1 = _rd.Redis(host="localhost", port=6379, db=1, decode_responses=True)
            from datetime import date as _d2
            _today = _d2.today().strftime("%Y%m%d")
            _raw = r1.get(f"dashboard:eod:{_today}")
            if _raw:
                for _tok, _v in _json.loads(_raw).items():
                    _name = _v.get("name", "")
                    if _tok and _name:
                        tok2sym[str(int(_tok))] = _name

        orders_ct: dict = {}
        fills_ct:  dict = {}
        re_send = _re.compile(r'order=\[(\d+),')
        re_ftrd = _re.compile(r'FTRD:[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,[^,]+,(\d+),')

        for line in lines:
            if 'send_order' in line:
                m = re_send.search(line)
                if m:
                    t = m.group(1)
                    orders_ct[t] = orders_ct.get(t, 0) + 1
            elif 'FTRD' in line:
                m = re_ftrd.search(line)
                if m:
                    t = m.group(1)
                    fills_ct[t] = fills_ct.get(t, 0) + 1

        result = {}
        for tok in set(list(orders_ct) + list(fills_ct)):
            sym = tok2sym.get(tok, tok)
            o   = orders_ct.get(tok, 0)
            f   = fills_ct.get(tok, 0)
            if f > 0:
                result[sym] = {'orders': o, 'fills': f, 'otr': round(o/f, 1)}
        return result
    except Exception:
        return {}


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
    kpis = {"net_exp": 0.0, "gross_exp": 0.0, "carry": 0.0,
             "day": 0.0, "net": 0.0, "expenses": 0.0, "slippages": []}

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
            "carry":        sum(x["carry"]        for x in exp_rows),

            "day":          sum(x["day"]           for x in exp_rows),
            "net":          sum(x["net"]           for x in exp_rows),
            "mtd":          sum(x["mtd"]           for x in exp_rows),
            "ltp":          None,
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

/* column filters */
.col-filter {
    width: 94%;
    box-sizing: border-box;
    background: #151821;
    border: 1px solid #252936;
    border-radius: 3px;
    color: #c0c6d4;
    font-family: 'JetBrains Mono', monospace;
    font-size: 9px;
    padding: 3px 4px;
    outline: none;
}
.col-filter::placeholder { color: #3f4658; }
.col-filter:focus { border-color: #3a4055; }
.filter-th { padding: 3px 2px 5px 2px !important; }

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
      <th>TV</th>
      <th>C PNL</th>
      <th>Realized</th>
      <th>Unrealized</th>
      <th>Day PnL</th>
      <th>Net PnL</th>
      <th>PnL%</th>
      <th>Slippage (bp)</th>
      <th>INR Slip (bp)</th>
      <th style="color:#565c6e">Last Fill</th>
    </tr>
    <tr>
      <th class="filter-th"></th>
      <th class="filter-th left">{_filter_input("sym", "symbol")}</th>
      <th class="filter-th">{_filter_input("token", "token")}</th>
      <th class="filter-th">{_filter_input("real", "real")}</th>
      <th class="filter-th">{_filter_input("final", "final")}</th>
      <th class="filter-th">{_filter_input("ltp", "ltp")}</th>
      <th class="filter-th">{_filter_input("buy", "buy")}</th>
      <th class="filter-th">{_filter_input("sell", "sell")}</th>
      <th class="filter-th">{_filter_input("carrylots", "carry lots")}</th>
      <th class="filter-th">{_filter_input("carryavg", "carry avg")}</th>
      <th class="filter-th">{_filter_input("lots", "lots")}</th>
      <th class="filter-th"></th>
      <th class="filter-th">{_filter_input("carryexp", "carry exp")}</th>
      <th class="filter-th">{_filter_input("netexp", "net exp")}</th>
      <th class="filter-th">{_filter_input("tv", "traded")}</th>
      <th class="filter-th">{_filter_input("cost", "cost")}</th>
      <th class="filter-th">{_filter_input("costbps", "bps")}</th>
      <th class="filter-th">{_filter_input("carry", "carry pnl")}</th>
      <th class="filter-th">{_filter_input("realized", "realized")}</th>
      <th class="filter-th">{_filter_input("unrealized", "unreal")}</th>
      <th class="filter-th">{_filter_input("day", "day pnl")}</th>
      <th class="filter-th">{_filter_input("net", "net pnl")}</th>
      <th class="filter-th">{_filter_input("pnlpct", "%")}</th>
      <th class="filter-th">{_filter_input("slip", "slip")}</th>
      <th class="filter-th">{_filter_input("inrslip", "inr")}</th>
      <th class="filter-th">{_filter_input("lastfill", "fill")}</th>
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

def build_position_table_html(data: list[dict], expand_all: bool, expanded_syms: set, filters: dict | None = None) -> str:
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
      <col style="width:75px">   <!-- Net PnL -->
      <col style="width:65px">   <!-- Last Fill -->
      <col style="width:45px">   <!-- Real Sig -->
      <col style="width:45px">   <!-- Final Sig -->
      <col style="width:75px">   <!-- LTP -->
      <col style="width:80px">   <!-- Buy Avg -->
      <col style="width:80px">   <!-- Sell Avg -->
      <col style="width:55px">   <!-- C Lot -->
      <col style="width:70px">   <!-- C Avg -->
      <col style="width:55px">   <!-- Lots -->
      <col style="width:20px">   <!-- mismatch -->
      <col style="width:70px">   <!-- Carry Exp.(Cr) -->
      <col style="width:70px">   <!-- Net Exp.(Cr) -->
      <col style="width:80px">   <!-- TV(Cr) -->
      <col style="width:50px">   <!-- Cost -->
      <col style="width:60px">   <!-- Cost%(Bips) -->
      <col style="width:60px">   <!-- C PNL -->
      <col style="width:80px">   <!-- Realized -->
      <col style="width:80px">   <!-- Unrealized -->
      <col style="width:75px">   <!-- Day PnL -->
      <col style="width:55px">   <!-- PnL% -->
      <col style="width:75px">   <!-- Slippage -->
      <col style="width:75px">   <!-- INR Slip -->
    </colgroup>"""

    filters = filters or {}
    def _fv(key: str) -> str:
        return _html.escape(str(filters.get(key, "") or ""), quote=True)
    def _filter_input(key: str, placeholder: str = "filter") -> str:
        return (
            f'<input class="col-filter" form="pos-filter-form" name="f_{key}" '
            f'value="{_fv(key)}" placeholder="{placeholder}" title="Press Enter to apply" />'
        )

    html = f"""
    <form id="pos-filter-form" method="get">
      <input type="hidden" name="sort" value="__ACTIVE_SORT__">
      <input type="hidden" name="asc" value="__ACTIVE_ASC__">
    </form>
    <table class="dash-table">
    {COLS}
    <thead><tr>
      <th class="left"></th>
      <th class="left" style="cursor:pointer"><a href="__HREF_sym__" style="text-decoration:none;color:inherit">Symbol / Expiry <span style="color:__SC_sym__;font-size:11px;font-weight:bold">__SA_sym__</span></a></th>
      <th>Token</th>
      <th style="cursor:pointer"><a href="__HREF_net__" style="text-decoration:none;color:inherit">Net PnL <span style="color:__SC_net__;font-size:11px;font-weight:bold">__SA_net__</span></a></th>
      <th style="color:#565c6e">Last Fill</th>
      <th>Real Sig</th>
      <th>Final Sig</th>
      <th style="cursor:pointer"><a href="__HREF_ltp__" style="text-decoration:none;color:inherit">LTP <span style="color:__SC_ltp__;font-size:11px;font-weight:bold">__SA_ltp__</span></a></th>
      <th>Buy Avg</th>
      <th>Sell Avg</th>
      <th>C Lot</th>
      <th style="color:#565c6e">C Avg</th>
      <th style="cursor:pointer"><a href="__HREF_lots__" style="text-decoration:none;color:inherit">Lots <span style="color:__SC_lots__;font-size:11px;font-weight:bold">__SA_lots__</span></a></th>
      <th title="Signal/Lots mismatch">⚡</th>
      <th>Carry Exp.(Cr)</th>
      <th style="cursor:pointer"><a href="__HREF_netexp__" style="text-decoration:none;color:inherit">Net Exp.(Cr) <span style="color:__SC_netexp__;font-size:11px;font-weight:bold">__SA_netexp__</span></a></th>
      <th style="cursor:pointer"><a href="__HREF_tv__" style="text-decoration:none;color:inherit">TV(Cr) <span style="color:__SC_tv__;font-size:11px;font-weight:bold">__SA_tv__</span></a></th>
      <th>Cost</th>
      <th>Cost%(Bips)</th>
      <th style="cursor:pointer"><a href="__HREF_carry__" style="text-decoration:none;color:inherit">C PNL <span style="color:__SC_carry__;font-size:11px;font-weight:bold">__SA_carry__</span></a></th>
      <th style="color:#2eca8a;cursor:pointer"><a href="__HREF_real__" style="text-decoration:none;color:inherit">Realized <span style="color:__SC_real__;font-size:11px;font-weight:bold">__SA_real__</span></a></th>
      <th style="color:#e8a825;cursor:pointer"><a href="__HREF_unreal__" style="text-decoration:none;color:inherit">Unrealized <span style="color:__SC_unreal__;font-size:11px;font-weight:bold">__SA_unreal__</span></a></th>
      <th style="cursor:pointer"><a href="__HREF_day__" style="text-decoration:none;color:inherit">Day PnL <span style="color:__SC_day__;font-size:11px;font-weight:bold">__SA_day__</span></a></th>
      <th>PnL%</th>
      <th>Slippage (bp)</th>
      <th>INR Slip (bp)</th>
    </tr>
    <tr>
      <th class="filter-th"></th>
      <th class="filter-th left">{_filter_input("sym", "symbol")}</th>
      <th class="filter-th">{_filter_input("token", "token")}</th>
      <th class="filter-th">{_filter_input("net", "net pnl")}</th>
      <th class="filter-th">{_filter_input("lastfill", "fill")}</th>
      <th class="filter-th">{_filter_input("real", "real")}</th>
      <th class="filter-th">{_filter_input("final", "final")}</th>
      <th class="filter-th">{_filter_input("ltp", "ltp")}</th>
      <th class="filter-th">{_filter_input("buy", "buy")}</th>
      <th class="filter-th">{_filter_input("sell", "sell")}</th>
      <th class="filter-th">{_filter_input("carrylots", "carry lots")}</th>
      <th class="filter-th">{_filter_input("carryavg", "carry avg")}</th>
      <th class="filter-th">{_filter_input("lots", "lots")}</th>
      <th class="filter-th"></th>
      <th class="filter-th">{_filter_input("carryexp", "carry exp")}</th>
      <th class="filter-th">{_filter_input("netexp", "net exp")}</th>
      <th class="filter-th">{_filter_input("tv", "traded")}</th>
      <th class="filter-th">{_filter_input("cost", "cost")}</th>
      <th class="filter-th">{_filter_input("costbps", "bps")}</th>
      <th class="filter-th">{_filter_input("carry", "carry pnl")}</th>
      <th class="filter-th">{_filter_input("realized", "realized")}</th>
      <th class="filter-th">{_filter_input("unrealized", "unreal")}</th>
      <th class="filter-th">{_filter_input("day", "day pnl")}</th>
      <th class="filter-th">{_filter_input("pnlpct", "%")}</th>
      <th class="filter-th">{_filter_input("slip", "slip")}</th>
      <th class="filter-th">{_filter_input("inrslip", "inr")}</th>
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
        # Weight by inr_slip_traded_val (Σ fill-level traded_val that contributed to
        # the per-expiry inr_slip computation), NOT by the expiry-level traded_val
        # (which is recomputed from avg prices and drifts from the fill-level sum).
        _inr_pairs = [(x["inr_slippage"], x["inr_slip_traded_val"])
                      for x in exp_calcs
                      if x.get("inr_slippage") is not None and (x.get("inr_slip_traded_val") or 0) > 0]
        if _inr_pairs:
            _inr_w        = sum(w for _, w in _inr_pairs)
            stock_inr_slip = sum(s * w for s, w in _inr_pairs) / _inr_w if _inr_w else None
        else:
            stock_inr_slip = None
        stock_open_qty = total_oq
        stock_lots = total_oq / lot_size if lot_size > 0 else None
        s_net_exp    = sum(x["net_exp"]    for x in exp_calcs)
        s_tval       = sum(x["traded_val"] for x in exp_calcs)
        s_carry        = sum(x["carry"] for x in exp_calcs)
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

        # C Lot aggregate
        s_carry_lots   = sum(x["carry_lots"]   for x in exp_calcs)
        s_carry_exp_cr = sum(x["carry_exp_cr"] for x in exp_calcs)

        # C Lot td
        cl = round(float(s_carry_lots), 1)
        carry_lots_td = f'<td style="color:#7a8294">{("+" if cl > 0 else "") + str(cl)}</td>' if cl != 0 else '<td class="zer">0.0</td>'
        # C Avg = weighted previous close of overnight carry, never today's fill price.
        _carry_notional = 0.0
        _carry_qty_abs  = 0.0
        for _e in item["expiries"]:
            _q = abs(float(_e.get("qty_overnight", 0.0) or 0.0))
            _pc = float(_e.get("prev_close", 0.0) or 0.0)
            if _q > 0 and _pc > 0:
                _carry_notional += _q * _pc
                _carry_qty_abs  += _q
        _carry_prev = (_carry_notional / _carry_qty_abs) if _carry_qty_abs else 0.0
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
          {pnl_td(s_net)}
          <td style="color:#565c6e;font-size:10px;text-align:right;padding-right:8px">{_stock_last_fill or "—"}</td>
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
          {stock_pct_td}
          {slip_td(stock_slip, stock_open_qty)}
          {slip_td(stock_inr_slip, stock_open_qty)}
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

                # C Lot
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
                  {pnl_td(ec["net"])}
                  <td style="color:#565c6e;font-size:10px;text-align:right;padding-right:8px">{ec.get("last_fill_time") or "—"}</td>
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
                  {pnl_pct_td}
                  {slip_td(ec.get("slippage"), ec.get("open_qty", 0))}
                  {slip_td(ec.get("inr_slippage"), ec.get("open_qty", 0))}
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



def _fmt_num_for_grid(v, decimals=2):
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return round(float(v), decimals)
    except Exception:
        return None


def build_aggrid_rows(data: list[dict], expand_all: bool, expanded_syms: set, otr_map: dict | None = None) -> pd.DataFrame:
    """Flat table for AgGrid. AgGrid provides native per-column filters + sorting."""
    signal_map = get_signal_map()
    out = []
    for item in data:
        sym = item["sym"]
        lot_size = item["lot_size"]
        sig = signal_map.get(sym.upper(), {})
        real_sig = sig.get("real_signal", "—")
        final_sig = sig.get("final_signal", "—")
        exp_calcs = [calc_expiry_pnl(e, lot_size) for e in item["expiries"]]
        s_open_qty = sum(x["open_qty"] for x in exp_calcs)
        s_tval = sum(x["traded_val"] for x in exp_calcs)
        s_buy_tv = sum(x.get("buy_tv", 0.0) for x in exp_calcs)
        s_sell_tv = sum(x.get("sell_tv", 0.0) for x in exp_calcs)
        s_cost = sum(x["cost"] for x in exp_calcs)
        s_cost_pct = sum(x["cost_pct"] for x in exp_calcs if x["traded_val"])
        latest_exp = sorted(item["expiries"], key=lambda x: x["label"])[-1] if item["expiries"] else {}
        latest_ec = next((ec for ec in exp_calcs if ec["label"] == latest_exp.get("label")), exp_calcs[-1] if exp_calcs else {})
        _slip_pairs = [(x.get("slippage"), x["traded_val"]) for x in exp_calcs if x.get("slippage") is not None and x.get("traded_val", 0) > 0]
        stock_slip = None
        if _slip_pairs:
            tw = sum(w for _, w in _slip_pairs)
            stock_slip = sum(s * w for s, w in _slip_pairs) / tw if tw else None
        _inr_pairs = [(x.get("inr_slippage"), x.get("inr_slip_traded_val")) for x in exp_calcs if x.get("inr_slippage") is not None and (x.get("inr_slip_traded_val") or 0) > 0]
        stock_inr_slip = None
        if _inr_pairs:
            tw = sum(w for _, w in _inr_pairs)
            stock_inr_slip = sum(s * w for s, w in _inr_pairs) / tw if tw else None
        s_day = sum(x["day"] for x in exp_calcs)
        # C Avg = weighted previous close of overnight carry, never today's fill price.
        carry_prev_num = 0.0
        carry_prev_den = 0.0
        for _e in item["expiries"]:
            _q = abs(float(_e.get("qty_overnight", 0.0) or 0.0))
            _pc = float(_e.get("prev_close", 0.0) or 0.0)
            if _q > 0 and _pc > 0:
                carry_prev_num += _q * _pc
                carry_prev_den += _q
        carry_avg_prev_close = (carry_prev_num / carry_prev_den) if carry_prev_den else None
        out.append({
            "Symbol / Expiry": sym,
            "Lot Size": lot_size,
            "Token": latest_exp.get("token", ""),
            "Net PnL": _fmt_num_for_grid(sum(x["net"] for x in exp_calcs), 0),
            "Last Fill": max([e.get("last_fill_time", "") for e in item["expiries"]] or [""]),
            "Real Sig": real_sig,
            "Final Sig": final_sig,
            "Fin Sig Lots":  _fmt_num_for_grid(exp_calcs[0].get("fin_sig_lots") if exp_calcs else None, 0),
            "LTP": _fmt_num_for_grid(latest_ec.get("ltp")),
            "Buy Avg": _fmt_num_for_grid(latest_ec.get("buy_avg")),
            "Sell Avg": _fmt_num_for_grid(latest_ec.get("sell_avg")),
            "C Lot": _fmt_num_for_grid(sum(x["carry_lots"] for x in exp_calcs), 1),
            "C Avg": _fmt_num_for_grid(carry_avg_prev_close),
            "Lots": _fmt_num_for_grid(s_open_qty / lot_size if lot_size else None, 1),
            "⚡": "●" if mismatch_td(final_sig, s_open_qty / lot_size if lot_size else None) != '<td></td>' else "",
            "Carry Exp.(Cr)": _fmt_num_for_grid(sum(x["carry_exp_cr"] for x in exp_calcs)),
            "Net Exp.(Cr)": _fmt_num_for_grid(sum(x["net_exp"] for x in exp_calcs) / 1e7),
            "TV(Cr)": _fmt_num_for_grid(s_tval / 1e7),
            "Cost": _fmt_num_for_grid(s_cost, 0),
            "Cost%(Bips)": _fmt_num_for_grid(s_cost_pct * 100 if s_tval else None),
            "C PNL": _fmt_num_for_grid(sum(x["carry"] for x in exp_calcs), 0),
            "Realized": _fmt_num_for_grid(sum(x.get("realized", 0) for x in exp_calcs), 0),
            "Unrealized": _fmt_num_for_grid(sum(x.get("unrealized", 0) for x in exp_calcs), 0),
            "Day PnL": _fmt_num_for_grid(s_day, 0),
            "PnL%": _fmt_num_for_grid((s_day / abs(s_tval) * 100) if s_tval else None),
            "Slippage (bp)": _fmt_num_for_grid(stock_slip * 10000 if stock_slip is not None else None),
            "INR Slip (bp)": _fmt_num_for_grid(stock_inr_slip * 10000 if stock_inr_slip is not None else None),
            "OTR":       (lambda _o: f'{_o["otr"]}' if _o else "")(
                         (otr_map or {}).get(item["sym"], {})),
            "_row_type": "stock",
        })
        if expand_all or sym in expanded_syms:
            for e, ec in zip(item["expiries"], exp_calcs):
                out.append({
                    "Symbol / Expiry": "  " + ec["label"],
                    "Lot Size": lot_size,
                    "Token": e.get("token", ""),
                    "Net PnL": _fmt_num_for_grid(ec.get("net"), 0),
                    "Last Fill": e.get("last_fill_time", ""),
                    "Real Sig": real_sig,
                    "Final Sig": final_sig,
                    "Fin Sig Lots":  _fmt_num_for_grid(ec.get("fin_sig_lots"), 0),
                    "LTP": _fmt_num_for_grid(ec.get("ltp")),
                    "Buy Avg": _fmt_num_for_grid(ec.get("buy_avg")),
                    "Sell Avg": _fmt_num_for_grid(ec.get("sell_avg")),
                    "C Lot": _fmt_num_for_grid(ec.get("carry_lots"), 1),
                    "C Avg": _fmt_num_for_grid(e.get("prev_close")),
                    "Lots": _fmt_num_for_grid(ec.get("lots"), 1),
                    "⚡": "●" if mismatch_td(final_sig, ec.get("lots")) != '<td></td>' else "",
                    "Carry Exp.(Cr)": _fmt_num_for_grid(ec.get("carry_exp_cr")),
                    "Net Exp.(Cr)": _fmt_num_for_grid(ec.get("net_exp") / 1e7),
                    "TV(Cr)": _fmt_num_for_grid(ec.get("traded_val") / 1e7),
                    "Cost": _fmt_num_for_grid(ec.get("cost"), 0),
                    "Cost%(Bips)": _fmt_num_for_grid(ec.get("cost_pct") * 100 if ec.get("traded_val") else None),
                    "C PNL": _fmt_num_for_grid(ec.get("carry"), 0),
                    "Realized": _fmt_num_for_grid(ec.get("realized"), 0),
                    "Unrealized": _fmt_num_for_grid(ec.get("unrealized"), 0),
                    "Day PnL": _fmt_num_for_grid(ec.get("day"), 0),
                    "PnL%": _fmt_num_for_grid(ec.get("pnl_pct")),
                    "Slippage (bp)": _fmt_num_for_grid(ec.get("slippage") * 10000 if ec.get("slippage") is not None else None),
                    "INR Slip (bp)": _fmt_num_for_grid(ec.get("inr_slippage") * 10000 if ec.get("inr_slippage") is not None else None),
                    "_row_type": "expiry",
                })
    return pd.DataFrame(out)


def render_aggrid_position_table(data: list[dict], expand_all: bool, expanded_syms: set, otr_map: dict | None = None):
    """Render a real interactive grid with in-header dropdown filters and auto-sized columns."""
    try:
        from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
    except Exception:
        st.error("Column filters need streamlit-aggrid. Install once: pip install streamlit-aggrid")
        return False

    grid_df = build_aggrid_rows(data, expand_all, expanded_syms, otr_map=otr_map)

    # PyArrow/AgGrid is strict about mixed dtypes. Keep ID/text columns as
    # strings and numeric columns as numeric so filters and sorting work cleanly.
    text_cols = ["Symbol / Expiry", "Token", "Real Sig", "Final Sig", "⚡", "Last Fill", "OTR", "_row_type"]
    for col in text_cols:
        if col in grid_df.columns:
            grid_df[col] = grid_df[col].fillna("").astype(str)
    for col in grid_df.columns:
        if col not in text_cols:
            grid_df[col] = pd.to_numeric(grid_df[col], errors="coerce")

    # Header dropdown filters: each column filter icon opens a checklist of
    # values directly from that column header. No separate filter panel.
    filter_cols = [c for c in grid_df.columns if c != "_row_type"]

    def _filter_values_for_col(col):
        vals = []
        for v in grid_df[col].tolist():
            if v is None or (isinstance(v, float) and math.isnan(v)):
                vals.append(None)
            elif col in text_cols:
                vals.append(str(v).strip())
            else:
                try:
                    vals.append(float(v))
                except Exception:
                    vals.append(v)
        def _sort_key(x):
            if x is None or x == "":
                return (2, "")
            if isinstance(x, (int, float)) and not isinstance(x, bool):
                return (0, float(x))
            return (1, str(x))
        return sorted(set(vals), key=_sort_key)

    _set_filter_params = {
        c: {
            "values": _filter_values_for_col(c),
            "buttons": ["reset", "apply"],
            "closeOnApply": True,
            "suppressMiniFilter": False,
        }
        for c in filter_cols
    }

    gb = GridOptionsBuilder.from_dataframe(grid_df)
    gb.configure_default_column(
        sortable=True,
        filter="agSetColumnFilter",
        resizable=True,
        floatingFilter=False,
        suppressMenu=False,
        menuTabs=["filterMenuTab"],
        minWidth=54,
        wrapHeaderText=False,
        autoHeaderHeight=False,
    )
    gb.configure_column("_row_type", hide=True)

    # Widths tuned for the dashboard screenshot: compact signal/flag columns,
    # wider PnL/price columns, and the symbol pinned on the left.
    width_map = {
        # Initial compact widths. After render, AgGrid auto-sizes each column
        # from visible header + cell contents, so names stay readable without
        # wasting horizontal space.
        "Symbol / Expiry": 140, "Lot Size": 78, "Token": 74, "Real Sig": 76, "Final Sig": 82,
        "LTP": 78, "Buy Avg": 82, "Sell Avg": 84, "C Lot": 62,
        "C Avg": 76, "Lots": 62, "⚡": 42, "Carry Exp.(Cr)": 104,
        "Net Exp.(Cr)": 96, "TV(Cr)": 82, "Cost": 72,
        "Cost%(Bips)": 94, "C PNL": 76, "Realized": 86,
        "Unrealized": 96, "Day PnL": 86, "Net PnL": 86, "PnL%": 66,
        "Slippage (bp)": 98, "INR Slip (bp)": 98, "Last Fill": 90,
        "Fin Sig Lots": 96,
    }
    gb.configure_column("Symbol / Expiry", pinned="left", width=width_map["Symbol / Expiry"], minWidth=120, suppressSizeToFit=True, filter="agSetColumnFilter", filterParams=_set_filter_params.get("Symbol / Expiry", {}))
    if "Lot Size" in grid_df.columns:
        gb.configure_column("Lot Size", width=width_map["Lot Size"], minWidth=70, suppressSizeToFit=True, filter="agSetColumnFilter", filterParams=_set_filter_params.get("Lot Size", {}))
    for c in ["Token", "Real Sig", "Final Sig", "Last Fill", "⚡"]:
        if c in grid_df.columns:
            gb.configure_column(c, width=width_map.get(c, 70), minWidth=48, suppressSizeToFit=True, filter="agSetColumnFilter", filterParams=_set_filter_params.get(c, {}))
    numeric_cols = [c for c in grid_df.columns if c not in ["Symbol / Expiry", "Token", "Real Sig", "Final Sig", "Last Fill", "⚡", "_row_type"]]
    value_formatter = JsCode("""
    function(params) {
      if (params.value === null || params.value === undefined || params.value === '') return '—';
      let v = Number(params.value);
      if (isNaN(v)) return params.value;
      const oneDec = ['C Lot','Lots'];
      const twoDec = ['LTP','Buy Avg','Sell Avg','C Avg','Carry Exp.(Cr)','Net Exp.(Cr)','TV(Cr)','Cost%(Bips)','PnL%','Slippage (bp)','INR Slip (bp)'];
      const zeroDec = ['Cost','C PNL','Realized','Unrealized','Day PnL','Net PnL'];
      let d = twoDec.includes(params.colDef.field) ? 2 : (oneDec.includes(params.colDef.field) ? 1 : 0);
      if (zeroDec.includes(params.colDef.field)) d = 0;
      return v.toLocaleString('en-IN', {minimumFractionDigits: d, maximumFractionDigits: d});
    }
    """)
    for c in numeric_cols:
        gb.configure_column(
            c,
            type=["numericColumn"],
            filter="agSetColumnFilter",
            filterParams=_set_filter_params.get(c, {}),
            width=width_map.get(c, 86),
            minWidth=70 if c == "Lot Size" else 54,
            suppressSizeToFit=True,
            valueFormatter=value_formatter,
        )

    cell_style = JsCode("""
    function(params) {
      const dark = {'backgroundColor':'#13151c','color':'#7a8294','fontFamily':'JetBrains Mono, monospace','fontSize':'11px'};
      const stock = {'backgroundColor':'#1a1d26','color':'#c0c6d4','fontWeight':'600','fontFamily':'JetBrains Mono, monospace','fontSize':'11px'};
      let s = params.data && params.data._row_type === 'stock' ? stock : dark;
      const pnlCols = ['C PNL','Realized','Unrealized','Day PnL','Net PnL','PnL%','Slippage (bp)','INR Slip (bp)'];
      if (pnlCols.includes(params.colDef.field) && params.value !== null && params.value !== undefined && params.value !== '') {
        let v = Number(params.value);
        if (v > 0) s = {...s, color:'#00e0a4'};
        if (v < 0) s = {...s, color:'#ff4d57'};
      }
      if (params.colDef.field === '⚡' && params.value === '●') s = {...s, color:'#ff4444'};
      if (params.colDef.field === 'Fin Sig Lots' && params.value !== null && params.value !== undefined && params.value !== '') {
        s = {...s, color:'#e8a825', fontWeight:'600'};
      }
      return s;
    }
    """)
    for c in grid_df.columns:
        if c != "_row_type":
            gb.configure_column(c, cellStyle=cell_style)
    grid_options = gb.build()
    grid_options["rowHeight"] = 27
    grid_options["headerHeight"] = 34
    grid_options["floatingFiltersHeight"] = 0
    grid_options["onFirstDataRendered"] = JsCode("""
    function(params) {
      setTimeout(function() {
        const cols = params.columnApi.getAllColumns().map(c => c.getColId());
        params.columnApi.autoSizeColumns(cols, false);
        const symCol = params.columnApi.getColumn('Symbol / Expiry');
        if (symCol && symCol.getActualWidth() < 130) {
          params.columnApi.setColumnWidth('Symbol / Expiry', 130);
        }
      }, 80);
    }
    """)
    grid_options["suppressRowHoverHighlight"] = False
    grid_options["enableCellTextSelection"] = True
    grid_options["suppressHorizontalScroll"] = False
    grid_options["animateRows"] = False

    # Keep the grid dark even in the empty area below the last row, and make
    # floating filters visually match the rest of the Streamlit dashboard.
    custom_css = {
        ".ag-root-wrapper": {"background-color": "#101218 !important", "border": "1px solid #1e2230 !important"},
        ".ag-root": {"background-color": "#101218 !important"},
        ".ag-body-viewport": {"background-color": "#101218 !important"},
        ".ag-center-cols-viewport": {"background-color": "#101218 !important"},
        ".ag-header": {"background-color": "#0f1117 !important", "border-bottom": "1px solid #1e2230 !important"},
        ".ag-header-cell-label": {"align-items": "center !important", "justify-content": "flex-start !important"},
        ".ag-header-cell-text": {"color": "#7a8294 !important", "font-size": "10px !important", "font-weight": "700 !important", "white-space": "nowrap !important", "line-height": "13px !important", "overflow": "hidden !important", "text-overflow": "clip !important"},
        ".ag-header-cell-menu-button, .ag-header-icon": {"color": "#7a8294 !important", "opacity": "1 !important"},
        ".ag-header-cell": {"padding-left": "6px !important", "padding-right": "3px !important"},
        ".ag-row": {"border-bottom": "1px solid #181b22 !important"},
        ".ag-cell": {"line-height": "27px !important", "padding-left": "6px !important", "padding-right": "6px !important"},
        ".ag-pinned-left-cols-container .ag-cell": {"background-color": "#151821 !important"},
        ".ag-menu": {"background-color": "#151821 !important", "color": "#c0c6d4 !important", "border": "1px solid #2a2f3d !important"},
        ".ag-filter-body-wrapper": {"background-color": "#151821 !important", "color": "#c0c6d4 !important"},
        ".ag-set-filter-list": {"background-color": "#151821 !important", "color": "#c0c6d4 !important"},
        ".ag-input-field-input": {"background-color": "#101218 !important", "color": "#c0c6d4 !important", "border": "1px solid #2a2f3d !important"},
    }

    height = min(620, max(240, 58 + 27 * (len(grid_df) + 1)))
    AgGrid(
        grid_df,
        gridOptions=grid_options,
        theme="streamlit",
        height=height,
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,
        enable_enterprise_modules=True,
        custom_css=custom_css,
        key="position_book_aggrid",
    )
    return True

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
        # asc=1 means ascending was requested by the link. Keep query params so column filters remain visible.
        asc_param = qp.get("asc", "0")
        st.session_state["sort_col"] = sort_col
        st.session_state["sort_asc"] = (asc_param == "1")

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
    k1, k2, k3, k_tv_buy, k_tv_sell, k4, k4b, k4c, k5, k6, k7, k8 = st.columns(12)
    k1.metric("Net Exposure",   fmt_inr(kpis["net_exp"]))
    k2.metric("Gross Exposure", fmt_inr(kpis["gross_exp"]))
    k3.metric("C PNL",      fmt_inr(kpis["carry"], show_sign=True))

    buy_tv_total = sum(
        (e.get("qty_today_buy", 0) or 0) * (e.get("buy_avg", 0) or e.get("ltp", 0) or 0)
        for stock in data for e in stock.get("expiries", [])
    )
    sell_tv_total = sum(
        (e.get("qty_today_sell", 0) or 0) * (e.get("sell_avg", 0) or e.get("ltp", 0) or 0)
        for stock in data for e in stock.get("expiries", [])
    )
    k_tv_buy.metric("Buy TV", fmt_inr(buy_tv_total))
    k_tv_sell.metric("Sell TV", fmt_inr(sell_tv_total))

    k4.metric("Day PnL",        fmt_inr(kpis["day"],   show_sign=True))
    cur_net = kpis["net"]

    # Persist Day High / Day Low in Redis with timestamp.
    from datetime import date as _date, datetime as _dt
    import json as _json
    _today = _date.today().strftime("%Y%m%d")
    _now_hm = _dt.now().strftime("%H:%M")
    _dh_key = f"dashboard:day_high:{_today}"
    _dl_key = f"dashboard:day_low:{_today}"
    _is_live = st.session_state.get("data_source", "dummy") == "log_file"

    def _read_extreme(_r, _key):
        raw = _r.get(_key)
        if not raw:
            return None, ""
        try:
            obj = _json.loads(raw)
            return float(obj.get("value", 0.0)), obj.get("time", "")
        except Exception:
            try:
                return float(raw), ""
            except Exception:
                return None, ""

    try:
        import redis as _redis_dh
        _r = _redis_dh.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True, socket_timeout=2.0)
        _dh, _dh_time = _read_extreme(_r, _dh_key)
        _dl, _dl_time = _read_extreme(_r, _dl_key)
        _sane = abs(cur_net) < 50000000

        if _is_live and _sane and (_dh is None or cur_net > _dh):
            _dh, _dh_time = cur_net, _now_hm
            _r.set(_dh_key, _json.dumps({"value": _dh, "time": _dh_time}), ex=86400)

        if _is_live and _sane and (_dl is None or cur_net < _dl):
            _dl, _dl_time = cur_net, _now_hm
            _r.set(_dl_key, _json.dumps({"value": _dl, "time": _dl_time}), ex=86400)

        _dh = cur_net if _dh is None else _dh
        _dl = cur_net if _dl is None else _dl

    except Exception:
        _dh = getattr(st.session_state, "day_high_pnl", cur_net)
        _dl = getattr(st.session_state, "day_low_pnl", cur_net)
        _dh_time = getattr(st.session_state, "day_high_time", _now_hm)
        _dl_time = getattr(st.session_state, "day_low_time", _now_hm)

        if _is_live and cur_net > _dh:
            _dh, _dh_time = cur_net, _now_hm
        if _is_live and cur_net < _dl:
            _dl, _dl_time = cur_net, _now_hm

    st.session_state.day_high_pnl = _dh
    st.session_state.day_low_pnl = _dl
    st.session_state.day_high_time = _dh_time
    st.session_state.day_low_time = _dl_time

    k4b.metric(f"Net High @ {_dh_time or '—'}", fmt_inr(_dh, show_sign=True))
    k4c.metric(f"Net Low @ {_dl_time or '—'}", fmt_inr(_dl, show_sign=True))
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
    if avg_t1 is not None and avg_t1 > 0:
        avg_t1_ms = avg_t1 * 60 * 1000   # minutes → milliseconds
        if avg_t1_ms < 1000:
            k8.metric("Avg T1 (ms)", f"{avg_t1_ms:.1f} ms")
        else:
            k8.metric("Avg T1 (ms)", f"{avg_t1_ms/1000:.2f} s")
    else:
        k8.metric("Avg T1 (ms)", "—")

    st.html("<div style='margin:10px 0 6px'></div>")

    # ── Section label ────────────────────────────────────────
    st.html("<div class='section-hdr'>Position Book — Intraday</div>")

    # ── Interactive position table ──────────────────────────
    # Uses AgGrid: every column has its own filter box under the header,
    # and every header remains sortable. This replaces the earlier static
    # HTML input row, which could render but could not reliably talk back
    # to Streamlit.
    _otr_map = fetch_otr_per_symbol(positions=data)
    render_aggrid_position_table(
        data,
        expand_all    = st.session_state.expand_all,
        expanded_syms = st.session_state.expanded_syms,
        otr_map       = _otr_map,
    )





if __name__ == "__main__":
    main()