"""
dashboard_chart_collector.py
=============================
Collects dashboard snapshots every 5 minutes for charting.
Stores time-series data in Redis for MTM vs NIFTY, Net Exposure vs NIFTY,
and symbol-wise MTM vs NIFTY charts.

Run:
    python3 dashboard_chart_collector.py

Cron (every 5 min during market hours):
    */5 9-16 * * 1-5 /home/report/devstudio/Prashant/Live_Dashboard/venv/bin/python3 \
        /home/report/devstudio/Prashant/Live_Dashboard/Prod/dashboard_chart_collector.py \
        >> /home/report/devstudio/Prashant/Live_Dashboard/logs/chart_collector.log 2>&1

Redis storage:
    DB 1, key: dashboard:chart:YYYYMMDD
    Value: JSON list of snapshots
    Each snapshot: {time, net_pnl, net_exp, gross_exp, carry_pnl, day_pnl,
                    nifty_ltp, banknifty_ltp, symbols: {SYM: {net_pnl, lots, net_exp}}}
    TTL: 7 days
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, date
from zoneinfo import ZoneInfo

import redis

# ── Config ────────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
DASH_DB        = int(os.getenv("REDIS_DB", "1"))      # dashboard positions
LTP_DB         = int(os.getenv("LTP_REDIS_DB", "2"))  # stock LTP
IDX_DB         = int(os.getenv("IDX_REDIS_DB", "0"))  # index LTP (NIFTY)
LTP_HASH_KEY   = os.getenv("LTP_HASH_KEY", "last_price")
DASH_KEY       = "dashboard:positions:latest2"
CHART_KEY_PFX  = "dashboard:chart:"
CHART_TTL      = 7 * 86400  # 7 days

IST = ZoneInfo("Asia/Kolkata")

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
# PNL ENGINE — mirrors dashboard calc_expiry_pnl
# ══════════════════════════════════════════════════════════════════

def calc_position_pnl(e: dict, lot_size: int) -> dict:
    """Calculate PnL for one expiry — mirrors dashboard logic."""
    net_today = e["qty_today_buy"] - e["qty_today_sell"]
    open_qty  = e["qty_overnight"] + net_today
    lots      = round(open_qty / lot_size, 2) if lot_size > 0 else 0

    carry = e["qty_overnight"] * (e["ltp"] - e["prev_close"])

    matched_qty  = min(e["qty_today_buy"], e["qty_today_sell"])
    open_buy_qty = e["qty_today_buy"]  - matched_qty
    open_sel_qty = e["qty_today_sell"] - matched_qty
    realized     = matched_qty  * (e["sell_avg"] - e["buy_avg"]) if matched_qty  > 0 else 0
    unreal_buy   = open_buy_qty * (e["ltp"] - e["buy_avg"])      if open_buy_qty > 0 else 0
    unreal_sell  = open_sel_qty * (e["sell_avg"] - e["ltp"])     if open_sel_qty > 0 else 0
    day          = realized + unreal_buy + unreal_sell

    buy_val  = e["qty_today_buy"]  * (e["buy_avg"]  or e["ltp"])
    sell_val = e["qty_today_sell"] * (e["sell_avg"] or e["ltp"])
    tval     = buy_val + sell_val
    expenses = (buy_val / 1e7) * 1018 + (sell_val / 1e7) * 5818
    net      = carry + day - expenses
    net_exp  = open_qty * e["ltp"]

    return {
        "carry":   carry,
        "day":     day,
        "net":     net,
        "net_exp": net_exp,
        "lots":    lots,
    }


# ══════════════════════════════════════════════════════════════════
# MAIN COLLECTOR
# ══════════════════════════════════════════════════════════════════

def collect_snapshot() -> dict | None:
    """Read current dashboard state and build a snapshot."""
    now = datetime.now(IST)
    time_str = now.strftime("%H:%M:%S")
    date_str = now.strftime("%Y%m%d")

    try:
        r_dash = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                             db=DASH_DB, decode_responses=True, socket_timeout=2)
        r_ltp  = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                             db=LTP_DB,  decode_responses=True, socket_timeout=2)
        r_idx  = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                             db=IDX_DB,  decode_responses=True, socket_timeout=2)
    except Exception as e:
        log.error("Redis connection failed: %s", e)
        return None

    # ── Read dashboard positions ──────────────────────────────────
    raw = r_dash.get(DASH_KEY)
    if not raw:
        log.warning("No dashboard data in Redis — skipping snapshot")
        return None

    payload   = json.loads(raw)
    positions = payload.get("positions", [])
    source    = payload.get("source", "")

    if not positions:
        log.warning("No positions in Redis — skipping snapshot")
        return None

    # ── Read NIFTY + BANKNIFTY LTP ────────────────────────────────
    try:
        nifty_ltp  = float(r_idx.hget("fo:index_spot:NIFTY",     "ltp") or 0)
        bnifty_ltp = float(r_idx.hget("fo:index_spot:BANKNIFTY", "ltp") or 0)
    except Exception:
        nifty_ltp  = 0.0
        bnifty_ltp = 0.0

    # ── Calculate aggregate PnL ───────────────────────────────────
    total_net_pnl  = 0.0
    total_net_exp  = 0.0
    total_gross_exp = 0.0
    total_carry    = 0.0
    total_day      = 0.0
    sym_data       = {}

    for stock in positions:
        sym      = stock["sym"]
        lot_size = stock["lot_size"]
        s_net    = 0.0
        s_netexp = 0.0
        s_carry  = 0.0
        s_lots   = 0.0

        for e in stock["expiries"]:
            p = calc_position_pnl(e, lot_size)
            s_net    += p["net"]
            s_netexp += p["net_exp"]
            s_carry  += p["carry"]
            s_lots   += p["lots"]

        total_net_pnl   += s_net
        total_net_exp   += s_netexp
        total_gross_exp += abs(s_netexp)
        total_carry     += s_carry
        total_day       += (s_net - s_carry)

        sym_data[sym] = {
            "net_pnl": round(s_net,    2),
            "net_exp": round(s_netexp, 2),
            "lots":    round(s_lots,   2),
        }

    snapshot = {
        "time":       time_str,
        "date":       date_str,
        "source":     source,
        "net_pnl":    round(total_net_pnl,   2),
        "net_exp":    round(total_net_exp,   2),
        "gross_exp":  round(total_gross_exp, 2),
        "carry_pnl":  round(total_carry,     2),
        "day_pnl":    round(total_day,       2),
        "nifty":      nifty_ltp,
        "banknifty":  bnifty_ltp,
        "symbols":    sym_data,
    }

    # Skip snapshot if NIFTY not yet available (pre-market)
    if nifty_ltp == 0:
        log.warning("NIFTY LTP = 0 — index feeder not ready, skipping snapshot")
        return None

    log.info("Snapshot: net_pnl=%s  net_exp=%s  nifty=%s  symbols=%d",
             snapshot["net_pnl"], snapshot["net_exp"],
             snapshot["nifty"], len(sym_data))

    return snapshot, date_str


def save_snapshot(snapshot: dict, date_str: str):
    """Append snapshot to Redis time-series list for today."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                        db=DASH_DB, decode_responses=True, socket_timeout=2)
        key = f"{CHART_KEY_PFX}{date_str}"

        # Load existing snapshots
        existing = r.get(key)
        snaps = json.loads(existing) if existing else []

        # Append new snapshot
        snaps.append(snapshot)

        # Save back with TTL
        r.set(key, json.dumps(snaps), ex=CHART_TTL)
        log.info("Saved snapshot %d for %s → Redis key: %s",
                 len(snaps), date_str, key)

    except Exception as e:
        log.error("Failed to save snapshot: %s", e)


def main():
    log.info("=== Dashboard Chart Collector ===")
    result = collect_snapshot()
    if result:
        snapshot, date_str = result
        save_snapshot(snapshot, date_str)
        log.info("Done — snapshot saved for %s at %s",
                 date_str, snapshot["time"])
    else:
        log.warning("No snapshot collected")


if __name__ == "__main__":
    main()
