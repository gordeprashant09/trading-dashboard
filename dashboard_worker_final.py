"""
dashboard_worker.py
===================
Reads trade data from ExecutionStrategySim log file (FTRD lines),
maps tokens to symbols via fo_contract_stream_info_<date>.csv,
fetches LTP from Redis DB2 (stock_realtime_feeder),
and publishes position data to Redis for the trading dashboard.

Inputs:
  1. Log file   : /home/report/devstudio/Prashant/Live_Dashboard/ExecutionStrategySim_simulator_1_<YYYYMMDD>.log
  2. Contract   : /home/report/devstudio/Prashant/Live_Dashboard/fo_contract_stream_info_<YYYYMMDD>.csv
  3. Redis DB2  : fo:stock_option:<SYM>:<TSYM>  → ltp field
                  fo:stock_spot:<SYM>            → spot field

Output:
  Redis key: dashboard:positions:latest  → JSON positions for trading_dashboard.py

Run:
    python dashboard_worker.py
"""

from __future__ import annotations

import os
import re
import time
import json
import logging
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import redis

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
BASE_DIR     = os.getenv("LIVE_DASHBOARD_DIR",
               "/home/report/devstudio/Prashant/Live_Dashboard")

# Redis for LTP (stock_realtime_feeder — DB 2)
LTP_REDIS_HOST = os.getenv("REDIS_HOST",     "localhost")
LTP_REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
LTP_REDIS_DB   = int(os.getenv("LTP_REDIS_DB", "2"))   # feeder uses DB 2

# Redis for dashboard output (DB 0 — same as before)
DASH_REDIS_HOST = os.getenv("REDIS_HOST",      "localhost")
DASH_REDIS_PORT = int(os.getenv("REDIS_PORT",  "6379"))
DASH_REDIS_DB   = int(os.getenv("REDIS_DB",    "0"))

DASH_REDIS_KEY  = "dashboard:positions:latest"

LOOP_SECONDS   = float(os.getenv("DASH_LOOP_SECONDS", "5.0"))
EXPENSE_PER_CR = float(os.getenv("EXPENSE_PER_CR",    "10000"))

# Price divisor — NSE FO prices in log are in paise (divide by 100)
PRICE_DIVISOR  = 100.0

# ══════════════════════════════════════════════════════════════
# LOT SIZE MAP — NSE F&O lot sizes (update when NSE revises)
# ══════════════════════════════════════════════════════════════
LOT_SIZE_MAP: dict[str, int] = {
    # Index
    "NIFTY":      75,    "BANKNIFTY":   35,
    "FINNIFTY":   65,    "MIDCPNIFTY":  75,
    "SENSEX":     10,    "BANKEX":      15,
    # Stocks
    "RELIANCE":   250,   "TCS":         175,
    "INFY":       400,   "HDFCBANK":    550,
    "ICICIBANK":  700,   "SBIN":        1500,
    "KOTAKBANK":  400,   "AXISBANK":    1200,
    "BHARTIARTL": 950,   "HEROMOTOCO":  100,
    "HAL":        150,   "BSE":         500,
    "M&M":        700,   "BAJFINANCE":  125,
    "TATAMOTORS": 1425,  "LT":          450,
    "ETERNAL":    2800,  "BEL":         4500,
    "MCX":        50,    "MARUTI":      45,
    "WIPRO":      1500,  "HINDUNILVR":  300,
    "ADANIENT":   625,   "ADANIPORTS":  625,
    "ULTRACEMCO": 100,   "TITAN":       375,
    "SUNPHARMA":  700,   "ONGC":        1925,
    "NTPC":       2925,  "POWERGRID":   2925,
    "COALINDIA":  1400,  "IDEA":        70000,
}

# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("dashboard_worker")


# ══════════════════════════════════════════════════════════════
# PATHS — resolved for today's date
# ══════════════════════════════════════════════════════════════

def today_str() -> str:
    return date.today().strftime("%Y%m%d")


def prev_trading_date() -> str:
    """Return previous trading day (skip weekends)."""
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:  # skip Sat/Sun
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def log_file_path(dt: str = None) -> str:
    dt = dt or today_str()
    return os.path.join(BASE_DIR,
        f"ExecutionStrategySim_simulator_1_{dt}.log")


def contract_file_path(dt: str = None) -> str:
    dt = dt or today_str()
    return os.path.join(BASE_DIR,
        f"fo_contract_stream_info_{dt}.csv")


# ══════════════════════════════════════════════════════════════
# CONTRACT CSV → TOKEN MAP
# Format per data line:
#   col0, col1, token(col2), col3, name(col4), expiry(col5), strike(col6), type(col7)
# First line is metadata (starts with a number) — skip it
# ══════════════════════════════════════════════════════════════

def load_token_map(dt: str = None) -> dict[int, dict]:
    """
    Returns { token(int): { "name": "NIFTY", "strike": 24500.0,
                             "type": "CE", "expiry": "1443709800",
                             "tsym": "NIFTY24500CE" } }
    """
    path = contract_file_path(dt)
    token_map: dict[int, dict] = {}

    if not os.path.exists(path):
        log.warning("Contract file not found: %s", path)
        return token_map

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            # Skip metadata/header lines (first char is digit or line has < 8 cols)
            if len(parts) < 8:
                continue
            if parts[0].strip().isdigit():
                continue  # metadata line like "1442426402,74320,"

            try:
                token    = int(parts[2].strip())
                name     = parts[4].strip().upper()
                expiry_ts = int(parts[5].strip())
                strike   = float(parts[6].strip()) / 100.0  # paise → rupees
                itype    = parts[7].strip().upper()   # CE or PE

                # Convert unix timestamp to expiry date
                # e.g. 1446129000 → 29MAY26
                exp_date = datetime.fromtimestamp(expiry_ts, tz=ZoneInfo("Asia/Kolkata"))
                exp_str  = exp_date.strftime("%d%b%y").upper()  # e.g. 29MAY26 → but feeder uses 26MAY
                # Feeder format: YYMMMDD e.g. 26MAY29 → actually DDMMMYY
                # From sample: INFY26MAY1200CE → format is YYMONSTRIKE
                exp_str  = exp_date.strftime("%y%b").upper()    # e.g. 26MAY

                # Strike as integer if whole number, else float
                strike_str = str(int(strike)) if strike == int(strike) else str(strike)

                # Build tsym matching feeder format: NAME+YYMON+STRIKE+TYPE
                # e.g. INFY26MAY1200CE
                tsym = f"{name}{exp_str}{strike_str}{itype}"

                token_map[token] = {
                    "name":   name,
                    "strike": strike,
                    "type":   itype,
                    "expiry": expiry_ts,
                    "tsym":   tsym,
                }
            except (ValueError, IndexError):
                continue

    log.info("Token map loaded: %d contracts from %s", len(token_map), path)
    return token_map


# ══════════════════════════════════════════════════════════════
# LOG FILE PARSER — extract FTRD (fill/trade) lines only
#
# FTRD header:
#   transactioncode, response_ordernumber, buy_sell,
#   originalvol, remaining_vol, price,
#   fillnumber, fillqty, fillprice, token
#
# buy_sell: 1 = Buy, 2 = Sell
# prices in log are * 100 (paise) → divide by PRICE_DIVISOR
# ══════════════════════════════════════════════════════════════

FTRD_RE = re.compile(
    r"FTRD:"
    r"(\d+),"           # transactioncode
    r"([\d.]+),"        # response_ordernumber
    r"(\d+),"           # buy_sell  (1=Buy, 2=Sell)
    r"(-?\d+),"         # originalvol
    r"(-?\d+),"         # remaining_vol
    r"(\d+),"           # price
    r"(\d+),"           # fillnumber
    r"(\d+),"           # fillqty
    r"(\d+),"           # fillprice
    r"(\d+)"            # token
)


def parse_ftrd_lines(log_path: str) -> list[dict]:
    """
    Parse all FTRD lines from the log file.
    Returns deduplicated list of fill dicts:
      { token, buy_sell, fillqty, fillprice, order_no, fillnumber }

    Deduplication key: (token, fillnumber)
    Same fill can appear multiple times in the log due to order modify
    replays (FOMT triggers re-broadcast of prior fills). We keep the
    LAST occurrence which reflects the most recent state.

    NOTE: FOMT (order modify) lines are intentionally NOT parsed here.
    Only FTRD (actual exchange fills) affect traded qty and avg price.
    A modify changes the pending order price/qty but does not change
    what has already been filled — so PnL is unaffected by FOMT.
    """
    seen: dict[tuple, dict] = {}   # (token, fillnumber) -> fill dict

    if not os.path.exists(log_path):
        log.warning("Log file not found: %s", log_path)
        return []

    with open(log_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = FTRD_RE.search(line)
            if not m:
                continue
            try:
                token      = int(m.group(10))
                fillnumber = int(m.group(7))
                seen[(token, fillnumber)] = {
                    "order_no":   m.group(2),
                    "buy_sell":   int(m.group(3)),   # 1=Buy 2=Sell
                    "fillqty":    int(m.group(8)),
                    "fillprice":  int(m.group(9)) / PRICE_DIVISOR,
                    "token":      token,
                    "fillnumber": fillnumber,
                }
            except (ValueError, IndexError):
                continue

    fills = list(seen.values())
    log.info("Parsed %d unique FTRD fills (deduped by token+fillnumber) from %s",
             len(fills), log_path)
    return fills


# ══════════════════════════════════════════════════════════════
# BUILD POSITIONS FROM FILLS + TOKEN MAP
# ══════════════════════════════════════════════════════════════

def build_positions_from_fills(
    fills: list[dict],
    token_map: dict[int, dict],
) -> dict[str, dict]:
    """
    Aggregate fills per token into position dict.
    Returns { token_str: { name, tsym, buy_qty, buy_val,
                           sell_qty, sell_val, lot_size } }
    """
    pos: dict[str, dict] = {}

    for fill in fills:
        token = fill["token"]
        info  = token_map.get(token)
        if not info:
            continue  # token not in contract file — skip

        key = str(token)
        if key not in pos:
            name     = info["name"]
            lot_size = LOT_SIZE_MAP.get(name, 1)  # from hardcoded NSE lot size map
            pos[key] = {
                "token":    token,
                "name":     name,
                "tsym":     info["tsym"],
                "strike":   info["strike"],
                "itype":    info["type"],
                "lot_size": lot_size,
                "buy_qty":  0.0,
                "buy_val":  0.0,
                "sell_qty": 0.0,
                "sell_val": 0.0,
            }

        qty   = fill["fillqty"]
        price = fill["fillprice"]

        if fill["buy_sell"] == 1:   # Buy
            pos[key]["buy_qty"] += qty
            pos[key]["buy_val"] += qty * price
        else:                        # Sell
            pos[key]["sell_qty"] += qty
            pos[key]["sell_val"] += qty * price

    # Compute avg prices
    for p in pos.values():
        p["buy_avg"]  = (p["buy_val"]  / p["buy_qty"])  if p["buy_qty"]  > 0 else 0.0
        p["sell_avg"] = (p["sell_val"] / p["sell_qty"]) if p["sell_qty"] > 0 else 0.0

    return pos


# ══════════════════════════════════════════════════════════════
# EOD LOADER — derive overnight positions from previous day log
# If prev log not found, uses DUMMY EOD data for testing
# ══════════════════════════════════════════════════════════════

def load_eod(token_map: dict) -> dict[int, dict]:
    """
    Load overnight positions (qty_overnight, prev_close) per token.

    Priority:
      1. Previous day log file — parse FTRD lines, compute net qty + last price
      2. DUMMY data — for testing when prev log not available

    Returns { token(int): { qty_overnight, prev_close } }
    """
    prev_dt   = prev_trading_date()
    prev_path = log_file_path(prev_dt)

    if os.path.exists(prev_path):
        log.info("Loading EOD from prev log: %s", prev_path)
        return _eod_from_log(prev_path)
    else:
        log.warning("Prev log not found (%s) — using DUMMY EOD data", prev_path)
        return _eod_dummy(token_map)


def _eod_from_log(log_path: str) -> dict[int, dict]:
    """
    Parse previous day log — compute net open qty and last fill price per token.
    net_qty    = total_buy_qty - total_sell_qty
    prev_close = last fillprice seen for that token
    """
    eod: dict[int, dict] = {}

    with open(log_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = FTRD_RE.search(line)
            if not m:
                continue
            try:
                token     = int(m.group(10))
                buy_sell  = int(m.group(3))
                fillqty   = int(m.group(8))
                fillprice = int(m.group(9)) / PRICE_DIVISOR

                if token not in eod:
                    eod[token] = {"net_qty": 0.0, "last_price": 0.0}

                if buy_sell == 1:
                    eod[token]["net_qty"] += fillqty
                else:
                    eod[token]["net_qty"] -= fillqty

                eod[token]["last_price"] = fillprice  # keep updating → last price
            except (ValueError, IndexError):
                continue

    result = {
        token: {
            "qty_overnight": v["net_qty"],
            "prev_close":    v["last_price"],
        }
        for token, v in eod.items()
    }
    log.info("EOD loaded from log: %d tokens", len(result))
    return result


def _eod_dummy(token_map: dict) -> dict[int, dict]:
    """
    DUMMY EOD — for testing when no previous log available.
    Takes first 5 tokens from today's contract map and assigns
    sample overnight positions.
    """
    dummy_positions = [
        {"qty_overnight":  75,   "prev_close": 0.0},   # long 1 lot
        {"qty_overnight": -150,  "prev_close": 0.0},   # short 2 lots
        {"qty_overnight":  225,  "prev_close": 0.0},   # long 3 lots
        {"qty_overnight": -75,   "prev_close": 0.0},   # short 1 lot
        {"qty_overnight":  300,  "prev_close": 0.0},   # long 4 lots
    ]
    result = {}
    tokens = list(token_map.keys())[:5]
    for i, token in enumerate(tokens):
        d = dummy_positions[i % len(dummy_positions)].copy()
        # Use a rough price based on strike as prev_close proxy
        strike = token_map[token].get("strike", 0)
        d["prev_close"] = (strike / 100.0) * 0.98   # 2% below strike as dummy
        result[token] = d

    log.info("DUMMY EOD loaded: %d tokens", len(result))
    return result


# ══════════════════════════════════════════════════════════════
# REDIS — LTP from stock_realtime_feeder (DB 2)
# Key structure:
#   fo:stock_option:<SYM>:<TSYM>  → hash field: ltp
#   fo:stock_spot:<SYM>           → hash field: spot  (fallback)
# ══════════════════════════════════════════════════════════════

def get_ltp_redis() -> redis.Redis:
    return redis.Redis(
        host=LTP_REDIS_HOST, port=LTP_REDIS_PORT, db=LTP_REDIS_DB,
        decode_responses=True, socket_timeout=2.0
    )


def get_ltp_map(r_ltp: redis.Redis, positions: dict) -> dict[str, float]:
    """
    For each position, try:
      1. HGET fo:stock_option:<SYM>:<TSYM>  ltp
      2. HGET fo:stock_spot:<SYM>           spot   (fallback)
    Returns { token_str: ltp_float }
    """
    ltp_map: dict[str, float] = {}

    for key, p in positions.items():
        sym  = p["name"]
        tsym = p["tsym"]
        ltp  = None

        # Try exact option key first
        try:
            val = r_ltp.hget(f"fo:stock_option:{sym}:{tsym}", "ltp")
            if val:
                ltp = float(val)
        except Exception:
            pass

        # Fallback: spot price
        if not ltp:
            try:
                val = r_ltp.hget(f"fo:stock_spot:{sym}", "ltp")
                if val:
                    ltp = float(val)
            except Exception:
                pass

        if ltp:
            ltp_map[key] = ltp
        else:
            # Last resort: use fill price as proxy
            ltp_map[key] = p["buy_avg"] or p["sell_avg"] or 0.0

    return ltp_map


# ══════════════════════════════════════════════════════════════
# PNL ENGINE (same logic as before)
# ══════════════════════════════════════════════════════════════

def calc_pnl(
    qty_overnight: float,
    prev_close: float,
    qty_today_buy: float,
    qty_today_sell: float,
    buy_avg: float,
    sell_avg: float,
    ltp: float,
) -> dict:
    carry    = qty_overnight * (ltp - prev_close)

    day_buy  = qty_today_buy  * (ltp - buy_avg)   if qty_today_buy  > 0 else 0.0
    day_sell = qty_today_sell * (sell_avg - ltp)   if qty_today_sell > 0 else 0.0
    day      = day_buy + day_sell

    traded_val = (qty_today_buy  * (buy_avg  or ltp)) + \
                 (qty_today_sell * (sell_avg or ltp))
    expenses   = (traded_val / 1e7) * EXPENSE_PER_CR

    open_qty   = qty_overnight + (qty_today_buy - qty_today_sell)
    net_exp    = open_qty * ltp
    net        = carry + day - expenses

    return {
        "open_qty":   open_qty,
        "net_exp":    net_exp,
        "traded_val": traded_val,
        "carry":      carry,
        "day":        day,
        "net":        net,
    }


# ══════════════════════════════════════════════════════════════
# GROUP BY STOCK → dashboard DATA format
# ══════════════════════════════════════════════════════════════

def group_by_stock(positions: dict, ltp_map: dict, eod_map: dict) -> list[dict]:
    """
    Returns dashboard DATA format:
    [
      { sym, lot_size, book, expiries: [ {label, qty_overnight, ...}, ... ] },
      ...
    ]
    eod_map: { token(int): { qty_overnight, prev_close } }
    """
    stock_map: dict[str, dict] = {}

    for key, p in positions.items():
        sym      = p["name"]
        lot_size = p["lot_size"]
        ltp      = ltp_map.get(key, 0.0)
        token    = p["token"]

        # EOD overnight data
        eod           = eod_map.get(token, {})
        qty_overnight = eod.get("qty_overnight", 0.0)
        prev_close    = eod.get("prev_close",    0.0)

        if sym not in stock_map:
            stock_map[sym] = {
                "sym":      sym,
                "book":     "prop",
                "lot_size": lot_size,
                "expiries": [],
            }

        stock_map[sym]["expiries"].append({
            "label":           p["tsym"],
            "qty_overnight":   qty_overnight,
            "prev_close":      prev_close,
            "qty_today_buy":   p["buy_qty"],
            "qty_today_sell":  p["sell_qty"],
            "buy_avg":         p["buy_avg"],
            "sell_avg":        p["sell_avg"],
            "ltp":             ltp,
            "mtd":             0.0,
        })

    return list(stock_map.values())


# ══════════════════════════════════════════════════════════════
# DASHBOARD REDIS OUTPUT (DB 0)
# ══════════════════════════════════════════════════════════════

def dash_redis_client() -> redis.Redis:
    return redis.Redis(
        host=DASH_REDIS_HOST, port=DASH_REDIS_PORT, db=DASH_REDIS_DB,
        decode_responses=True, socket_timeout=2.0
    )


# ══════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("  dashboard_worker starting")
    log.info("  Log dir  : %s", BASE_DIR)
    log.info("  LTP Redis: %s:%d db=%d", LTP_REDIS_HOST, LTP_REDIS_PORT, LTP_REDIS_DB)
    log.info("  Out Redis: %s:%d db=%d", DASH_REDIS_HOST, DASH_REDIS_PORT, DASH_REDIS_DB)
    log.info("=" * 60)

    r_dash = dash_redis_client()
    try:
        r_dash.ping()
        log.info("Dashboard Redis connected (DB %d)", DASH_REDIS_DB)
    except Exception as e:
        raise RuntimeError(f"Dashboard Redis not reachable: {e}")

    r_ltp = get_ltp_redis()
    try:
        r_ltp.ping()
        log.info("LTP Redis connected (DB %d)", LTP_REDIS_DB)
    except Exception as e:
        log.warning("LTP Redis not reachable: %s — will use fill price as proxy", e)

    # Cache token map — reload on new day
    current_date = today_str()
    token_map    = load_token_map(current_date)

    while True:
        tick_start = time.time()

        try:
            # Reload token map on new day
            dt = today_str()
            if dt != current_date:
                log.info("New day — reloading contract file for %s", dt)
                token_map    = load_token_map(dt)
                current_date = dt

            # 1. Parse FTRD fills from log
            fills = parse_ftrd_lines(log_file_path(dt))

            if not fills:
                log.warning("No FTRD fills found in log — publishing empty positions")
                data    = []
                payload = json.dumps({
                    "as_of":     datetime.now().isoformat(timespec="seconds"),
                    "positions": data,
                    "source":    "log_file",
                })
                r_dash.set(DASH_REDIS_KEY, payload)
                time.sleep(LOOP_SECONDS)
                continue

            # 2. Build per-token positions
            positions = build_positions_from_fills(fills, token_map)
            log.info("Positions built: %d tokens", len(positions))

            # 3. Get LTP from Redis DB2
            ltp_map = get_ltp_map(r_ltp, positions)

            # 4. Load EOD (prev day log or dummy)
            eod_map = load_eod(token_map)

            # 5. Group by stock for dashboard format
            data = group_by_stock(positions, ltp_map, eod_map)
            log.info("Stocks grouped: %d underlyings", len(data))

            # 6. Publish to Redis
            payload = json.dumps({
                "as_of":     datetime.now().isoformat(timespec="seconds"),
                "positions": data,
                "source":    "log_file",
            }, ensure_ascii=False)

            r_dash.set(DASH_REDIS_KEY, payload)
            log.info("Published %d stocks to Redis key=%s", len(data), DASH_REDIS_KEY)

        except Exception as e:
            import traceback
            log.error("tick failed: %s\n%s", e, traceback.format_exc())

        elapsed = time.time() - tick_start
        time.sleep(max(0.1, LOOP_SECONDS - elapsed))


if __name__ == "__main__":
    main()
