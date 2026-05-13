"""
dashboard_worker.py
===================
Reads trade data from Sample-Strategy log file (FTRD lines) via SSH,
maps tokens to symbols via fo_contract_stream_info_<date>.csv (local),
fetches LTP from Redis DB2 (stock_realtime_feeder),
and publishes position data to Redis for the trading dashboard.

Inputs:
  1. Log file   : [SSH] Data_colo@192.168.74.138:/data/logs/Sample-Strategy-excution_algo_1_<YYYYMMDD>.log
  2. Contract   : /home/report/devstudio/Prashant/Live_Dashboard/fo_contract_stream_info_<YYYYMMDD>.csv
  3. Redis DB2  : fo:stock_option:<SYM>:<TSYM>  → ltp field
                  fo:stock_spot:<SYM>            → spot field

Output:
  Redis key: dashboard:positions:latest  → JSON positions for trading_dashboard.py

Run:
    pip install paramiko
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
import paramiko

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
# ── SSH config for remote log server ──────────────────────────
SSH_HOST = os.getenv("SSH_HOST", "192.168.74.138")
SSH_PORT = int(os.getenv("SSH_PORT", "22"))
SSH_USER = os.getenv("SSH_USER", "Data_colo")
SSH_PASS = os.getenv("SSH_PASS", "Datacolo@2026")
REMOTE_LOG_DIR       = os.getenv("REMOTE_LOG_DIR",       "/data/logs")
REMOTE_DASHBOARD_DIR = os.getenv("REMOTE_DASHBOARD_DIR", "/data/Dashboard")
REMOTE_PCAP_DIR      = os.getenv("REMOTE_PCAP_DIR",      "/data/pcapdata")

# Local paths (contract CSV, stocks.csv — unchanged)
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

# stocks.csv — same file used by stock_realtime_feeder
# contains symbol, lot_size, strike_step
STOCKS_CSV = os.getenv("STOCKS_CSV",
    "/home/report/devstudio/Prashant/Stock/stocks.csv")

LOOP_SECONDS   = float(os.getenv("DASH_LOOP_SECONDS", "5.0"))
EXPENSE_PER_CR = float(os.getenv("EXPENSE_PER_CR",    "10000"))

# Price divisor — NSE FO prices in log are in paise (divide by 100)
PRICE_DIVISOR  = 100.0

# ══════════════════════════════════════════════════════════════
# LOT SIZE FALLBACK — used only if Redis DB2 has no lot_size
# for a symbol. Keep minimal — Redis is the primary source.
# ══════════════════════════════════════════════════════════════
LOT_SIZE_FALLBACK: dict[str, int] = {
    "NIFTY": 75, "BANKNIFTY": 35, "SENSEX": 10,
}

# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("dashboard_worker")
logging.getLogger("paramiko").setLevel(logging.WARNING)


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
    """
    Auto-detect the latest Sample-Strategy-excution_algo_1_*.log on remote server.
    Falls back to today date filename if detection fails.
    """
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(SSH_HOST, port=SSH_PORT,
                       username=SSH_USER, password=SSH_PASS,
                       timeout=10)
        _, stdout, _ = client.exec_command(
            f"ls -t {REMOTE_LOG_DIR}/Sample-Strategy-excution_algo_1_*.log 2>/dev/null | head -1"
        )
        path = stdout.read().decode().strip()
        client.close()
        if path:
            log.info("Auto-detected latest log: %s", path)
            return path
    except Exception as e:
        log.warning("Could not auto-detect log file: %s", e)

    # fallback
    dt = dt or today_str()
    return f"{REMOTE_LOG_DIR}/Sample-Strategy-excution_algo_1_{dt}.log"


# Remote contract base dir
REMOTE_PCAP_DIR = os.getenv("REMOTE_PCAP_DIR", "/data/pcapdata")


def get_ssh_client() -> paramiko.SSHClient:
    """Return a connected SSH client."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, port=SSH_PORT,
                   username=SSH_USER, password=SSH_PASS, timeout=10)
    return client


def extract_date_from_log_path(log_path: str) -> str:
    """Extract date string (YYYYMMDD) from log filename."""
    m = re.search(r"(\d{8})", os.path.basename(log_path))
    return m.group(1) if m else today_str()


def contract_file_path(log_path: str = None) -> str:
    """
    Return remote contract CSV path for the same date as the log file.
    Logic:
      1. Extract date from log filename (e.g. 20260509)
      2. Look for /data/pcapdata/{dt}/fo_contract_stream_info_{dt}.csv
      3. If not found, use latest available date folder in /data/pcapdata/
    """
    dt = extract_date_from_log_path(log_path) if log_path else today_str()
    primary = f"{REMOTE_PCAP_DIR}/{dt}/fo_contract_stream_info_{dt}.csv"

    try:
        client = get_ssh_client()
        sftp = client.open_sftp()
        try:
            sftp.stat(primary)
            log.info("Contract file found for date %s: %s", dt, primary)
            sftp.close()
            client.close()
            return primary
        except FileNotFoundError:
            log.warning("Contract file not found for date %s — searching latest", dt)

        # Find latest available date folder
        _, stdout, _ = client.exec_command(
            f"ls -t {REMOTE_PCAP_DIR}/*/fo_contract_stream_info_*.csv 2>/dev/null | head -1"
        )
        latest = stdout.read().decode().strip()
        sftp.close()
        client.close()

        if latest:
            log.info("Using latest available contract file: %s", latest)
            return latest
    except Exception as e:
        log.error("Error finding contract file: %s", e)

    # hard fallback
    return primary


# ══════════════════════════════════════════════════════════════
# CONTRACT CSV → TOKEN MAP
# Format per data line:
#   col0, col1, token(col2), col3, name(col4), expiry(col5), strike(col6), type(col7)
# First line is metadata (starts with a number) — skip it
# ══════════════════════════════════════════════════════════════

def load_token_map(log_path: str = None) -> dict[int, dict]:
    """
    Load contract token map from remote SSH server.
    Picks contract file matching the log file date, or latest available.
    Returns { token(int): { "name": "NIFTY", "strike": 24500.0,
                             "type": "CE", "expiry": "1443709800",
                             "tsym": "NIFTY24500CE" } }
    """
    path = contract_file_path(log_path)
    token_map: dict[int, dict] = {}

    lines = read_remote_file_lines(path)
    if not lines:
        log.warning("Contract file empty or not found: %s", path)
        return token_map

    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) < 8:
            continue
        if parts[0].strip().isdigit():
            continue  # metadata line

        try:
            token     = int(parts[2].strip())
            inst_type = parts[3].strip().upper()   # OPTSTK, OPTIDX, FUTSTK, FUTIDX
            name      = parts[4].strip().upper()
            expiry_ts = int(parts[5].strip())
            strike    = float(parts[6].strip()) / 100.0  # paise → rupees
            itype     = parts[7].strip().upper()         # CE, PE, or XX (futures)

            # NSE uses internal epoch for timestamps starting with 14xxxxxxxx
            # Offset = 315513000 seconds (315513000000000000 nanoseconds)
            NSE_OFFSET = 315513000
            adj_ts = expiry_ts + NSE_OFFSET if str(expiry_ts).startswith("14") else expiry_ts
            exp_date = datetime.fromtimestamp(adj_ts, tz=ZoneInfo("Asia/Kolkata"))
            exp_str  = exp_date.strftime("%y%b").upper()   # e.g. 26MAY

            # Build tsym based on instrument type
            is_future = inst_type in ("FUTSTK", "FUTIDX") or itype == "XX"
            if is_future:
                # Futures: NAME + YYMON + FUT  e.g. BSE26MAYFUT, NIFTY26MAYFUT
                tsym = f"{name}{exp_str}FUT"
            else:
                # Options: NAME + YYMON + STRIKE + CE/PE  e.g. ICICIBANK26MAY1340CE
                strike_str = str(int(strike)) if strike == int(strike) else str(strike)
                tsym = f"{name}{exp_str}{strike_str}{itype}"

            token_map[token] = {
                "name":      name,
                "strike":    strike,
                "type":      itype,
                "inst_type": inst_type,
                "is_future": is_future,
                "expiry":    expiry_ts,
                "tsym":      tsym,
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

# ══════════════════════════════════════════════════════════════
# SSH HELPER — read remote file lines via paramiko
# ══════════════════════════════════════════════════════════════

def read_remote_file_lines(remote_path: str) -> list[str]:
    """
    Connect to remote SSH server and return lines of a file.
    Returns empty list if file not found or connection fails.
    """
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(SSH_HOST, port=SSH_PORT,
                       username=SSH_USER, password=SSH_PASS,
                       timeout=10)
        sftp = client.open_sftp()
        try:
            with sftp.open(remote_path, "r") as f:
                lines = f.read().decode("utf-8", errors="replace").splitlines()
            log.info("SSH read %d lines from %s:%s", len(lines), SSH_HOST, remote_path)
            return lines
        except FileNotFoundError:
            log.warning("Remote file not found: %s:%s", SSH_HOST, remote_path)
            return []
        finally:
            sftp.close()
            client.close()
    except Exception as e:
        log.error("SSH read failed for %s:%s — %s", SSH_HOST, remote_path, e)
        return []


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
    Parse all FTRD lines from the remote log file via SSH.
    Returns deduplicated list of fill dicts.
    Deduplication key: (token, fillnumber)
    """
    seen: dict[tuple, dict] = {}

    lines = read_remote_file_lines(log_path)
    if not lines:
        log.warning("No lines read from remote log: %s", log_path)
        return []

    for line in lines:
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
    lot_size_map: dict[str, int] = None,
) -> dict[str, dict]:
    """
    Aggregate fills per token into position dict.
    Returns { token_str: { name, tsym, buy_qty, buy_val,
                           sell_qty, sell_val, lot_size } }
    lot_size_map: from Redis DB2 via load_lot_sizes_from_redis()
    """
    if lot_size_map is None:
        lot_size_map = {}

    pos: dict[str, dict] = {}

    for fill in fills:
        token = fill["token"]
        info  = token_map.get(token)
        if not info:
            continue  # token not in contract file — skip

        key = str(token)
        if key not in pos:
            name     = info["name"]
            lot_size = lot_size_map.get(name) or LOT_SIZE_FALLBACK.get(name, 1)
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

EOD_CSV_PATH = f"{REMOTE_DASHBOARD_DIR}/eod_positions.csv"


def load_eod(token_map: dict) -> dict[int, dict]:
    """
    Load overnight positions (qty_overnight, prev_close) per token.

    Priority:
      1. /data/Dashboard/eod_positions.csv  — generated by generate_eod.py at EOD
      2. Zero overnight positions (pure intraday) if CSV not found
    """
    result = _eod_from_csv()
    if result:
        return result

    log.warning("EOD CSV not found or empty — using zero overnight positions")
    return {}


def _eod_from_csv() -> dict[int, dict]:
    """
    Read eod_positions.csv from remote SSH server.
    Returns { token(int): { qty_overnight, prev_close } }
    """
    import csv as _csv
    lines = read_remote_file_lines(EOD_CSV_PATH)
    if not lines:
        return {}

    result = {}
    reader = _csv.DictReader(lines)
    for row in reader:
        try:
            token         = int(row["token"])
            qty_overnight = float(row["qty_overnight"])
            prev_close    = float(row["prev_close"])
            result[token] = {
                "qty_overnight": qty_overnight,
                "prev_close":    prev_close,
            }
        except (KeyError, ValueError):
            continue

    log.info("EOD loaded from CSV: %d tokens from %s", len(result), EOD_CSV_PATH)
    return result


def _eod_from_log(log_path: str) -> dict[int, dict]:
    """
    Parse previous day remote log — compute net open qty and last fill price per token.
    net_qty    = total_buy_qty - total_sell_qty
    prev_close = last fillprice seen for that token
    """
    eod: dict[int, dict] = {}

    lines = read_remote_file_lines(log_path)
    if not lines:
        return eod

    for line in lines:
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

            eod[token]["last_price"] = fillprice
        except (ValueError, IndexError):
            continue

    result = {
        token: {
            "qty_overnight": v["net_qty"],
            "prev_close":    v["last_price"],
        }
        for token, v in eod.items()
    }
    log.info("EOD loaded from remote log: %d tokens", len(result))
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


def load_lot_sizes_from_redis() -> dict[str, int]:
    """
    Read lot sizes from Redis DB2 fo:stock_spot:<SYM> hash field lot_size.
    Falls back to LOT_SIZE_FALLBACK if Redis not available or symbol missing.
    """
    result = {}
    try:
        r = redis.Redis(host=LTP_REDIS_HOST, port=LTP_REDIS_PORT,
                        db=LTP_REDIS_DB, decode_responses=True, socket_timeout=2.0)
        # Scan all fo:stock_spot:* keys
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="fo:stock_spot:*", count=200)
            for key in keys:
                sym = key.split(":")[-1].upper()
                val = r.hget(key, "lot_size")
                if val:
                    try:
                        result[sym] = int(float(val))
                    except ValueError:
                        pass
            if cursor == 0:
                break
        log.info("Lot sizes loaded from Redis DB%d: %d symbols", LTP_REDIS_DB, len(result))
    except Exception as e:
        log.warning("Redis lot size fetch failed: %s — using fallback", e)

    # merge fallback for any missing
    for sym, lot in LOT_SIZE_FALLBACK.items():
        if sym not in result:
            result[sym] = lot

    return result


# Keep old name as alias for backward compatibility
def load_lot_sizes_from_csv() -> dict[str, int]:
    return load_lot_sizes_from_redis()


# Index names covered by fo_realtime_feeder (DB 0)
FEEDER_INDICES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"}


def get_ltp_map(r_ltp: redis.Redis, positions: dict) -> dict[str, float]:
    """
    Fetch LTP for each position from Redis.

    Priority per position:
      For INDEX options  (NIFTY, BANKNIFTY etc):
        1. HGET fo:index_option:<IDX>:<TSYM>  ltp   ← feeder DB0
        2. HGET fo:index_spot:<IDX>           ltp   ← feeder spot fallback

      For INDEX futures:
        1. HGET fo:index_futures:<IDX>        ltp   ← feeder DB0
        2. HGET fo:index_spot:<IDX>           ltp   ← feeder spot fallback

      For STOCK options/futures (BSE, ICICIBANK etc):
        1. HGET fo:stock_option:<SYM>:<TSYM>  ltp   ← old feeder key
        2. HGET fo:stock_spot:<SYM>           ltp   ← old feeder key
        3. fill price proxy (buy_avg or sell_avg)

    Returns { token_str: ltp_float }
    """
    ltp_map: dict[str, float] = {}

    for key, p in positions.items():
        sym       = p["name"]
        tsym      = p["tsym"]
        is_future = p.get("is_future", False)
        ltp       = None

        if sym in FEEDER_INDICES:
            # ── Index instrument — use fo_realtime_feeder keys (DB 0) ─────────
            if is_future:
                # fo:index_futures:<IDX>  → ltp field
                try:
                    val = r_ltp.hget(f"fo:index_futures:{sym}", "ltp")
                    if val:
                        ltp = float(val)
                except Exception:
                    pass
            else:
                # fo:index_option:<IDX>:<TSYM>  → ltp field
                try:
                    val = r_ltp.hget(f"fo:index_option:{sym}:{tsym}", "ltp")
                    if val:
                        ltp = float(val)
                except Exception:
                    pass

            # Fallback: index spot
            if not ltp:
                try:
                    val = r_ltp.hget(f"fo:index_spot:{sym}", "ltp")
                    if val:
                        ltp = float(val)
                except Exception:
                    pass
        else:
            # ── Stock instrument — use old feeder keys ────────────────────────
            try:
                val = r_ltp.hget(f"fo:stock_option:{sym}:{tsym}", "ltp")
                if val:
                    ltp = float(val)
            except Exception:
                pass

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
    current_log  = log_file_path()
    token_map    = load_token_map(current_log)

    while True:
        tick_start = time.time()

        try:
            # Detect latest log — reload token map if log file changes
            latest_log = log_file_path()
            if latest_log != current_log:
                log.info("New log file detected — reloading contract map: %s", latest_log)
                token_map   = load_token_map(latest_log)
                current_log = latest_log

            # 1. Parse FTRD fills from log
            fills = parse_ftrd_lines(latest_log)

            if not fills:
                log.warning("No FTRD fills found in log — publishing empty positions")
                data    = []
                payload = json.dumps({
                    "as_of":     datetime.now().isoformat(timespec="seconds"),
                    "log_date":  latest_log,
                    "positions": data,
                    "source":    "log_file",
                })
                r_dash.set(DASH_REDIS_KEY, payload)
                time.sleep(LOOP_SECONDS)
                continue

            # 2. Get lot sizes from stocks.csv
            lot_size_map = load_lot_sizes_from_csv()

            # 3. Build per-token positions
            positions = build_positions_from_fills(fills, token_map, lot_size_map)
            log.info("Positions built: %d tokens", len(positions))

            # 4. Get LTP from Redis DB2
            ltp_map = get_ltp_map(r_ltp, positions)

            # 5. Load EOD (prev day log or dummy)
            eod_map = load_eod(token_map)

            # 6. Group by stock for dashboard format
            data = group_by_stock(positions, ltp_map, eod_map)
            log.info("Stocks grouped: %d underlyings", len(data))

            # 7. Publish to Redis
            # Extract date from log filename e.g. 20260509
            import re as _re
            _m = _re.search(r"(\d{8})", os.path.basename(latest_log))
            _log_date_str = _m.group(1) if _m else ""
            try:
                _log_date_fmt = datetime.strptime(_log_date_str, "%Y%m%d").strftime("%Y-%m-%d")
            except Exception:
                _log_date_fmt = _log_date_str

            payload = json.dumps({
                "as_of":     datetime.now().isoformat(timespec="seconds"),
                "log_date":  _log_date_fmt,
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
