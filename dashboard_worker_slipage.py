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
import csv
import io
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

# ── Day-end snapshot ───────────────────────────────────────────
# Snapshot saved once per day when IST time crosses EOD_SNAPSHOT_TIME.
# Filename: dashboard_snapshot_<YYYYMMDD>.csv  (date comes from log filename)
# Saved to REMOTE_DASHBOARD_DIR/snapshots/ on the colo server via SFTP.
EOD_SNAPSHOT_TIME = os.getenv("EOD_SNAPSHOT_TIME", "15:30")   # HH:MM IST
SNAPSHOT_SUBDIR   = os.getenv("SNAPSHOT_SUBDIR",   "snapshots")  # under REMOTE_DASHBOARD_DIR

# stocks.csv — same file used by stock_realtime_feeder
# contains symbol, lot_size, strike_step
STOCKS_CSV = os.getenv("STOCKS_CSV",
    "/home/report/devstudio/Prashant/Stock/stocks.csv")

LOOP_SECONDS   = float(os.getenv("DASH_LOOP_SECONDS", "5.0"))
EXPENSE_PER_CR = float(os.getenv("EXPENSE_PER_CR",    "1906"))

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
    Auto-detect the latest Sample-Strategy_algo_1_*.log on remote server.
    Falls back to today date filename if detection fails.
    """
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(SSH_HOST, port=SSH_PORT,
                       username=SSH_USER, password=SSH_PASS,
                       timeout=10)
        _, stdout, _ = client.exec_command(
            f"ls -t {REMOTE_LOG_DIR}/*algo_1_*.log 2>/dev/null | head -1"
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
    return f"{REMOTE_LOG_DIR}/Sample-Strategy_algo_1_{dt}.log"


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


# Regex to extract timestamp from FTRD fill line
# Format: ...FTRD:...,2026-05-18 09:20:00.230661519
FTRD_TS_RE = re.compile(r"emit_trade_fill::FTRD:.*?,(\d{4}-\d{2}-\d{2})\s")


def extract_date_from_fills(log_path: str) -> str:
    """
    Extract actual trade date from the first FTRD fill timestamp in the log.
    Fill lines contain: ...2026-05-18 09:20:00.230661519
    This is more reliable than the log filename which may have an old date.

    Falls back to filename date if no fill timestamp found.
    """
    try:
        client = get_ssh_client()
        # Read LAST fill line to get most recent trade date
        _, stdout, _ = client.exec_command(
            f"grep 'emit_trade_fill::FTRD' {log_path} | tail -1"
        )
        last_fill = stdout.read().decode().strip()
        client.close()

        if last_fill:
            m = FTRD_TS_RE.search(last_fill)
            if m:
                # Convert 2026-05-18 → 20260518
                dt = datetime.strptime(m.group(1), "%Y-%m-%d").strftime("%Y%m%d")
                log.info("Trade date from fill timestamp: %s", dt)
                return dt
    except Exception as e:
        log.warning("Could not extract date from fill timestamp: %s", e)

    # Fallback to filename date
    dt = extract_date_from_log_path(log_path)
    log.info("Trade date from log filename (fallback): %s", dt)
    return dt


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
    Parse FTRD lines from the remote log file via SSH.
    
    Step 1: Find latest fill date using tail (fast — avoids reading 1.7GB file)
    Step 2: Grep only fills matching that date (fast — server-side filter)
    
    Returns deduplicated list of fill dicts.
    Deduplication key: (token, fillnumber)
    """
    seen: dict[tuple, dict] = {}

    try:
        # Step 1: Get latest fill date using tac with server-side timeout
        # tac reads from end — finds last fill instantly without reading 1.7GB
        client = get_ssh_client()
        _, stdout, _ = client.exec_command(
            f"timeout 10 tac {log_path} | grep -m1 'emit_trade_fill::FTRD'"
        )
        last_line = stdout.read().decode().strip()
        client.close()

        latest_date = None
        if last_line:
            m = FTRD_TS_RE.search(last_line)
            if m:
                latest_date = m.group(1)  # e.g. 2026-05-18
                log.info("Latest fill date detected: %s", latest_date)

        if not latest_date:
            log.warning("Could not detect latest fill date — processing all fills")

        # Step 2: Grep only today's fills server-side
        client = get_ssh_client()
        if latest_date:
            cmd = f"grep 'emit_trade_fill::FTRD' {log_path} | grep '{latest_date}'"
        else:
            cmd = f"grep 'emit_trade_fill::FTRD' {log_path}"

        _, stdout, _ = client.exec_command(cmd)
        lines = stdout.read().decode("utf-8", errors="replace").splitlines()
        client.close()

        log.info("SSH grep returned %d fill lines for date %s", len(lines), latest_date)

    except Exception as e:
        log.error("SSH fill fetch failed: %s", e)
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
    log.info("Parsed %d unique FTRD fills for date %s (deduped by token+fillnumber)",
             len(fills), latest_date or "all")
    return fills


# ══════════════════════════════════════════════════════════════
# BUILD POSITIONS FROM FILLS + TOKEN MAP
# ══════════════════════════════════════════════════════════════

def build_positions_from_fills(
    fills: list[dict],
    token_map: dict[int, dict],
    lot_size_map: dict[str, int] = None,
    eod_map: dict = None,
) -> dict[str, dict]:
    """
    Aggregate fills per token into position dict.
    Also merges EOD-only tokens (overnight positions with no fills today)
    so they are always visible on dashboard even without intraday activity.
    Returns { token_str: { name, tsym, buy_qty, buy_val,
                           sell_qty, sell_val, lot_size } }
    lot_size_map: from Redis DB2 via load_lot_sizes_from_redis()
    eod_map: overnight positions — tokens in EOD but not in fills are added
    """
    if lot_size_map is None:
        lot_size_map = {}
    if eod_map is None:
        eod_map = {}

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

    # ── Merge EOD-only tokens (overnight positions with no fills today) ──
    # Critical: without this, positions held overnight but not traded today
    # are invisible on the dashboard — dangerous for risk monitoring
    for token, eod in eod_map.items():
        key = str(token)
        if key in pos:
            continue  # already has fills today — skip
        info = token_map.get(token)
        if not info:
            continue
        name     = info["name"]
        lot_size = lot_size_map.get(name) or LOT_SIZE_FALLBACK.get(name, 1)
        pos[key] = {
            "token":    token,
            "name":     name,
            "tsym":     info["tsym"],
            "strike":   info.get("strike", 0),
            "itype":    info.get("type", ""),
            "lot_size": lot_size,
            "buy_qty":  0.0,
            "buy_val":  0.0,
            "buy_avg":  0.0,
            "sell_qty": 0.0,
            "sell_val": 0.0,
            "sell_avg": 0.0,
        }
        log.debug("EOD-only token added: %s %s", name, info["tsym"])

    return pos


# ══════════════════════════════════════════════════════════════
# EOD LOADER — derive overnight positions from previous day log
# If prev log not found, uses DUMMY EOD data for testing
# ══════════════════════════════════════════════════════════════

EOD_SUBDIR   = "Eod"   # subfolder under REMOTE_DASHBOARD_DIR — matches generate_eod.py


def eod_csv_path(dt: str) -> str:
    """
    Returns the dated EOD CSV path on colo for a given date string (YYYYMMDD).
    e.g. /data/Dashboard/Eod/eod_positions_20260514.csv
    Mirrors eod_output_path() in generate_eod.py.
    """
    return f"{REMOTE_DASHBOARD_DIR}/{EOD_SUBDIR}/eod_positions_{dt}.csv"


def load_eod(token_map: dict, log_path: str = "") -> dict[int, dict]:
    """
    Load overnight positions (qty_overnight, prev_close) per token.

    Priority:
      1. /data/Dashboard/Eod/eod_positions_YYYYMMDD.csv  — generated by generate_eod.py
         Date is derived from the current log filename (same trading day).
         Falls back to previous trading day if today's file not found yet.
      2. Zero overnight positions (pure intraday) if no CSV found.
    """
    # Derive actual trade date from fill timestamps in log (not filename)
    # e.g. fill timestamp 2026-05-18 09:20:00 → 20260518
    log_dt = ""
    if log_path:
        log_dt = extract_date_from_fills(log_path)

    # Try today's log date first, then previous trading day as fallback
    dates_to_try = []
    if log_dt:
        dates_to_try.append(log_dt)
    # Previous trading day fallback
    dates_to_try.append(prev_trading_date())

    for dt in dates_to_try:
        result = _eod_from_csv(dt)
        if result:
            return result

    log.warning("EOD CSV not found for any date tried %s — using zero overnight positions",
                dates_to_try)
    return {}


def _eod_from_csv(dt: str) -> dict[int, dict]:
    """
    Read dated eod_positions_YYYYMMDD.csv from remote SSH server.
    Returns { token(int): { qty_overnight, prev_close } }
    """
    import csv as _csv
    path  = eod_csv_path(dt)
    lines = read_remote_file_lines(path)
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

    log.info("EOD loaded from CSV: %d tokens from %s", len(result), path)
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

    buy_val    = qty_today_buy  * (buy_avg  or ltp)
    sell_val   = qty_today_sell * (sell_avg or ltp)
    traded_val = buy_val + sell_val

    # Cost split: buy side ₹1018/Cr, sell side ₹5818/Cr
    buy_cost   = (buy_val  / 1e7) * 1018
    sell_cost  = (sell_val / 1e7) * 5818
    expenses   = buy_cost + sell_cost

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

def build_positions_from_eod(eod_map: dict, token_map: dict, lot_size_map: dict) -> dict:
    """
    Build positions dict from EOD overnight data only (no intraday fills).
    Used when no FTRD fills found in log (pre-market / no-trade day).
    """
    positions = {}
    for token, eod in eod_map.items():
        meta = token_map.get(token)
        if not meta:
            continue
        name      = meta.get("name", "")
        inst_type = meta.get("inst_type", "")
        expiry    = meta.get("expiry", 0)
        lot_size  = lot_size_map.get(name, 1)

        positions[token] = {
            "token":         token,
            "name":          name,
            "inst_type":     inst_type,
            "expiry":        expiry,
            "lot_size":      lot_size,
            "tsym":          meta.get("tsym", ""),
            "buy_qty":       0.0,
            "buy_val":       0.0,
            "buy_avg":       0.0,
            "sell_qty":      0.0,
            "sell_val":      0.0,
            "sell_avg":      0.0,
            "qty_overnight": eod.get("qty_overnight", 0.0),
            "prev_close":    eod.get("prev_close",    0.0),
        }
    return positions


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

        # ── Slippage calculation (mirrors summary.py logic) ──────────
        # slippage = (avg_fill_price - mid_price) / mid_price  for net buys
        #          = (mid_price - avg_fill_price) / mid_price  for net sells
        # mid_price proxy: use ltp (best available real-time mid)
        # avg_fill_price: weighted avg of buy+sell fills
        qty_today_buy  = p["buy_qty"]
        qty_today_sell = p["sell_qty"]
        net_qty_today  = qty_today_buy - qty_today_sell
        # Use side-specific avg: buy_avg for net long, sell_avg for net short
        buy_avg_p      = p["buy_avg"]  if p["buy_qty"]  > 0 else None
        sell_avg_p     = p["sell_avg"] if p["sell_qty"] > 0 else None
        mid_price      = ltp if ltp and ltp > 0 else None
        slippage       = None
        if mid_price:
            if net_qty_today > 0 and buy_avg_p:
                slippage = (buy_avg_p - mid_price) / mid_price
            elif net_qty_today < 0 and sell_avg_p:
                slippage = (sell_avg_p - mid_price) / mid_price
        # blended avg kept for snapshot reference
        fill_qty       = qty_today_buy + qty_today_sell
        traded_value   = p["buy_val"] + p["sell_val"]
        avg_fill_price = (traded_value / fill_qty) if fill_qty > 0 else None

        stock_map[sym]["expiries"].append({
            "label":           p["tsym"],
            "token":           token,
            "qty_overnight":   qty_overnight,
            "prev_close":      prev_close,
            "qty_today_buy":   qty_today_buy,
            "qty_today_sell":  qty_today_sell,
            "buy_avg":         p["buy_avg"],
            "sell_avg":        p["sell_avg"],
            "ltp":             ltp,
            "mtd":             0.0,
            "slippage":        slippage,       # float or None
            "avg_fill_price":  avg_fill_price, # weighted avg across buy+sell
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
# DAY-END SNAPSHOT  — saves dashboard data to dated CSV on colo
# ══════════════════════════════════════════════════════════════

EXPENSE_PER_CR_SNAP = EXPENSE_PER_CR   # reuse global


def _calc_pnl_for_snapshot(e: dict, lot_size: int) -> dict:
    """Mirror of dashboard PnL engine — produces one row per expiry."""
    qty_buy   = e["qty_today_buy"]
    qty_sell  = e["qty_today_sell"]
    qty_on    = e["qty_overnight"]
    ltp       = e["ltp"]
    prev_cl   = e["prev_close"]
    b_avg     = e["buy_avg"]
    s_avg     = e["sell_avg"]

    net_today = qty_buy - qty_sell
    open_qty  = qty_on + net_today
    lots      = round(open_qty / lot_size, 2) if lot_size > 0 else None

    carry     = qty_on * (ltp - prev_cl)
    day_buy   = qty_buy  * (ltp - b_avg)  if qty_buy  > 0 else 0.0
    day_sell  = qty_sell * (s_avg - ltp)  if qty_sell > 0 else 0.0
    day       = day_buy + day_sell

    tval      = (qty_buy  * (b_avg  or ltp)) + (qty_sell * (s_avg or ltp))
    expenses  = (tval / 1e7) * EXPENSE_PER_CR_SNAP
    net       = carry + day - expenses
    net_exp   = open_qty * ltp

    return {
        "open_qty":   open_qty,
        "lots":       lots,
        "net_exp":    round(net_exp,   2),
        "traded_val": round(tval,      2),
        "carry_pnl":  round(carry,     2),
        "day_pnl":    round(day,       2),
        "expenses":   round(expenses,  2),
        "net_pnl":    round(net,       2),
    }


def build_snapshot_rows(data: list[dict], as_of: str, log_date: str) -> list[dict]:
    """
    Flatten dashboard data into CSV rows.
    One row per expiry, with parent stock aggregates included as extra cols.
    Columns:
        snapshot_time, trade_date, sym, lot_size,
        expiry_label, ltp, qty_overnight, prev_close,
        qty_today_buy, buy_avg, qty_today_sell, sell_avg,
        open_qty, lots, net_exp, traded_val,
        carry_pnl, day_pnl, expenses, net_pnl,
        stock_net_pnl  (sum across all expiries for that stock)
    """
    rows = []
    for stock in data:
        sym      = stock["sym"]
        lot_size = stock["lot_size"]

        exp_pnls = [
            _calc_pnl_for_snapshot(e, lot_size)
            for e in stock["expiries"]
        ]
        stock_net_pnl = round(sum(p["net_pnl"] for p in exp_pnls), 2)

        for e, pnl in zip(stock["expiries"], exp_pnls):
            rows.append({
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
                "open_qty":       pnl["open_qty"],
                "lots":           pnl["lots"],
                "net_exp":        pnl["net_exp"],
                "traded_val":     pnl["traded_val"],
                "carry_pnl":      pnl["carry_pnl"],
                "day_pnl":        pnl["day_pnl"],
                "expenses":       pnl["expenses"],
                "net_pnl":        pnl["net_pnl"],
                "stock_net_pnl":  stock_net_pnl,
                "slippage":       round(e["slippage"], 8) if e.get("slippage") is not None else None,
            })
    return rows


def save_snapshot_to_colo(data: list[dict], as_of: str, log_date: str) -> bool:
    """
    Build a dated CSV from current dashboard data and write it to
    REMOTE_DASHBOARD_DIR/snapshots/dashboard_snapshot_<YYYYMMDD>.csv
    on the colo server via SFTP.

    log_date : trade date string — either 'YYYY-MM-DD' or 'YYYYMMDD'
    Returns True on success, False on any error.
    """
    # Normalise date to YYYYMMDD for filename
    try:
        if "-" in log_date:
            date_tag = datetime.strptime(log_date, "%Y-%m-%d").strftime("%Y%m%d")
        else:
            date_tag = log_date[:8]   # already YYYYMMDD
    except Exception:
        date_tag = date.today().strftime("%Y%m%d")

    remote_dir  = f"{REMOTE_DASHBOARD_DIR}/{SNAPSHOT_SUBDIR}"
    remote_path = f"{remote_dir}/dashboard_snapshot_{date_tag}.csv"

    rows = build_snapshot_rows(data, as_of, log_date)
    if not rows:
        log.warning("Snapshot: no data rows to save — skipping")
        return False

    # Build CSV in memory
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8")

    # Upload via SFTP
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(SSH_HOST, port=SSH_PORT,
                       username=SSH_USER, password=SSH_PASS, timeout=15)
        sftp = client.open_sftp()

        # Ensure remote directory exists
        try:
            sftp.stat(remote_dir)
        except FileNotFoundError:
            # mkdir -p equivalent via SSH command
            client.exec_command(f"mkdir -p {remote_dir}")
            time.sleep(0.5)   # give shell a moment

        with sftp.open(remote_path, "wb") as f:
            f.write(csv_bytes)

        sftp.close()
        client.close()
        log.info("Snapshot saved → %s:%s  (%d rows)", SSH_HOST, remote_path, len(rows))
        return True

    except Exception as e:
        log.error("Snapshot SFTP upload failed: %s", e)
        return False


def should_take_snapshot(now_ist: datetime, last_snapshot_date: date) -> bool:
    """
    Return True exactly once per calendar day, after EOD_SNAPSHOT_TIME IST.
    Prevents repeated saves if the worker loop runs past 15:30 many times.
    """
    eod_h, eod_m = map(int, EOD_SNAPSHOT_TIME.split(":"))
    after_cutoff = (now_ist.hour, now_ist.minute) >= (eod_h, eod_m)
    new_day      = now_ist.date() > last_snapshot_date
    return after_cutoff and new_day


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

    # Snapshot tracker — stores the date of the last saved snapshot
    # Initialise to yesterday so a snapshot can be taken today if already past 15:30
    IST = ZoneInfo("Asia/Kolkata")
    last_snapshot_date: date = date.today() - timedelta(days=1)
    last_good_data: list     = []   # last non-empty positions payload
    last_good_as_of: str     = ""
    last_good_log_date: str  = ""

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
                log.warning("No FTRD fills found in log — showing overnight positions only")
                # Still load EOD and show overnight positions with carry PnL
                # This handles pre-market / no-trade days correctly
                lot_size_map = load_lot_sizes_from_csv()
                eod_map      = load_eod(token_map, log_path=latest_log)

                if eod_map:
                    # Build positions from EOD only (zero intraday fills)
                    positions = build_positions_from_eod(eod_map, token_map, lot_size_map)
                    ltp_map   = get_ltp_map(r_ltp, positions)
                    data      = group_by_stock(positions, ltp_map, eod_map)
                    log.info("EOD-only positions: %d stocks", len(data))
                else:
                    data = []
                    log.warning("No EOD file found — publishing empty positions")

                _log_date_str = extract_date_from_fills(latest_log) or today_str()
                try:
                    _log_date_fmt = datetime.strptime(_log_date_str, "%Y%m%d").strftime("%Y-%m-%d")
                except Exception:
                    _log_date_fmt = _log_date_str

                payload = json.dumps({
                    "as_of":     datetime.now().isoformat(timespec="seconds"),
                    "log_date":  _log_date_fmt,
                    "positions": data,
                    "source":    "log_file",
                })
                r_dash.set(DASH_REDIS_KEY, payload)
                time.sleep(LOOP_SECONDS)
                continue

            # 2. Get lot sizes from stocks.csv
            lot_size_map = load_lot_sizes_from_csv()

            # 3. Load EOD — must be before build_positions so EOD-only tokens are merged
            eod_map = load_eod(token_map, log_path=latest_log)

            # 4. Build per-token positions (merges EOD-only tokens too)
            positions = build_positions_from_fills(fills, token_map, lot_size_map, eod_map)
            log.info("Positions built: %d tokens", len(positions))

            # 5. Get LTP from Redis DB2
            ltp_map = get_ltp_map(r_ltp, positions)

            # 6. Group by stock for dashboard format
            data = group_by_stock(positions, ltp_map, eod_map)
            log.info("Stocks grouped: %d underlyings", len(data))

            # 7. Publish to Redis
            # Extract actual trade date from fill timestamps (not filename)
            _log_date_str = extract_date_from_fills(latest_log)
            try:
                _log_date_fmt = datetime.strptime(_log_date_str, "%Y%m%d").strftime("%Y-%m-%d")
            except Exception:
                _log_date_fmt = _log_date_str

            _as_of = datetime.now().isoformat(timespec="seconds")

            payload = json.dumps({
                "as_of":     _as_of,
                "log_date":  _log_date_fmt,
                "positions": data,
                "source":    "log_file",
            }, ensure_ascii=False)

            r_dash.set(DASH_REDIS_KEY, payload)
            log.info("Published %d stocks to Redis key=%s", len(data), DASH_REDIS_KEY)

            # Keep last good snapshot in memory for EOD save
            if data:
                last_good_data      = data
                last_good_as_of     = _as_of
                last_good_log_date  = _log_date_fmt

            # ── 8. Day-end snapshot check ────────────────────────────
            now_ist = datetime.now(tz=IST)
            if should_take_snapshot(now_ist, last_snapshot_date):
                snap_data     = last_good_data or data
                snap_as_of    = last_good_as_of or _as_of
                snap_log_date = last_good_log_date or _log_date_fmt
                log.info("EOD snapshot triggered at %s IST", now_ist.strftime("%H:%M:%S"))
                ok = save_snapshot_to_colo(snap_data, snap_as_of, snap_log_date)
                if ok:
                    last_snapshot_date = now_ist.date()
                else:
                    log.warning("Snapshot failed — will retry next tick")

        except Exception as e:
            import traceback
            log.error("tick failed: %s\n%s", e, traceback.format_exc())

        elapsed = time.time() - tick_start
        time.sleep(max(0.1, LOOP_SECONDS - elapsed))


if __name__ == "__main__":
    main()
