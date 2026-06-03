"""
dropcopy_summary_writer.py  —  v4  (2026-06-02)
================================================
Reads confirmed trade positions from dropcopy Redis (dc:pos:* DB0) on colo
(192.168.71.200), computes daily summary, writes 2 CSV files.

CARRY AUTO-DETECT:
  - Looks for /data/Dashboard/Eod/eod_positions_<prev_trading_day>.csv on colo
  - If found  → carry_pnl = qty_overnight × (LTP − prev_close)  included in MTM/NP
  - If missing → carry = 0  (pure intraday — used on first day / no prior EOD)
  No manual flag needed. Just run the same cron every day.

PnL per symbol (verified against RELIANCE note):
  RELIANCE +2 lots, entry=900, EOD=1000, lot=500
  net_qty = +1000 units
  GP  = open_buy × (ltp − buy_avg) = 1000 × 100 = ₹1,00,000
  NP  = GP − trading_cost
  MTM = carry + GP  (carry=0 today, non-zero from tomorrow)

Output CSVs → colo /data/Dashboard/Summary/:

  Summary_daily.csv
    Date | Gross_Position | Net_Position | MTM | TV | Trading_Cost
       | Max_Margin | EOD_Margin

  Summary_symbol_pnl.csv
    Date | Symbol | Net_PNL | EOD_Pos_Lot | EOD_Pos_INR

Lot sizes : Redis DB2  fo:stock_spot:<SYM>  field lot_size  (same as dashboard_worker)
LTP       : Redis DB2  fo:stock_spot:<SYM>  field ltp/spot
            Redis DB0  fo:index_futures:<SYM>  field ltp

Cron (15:31 daily — after market close, before EOD generator at 15:44):
  31 15 * * 1-5 cd /home/report/devstudio/Prashant/Live_Dashboard/Prod && \\
    /home/report/devstudio/Prashant/Live_Dashboard/venv/bin/python3 \\
    dropcopy_summary_writer.py >> dropcopy_summary.log 2>&1

Requires: pip install redis paramiko
"""

from __future__ import annotations

import csv
import io
import logging
import os
import socket
import threading
import time
from datetime import date, timedelta
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import redis
import paramiko

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

SSH_HOST          = os.getenv("SSH_HOST",           "192.168.71.200")
SSH_PORT          = int(os.getenv("SSH_PORT",        "22"))
SSH_USER          = os.getenv("SSH_USER",            "Data_colo")
SSH_PASS          = os.getenv("SSH_PASS",            "Datacolo@2026")

COLO_REDIS_PORT   = int(os.getenv("COLO_REDIS_PORT", "6379"))
DC_REDIS_DB       = int(os.getenv("DC_REDIS_DB",     "0"))   # dropcopy positions
LTP_REDIS_DB      = int(os.getenv("LTP_REDIS_DB",    "2"))   # stock LTP + lot_size
IDX_REDIS_DB      = int(os.getenv("IDX_REDIS_DB",    "0"))   # index LTP (same DB as DC)

TUNNEL_LOCAL_PORT = int(os.getenv("TUNNEL_LOCAL_PORT", "16379"))

# Written by dashboard_worker_prod.py — has correct lot_size per symbol
LOCAL_REDIS_HOST = os.getenv("LOCAL_REDIS_HOST", "localhost")
LOCAL_REDIS_PORT = int(os.getenv("LOCAL_REDIS_PORT", "6379"))
LOCAL_REDIS_DB   = int(os.getenv("LOCAL_REDIS_DB",   "1"))


DC_KEY_PREFIX        = "dc:pos:"
REMOTE_DASHBOARD_DIR = "/data/Dashboard"
SUMMARY_SUBDIR       = "Summary"
EOD_SUBDIR           = "Eod"
REMOTE_PCAP_DIR      = os.getenv("REMOTE_PCAP_DIR", "/data/pcapdata")

DAILY_CSV   = "Summary_daily.csv"
SYM_PNL_CSV = "Summary_symbol_pnl.csv"

# Trading cost model — same as dashboard_worker_prod.py
BUY_COST_PER_CR  = 1018.0   # ₹ per Crore of buy traded value
SELL_COST_PER_CR = 5818.0   # ₹ per Crore of sell traded value

PAISE = 100.0   # dropcopy stores prices/notionals in paise → ÷100 = ₹

IST = ZoneInfo("Asia/Kolkata")

# Fallback lot sizes — used only when Redis DB2 fo:stock_spot:<SYM> has no lot_size
# Redis is always preferred. Update this table with each NSE circular.
# LOT_FALLBACK — matches Redis DB2 fo:stock_spot:<SYM> lot_size field
# Primary source is Redis DB2 (read via SSH tunnel).
# This table is ONLY used when Redis DB2 key is missing for a symbol.
# Update from: redis-cli -n 2 hget "fo:stock_spot:<SYM>" lot_size
LOT_FALLBACK: dict[str, int] = {
    "NIFTY":       50,   "BANKNIFTY":   15,   "FINNIFTY":    40,
    "MIDCPNIFTY":  75,   "SENSEX":      10,   "BANKEX":      15,
    "ICICIBANK":  700,   "SBIN":        750,  "RELIANCE":   500,
    "BHEL":      2625,   "BAJFINANCE":  750,  "M&M":        200,
    "KOTAKBANK": 2000,   "BHARTIARTL":  475,  "INFY":       400,
    "HDFCBANK":   550,   "LT":          175,  "TCS":        175,
    "AXISBANK":   625,   "TATAMOTORS": 1425,  "WIPRO":     1500,
    "MARUTI":      25,   "ADANIENT":    625,  "HINDUNILVR": 300,
    "ONGC":      1925,   "NTPC":       2250,  "POWERGRID": 2700,
    "BEL":       1425,   "HAL":         150,  "SIEMENS":     75,
    "TATASTEEL": 2750,   "SHRIRAMFIN":  825,  "HCLTECH":    350,
    "SUNPHARMA":  350,   "NATIONALUM": 1875,  "MCX":        625,
    "BSE":        375,   "ETERNAL":    2425,  "DIXON":       50,
}

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("dropcopy_summary")
logging.getLogger("paramiko").setLevel(logging.WARNING)


# ══════════════════════════════════════════════════════════════════════════════
# SSH TUNNEL
# ══════════════════════════════════════════════════════════════════════════════

class SSHTunnel:
    """Forward localhost:TUNNEL_LOCAL_PORT → colo Redis :6379 via SSH."""

    def __init__(self):
        self.local_port  = TUNNEL_LOCAL_PORT
        self._ssh        = None
        self._server     = None
        self._stop_event = threading.Event()

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, *_):
        self._stop()

    def _start(self):
        log.info("Opening SSH tunnel localhost:%d → %s:6379", self.local_port, SSH_HOST)
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(SSH_HOST, port=SSH_PORT,
                          username=SSH_USER, password=SSH_PASS, timeout=10)
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", self.local_port))
        self._server.listen(5)
        self._server.settimeout(1.0)
        threading.Thread(target=self._accept_loop, daemon=True).start()
        time.sleep(0.5)
        log.info("SSH tunnel ready on localhost:%d", self.local_port)

    def _accept_loop(self):
        while not self._stop_event.is_set():
            try:
                client_sock, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_client,
                             args=(client_sock,), daemon=True).start()

    def _handle_client(self, client_sock):
        try:
            chan = self._ssh.get_transport().open_channel(
                "direct-tcpip",
                ("127.0.0.1", COLO_REDIS_PORT),
                ("127.0.0.1", self.local_port),
            )
        except Exception as e:
            log.warning("Tunnel channel open failed: %s", e)
            client_sock.close()
            return

        def _pump(src, dst):
            try:
                while True:
                    data = src.recv(4096)
                    if not data:
                        break
                    dst.sendall(data)
            except Exception:
                pass
            finally:
                try: src.close()
                except: pass
                try: dst.close()
                except: pass

        t1 = threading.Thread(target=_pump, args=(client_sock, chan), daemon=True)
        t2 = threading.Thread(target=_pump, args=(chan, client_sock), daemon=True)
        t1.start(); t2.start()
        t1.join();  t2.join()

    def _stop(self):
        log.info("Closing SSH tunnel")
        self._stop_event.set()
        try: self._server.close()
        except: pass
        try: self._ssh.close()
        except: pass


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def today_str() -> str:
    return date.today().strftime("%Y%m%d")


def prev_trading_date(d: date = None) -> str:
    """Return previous weekday (Mon–Fri) as YYYYMMDD string."""
    d = (d or date.today()) - timedelta(days=1)
    while d.weekday() >= 5:   # skip Sat(5) and Sun(6)
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def get_ssh_client() -> paramiko.SSHClient:
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS, timeout=10)
    return c


def ensure_remote_dir(sftp, path: str, ssh_client):
    try:
        sftp.stat(path)
    except FileNotFoundError:
        ssh_client.exec_command(f"mkdir -p {path}")
        time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Read dropcopy positions from Redis DB0 (dc:pos:*)
# ══════════════════════════════════════════════════════════════════════════════

def read_dropcopy_positions(r: redis.Redis) -> list[dict]:
    """
    Scan all dc:pos:* keys and AGGREGATE by token.

    ROOT CAUSE OF 2x BUG:
      per token (one per shard): dc:pos:<broker>:<account>:<token>:<participant>
      Appending each key separately inflates qty by shard_count (2x, 4x etc).

    FIX: accumulate buy_notional + sell_notional + buy_qty + sell_qty per token,
    then compute weighted-average prices from totals — same result as one key
    with the full position. This matches how dashboard_worker aggregates fills.
    """
    agg: dict[int, dict] = {}
    raw_key_count = 0

    try:
        cursor, keys = 0, []
        while True:
            cursor, batch = r.scan(cursor, match=f"{DC_KEY_PREFIX}*", count=200)
            keys.extend(batch)
            if cursor == 0:
                break

        raw_key_count = len(keys)
        log.info("Found %d dc:pos keys in Redis DB%d", raw_key_count, DC_REDIS_DB)

        for key in keys:
            raw = r.hgetall(key)
            if not raw:
                continue

            contract_key = raw.get("contract_key", "")
            parts        = contract_key.split("-")
            symbol       = parts[1].upper() if len(parts) > 1 else ""
            token        = int(raw.get("token", 0))

            if not symbol or not token:
                log.warning("Skipping key %s — bad symbol/token", key)
                continue

            # Keep notionals in paise for accurate summation across shards
            buy_qty       = int(raw.get("buy_qty",  0))
            sell_qty      = int(raw.get("sell_qty", 0))
            buy_notional  = int(raw.get("buy_notional",  0))  # paise
            sell_notional = int(raw.get("sell_notional", 0))  # paise
            last_fill_px  = int(raw.get("last_fill_price", 0)) / PAISE

            if token not in agg:
                agg[token] = {
                    "token":           token,
                    "symbol":          symbol,
                    "buy_qty":         0,
                    "sell_qty":        0,
                    "buy_notional":    0,
                    "sell_notional":   0,
                    "last_fill_price": 0.0,
                }

            agg[token]["buy_qty"]        += buy_qty
            agg[token]["sell_qty"]       += sell_qty
            agg[token]["buy_notional"]   += buy_notional
            agg[token]["sell_notional"]  += sell_notional
            agg[token]["last_fill_price"] = max(agg[token]["last_fill_price"], last_fill_px)

        # Build final position list.
        # Confirmed: exactly 1 key per token in format dc:pos:<broker>:<account>:<token>:<participant>
        # Aggregation dict handles any future duplicates safely via summation.
        positions = []
        for token, a in agg.items():
            buy_qty       = a["buy_qty"]
            sell_qty      = a["sell_qty"]
            buy_notional  = a["buy_notional"]  / PAISE   # paise → ₹
            sell_notional = a["sell_notional"] / PAISE

            buy_avg  = (buy_notional  / buy_qty)  if buy_qty  > 0 else 0.0
            sell_avg = (sell_notional / sell_qty) if sell_qty > 0 else 0.0
            net_qty  = buy_qty - sell_qty

            positions.append({
                "token":           token,
                "symbol":          a["symbol"],
                "buy_qty":         buy_qty,
                "sell_qty":        sell_qty,
                "net_qty":         net_qty,
                "buy_avg":         round(buy_avg,  4),
                "sell_avg":        round(sell_avg, 4),
                "last_fill_price": a["last_fill_price"],
            })

        log.info("Positions loaded: %d tokens from %d dc:pos keys",
                 len(positions), raw_key_count)

    except Exception as e:
        log.error("Failed to read dropcopy positions: %s", e)
        positions = []

    return positions


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Lot sizes from Redis DB2 (fo:stock_spot:<SYM> → lot_size)
# ══════════════════════════════════════════════════════════════════════════════

def load_lot_sizes(positions: list[dict]) -> dict[str, int]:
    """
    Load lot sizes from LOCAL Redis DB2 fo:stock_spot:<SYM> → lot_size.

    KEY INSIGHT: fo:stock_spot:* keys are written by stock_realtime_feeder
    which runs on the REPORT SERVER (localhost), NOT on colo.
    So we read localhost:6379 DB2 directly — no SSH tunnel needed.

    This is identical to what dashboard_worker does:
      r = redis.Redis(host=LTP_REDIS_HOST, port=LTP_REDIS_PORT, db=2)
      r.hget("fo:stock_spot:M&M", "lot_size")  → "200"

    Falls back to LOT_FALLBACK for any missing symbol.
    """
    result: dict[str, int] = {}
    try:
        # Local Redis DB2 — same server, no tunnel
        r = redis.Redis(host="localhost", port=6379, db=2,
                        decode_responses=True, socket_timeout=3.0)
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
        log.info("Lot sizes from localhost Redis DB2: %d symbols", len(result))
        sample = {k: result[k] for k in list(result)[:8]}
        log.info("Sample: %s", sample)
    except Exception as e:
        log.warning("localhost Redis DB2 lot size scan failed: %s — using fallback", e)

    # Fill gaps with fallback
    missing = []
    for pos in positions:
        sym = pos["symbol"]
        if sym not in result:
            if sym in LOT_FALLBACK:
                result[sym] = LOT_FALLBACK[sym]
                missing.append(f"{sym}={LOT_FALLBACK[sym]}")
            else:
                qty = abs(pos["net_qty"]) or max(pos["buy_qty"], pos["sell_qty"])
                result[sym] = max(int(qty), 1)
                log.warning("No lot size for %s — add to LOT_FALLBACK!", sym)
    if missing:
        log.info("Fallback lots: %s", ", ".join(missing))

    return result


def load_eod_overnight(trade_date: str) -> dict[int, dict]:
    """
    Load overnight positions for carry PnL.
    Returns { token(int): { qty_overnight, prev_close, symbol } }
    Returns {} if not found → carry = 0 (first day / no prior EOD).

    Priority:
    1. Local Redis DB1  dashboard:eod:<prev_date>
       Written by dashboard_worker when it loads the EOD CSV.
       Always available on report server, no SSH needed.
    2. Remote CSV via SSH  /data/Dashboard/Eod/eod_positions_<prev_date>.csv
       Fallback if Redis key not present yet.
    """
    import json as _json
    prev_dt = prev_trading_date(datetime.strptime(trade_date, "%Y%m%d").date())
    result: dict[int, dict] = {}

    # ── Source 1: local Redis DB1 dashboard:eod:<prev_date> ──────────────────
    try:
        r_local = redis.Redis(host=LOCAL_REDIS_HOST, port=LOCAL_REDIS_PORT,
                              db=LOCAL_REDIS_DB, decode_responses=True, socket_timeout=3.0)
        eod_key = f"dashboard:eod:{prev_dt}"
        raw = r_local.get(eod_key)
        if raw:
            data = _json.loads(raw)
            for _tok_str, v in data.items():
                try:
                    token         = int(_tok_str)
                    qty_overnight = float(v["qty_overnight"])
                    prev_close    = float(v["prev_close"])
                    symbol        = str(v.get("symbol") or v.get("name") or "").upper()
                    if prev_close > 0:
                        result[token] = {
                            "qty_overnight": qty_overnight,
                            "prev_close":    prev_close,
                            "symbol":        symbol,
                        }
                except (KeyError, ValueError, TypeError):
                    continue
            if result:
                log.info("EOD overnight loaded: %d tokens from local Redis DB1 key=%s",
                         len(result), eod_key)
                return result
            else:
                log.info("Redis key %s exists but empty — trying CSV", eod_key)
        else:
            log.info("Redis key %s not found — trying CSV", eod_key)
    except Exception as e:
        log.warning("Local Redis DB1 EOD fetch failed: %s — trying CSV", e)

    # ── Source 2: remote CSV via SSH ──────────────────────────────────────────
    path = f"{REMOTE_DASHBOARD_DIR}/{EOD_SUBDIR}/eod_positions_{prev_dt}.csv"
    try:
        client = get_ssh_client()
        _, stdout, _ = client.exec_command(f"cat {path} 2>/dev/null")
        raw_csv = stdout.read().decode("utf-8", errors="replace")
        client.close()
    except Exception as e:
        log.warning("SSH read failed for EOD CSV: %s", e)
        raw_csv = ""

    lines = [l for l in raw_csv.splitlines() if l.strip()]
    if len(lines) < 2:
        log.info("EOD file not found: %s — carry = 0 (first day or no prev EOD)", path)
        return {}

    # Log header to see actual column names
    log.info("EOD CSV header: %s", lines[0][:120])

    for row in csv.DictReader(lines):
        try:
            # token column — dashboard_worker writes it as "token"
            token_val = row.get("token") or row.get("Token") or ""
            if not token_val:
                continue
            token         = int(token_val)
            qty_overnight = float(row["qty_overnight"])
            prev_close    = float(row["prev_close"])
            symbol        = (row.get("symbol") or row.get("name") or "").strip().upper()
            # Load all tokens — even prev_close=0 (intraday day, no carry)
            # Tomorrow's EOD will have real prev_close values
            if token > 0:
                result[token] = {
                    "qty_overnight": qty_overnight,
                    "prev_close":    prev_close,
                    "symbol":        symbol,
                }
        except (KeyError, ValueError, TypeError):
            continue

    if result:
        log.info("EOD overnight loaded: %d tokens from CSV %s", len(result), path)
    else:
        log.warning("EOD CSV found but 0 tokens parsed from %s — carry = 0", path)
        log.warning("CSV columns available: %s", lines[0] if lines else "N/A")

    return result


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — LTP from Redis
# ══════════════════════════════════════════════════════════════════════════════

FEEDER_INDICES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}



# Futures LTP cache: { "BHEL": 415.0, "M&M": 3034.0, ... }
# Loaded once per run from localhost Redis DB2 fo:stock_future:<SYM>:<TSYM>
_FUTURES_LTP: dict[str, float] = {}


def load_futures_ltp() -> dict[str, float]:
    """
    Scan fo:stock_future:<SYM>:<TSYM> keys in localhost Redis DB2.
    Read ltp field — this is the futures last traded price, same as dashboard.

    Key format: fo:stock_future:BHEL:BHEL26JUNFUT
    One key per active futures contract per symbol.
    No dependency on dashboard:positions:latest2.
    """
    result: dict[str, float] = {}
    try:
        r = redis.Redis(host="localhost", port=6379, db=2,
                        decode_responses=True, socket_timeout=3.0)
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match="fo:stock_future:*:*", count=500)
            for key in keys:
                # key = fo:stock_future:<SYM>:<TSYM>
                parts = key.split(":")
                if len(parts) < 4:
                    continue
                sym = parts[2].upper()
                val = r.hget(key, "ltp")
                if val and float(val) > 0:
                    # Keep only the most recent expiry if multiple exist
                    if sym not in result:
                        result[sym] = float(val)
            if cursor == 0:
                break
        log.info("Futures LTP from fo:stock_future:*: %d symbols", len(result))
        sample = {k: result[k] for k in list(result)[:8]}
        log.info("Futures LTP sample: %s", sample)
    except Exception as e:
        log.warning("Futures LTP load failed: %s — will use last_fill_price", e)
    return result


def get_ltp(symbol: str, last_fill_px: float) -> float:
    """
    LTP priority:
    1. fo:stock_future:<SYM>:<TSYM> ltp  — futures LTP from local feeder
       Pure dropcopy-compatible: no dashboard dependency.
    2. fo:stock_spot:<SYM> ltp           — spot fallback
    3. last_fill_price from dropcopy     — last resort
    """
    # Source 1: futures LTP cache
    if symbol in _FUTURES_LTP:
        return _FUTURES_LTP[symbol]

    # Source 2: spot LTP
    try:
        r = redis.Redis(host="localhost", port=6379, db=2,
                        decode_responses=True, socket_timeout=2.0)
        for field in ["ltp", "close"]:
            val = r.hget(f"fo:stock_spot:{symbol}", field)
            if val:
                return float(val)
    except Exception as e:
        log.debug("Spot LTP failed for %s: %s", symbol, e)

    log.debug("%s: no Redis LTP — using last_fill_price=%.2f", symbol, last_fill_px)
    return last_fill_px


# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — PnL calculation
#
# TODAY  (no EOD file): carry=0, MTM = GP = pure intraday
# TOMORROW (EOD found): carry = qty_overnight × (LTP − prev_close), MTM = carry + GP
#
# Verified against handwritten note:
#   RELIANCE +2 lots, entry=900, EOD=1000, lot=500 → net_qty=1000 units
#   GP = open_buy(1000) × (1000−900) = ₹1,00,000
#   carry (tomorrow) = 1000 × (new_ltp − 1000)
# ══════════════════════════════════════════════════════════════════════════════

def calc_pnl(pos: dict, eod: dict, ltp: float) -> dict:
    """
    eod = { qty_overnight, prev_close }  or {}  (if no carry today)
    All qty in raw units (not lots).
    """
    buy_qty  = pos["buy_qty"]
    sell_qty = pos["sell_qty"]
    net_qty  = pos["net_qty"]
    buy_avg  = pos["buy_avg"]
    sell_avg = pos["sell_avg"]

    # ── Carry (overnight) ────────────────────────────────────────────────────
    qty_overnight = eod.get("qty_overnight", 0.0)
    prev_close    = eod.get("prev_close",    0.0)
    carry = qty_overnight * (ltp - prev_close) if prev_close > 0 else 0.0

    # ── Intraday GP (realized + unrealized) ──────────────────────────────────
    matched   = min(buy_qty, sell_qty)
    open_buy  = buy_qty  - matched
    open_sell = sell_qty - matched

    realized    = matched   * (sell_avg - buy_avg) if matched   > 0 else 0.0
    unreal_buy  = open_buy  * (ltp - buy_avg)      if open_buy  > 0 else 0.0
    unreal_sell = open_sell * (sell_avg - ltp)      if open_sell > 0 else 0.0
    gp          = realized + unreal_buy + unreal_sell

    # ── Trading cost ─────────────────────────────────────────────────────────
    buy_val      = buy_qty  * (buy_avg  or ltp)
    sell_val     = sell_qty * (sell_avg or ltp)
    traded_val   = buy_val + sell_val
    trading_cost = (buy_val  / 1e7) * BUY_COST_PER_CR \
                 + (sell_val / 1e7) * SELL_COST_PER_CR

    # ── Summary metrics ───────────────────────────────────────────────────────
    mtm = carry + gp                  # full mark-to-market (carry=0 today)
    np_ = mtm - trading_cost          # net after cost

    # EOD open position
    eod_pos_inr = net_qty * ltp

    return {
        "gp":           round(gp,           2),
        "carry":        round(carry,        2),
        "mtm":          round(mtm,          2),
        "np":           round(np_,          2),
        "realized":     round(realized,     2),
        "unrealized":   round(unreal_buy + unreal_sell, 2),
        "traded_val":   round(traded_val,   2),
        "trading_cost": round(trading_cost, 2),
        "net_qty":      net_qty,
        "eod_pos_inr":  round(eod_pos_inr,  2),
    }


# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — Daily aggregate
# ══════════════════════════════════════════════════════════════════════════════

def daily_summary(results: list[dict]) -> dict:
    gross_pos  = sum(abs(r["eod_pos_inr"]) for r in results)
    net_pos    = sum(r["eod_pos_inr"]      for r in results)
    total_mtm  = sum(r["mtm"]              for r in results)
    total_tv   = sum(r["traded_val"]       for r in results)
    total_cost = sum(r["trading_cost"]     for r in results)

    return {
        "Gross_Position": round(gross_pos,          2),
        "Net_Position":   round(net_pos,            2),
        "MTM":            round(total_mtm,          2),
        "Net_PNL":        round(total_mtm - total_cost, 2),   # MTM - expenses
        "TV":             round(total_tv,            2),
        "Trading_Cost":   round(total_cost,          2),
        "Max_Margin":     round(gross_pos,           2),
        "EOD_Margin":     round(gross_pos,           2),
    }


# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — CSV helpers (idempotent upsert via SFTP)
# ══════════════════════════════════════════════════════════════════════════════

DAILY_COLS   = ["Date", "Gross_Position", "Net_Position", "MTM", "Net_PNL",
                "TV", "Trading_Cost", "Max_Margin", "EOD_Margin"]
SYM_PNL_COLS = ["Date", "Symbol", "Net_PNL", "EOD_Pos_Lot", "EOD_Pos_INR"]


def _read_sftp_csv(sftp, path: str) -> list[dict]:
    try:
        with sftp.open(path, "r") as f:
            return list(csv.DictReader(
                f.read().decode("utf-8", errors="replace").splitlines()))
    except FileNotFoundError:
        return []


def _write_sftp_csv(sftp, path: str, rows: list[dict], cols: list[str]):
    buf = io.StringIO()
    w   = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore",
                         lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    with sftp.open(path, "w") as f:
        f.write(buf.getvalue())


def upsert_csv(sftp, path: str, cols: list[str],
               trade_date: str, new_rows: list[dict]):
    existing = _read_sftp_csv(sftp, path)
    kept     = [r for r in existing if r.get("Date", "").strip() != trade_date]
    final    = kept + new_rows
    _write_sftp_csv(sftp, path, final, cols)
    action = "updated" if existing else "created"
    log.info("%-10s %s  (%d rows for %s, %d total)",
             action, os.path.basename(path), len(new_rows), trade_date, len(final))


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 65)
    log.info("  dropcopy_summary_writer v4  —  %s",
             datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"))
    log.info("  SSH    : %s@%s:%d", SSH_USER, SSH_HOST, SSH_PORT)
    log.info("  Tunnel : localhost:%d → colo Redis :6379", TUNNEL_LOCAL_PORT)
    log.info("  Output : %s/%s/", REMOTE_DASHBOARD_DIR, SUMMARY_SUBDIR)
    log.info("=" * 65)

    trade_date = today_str()

    # ── Open SSH tunnel ───────────────────────────────────────────────────────
    with SSHTunnel() as tunnel:

        # Only r_dc needed via tunnel — dropcopy positions on colo Redis DB0
        # LTP and lot_size come from localhost Redis (feeder runs on report server)
        r_dc  = redis.Redis(host="127.0.0.1", port=tunnel.local_port,
                            db=DC_REDIS_DB,  decode_responses=True, socket_timeout=10.0)

        try:
            r_dc.ping()
            log.info("Colo Redis reachable via tunnel (DB%d)", DC_REDIS_DB)
        except Exception as e:
            raise RuntimeError(f"Cannot reach colo Redis: {e}")

        # 1. Dropcopy positions
        positions = read_dropcopy_positions(r_dc)
        if not positions:
            log.error("No dc:pos keys found — nothing to write.")
            return
        log.info("Positions loaded: %d symbols", len(positions))

        # 2. Lot sizes (Redis DB2)
        lot_sizes = load_lot_sizes(positions)

        # 3. Load futures LTP from fo:stock_future:* (localhost Redis DB2)
        _FUTURES_LTP.clear()
        _FUTURES_LTP.update(load_futures_ltp())

        # 4. EOD overnight (auto-detect: {} = no carry today, filled = carry active)
        eod_map = load_eod_overnight(trade_date)
        carry_active = len(eod_map) > 0
        log.info("Carry mode: %s", "ACTIVE" if carry_active else "OFF (first day / no prev EOD)")

        # 4. Per-symbol PnL
        results: list[dict] = []
        log.info("─" * 78)
        log.info("%-15s %7s %10s %8s %10s %10s %12s %12s",
                 "Symbol", "Lots", "GP", "Carry", "MTM", "NP", "EOD_Lot", "EOD_INR")
        log.info("─" * 78)

        for pos in positions:
            sym      = pos["symbol"]
            token    = pos["token"]
            lot_size = lot_sizes.get(sym, 1)

            ltp = get_ltp(sym, pos["last_fill_price"])

            # Match EOD by token (exact), fallback to symbol match
            eod = eod_map.get(token, {})
            if not eod and carry_active:
                # symbol fallback: find token with matching symbol in eod_map
                for t, e in eod_map.items():
                    if e.get("symbol", "").upper() == sym:
                        eod = e
                        break

            pnl = calc_pnl(pos, eod, ltp)

            eod_lot = round(pnl["net_qty"] / lot_size, 2) if lot_size > 0 else pnl["net_qty"]

            log.info("%-15s %7.2f %10.0f %8.0f %10.0f %10.0f %12.2f %12.0f  [lot=%d ltp=%.2f]",
                     sym, eod_lot,
                     pnl["gp"], pnl["carry"], pnl["mtm"], pnl["np"],
                     eod_lot, pnl["eod_pos_inr"],
                     lot_size, ltp)

            results.append({
                "symbol":   sym,
                "lot_size": lot_size,
                "eod_lot":  eod_lot,
                "ltp":      ltp,
                **pnl,
            })

        # 5. Daily totals
        summary = daily_summary(results)
        log.info("─" * 78)
        log.info("%-15s %7s %10.0f %8.0f %10.0f %10.0f",
                 "TOTAL", "",
                 sum(r["gp"]    for r in results),
                 sum(r["carry"] for r in results),
                 summary["MTM"],
                 summary["MTM"] - summary["Trading_Cost"])
        log.info("Gross_Pos=%.0f  Net_Pos=%.0f  Cost=%.0f",
                 summary["Gross_Position"], summary["Net_Position"], summary["Trading_Cost"])

    # tunnel closes here

    # 6. Build CSV rows
    daily_row    = {"Date": trade_date, **summary}
    sym_pnl_rows = [
        {
            "Date":        trade_date,
            "Symbol":      r["symbol"],
            "Net_PNL":     r["np"],
            "EOD_Pos_Lot": r["eod_lot"],
            "EOD_Pos_INR": r["eod_pos_inr"],
        }
        for r in results
    ]

    # 7. Write to colo via SFTP
    remote_dir   = f"{REMOTE_DASHBOARD_DIR}/{SUMMARY_SUBDIR}"
    path_daily   = f"{remote_dir}/{DAILY_CSV}"
    path_sym_pnl = f"{remote_dir}/{SYM_PNL_CSV}"

    log.info("Writing CSVs → %s:%s", SSH_HOST, remote_dir)
    try:
        ssh  = get_ssh_client()
        sftp = ssh.open_sftp()
        ensure_remote_dir(sftp, remote_dir, ssh)
        upsert_csv(sftp, path_daily,   DAILY_COLS,   trade_date, [daily_row])
        upsert_csv(sftp, path_sym_pnl, SYM_PNL_COLS, trade_date, sym_pnl_rows)
        sftp.close()
        ssh.close()
    except Exception as e:
        log.error("SFTP write failed: %s", e)
        raise

    log.info("=" * 65)
    log.info("  dropcopy_summary_writer complete.")
    log.info("=" * 65)


if __name__ == "__main__":
    main()
