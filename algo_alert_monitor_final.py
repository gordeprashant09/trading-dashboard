#!/usr/bin/env python3
"""
algo_alert_monitor.py
=====================
Monitors algo log + Redis for 5 types of alerts → Google Chat webhook.

Alert 1 — Trade Delay      : publish_desired_position seen but no FTRD within 10 mins
                             Only when desired_exec_lots changed from previous
Alert 2 — PnL Loss         : Symbol Day PnL < -20,000 (from Redis) — once per breach per symbol
Alert 3 — Position Mismatch: actual_lots > desired_exec_lots — once per breach per symbol
Alert 4 — Absolute Lot Size: sum(abs(lots)) > 50 — once per breach
Alert 5 — Log Stale        : No new log line for 30 mins during market hours

Features:
  - Market hours only: 09:00 - 15:45
  - Auto-detects latest log file every 5 mins
  - Switches to new log file automatically
  - PnL recovery notification
  - Startup Google Chat message with market summary
  - No cooldown — alerts fire on each new breach

Data sources:
  - Log  : SSH tail -f on colo (Alert 1, 5)
  - Redis: dashboard:positions:latest (Alert 2, 3, 4)

Usage:
  python algo_alert_monitor.py
"""

import json
import re
import threading
import time
import subprocess
from datetime import datetime, time as dtime
from collections import defaultdict

import redis
import requests

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════

COLO_HOST  = "192.168.74.138"
COLO_USER  = "Data_colo"
COLO_PASS  = "Datacolo@2026"
LOG_GLOB   = "/data/logs/*algo_1_*.log"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB   = 0
REDIS_KEY  = "dashboard:positions:latest"

# Thresholds
DELAY_THRESHOLD_SECS = 600      # Alert 1: 10 mins
PNL_LOSS_THRESHOLD   = -20000   # Alert 2: symbol Day PnL < -20,000
ABS_LOT_THRESHOLD    = 50       # Alert 4: total abs lots > 50
LOG_STALE_SECS       = 1800     # Alert 5: 30 mins
LOG_TS_LAG_SECS      = 1800     # Alert 5b: log timestamp lag > 30 mins

# Market hours (IST)
MARKET_START = dtime(9, 0)
MARKET_END   = dtime(15, 45)

# Intervals
REDIS_POLL_SECS    = 60
TIMER_CHECK_SECS   = 30
LOG_ROTATE_SECS    = 300   # check for new log file every 5 mins

# Google Chat Webhook
WEBHOOK_URL = (
    "https://chat.googleapis.com/v1/spaces/AAQAwvp8n0U/messages"
    "?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI"
    "&token=kdO8wFkx1DOtYe2kw136pkVNR3HD3cuSNJZmRNLH_wY"
)

# ══════════════════════════════════════════════════════════════════
# PATTERNS
# ══════════════════════════════════════════════════════════════════

# Runtime DES_POS — only publish_desired_position (skip init_from_open_positions)
# :publish_desired_position::DES_POS live stream apply success
#   contract_id=[66180] ... desired_exec_lots=[-4] previous_exec_lots=[0]
RE_DES_POS = re.compile(
    r"(\d{2}:\d{2}:\d{2}):\d+"
    r".*:publish_desired_position::DES_POS live stream apply success"
    r".*contract_id=\[(\d+)\]"
    r".*net_exec_lots=\[(-?\d+)\]"
    r".*previous_net_exec_lots=\[(-?\d+)\]"
)

# FTRD — trade fill
# token is field index 9 after FTRD:
RE_FTRD = re.compile(
    r"(\d{2}:\d{2}:\d{2}):\d+"
    r".*:emit_trade_fill::FTRD:"
    r"[\d.]+,"
    r"[\d.]+,"
    r"\d+,"
    r"\d+,"
    r"\d+,"
    r"\d+,"
    r"\d+,"
    r"\d+,"
    r"\d+,"
    r"(\d+),"
)

# ══════════════════════════════════════════════════════════════════
# SHARED STATE
# ══════════════════════════════════════════════════════════════════

# ── Unified cooldown tracker for all alerts ──────────────────────
# key: alert type + identifier (e.g. "delay_66180", "pnl_TCS", "mismatch_INFY", "abs_lots", "stale")
# value: datetime of last alert sent
COOLDOWN_SECS     = 600  # 10 mins for all alerts
alert_cooldown    = {}
cooldown_lock     = threading.Lock()

def cooldown_ok(key: str) -> bool:
    """Returns True if cooldown has passed for this alert key."""
    now = datetime.now()
    with cooldown_lock:
        last = alert_cooldown.get(key)
        if last is None or (now - last).total_seconds() >= COOLDOWN_SECS:
            alert_cooldown[key] = now
            return True
        return False

def cooldown_reset(key: str):
    """Reset cooldown for this key (e.g. on recovery)."""
    with cooldown_lock:
        alert_cooldown.pop(key, None)

# Token → symbol lookup (populated from Redis on each poll)
token_to_symbol   = {}
token_sym_lock    = threading.Lock()

# Alert 1: token -> {time, desired_exec_lots, alerted}
pending_des_pos   = {}
pending_lock      = threading.Lock()

# Alert 3: desired lots per token (from log)
desired_lots_map  = {}
desired_lots_lock = threading.Lock()

# Alert 2: track PnL breach state per symbol
pnl_in_breach      = {}  # symbol -> True/False
pnl_last_alert     = {}  # symbol -> datetime of last alert
PNL_COOLDOWN_SECS  = 600  # 10 min cooldown per symbol
pnl_breach_lock    = threading.Lock()

# Alert 4: abs lot breach state
abs_lot_in_breach = {"state": False}
abs_lot_lock      = threading.Lock()

# Alert 3: per symbol breach state
pos_mismatch_breached = {}
pos_mismatch_lock     = threading.Lock()

# Alert 5: last log line time
last_log_time      = {"ts": datetime.now()}
last_log_lock      = threading.Lock()
stale_alerted      = {"state": False}

# Alert 5b: last timestamp seen inside log lines
last_log_ts        = {"ts": datetime.now()}
last_log_ts_lock   = threading.Lock()
ts_lag_alerted     = {"state": False}

# Current log file being tailed
current_log_file  = {"path": None}
log_file_lock     = threading.Lock()

# Restart flag for log tailer
restart_tail      = {"flag": False}


# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════

def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def is_market_hours() -> bool:
    now = datetime.now().time()
    return MARKET_START <= now <= MARKET_END


def parse_time(hms: str) -> datetime:
    now = datetime.now()
    t   = datetime.strptime(hms, "%H:%M:%S")
    return now.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)


def send_gchat(message: str, cooldown_key: str = None) -> bool:
    """Send Google Chat alert. Returns True if sent successfully."""
    for attempt in range(3):
        try:
            resp = requests.post(
                WEBHOOK_URL,
                json={"text": message},
                timeout=15,
            )
            if resp.status_code == 200:
                log(f"[GCHAT] Alert sent ✅")
                return True
            else:
                log(f"[GCHAT] Failed: {resp.status_code} {resp.text[:100]}")
                return False
        except requests.exceptions.Timeout:
            log(f"[GCHAT] Timeout attempt {attempt+1}/3 — retrying ...")
            time.sleep(3)
        except Exception as e:
            log(f"[GCHAT] Error attempt {attempt+1}/3: {e}")
            time.sleep(3)

    log(f"[GCHAT] Failed after 3 attempts")
    # Reset cooldown so alert fires again on next check
    if cooldown_key:
        cooldown_reset(cooldown_key)
    return False


def get_latest_log() -> str:
    """Get latest algo log file from colo via SSH."""
    cmd = [
        "sshpass", "-p", COLO_PASS,
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        f"{COLO_USER}@{COLO_HOST}",
        f"ls -t {LOG_GLOB} 2>/dev/null | head -1"
    ]
    for attempt in range(3):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            path   = result.stdout.strip()
            if path:
                return path
        except subprocess.TimeoutExpired:
            log(f"[LOG_DETECT] Timeout attempt {attempt+1}/3 — retrying ...")
            time.sleep(5)
        except Exception as e:
            log(f"[LOG_DETECT] Error: {e}")
            time.sleep(5)
    log(f"[LOG_DETECT] Failed to detect log after 3 attempts")
    return None


# ══════════════════════════════════════════════════════════════════
# ALERT FORMATTERS
# ══════════════════════════════════════════════════════════════════

def alert_startup(log_file: str):
    now = datetime.now()
    msg = (
        f"✅ *Algo Alert Monitor Started*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Date          : `{now.strftime('%A %d-%b-%Y')}`\n"
        f"Time          : `{now.strftime('%H:%M:%S')}`\n"
        f"Market Hours  : `09:00 - 15:45`\n"
        f"Log File      : `{log_file}`\n"
        f"Colo          : `{COLO_HOST}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Active Alerts :\n"
        f"  1️⃣ Trade Delay > 10 mins\n"
        f"  2️⃣ Day PnL < ₹{PNL_LOSS_THRESHOLD:,}\n"
        f"  3️⃣ Actual Lots > Desired Lots\n"
        f"  4️⃣ Total Abs Lots > {ABS_LOT_THRESHOLD}\n"
        f"  5️⃣ Log Stale > 30 mins\n"
        f"  5️⃣b Log Timestamp Lag > 30 mins"
    )
    send_gchat(msg)


def alert_trade_delay(delays: list):
    """
    Send combined trade delay alert.
    delays: list of (token, symbol, des_time, elapsed_secs, desired_exec_lots)
    """
    rows = "\n".join(
        f"`{sym:<12}  {des_time.strftime('%H:%M:%S')}  {elapsed//60}m{elapsed%60:02d}s  lots={lots:+d}`"
        for token, sym, des_time, elapsed, lots in delays
    )
    msg = (
        f"🚨 *ALERT 1 — Trade Delay*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Time      : `{datetime.now().strftime('%H:%M:%S')}`\n"
        f"Threshold : `10 mins`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"`{'Symbol':<12}  {'DesiredAt':>8}  {'Delay':>8}  {'Lots':>6}`\n"
        f"{rows}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ {len(delays)} token(s) not executed within threshold!"
    )
    send_gchat(msg, f"delay_{delays[0][0]}")


def alert_pnl_loss(breaches: list):
    """
    Send combined PnL loss alert.
    breaches: list of (symbol, day_pnl)
    """
    rows = "\n".join(
        f"`{sym:<12}  ₹{pnl:>12,.0f}`"
        for sym, pnl in breaches
    )
    msg = (
        f"🔴 *ALERT 2 — PnL Loss Breach*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Time      : `{datetime.now().strftime('%H:%M:%S')}`\n"
        f"Threshold : `₹{PNL_LOSS_THRESHOLD:,.0f}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"`{'Symbol':<12}  {'Day PnL':>12}`\n"
        f"{rows}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ {len(breaches)} symbol(s) crossed PnL threshold!"
    )
    send_gchat(msg)


def alert_pnl_recovery(symbol: str, day_pnl: float):
    msg = (
        f"✅ *ALERT 2 — Symbol PnL Loss Recovered*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Symbol     : `{symbol}`\n"
        f"Day PnL    : `₹{day_pnl:,.0f}`\n"
        f"Threshold  : `₹{PNL_LOSS_THRESHOLD:,.0f}`\n"
        f"Time       : `{datetime.now().strftime('%H:%M:%S')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ {symbol} Day PnL recovered above loss threshold!"
    )
    send_gchat(msg)


def alert_position_mismatch(mismatches: list):
    """
    Send combined alert for all mismatching symbols.
    mismatches: list of (symbol, actual_lots, desired_lots)
    """
    rows = "\n".join(
        f"`{sym:<12}  {actual:>+7.1f}  {desired:>+7d}`"
        for sym, actual, desired in mismatches
    )
    msg = (
        f"⚠️ *ALERT 3 — Position Mismatch*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Time : `{datetime.now().strftime('%H:%M:%S')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"`{'Symbol':<12}  {'Actual':>7}  {'Desired':>7}`\n"
        f"{rows}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ {len(mismatches)} symbol(s) exceed desired position!"
    )
    send_gchat(msg)


def alert_abs_lot_breach(total_abs_lots: float, positions: list):
    """
    positions: list of (symbol, lots) sorted by abs lots desc
    """
    rows = "\n".join(
        f"`{sym:<12}  {lots:>+8.1f}`"
        for sym, lots in positions
    )
    msg = (
        f"📊 *ALERT 4 — Absolute Lot Size Breach*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total Abs Lots : `{total_abs_lots:.1f}`\n"
        f"Threshold      : `{ABS_LOT_THRESHOLD}`\n"
        f"Time           : `{datetime.now().strftime('%H:%M:%S')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"`{'Symbol':<12}  {'Lots':>8}`\n"
        f"{rows}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ Total absolute lot size exceeded threshold!"
    )
    send_gchat(msg, "abs_lots")


def alert_abs_lot_recovered(total_abs_lots: float):
    msg = (
        f"✅ *ALERT 4 — Abs Lot Size Recovered*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total Abs Lots : `{total_abs_lots:.1f}`\n"
        f"Threshold      : `{ABS_LOT_THRESHOLD}`\n"
        f"Time           : `{datetime.now().strftime('%H:%M:%S')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Total abs lots back within threshold!"
    )
    send_gchat(msg)


def alert_log_stale(mins: int, log_file: str):
    msg = (
        f"🔇 *ALERT 5 — Log Stale*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"No new log line for : `{mins} mins`\n"
        f"Threshold           : `30 mins`\n"
        f"Log file            : `{log_file}`\n"
        f"Time                : `{datetime.now().strftime('%H:%M:%S')}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ Algo may have stopped or disconnected!"
    )
    send_gchat(msg, "stale")


def alert_log_timestamp_lag(lag_mins: int, last_ts: str, log_file: str):
    msg = (
        f"⏱️ *ALERT 5b — Log Timestamp Lag*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Last log timestamp  : `{last_ts}`\n"
        f"Current time        : `{datetime.now().strftime('%H:%M:%S')}`\n"
        f"Lag                 : `{lag_mins} mins`\n"
        f"Threshold           : `30 mins`\n"
        f"Log file            : `{log_file}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ Log is active but timestamps are lagging — algo may be replaying old data!"
    )
    send_gchat(msg, "ts_lag")


def alert_log_rotated(old_log: str, new_log: str):
    msg = (
        f"🔄 *Log File Rotated*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Old : `{old_log}`\n"
        f"New : `{new_log}`\n"
        f"Time: `{datetime.now().strftime('%H:%M:%S')}`"
    )
    send_gchat(msg)


# ══════════════════════════════════════════════════════════════════
# LINE PROCESSOR
# ══════════════════════════════════════════════════════════════════

RE_LOG_TS = re.compile(r"^\s*(\d{2}:\d{2}:\d{2}):")

def process_line(line: str):
    # Update last log activity time (Alert 5)
    with last_log_lock:
        last_log_time["ts"] = datetime.now()
        stale_alerted["state"] = False

    # Extract timestamp from log line (Alert 5b)
    m_ts = RE_LOG_TS.match(line)
    if m_ts:
        try:
            t = datetime.strptime(m_ts.group(1), "%H:%M:%S")
            now = datetime.now()
            log_ts = now.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=0)
            with last_log_ts_lock:
                last_log_ts["ts"] = log_ts
                ts_lag_alerted["state"] = False  # reset if timestamps are fresh
        except Exception:
            pass

    if not is_market_hours():
        return

    # ── Alert 1: Runtime DES_POS only ────────────────────────────
    m = RE_DES_POS.search(line)
    if m:
        hms               = m.group(1)
        token             = m.group(2)
        desired_exec_lots = int(m.group(3))
        previous_exec_lots= int(m.group(4))

        # Only alert when desired actually changed
        if desired_exec_lots == previous_exec_lots:
            return
        if desired_exec_lots == 0:
            return

        des_time = parse_time(hms)
        with pending_lock:
            pending_des_pos[token] = {
                "time":              des_time,
                "desired_exec_lots": desired_exec_lots,
                "alerted":           False,
            }
        with desired_lots_lock:
            desired_lots_map[token] = desired_exec_lots

        log(f"[DES_POS] Token {token} at {hms} "
            f"desired={desired_exec_lots} prev={previous_exec_lots}")
        return

    # ── Alert 1: FTRD — clear timer ───────────────────────────────
    m = RE_FTRD.search(line)
    if m:
        hms   = m.group(1)
        token = m.group(2)
        with pending_lock:
            if token in pending_des_pos and not pending_des_pos[token]["alerted"]:
                des_time  = pending_des_pos[token]["time"]
                ftrd_time = parse_time(hms)
                elapsed   = int((ftrd_time - des_time).total_seconds())
                mins      = elapsed // 60
                secs      = elapsed % 60
                log(f"[FTRD] Token {token} filled (gap: {mins}m {secs}s) ✅")
                del pending_des_pos[token]


# ══════════════════════════════════════════════════════════════════
# ALERT 1 TIMER CHECKER
# ══════════════════════════════════════════════════════════════════

def timer_checker():
    while True:
        time.sleep(TIMER_CHECK_SECS)
        if not is_market_hours():
            continue
        now = datetime.now()
        delays_to_alert = []
        with pending_lock:
            for token, info in list(pending_des_pos.items()):
                elapsed = int((now - info["time"]).total_seconds())
                if elapsed >= DELAY_THRESHOLD_SECS:
                    if cooldown_ok(f"delay_{token}"):
                        with token_sym_lock:
                            sym = token_to_symbol.get(token, token)
                        delays_to_alert.append((
                            token, sym,
                            info["time"], elapsed,
                            info["desired_exec_lots"]
                        ))
                    pending_des_pos[token]["alerted"] = True
        if delays_to_alert:
            log(f"[DELAY] {len(delays_to_alert)} tokens delayed: "
                f"{[s for _,s,_,_,_ in delays_to_alert]}")
            alert_trade_delay(delays_to_alert)


# ══════════════════════════════════════════════════════════════════
# ALERT 2, 3, 4 — REDIS POLLER
# ══════════════════════════════════════════════════════════════════

def redis_poller():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    log(f"[REDIS] Connected to {REDIS_HOST}:{REDIS_PORT} DB{REDIS_DB}")

    while True:
        try:
            if not is_market_hours():
                time.sleep(REDIS_POLL_SECS)
                continue

            raw = r.get(REDIS_KEY)
            if not raw:
                log("[REDIS] No data in Redis")
                time.sleep(REDIS_POLL_SECS)
                continue

            data      = json.loads(raw)
            positions = data.get("positions", [])

            total_day_pnl  = 0.0
            total_abs_lots = 0.0
            abs_positions  = []  # (symbol, lots) for Alert 4
            symbol_day_pnl  = {}
            mismatch_to_alert = []

            for pos in positions:
                symbol   = pos.get("sym", "?")
                lot_size = float(pos.get("lot_size", 1) or 1)
                sym_pnl_accum = 0.0

                for exp in pos.get("expiries", []):
                    token    = str(exp.get("token", ""))
                    ltp      = float(exp.get("ltp", 0) or 0)
                    buy_avg  = float(exp.get("buy_avg", 0) or 0)
                    sell_avg = float(exp.get("sell_avg", 0) or 0)
                    qty_buy  = float(exp.get("qty_today_buy", 0) or 0)
                    qty_sell = float(exp.get("qty_today_sell", 0) or 0)

                    # Update token→symbol map
                    if token:
                        with token_sym_lock:
                            token_to_symbol[token] = symbol

                    # Calculate Day PnL
                    day_pnl  = (qty_buy * (ltp - buy_avg)) + (qty_sell * (sell_avg - ltp))

                    # Calculate Lots
                    open_qty = qty_buy - qty_sell
                    lots     = open_qty / lot_size if lot_size else 0

                    total_day_pnl  += day_pnl
                    sym_pnl_accum  += day_pnl
                    total_abs_lots += abs(lots)

                    if abs(lots) > 0:
                        abs_positions.append((symbol, lots))

                    # Alert 3 — position mismatch (combined message, cooldown per symbol)
                    if token:
                        with desired_lots_lock:
                            desired = desired_lots_map.get(token)
                        if desired is not None:
                            if abs(lots) > abs(desired):
                                mismatch_to_alert.append((symbol, lots, desired))

                # Store symbol day pnl after all expiries processed
                symbol_day_pnl[symbol] = sym_pnl_accum

            # Alert 3 — send combined message for all mismatching symbols
            # New symbols → alert immediately, existing → respect 10 min cooldown
            due_for_alert = [
                (sym, actual, desired)
                for sym, actual, desired in mismatch_to_alert
                if cooldown_ok(f"mismatch_{sym}")
            ]
            if due_for_alert:
                log(f"[MISMATCH] {len(due_for_alert)} symbols: "
                    f"{[s for s,_,_ in due_for_alert]}")
                alert_position_mismatch(due_for_alert)

            # Alert 2 — PnL breach/recovery per symbol (combined message)
            now = datetime.now()
            pnl_breaches_to_alert = []
            pnl_recoveries        = []

            for sym, sym_pnl in symbol_day_pnl.items():
                with pnl_breach_lock:
                    was_in_breach = pnl_in_breach.get(sym, False)
                    last_alert    = pnl_last_alert.get(sym)

                if sym_pnl < PNL_LOSS_THRESHOLD:
                    pnl_cd_ok = (
                        last_alert is None or
                        (now - last_alert).total_seconds() >= PNL_COOLDOWN_SECS
                    )
                    if pnl_cd_ok:
                        pnl_breaches_to_alert.append((sym, sym_pnl))
                        with pnl_breach_lock:
                            pnl_in_breach[sym]  = True
                            pnl_last_alert[sym] = now
                elif sym_pnl >= PNL_LOSS_THRESHOLD and was_in_breach:
                    pnl_recoveries.append((sym, sym_pnl))
                    with pnl_breach_lock:
                        pnl_in_breach[sym]  = False
                        pnl_last_alert[sym] = None

            # Send combined breach alert
            if pnl_breaches_to_alert:
                log(f"[PNL] Breach: {[s for s,_ in pnl_breaches_to_alert]}")
                alert_pnl_loss(pnl_breaches_to_alert)

            # Send recovery alerts individually
            for sym, sym_pnl in pnl_recoveries:
                log(f"[PNL] Recovered: {sym} {sym_pnl:,.0f}")
                alert_pnl_recovery(sym, sym_pnl)

            # Alert 4 — abs lot breach/recovery
            with abs_lot_lock:
                was_abs_breached = abs_lot_in_breach["state"]
            if total_abs_lots > ABS_LOT_THRESHOLD and not was_abs_breached:
                if cooldown_ok("abs_lots"):
                    log(f"[ABS_LOTS] Breach: {total_abs_lots:.1f}")
                    alert_abs_lot_breach(total_abs_lots, abs_positions[:20])
                with abs_lot_lock:
                    abs_lot_in_breach["state"] = True
            elif total_abs_lots <= ABS_LOT_THRESHOLD and was_abs_breached:
                log(f"[ABS_LOTS] Recovered: {total_abs_lots:.1f}")
                alert_abs_lot_recovered(total_abs_lots)
                cooldown_reset("abs_lots")
                with abs_lot_lock:
                    abs_lot_in_breach["state"] = False

            log(f"[REDIS] Day PnL={total_day_pnl:,.0f}  "
                f"Abs Lots={total_abs_lots:.1f}  Positions={len(positions)}")

        except Exception as e:
            log(f"[REDIS] Error: {e}")

        time.sleep(REDIS_POLL_SECS)


# ══════════════════════════════════════════════════════════════════
# ALERT 5 — LOG STALE CHECKER
# ══════════════════════════════════════════════════════════════════

def stale_checker():
    while True:
        time.sleep(60)
        if not is_market_hours():
            continue
        now = datetime.now()

        # Alert 5 — log stale (no new lines)
        with last_log_lock:
            elapsed     = int((now - last_log_time["ts"]).total_seconds())
            was_alerted = stale_alerted["state"]
        if elapsed >= LOG_STALE_SECS and not was_alerted:
            if cooldown_ok("stale"):
                mins = elapsed // 60
                with log_file_lock:
                    lf = current_log_file["path"] or "unknown"
                log(f"[STALE] No log update for {mins} mins")
                alert_log_stale(mins, lf)
            with last_log_lock:
                stale_alerted["state"] = True

        # Alert 5b — log timestamp lag (lines coming but timestamps are old)
        with last_log_ts_lock:
            last_ts      = last_log_ts["ts"]
            was_ts_alerted = ts_lag_alerted["state"]
        lag = int((now - last_ts).total_seconds())
        if lag >= LOG_TS_LAG_SECS and not was_ts_alerted:
            if cooldown_ok("ts_lag"):
                lag_mins = lag // 60
                with log_file_lock:
                    lf = current_log_file["path"] or "unknown"
                log(f"[TS_LAG] Log timestamp lagging by {lag_mins} mins")
                alert_log_timestamp_lag(lag_mins, last_ts.strftime("%H:%M:%S"), lf)
            with last_log_ts_lock:
                ts_lag_alerted["state"] = True


# ══════════════════════════════════════════════════════════════════
# LOG FILE ROTATION CHECKER
# ══════════════════════════════════════════════════════════════════

def log_rotation_checker():
    """Check every 5 mins if a newer log file exists — signal tailer to restart."""
    while True:
        time.sleep(LOG_ROTATE_SECS)
        new_log = get_latest_log()
        if not new_log:
            continue
        with log_file_lock:
            current = current_log_file["path"]
        if new_log != current:
            log(f"[ROTATE] New log detected: {new_log}")
            if current:
                alert_log_rotated(current, new_log)
            with log_file_lock:
                current_log_file["path"] = new_log
            restart_tail["flag"] = True


# ══════════════════════════════════════════════════════════════════
# LOG TAILER
# ══════════════════════════════════════════════════════════════════

def backfill_desired_lots(log_path: str):
    """
    Read last 2000 lines of log on startup to populate desired_lots_map.
    This ensures Alert 3 works immediately without waiting for new DES_POS lines.
    """
    log(f"[BACKFILL] Reading last 2000 lines from {log_path} ...")
    cmd = [
        "sshpass", "-p", COLO_PASS,
        "ssh", "-o", "StrictHostKeyChecking=no",
        f"{COLO_USER}@{COLO_HOST}",
        f"tail -n 2000 {log_path}"
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        count  = 0
        for line in result.stdout.splitlines():
            m = RE_DES_POS.search(line)
            if m:
                token             = m.group(2)
                desired_exec_lots = int(m.group(3))
                if desired_exec_lots != 0:
                    with desired_lots_lock:
                        desired_lots_map[token] = desired_exec_lots
                    count += 1
        log(f"[BACKFILL] Loaded {count} desired positions for {len(desired_lots_map)} tokens")
    except Exception as e:
        log(f"[BACKFILL] Error: {e}")


def tail_log():
    """SSH tail -f — auto reconnects, handles log rotation."""
    while True:
        with log_file_lock:
            log_path = current_log_file["path"]

        if not log_path:
            log("[TAIL] No log file found yet — retrying in 10s ...")
            time.sleep(10)
            continue

        cmd = [
            "sshpass", "-p", COLO_PASS,
            "ssh", "-o", "StrictHostKeyChecking=no",
            f"{COLO_USER}@{COLO_HOST}",
            f"tail -f {log_path}"
        ]
        log(f"[TAIL] Tailing: {log_path}")
        restart_tail["flag"] = False

        try:
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True,
            )
            for line in proc.stdout:
                process_line(line.strip())
                if restart_tail["flag"]:
                    log("[TAIL] Log rotation detected — restarting tail ...")
                    proc.kill()
                    break
            proc.wait()
            if not restart_tail["flag"]:
                log("[TAIL] Connection lost — retrying in 10s ...")
                time.sleep(10)
        except Exception as e:
            log(f"[TAIL] Error: {e} — retrying in 10s ...")
            time.sleep(10)


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    log("=" * 60)
    log("  ALGO ALERT MONITOR — Starting up ...")
    log(f"  Colo          : {COLO_HOST}")
    log(f"  Redis         : {REDIS_HOST}:{REDIS_PORT} DB{REDIS_DB}")
    log(f"  Market Hours  : {MARKET_START} - {MARKET_END}")
    log(f"  Alert 1       : Trade delay > {DELAY_THRESHOLD_SECS//60} mins")
    log(f"  Alert 2       : Day PnL < ₹{PNL_LOSS_THRESHOLD:,}")
    log(f"  Alert 3       : Actual lots > Desired lots")
    log(f"  Alert 4       : Total abs lots > {ABS_LOT_THRESHOLD}")
    log(f"  Alert 5       : Log stale > {LOG_STALE_SECS//60} mins")
    log("=" * 60)

    # Detect latest log file
    log("[INIT] Detecting latest log file ...")
    latest = get_latest_log()
    if latest:
        with log_file_lock:
            current_log_file["path"] = latest
        log(f"[INIT] Log file: {latest}")
    else:
        log("[INIT] WARNING: No log file found — will retry ...")

    # Send startup alert to Google Chat
    alert_startup(latest or "Not found yet")

    # Backfill desired lots from existing log
    if latest:
        backfill_desired_lots(latest)

    # Start background threads
    threads = [
        threading.Thread(target=timer_checker,       daemon=True, name="TimerChecker"),
        threading.Thread(target=redis_poller,        daemon=True, name="RedisPoller"),
        threading.Thread(target=stale_checker,       daemon=True, name="StaleChecker"),
        threading.Thread(target=log_rotation_checker,daemon=True, name="LogRotation"),
    ]
    for t in threads:
        t.start()
        log(f"[INIT] Thread started: {t.name}")

    # Main thread: tail log (blocking)
    tail_log()


if __name__ == "__main__":
    main()
