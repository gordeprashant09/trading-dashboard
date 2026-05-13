"""
generate_eod.py
===============
Run this script at End of Day (after 3:30 PM) to generate
eod_positions.csv from today's log file.

This CSV is used by dashboard_worker.py next morning as
overnight positions (qty_overnight + prev_close).

Output:
    [SSH] 192.168.74.138:/data/Dashboard/eod_positions.csv

Usage:
    python generate_eod.py              # uses today's date
    python generate_eod.py 20260509     # uses specific date

Format of output CSV:
    token, symbol, qty_overnight, prev_close, date
"""

from __future__ import annotations

import os
import re
import sys
import csv
import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from io import StringIO

import paramiko

# ══════════════════════════════════════════════════════════════
# CONFIG — same as dashboard_worker.py
# ══════════════════════════════════════════════════════════════
SSH_HOST     = os.getenv("SSH_HOST",     "192.168.74.138")
SSH_PORT     = int(os.getenv("SSH_PORT", "22"))
SSH_USER     = os.getenv("SSH_USER",     "Data_colo")
SSH_PASS     = os.getenv("SSH_PASS",     "Datacolo@2026")

REMOTE_LOG_DIR       = os.getenv("REMOTE_LOG_DIR",       "/data/logs")
REMOTE_DASHBOARD_DIR = os.getenv("REMOTE_DASHBOARD_DIR", "/data/Dashboard")
REMOTE_PCAP_DIR      = os.getenv("REMOTE_PCAP_DIR",      "/data/pcapdata")

PRICE_DIVISOR = 100.0
NSE_OFFSET    = 315513000   # seconds

OUTPUT_FILE = f"{REMOTE_DASHBOARD_DIR}/eod_positions.csv"

# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("generate_eod")
logging.getLogger("paramiko").setLevel(logging.WARNING)

# ══════════════════════════════════════════════════════════════
# REGEX
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

# ══════════════════════════════════════════════════════════════
# SSH HELPERS
# ══════════════════════════════════════════════════════════════

def get_ssh_client() -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, port=SSH_PORT,
                   username=SSH_USER, password=SSH_PASS, timeout=10)
    return client


def read_remote_file_lines(remote_path: str) -> list[str]:
    try:
        client = get_ssh_client()
        sftp = client.open_sftp()
        try:
            with sftp.open(remote_path, "r") as f:
                lines = f.read().decode("utf-8", errors="replace").splitlines()
            log.info("Read %d lines from %s:%s", len(lines), SSH_HOST, remote_path)
            return lines
        except FileNotFoundError:
            log.warning("File not found: %s:%s", SSH_HOST, remote_path)
            return []
        finally:
            sftp.close()
            client.close()
    except Exception as e:
        log.error("SSH read failed: %s", e)
        return []


def write_remote_file(remote_path: str, content: str):
    try:
        client = get_ssh_client()
        sftp = client.open_sftp()
        with sftp.open(remote_path, "w") as f:
            f.write(content)
        sftp.close()
        client.close()
        log.info("Written to %s:%s", SSH_HOST, remote_path)
    except Exception as e:
        log.error("SSH write failed: %s", e)


def run_remote_cmd(cmd: str) -> str:
    try:
        client = get_ssh_client()
        _, stdout, _ = client.exec_command(cmd)
        result = stdout.read().decode().strip()
        client.close()
        return result
    except Exception as e:
        log.error("SSH cmd failed: %s", e)
        return ""

# ══════════════════════════════════════════════════════════════
# DATE / PATH HELPERS
# ══════════════════════════════════════════════════════════════

def today_str() -> str:
    return date.today().strftime("%Y%m%d")


def detect_latest_log() -> tuple[str, str]:
    """
    Returns (log_path, date_str) of the latest log on remote.
    """
    path = run_remote_cmd(
        f"ls -t {REMOTE_LOG_DIR}/Sample-Strategy-excution_algo_1_*.log 2>/dev/null | head -1"
    )
    if not path:
        raise FileNotFoundError("No log file found on remote server")
    # Extract date from filename
    m = re.search(r"(\d{8})", os.path.basename(path))
    dt = m.group(1) if m else today_str()
    return path, dt


def find_contract_file(dt: str) -> str:
    """Find contract CSV for given date, fallback to latest."""
    primary = f"{REMOTE_PCAP_DIR}/{dt}/fo_contract_stream_info_{dt}.csv"
    try:
        client = get_ssh_client()
        sftp = client.open_sftp()
        try:
            sftp.stat(primary)
            sftp.close(); client.close()
            return primary
        except FileNotFoundError:
            pass
        sftp.close()
        latest = run_remote_cmd(
            f"ls -t {REMOTE_PCAP_DIR}/*/fo_contract_stream_info_*.csv 2>/dev/null | head -1"
        )
        client.close()
        return latest if latest else primary
    except Exception as e:
        log.error("Contract file search failed: %s", e)
        return primary

# ══════════════════════════════════════════════════════════════
# CONTRACT TOKEN MAP
# ══════════════════════════════════════════════════════════════

def load_token_map(contract_path: str) -> dict[int, dict]:
    lines = read_remote_file_lines(contract_path)
    token_map = {}

    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) < 8:
            continue
        if parts[0].strip().isdigit():
            continue

        try:
            token     = int(parts[2].strip())
            inst_type = parts[3].strip().upper()
            name      = parts[4].strip().upper()
            expiry_ts = int(parts[5].strip())
            strike    = float(parts[6].strip()) / 100.0
            itype     = parts[7].strip().upper()

            # NSE epoch fix
            adj_ts   = expiry_ts + NSE_OFFSET if str(expiry_ts).startswith("14") else expiry_ts
            exp_date = datetime.fromtimestamp(adj_ts, tz=ZoneInfo("Asia/Kolkata"))
            exp_str  = exp_date.strftime("%y%b").upper()

            is_future = inst_type in ("FUTSTK", "FUTIDX") or itype == "XX"
            if is_future:
                tsym = f"{name}{exp_str}FUT"
            else:
                strike_str = str(int(strike)) if strike == int(strike) else str(strike)
                tsym = f"{name}{exp_str}{strike_str}{itype}"

            token_map[token] = {
                "name":      name,
                "tsym":      tsym,
                "is_future": is_future,
            }
        except (ValueError, IndexError):
            continue

    log.info("Token map loaded: %d contracts", len(token_map))
    return token_map

# ══════════════════════════════════════════════════════════════
# PARSE LOG → EOD POSITIONS
# ══════════════════════════════════════════════════════════════

def parse_eod_from_log(log_path: str) -> dict[int, dict]:
    """
    Parse all FTRD lines from log.
    Returns { token: { net_qty, last_price, buy_qty, sell_qty } }
    net_qty    = total_buy - total_sell  → qty_overnight for next day
    last_price = last fill price         → prev_close for next day
    """
    lines = read_remote_file_lines(log_path)
    eod: dict[int, dict] = {}

    # Deduplicate by (token, fillnumber) — same as worker
    seen: dict[tuple, dict] = {}
    for line in lines:
        m = FTRD_RE.search(line)
        if not m:
            continue
        try:
            token      = int(m.group(10))
            fillnumber = int(m.group(7))
            seen[(token, fillnumber)] = {
                "token":      token,
                "buy_sell":   int(m.group(3)),
                "fillqty":    int(m.group(8)),
                "fillprice":  int(m.group(9)) / PRICE_DIVISOR,
                "fillnumber": fillnumber,
            }
        except (ValueError, IndexError):
            continue

    # Aggregate
    for fill in seen.values():
        token = fill["token"]
        if token not in eod:
            eod[token] = {
                "buy_qty":    0.0,
                "sell_qty":   0.0,
                "last_price": 0.0,
            }
        if fill["buy_sell"] == 1:
            eod[token]["buy_qty"]  += fill["fillqty"]
        else:
            eod[token]["sell_qty"] += fill["fillqty"]
        eod[token]["last_price"] = fill["fillprice"]  # keep updating → last price

    # Compute net qty
    result = {}
    for token, v in eod.items():
        net_qty = v["buy_qty"] - v["sell_qty"]
        result[token] = {
            "net_qty":    net_qty,
            "last_price": v["last_price"],
            "buy_qty":    v["buy_qty"],
            "sell_qty":   v["sell_qty"],
        }

    log.info("EOD positions computed: %d tokens", len(result))
    return result

# ══════════════════════════════════════════════════════════════
# GENERATE EOD CSV
# ══════════════════════════════════════════════════════════════

def generate_eod_csv(dt: str = None):
    # Step 1: detect log file
    log_path, log_dt = detect_latest_log()
    dt = dt or log_dt
    log.info("Using log: %s (date=%s)", log_path, dt)

    # Step 2: load contract map
    contract_path = find_contract_file(dt)
    log.info("Using contract: %s", contract_path)
    token_map = load_token_map(contract_path)

    # Step 3: parse EOD from log
    eod = parse_eod_from_log(log_path)

    # Step 4: build CSV rows
    rows = []
    for token, v in eod.items():
        info = token_map.get(token)
        if not info:
            log.warning("Token %d not in contract map — skipping", token)
            continue

        rows.append({
            "token":          token,
            "symbol":         info["tsym"],
            "name":           info["name"],
            "qty_overnight":  v["net_qty"],
            "prev_close":     round(v["last_price"], 2),
            "buy_qty":        v["buy_qty"],
            "sell_qty":       v["sell_qty"],
            "date":           dt,
            "generated_at":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    if not rows:
        log.error("No rows generated — check log and contract file")
        return

    # Step 5: write CSV to remote
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=[
        "token", "symbol", "name",
        "qty_overnight", "prev_close",
        "buy_qty", "sell_qty",
        "date", "generated_at"
    ])
    writer.writeheader()
    writer.writerows(rows)

    write_remote_file(OUTPUT_FILE, buf.getvalue())

    # Step 6: summary
    log.info("=" * 55)
    log.info("  EOD CSV generated successfully!")
    log.info("  Date     : %s", dt)
    log.info("  Tokens   : %d", len(rows))
    log.info("  Output   : %s:%s", SSH_HOST, OUTPUT_FILE)
    log.info("=" * 55)

    # Print summary table
    print("\n── EOD Position Summary ──────────────────────────────")
    print(f"{'Symbol':<30} {'Net Qty':>10} {'Prev Close':>12}")
    print("-" * 55)
    for r in sorted(rows, key=lambda x: x["symbol"]):
        net = r["qty_overnight"]
        direction = "LONG" if net > 0 else ("SHORT" if net < 0 else "FLAT")
        print(f"{r['symbol']:<30} {net:>10.0f} {r['prev_close']:>12.2f}  {direction}")
    print("-" * 55)
    print(f"Total positions: {len(rows)}")
    print()


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    dt = sys.argv[1] if len(sys.argv) > 1 else None
    generate_eod_csv(dt)
