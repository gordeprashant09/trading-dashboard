#!/usr/bin/env python3
"""
validate_morning_table.py
=========================
Morning validation script — runs at 8:45 AM after bhavcopy push.
Sends one combined HTML-style report with table sections:

  1. EOD vs Dropcopy position match
  2. Redis DB1 position match (what dashboard loaded)
  3. Bhavcopy settlement vs Redis bhav_close prices

Important fix:
  - Zero positions are ignored on both sides for position comparisons.
    This prevents rows like Dropcopy=MISSING and EOD=0 being reported as issues.
  - Missing values are shown as MISSING instead of dash.

Usage:
  python3 validate_morning_table.py [YYYYMMDD]
  If no date given, uses previous trading day.

Cron:
  45 8 * * 1-5 cd /home/report/devstudio/Prashant/Live_Dashboard/Prod && \
    /home/report/devstudio/Prashant/Live_Dashboard/venv/bin/python3 \
    validate_morning_table.py >> validate_morning.log 2>&1
"""

from __future__ import annotations

import csv
import html
import inspect
import io
import json
import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import paramiko

# ── Config ────────────────────────────────────────────────────────────────────
SSH_HOST = "192.168.71.200"
SSH_PORT = 22
SSH_USER = "Data_colo"
SSH_PASS = "Datacolo@2026"

BHAVCOPY_DIR = Path("/home/report/devstudio/Prashant/Bhavcopy")
TARGET_EXPIRY = "2026-06-30"   # Update on series rollover
REDIS_DB_POS = 1                # Dashboard positions DB
REDIS_DB_LTP = 2                # Feeder / bhav_close DB

MAILER_DIR = Path("/media/svipl/Data/historical_data")
MAIL_TO = "prashant.gorde@subhkam.com"

DROPCOPY_FILE = "/data/trades/dropcopy_positions_eod.tsv"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("validate_morning")


# ── Generic helpers ───────────────────────────────────────────────────────────
def prev_trading_date() -> str:
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def read_remote(path: str) -> str:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS, timeout=10)
    sftp = client.open_sftp()
    try:
        with sftp.open(path, "r") as f:
            return f.read().decode("utf-8", errors="replace")
    finally:
        sftp.close()
        client.close()


def redis_get(db: int, *args: str) -> str:
    r = subprocess.run(
        ["redis-cli", "-n", str(db)] + list(args),
        capture_output=True,
        text=True,
        timeout=5,
    )
    return r.stdout.strip().strip('"')


def redis_scan(db: int, pattern: str) -> list[str]:
    r = subprocess.run(
        ["redis-cli", "-n", str(db), "--scan", "--pattern", pattern],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return r.stdout.strip().split()


def to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def qty_equal(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) < 1e-9


def fmt_qty(v: float | int | None) -> str:
    if v is None:
        return "MISSING"
    f = float(v)
    return str(int(f)) if f.is_integer() else f"{f:.4f}".rstrip("0").rstrip(".")


def fmt_price(v: float | int | None) -> str:
    if v is None:
        return "MISSING"
    return f"{float(v):.2f}"


def esc(v: Any) -> str:
    return html.escape(str(v))


def status_bad(status: str) -> bool:
    return status not in {"OK", "✅ OK"}


# ── Report rendering ──────────────────────────────────────────────────────────
def render_text_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    if not rows:
        return ["No rows"]
    all_rows = [headers] + [[str(c) for c in r] for r in rows]
    widths = [max(len(str(row[i])) for row in all_rows) for i in range(len(headers))]

    def line(row: list[Any]) -> str:
        out = []
        for i, value in enumerate(row):
            text = str(value)
            # Right-align numeric-looking columns except first/symbol/status.
            if i not in (0, 1, len(headers) - 1):
                out.append(text.rjust(widths[i]))
            else:
                out.append(text.ljust(widths[i]))
        return " | ".join(out)

    sep = "-+-".join("-" * w for w in widths)
    return [line(headers), sep] + [line(r) for r in rows]


def render_html_table(headers: list[str], rows: list[list[Any]], numeric_cols: set[int] | None = None) -> str:
    numeric_cols = numeric_cols or set()
    header_html = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body = []
    for row in rows:
        status = str(row[-1]) if row else ""
        tr_class = "ok" if status == "OK" else "bad"
        cells = []
        for i, value in enumerate(row):
            cls = "num" if i in numeric_cols else ""
            if i == len(row) - 1:
                if status == "OK":
                    cells.append(f'<td class="status oktext">✅ {esc(status)}</td>')
                else:
                    cells.append(f'<td class="status badtext">❌ {esc(status)}</td>')
            else:
                cells.append(f'<td class="{cls}">{esc(value)}</td>')
        body.append(f'<tr class="{tr_class}">' + "".join(cells) + "</tr>")
    return "<table>" + f"<thead><tr>{header_html}</tr></thead>" + "<tbody>" + "".join(body) + "</tbody></table>"


def build_html_report(dt: str, sections: list[dict[str, Any]], total_issues: int) -> str:
    status_text = "ALL CHECKS PASSED" if total_issues == 0 else "ACTION REQUIRED"
    status_class = "success" if total_issues == 0 else "failed"
    parts = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        "<style>",
        "body{font-family:Arial,Helvetica,sans-serif;background:#f6f8fb;color:#17202a;margin:0;padding:16px;}",
        ".wrap{max-width:1100px;margin:0 auto;background:#fff;border:1px solid #dde3ea;border-radius:10px;padding:18px;}",
        "h1{font-size:20px;margin:0 0 6px 0;}",
        "h2{font-size:16px;margin:22px 0 8px 0;border-bottom:1px solid #e5e9ef;padding-bottom:6px;}",
        ".summary{font-weight:bold;padding:10px 12px;border-radius:8px;margin:12px 0;}",
        ".success{background:#e9f8ef;color:#0b6b2f;border:1px solid #bfe7cc;}",
        ".failed{background:#fdecec;color:#9b1c1c;border:1px solid #f5b5b5;}",
        ".meta{font-size:13px;color:#526070;margin:4px 0 10px 0;}",
        "table{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px;}",
        "th{background:#1f3b63;color:#fff;text-align:left;padding:7px 8px;border:1px solid #d6dce4;}",
        "td{padding:6px 8px;border:1px solid #d6dce4;}",
        "tr:nth-child(even){background:#f7f9fc;}",
        "tr.bad{background:#fff4f4;}",
        ".num{text-align:right;font-variant-numeric:tabular-nums;}",
        ".status{font-weight:bold;white-space:nowrap;}",
        ".oktext{color:#087a36;}",
        ".badtext{color:#b42318;}",
        ".error{background:#fff4f4;border:1px solid #f5b5b5;color:#9b1c1c;padding:10px;border-radius:8px;white-space:pre-wrap;}",
        "</style></head><body><div class='wrap'>",
        f"<h1>Morning Validation Report — {esc(dt)}</h1>",
        f"<div class='summary {status_class}'>Total Issues: {total_issues} — {esc(status_text)}</div>",
    ]
    for section in sections:
        parts.append(f"<h2>{esc(section['title'])}</h2>")
        if section.get("meta"):
            parts.append(f"<div class='meta'>{esc(section['meta'])}</div>")
        if section.get("error"):
            parts.append(f"<div class='error'>{esc(section['error'])}</div>")
        else:
            parts.append(section["html_table"])
        result_cls = "success" if section["issues"] == 0 else "failed"
        result_text = "OK" if section["issues"] == 0 else f"{section['issues']} issues"
        parts.append(f"<div class='summary {result_cls}'>Result: {esc(result_text)}</div>")
    parts.append("</div></body></html>")
    return "\n".join(parts)


def send_mail(subject: str, text_body: str, html_body: str | None = None) -> None:
    """Send mail using existing mailer_helper.

    The helper used in this environment may differ between machines, so this
    function tries HTML-capable signatures first and falls back to normal body.
    """
    try:
        sys.path.insert(0, str(MAILER_DIR))
        from mailer_helper import send_mail as _send  # type: ignore

        body_to_send = html_body or text_body
        try:
            sig = inspect.signature(_send)
            params = sig.parameters
            if html_body and "html_body" in params:
                _send(subject=subject, body=text_body, html_body=html_body, receiver=MAIL_TO, cc=[])
            elif html_body and "is_html" in params:
                _send(subject=subject, body=html_body, receiver=MAIL_TO, cc=[], is_html=True)
            elif html_body and "html" in params:
                _send(subject=subject, body=html_body, receiver=MAIL_TO, cc=[], html=True)
            else:
                # Fallback: many internal helpers already send body as HTML if it starts with <!doctype html>.
                _send(subject=subject, body=body_to_send, receiver=MAIL_TO, cc=[])
        except TypeError:
            _send(subject=subject, body=body_to_send, receiver=MAIL_TO, cc=[])
        log.info("Mail sent: %s", subject)
    except Exception as e:
        log.warning("Mail send failed: %s", e)


# ── Section 1: EOD vs Dropcopy ────────────────────────────────────────────────
def check_eod_vs_dropcopy(dt: str) -> dict[str, Any]:
    eod_file = f"/data/Dashboard/Eod/eod_positions_{dt}.csv"
    headers = ["Token", "Symbol", "Dropcopy", "EOD", "Status"]

    try:
        eod_raw = read_remote(eod_file)
        drop_raw = read_remote(DROPCOPY_FILE)
    except Exception as e:
        return {"title": "1. EOD vs Dropcopy Position Check", "issues": 1, "error": f"ERROR reading files: {e}"}

    expected_date = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
    first_line = drop_raw.splitlines()[0] if drop_raw else ""
    if "generated_at=" not in first_line or expected_date not in first_line:
        msg = "\n".join([
            "ERROR: Dropcopy file is stale or wrong date.",
            f"Expected : {expected_date}",
            f"Header   : {first_line}",
        ])
        return {"title": "1. EOD vs Dropcopy Position Check", "issues": 1, "error": msg}

    # Load EOD. Important fix: ignore zero qty, same as dropcopy.
    eod: dict[str, dict[str, Any]] = {}
    for row in csv.DictReader(io.StringIO(eod_raw)):
        try:
            token = str(int(row["token"]))
            qty = float(row["qty_overnight"])
        except Exception:
            continue
        if qty == 0:
            continue
        eod[token] = {"symbol": row.get("name", "").strip().upper(), "qty": qty}

    # Load dropcopy. Zero qty is ignored.
    drop: dict[str, dict[str, Any]] = {}
    drop_lines = [l for l in drop_raw.splitlines() if l.strip() and not l.startswith("#")]
    for row in csv.DictReader(drop_lines, delimiter="\t"):
        try:
            token = str(int(row["token"]))
            qty = float(row["net_qty"])
        except Exception:
            continue
        if qty == 0:
            continue
        contract_key = row.get("contract_key", "")
        parts = contract_key.split("-")
        symbol = parts[1].strip().upper() if len(parts) > 1 else row.get("symbol", token).strip().upper()
        drop[token] = {"symbol": symbol, "qty": qty}

    rows: list[list[str]] = []
    issues = 0
    for token in sorted(set(drop) | set(eod), key=int):
        d = drop.get(token)
        e = eod.get(token)
        symbol = (d or e or {}).get("symbol", token)
        drop_qty = d["qty"] if d else None
        eod_qty = e["qty"] if e else None

        if d is None:
            status = "ONLY_EOD"
            issues += 1
        elif e is None:
            status = "MISSING_EOD"
            issues += 1
        elif not qty_equal(drop_qty, eod_qty):
            status = "MISMATCH"
            issues += 1
        else:
            status = "OK"

        rows.append([token, symbol, fmt_qty(drop_qty), fmt_qty(eod_qty), status])

    return {
        "title": "1. EOD vs Dropcopy Position Check",
        "issues": issues,
        "meta": f"Total non-zero positions checked: {len(rows)} | Issues: {issues}",
        "headers": headers,
        "rows": rows,
        "html_table": render_html_table(headers, rows, numeric_cols={2, 3}),
        "text_table": render_text_table(headers, rows),
    }


# ── Section 2: Redis DB1 positions ────────────────────────────────────────────
def check_redis_positions(dt: str) -> dict[str, Any]:
    headers = ["Symbol", "EOD Qty", "Redis Qty", "Status"]

    try:
        raw = redis_get(REDIS_DB_POS, "GET", "dashboard:positions:latest2")
        data = json.loads(raw)
    except Exception as e:
        return {"title": "2. Redis DB1 Position Check (Dashboard)", "issues": 1, "error": f"ERROR reading Redis positions: {e}"}

    as_of = data.get("as_of", "?")
    redis_pos: dict[str, float] = {}
    for st in data.get("positions", []):
        sym = str(st.get("sym", "")).strip().upper()
        for e in st.get("expiries", []):
            qty = to_float(e.get("qty_overnight", 0))
            if qty != 0 and sym:
                redis_pos[sym] = qty

    eod_file = f"/data/Dashboard/Eod/eod_positions_{dt}.csv"
    eod_pos: dict[str, float] = {}
    try:
        eod_raw = read_remote(eod_file)
        for row in csv.DictReader(io.StringIO(eod_raw)):
            sym = row.get("name", "").strip().upper()
            qty = to_float(row.get("qty_overnight", 0))
            if qty != 0 and sym:
                eod_pos[sym] = qty
    except Exception as e:
        return {"title": "2. Redis DB1 Position Check (Dashboard)", "issues": 1, "error": f"ERROR reading EOD: {e}"}

    rows: list[list[str]] = []
    issues = 0
    for sym in sorted(set(redis_pos) | set(eod_pos)):
        rqty = redis_pos.get(sym)
        eqty = eod_pos.get(sym)
        if rqty is None:
            status = "MISSING_REDIS"
            issues += 1
        elif eqty is None:
            status = "ONLY_REDIS"
            issues += 1
        elif not qty_equal(rqty, eqty):
            status = "MISMATCH"
            issues += 1
        else:
            status = "OK"
        rows.append([sym, fmt_qty(eqty), fmt_qty(rqty), status])

    return {
        "title": "2. Redis DB1 Position Check (Dashboard)",
        "issues": issues,
        "meta": f"As of: {as_of} | Total non-zero positions checked: {len(rows)} | Issues: {issues}",
        "headers": headers,
        "rows": rows,
        "html_table": render_html_table(headers, rows, numeric_cols={1, 2}),
        "text_table": render_text_table(headers, rows),
    }


# ── Section 3: Bhavcopy vs Redis bhav_close ───────────────────────────────────
def check_bhavcopy_prices() -> dict[str, Any]:
    headers = ["Symbol", "Bhavcopy", "Redis", "Diff", "Status"]

    files = sorted(BHAVCOPY_DIR.glob("BhavCopy_*_FUT.csv"))
    if not files:
        return {"title": "3. Bhavcopy Settlement vs Redis bhav_close", "issues": 1, "error": "ERROR: No BhavCopy_*_FUT.csv found"}
    bhav_file = files[-1]

    bhav: dict[str, float] = {}
    with open(bhav_file, newline="") as f:
        for row in csv.DictReader(f):
            sym = row.get("symbol", "").strip().upper()
            exp = row.get("expiry", "").strip()
            if exp == TARGET_EXPIRY and sym and sym not in bhav:
                price_s = row.get("settlement_price", "") or row.get("close", "")
                price = to_float(price_s, 0)
                if price > 0:
                    bhav[sym] = price

    try:
        raw = redis_get(REDIS_DB_POS, "GET", "dashboard:positions:latest2")
        data = json.loads(raw)
        symbols = sorted({
            str(st.get("sym", "")).strip().upper()
            for st in data.get("positions", [])
            for e in st.get("expiries", [])
            if to_float(e.get("qty_overnight", 0)) != 0 and st.get("sym")
        })
    except Exception:
        symbols = sorted(list(bhav.keys())[:20])

    rows: list[list[str]] = []
    issues = 0
    for sym in symbols:
        bprice = bhav.get(sym, 0.0)
        keys = [k for k in redis_scan(REDIS_DB_LTP, f"fo:stock_future:{sym}:*JUN*") if "FUT" in k]
        rprice = 0.0
        if keys:
            try:
                rprice = float(redis_get(REDIS_DB_LTP, "hget", keys[0], "bhav_close"))
            except Exception:
                rprice = 0.0

        if bprice == 0:
            status = "NO_BHAV"
            issues += 1
        elif rprice == 0:
            status = "NO_REDIS"
            issues += 1
        elif abs(bprice - rprice) < 1:
            status = "OK"
        else:
            status = "MISMATCH"
            issues += 1

        diff = bprice - rprice if bprice and rprice else 0.0
        rows.append([sym, fmt_price(bprice), fmt_price(rprice), f"{diff:+.2f}", status])

    return {
        "title": "3. Bhavcopy Settlement vs Redis bhav_close",
        "issues": issues,
        "meta": f"File: {bhav_file.name} | Target expiry: {TARGET_EXPIRY} | Symbols checked: {len(rows)} | Issues: {issues}",
        "headers": headers,
        "rows": rows,
        "html_table": render_html_table(headers, rows, numeric_cols={1, 2, 3}),
        "text_table": render_text_table(headers, rows),
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def build_text_report(dt: str, sections: list[dict[str, Any]], total_issues: int) -> str:
    lines: list[str] = []
    lines += ["=" * 80, f"Morning Validation Report - {dt}", "=" * 80, ""]
    for sec in sections:
        lines.append(sec["title"])
        lines.append("-" * len(sec["title"]))
        if sec.get("meta"):
            lines.append(sec["meta"])
        if sec.get("error"):
            lines.append(sec["error"])
        else:
            lines += sec.get("text_table", [])
        lines.append(f"Result: {'OK' if sec['issues'] == 0 else str(sec['issues']) + ' issues'}")
        lines.append("")
    lines += ["=" * 80, f"TOTAL ISSUES: {total_issues}", "ALL CHECKS PASSED" if total_issues == 0 else "ACTION REQUIRED", "=" * 80]
    return "\n".join(lines)


def main() -> int:
    dt = sys.argv[1] if len(sys.argv) > 1 else prev_trading_date()
    log.info("Validating for date: %s", dt)

    sections = [
        check_eod_vs_dropcopy(dt),
        check_redis_positions(dt),
        check_bhavcopy_prices(),
    ]
    total_issues = sum(int(sec.get("issues", 0)) for sec in sections)

    text_report = build_text_report(dt, sections, total_issues)
    html_report = build_html_report(dt, sections, total_issues)

    print(text_report)

    status = "SUCCESS" if total_issues == 0 else "FAILED"
    subject = f"[{status}] Morning Validation - {dt} - Issues: {total_issues}"
    send_mail(subject, text_report, html_report)

    return 0 if total_issues == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
