#!/usr/bin/env python3
"""
validate_eod_vs_dropcopy_fixed.py
=================================

Validates EOD positions against dropcopy EOD positions.

Fixes:
  1. Zero quantity rows are ignored on BOTH sides.
     - EOD qty_overnight == 0 is ignored.
     - Dropcopy net_qty == 0 is ignored.
     - This removes false MISSING/0 or blank/0 issues.
  2. Email is sent as real HTML using mailer_helper(is_html=True).
  3. Output email uses the same clean table style as validate_morning.
  4. Console output remains plain text.

Usage:
  python3 validate_eod_vs_dropcopy_fixed.py YYYYMMDD
"""

from __future__ import annotations

import csv
import html
import io
import sys
from pathlib import Path
from typing import Any

import paramiko

# ── Config ────────────────────────────────────────────────────────────────────
SSH_HOST = "192.168.71.200"
SSH_PORT = 22
SSH_USER = "Data_colo"
SSH_PASS = "Datacolo@2026"

MAILER_DIR = Path("/home/report/devstudio/Prashant/Live_Dashboard/Prod")
MAIL_TO = "prashant.gorde@subhkam.com"

dt = sys.argv[1] if len(sys.argv) > 1 else "20260610"

EOD_FILE = f"/data/Dashboard/Eod/eod_positions_{dt}.csv"
DROPCOPY_FILE = "/data/trades/dropcopy_positions_eod.tsv"

EPS = 1e-9


# ── Helpers ───────────────────────────────────────────────────────────────────
def read_remote(path: str) -> str:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        SSH_HOST,
        port=SSH_PORT,
        username=SSH_USER,
        password=SSH_PASS,
        timeout=10,
    )
    sftp = client.open_sftp()
    try:
        with sftp.open(path, "r") as f:
            return f.read().decode("utf-8", errors="replace")
    finally:
        sftp.close()
        client.close()


def send_report_mail(subject: str, text_body: str, html_body: str | None = None) -> None:
    """
    Send email through local mailer_helper.

    Important:
      mailer_helper supports is_html=True. Without this, Gmail shows raw HTML.
    """
    sys.path.insert(0, str(MAILER_DIR))
    from mailer_helper import send_mail

    if html_body:
        send_mail(
            subject=subject,
            body=html_body,
            receiver=MAIL_TO,
            cc=[],
            is_html=True,
        )
    else:
        send_mail(
            subject=subject,
            body=text_body,
            receiver=MAIL_TO,
            cc=[],
            is_html=False,
        )


def safe_send_mail(subject: str, text_body: str, html_body: str | None = None) -> None:
    try:
        send_report_mail(subject, text_body, html_body)
    except Exception as e:
        print(f"WARN: mail send failed/non-fatal: {e}")


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def is_zero_qty(value: Any) -> bool:
    return abs(to_float(value)) < EPS


def qty_equal(a: float | None, b: float | None) -> bool:
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) < EPS


def fmt_qty(value: float | int | None) -> str:
    if value is None:
        return "MISSING"
    v = float(value)
    return str(int(v)) if v.is_integer() else f"{v:.4f}".rstrip("0").rstrip(".")


def esc(value: Any) -> str:
    return html.escape(str(value))


def validate_dropcopy_date(drop_raw: str, dt: str) -> tuple[bool, str]:
    lines = drop_raw.splitlines()
    first_line = lines[0] if lines else ""
    expected_date = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"

    if "generated_at=" not in first_line:
        return False, (
            "ERROR: Dropcopy EOD file header does not contain generated_at.\n"
            f"Expected date : {expected_date}\n"
            f"Header        : {first_line}\n"
            f"Dropcopy file : {DROPCOPY_FILE}\n"
        )

    if expected_date not in first_line:
        return False, (
            "ERROR: Dropcopy EOD file is stale/wrong date.\n"
            f"Expected date : {expected_date}\n"
            f"Header        : {first_line}\n"
            f"EOD file      : {EOD_FILE}\n"
            f"Dropcopy file : {DROPCOPY_FILE}\n"
        )

    return True, ""


# ── Loaders ───────────────────────────────────────────────────────────────────
def load_eod_positions(eod_raw: str) -> dict[str, dict[str, Any]]:
    """
    Load EOD non-zero positions only.

    This is the key fix for false ONLY_EOD rows:
    if EOD has qty 0 and Dropcopy qty 0 is skipped, EOD must also be skipped.
    """
    eod: dict[str, dict[str, Any]] = {}

    for row in csv.DictReader(io.StringIO(eod_raw)):
        token_raw = str(row.get("token", "")).strip()
        if not token_raw:
            continue

        try:
            token = str(int(float(token_raw)))
        except ValueError:
            continue

        qty = to_float(row.get("qty_overnight", 0))
        if is_zero_qty(qty):
            continue

        eod[token] = {
            "symbol": str(row.get("name", "")).strip().upper(),
            "qty": qty,
        }

    return eod


def load_dropcopy_positions(drop_raw: str) -> dict[str, dict[str, Any]]:
    """
    Load Dropcopy non-zero positions only.
    """
    drop: dict[str, dict[str, Any]] = {}

    drop_lines = [
        line for line in drop_raw.splitlines()
        if line.strip() and not line.startswith("#")
    ]

    reader = csv.DictReader(drop_lines, delimiter="\t")

    for row in reader:
        token_raw = str(row.get("token", "")).strip()
        if not token_raw:
            continue

        try:
            token = str(int(float(token_raw)))
        except ValueError:
            continue

        qty = to_float(row.get("net_qty", 0))
        if is_zero_qty(qty):
            continue

        contract_key = str(row.get("contract_key", "")).strip()
        parts = contract_key.split("-")
        symbol = parts[1].strip().upper() if len(parts) > 1 else contract_key.upper()

        drop[token] = {
            "symbol": symbol,
            "qty": qty,
        }

    return drop


def compare_positions(
    eod: dict[str, dict[str, Any]],
    drop: dict[str, dict[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    bad = 0
    rows: list[dict[str, Any]] = []

    all_tokens = sorted(set(drop) | set(eod), key=lambda x: int(x))

    for token in all_tokens:
        d = drop.get(token)
        e = eod.get(token)

        symbol = (d or e or {}).get("symbol", token)
        drop_qty = d["qty"] if d else None
        eod_qty = e["qty"] if e else None

        if d is None:
            status = "ONLY_EOD"
            bad += 1
        elif e is None:
            status = "MISSING_EOD"
            bad += 1
        elif not qty_equal(drop_qty, eod_qty):
            status = "MISMATCH"
            bad += 1
        else:
            status = "OK"

        rows.append({
            "token": token,
            "symbol": symbol,
            "dropcopy": drop_qty,
            "eod": eod_qty,
            "status": status,
        })

    return bad, rows


# ── Rendering ─────────────────────────────────────────────────────────────────
def render_text_report(rows: list[dict[str, Any]], bad: int, dt: str) -> str:
    lines: list[str] = []
    lines.append("=" * 92)
    lines.append(f"EOD vs Dropcopy Validation Report - {dt}")
    lines.append("=" * 92)
    lines.append("")
    lines.append(f"EOD file      : {EOD_FILE}")
    lines.append(f"Dropcopy file : {DROPCOPY_FILE}")
    lines.append("")
    lines.append(f"{'TOKEN':<8} | {'SYMBOL':<14} | {'DROPCOPY':>10} | {'EOD':>10} | STATUS")
    lines.append("-" * 92)

    for row in rows:
        lines.append(
            f"{row['token']:<8} | "
            f"{row['symbol']:<14} | "
            f"{fmt_qty(row['dropcopy']):>10} | "
            f"{fmt_qty(row['eod']):>10} | "
            f"{row['status']}"
        )

    lines.append("-" * 92)
    lines.append(f"Total checked: {len(rows)} | Issues: {bad}")
    return "\n".join(lines)


def render_html_report(rows: list[dict[str, Any]], bad: int, dt: str) -> str:
    status_text = "ALL CHECKS PASSED" if bad == 0 else "ACTION REQUIRED"
    status_class = "success" if bad == 0 else "failed"
    result_text = "SUCCESS" if bad == 0 else "FAILED"

    html_rows = []
    for row in rows:
        is_ok = row["status"] == "OK"
        tr_class = "ok" if is_ok else "bad"
        status_cell = (
            f'<td class="status oktext">✅ {esc(row["status"])}</td>'
            if is_ok
            else f'<td class="status badtext">❌ {esc(row["status"])}</td>'
        )

        html_rows.append(
            f'<tr class="{tr_class}">'
            f'<td>{esc(row["token"])}</td>'
            f'<td>{esc(row["symbol"])}</td>'
            f'<td class="num">{esc(fmt_qty(row["dropcopy"]))}</td>'
            f'<td class="num">{esc(fmt_qty(row["eod"]))}</td>'
            f'{status_cell}'
            "</tr>"
        )

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body{{font-family:Arial,Helvetica,sans-serif;background:#f6f8fb;color:#17202a;margin:0;padding:16px;}}
    .wrap{{max-width:1100px;margin:0 auto;background:#fff;border:1px solid #dde3ea;border-radius:10px;padding:18px;}}
    h1{{font-size:20px;margin:0 0 6px 0;}}
    h2{{font-size:16px;margin:22px 0 8px 0;border-bottom:1px solid #e5e9ef;padding-bottom:6px;}}
    .summary{{font-weight:bold;padding:10px 12px;border-radius:8px;margin:12px 0;}}
    .success{{background:#e9f8ef;color:#0b6b2f;border:1px solid #bfe7cc;}}
    .failed{{background:#fdecec;color:#9b1c1c;border:1px solid #f5b5b5;}}
    .meta{{font-size:13px;color:#526070;margin:4px 0 10px 0;}}
    table{{border-collapse:collapse;width:100%;font-size:13px;margin-top:8px;}}
    th{{background:#1f3b63;color:#fff;text-align:left;padding:7px 8px;border:1px solid #d6dce4;}}
    td{{padding:6px 8px;border:1px solid #d6dce4;}}
    tr:nth-child(even){{background:#f7f9fc;}}
    tr.bad{{background:#fff4f4;}}
    .num{{text-align:right;font-variant-numeric:tabular-nums;}}
    .status{{font-weight:bold;white-space:nowrap;}}
    .oktext{{color:#087a36;}}
    .badtext{{color:#b42318;}}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>EOD vs Dropcopy Validation Report — {esc(dt)}</h1>
    <div class="summary {status_class}">Total Issues: {bad} — {esc(status_text)}</div>

    <h2>1. EOD vs Dropcopy Position Check</h2>
    <div class="meta">Total non-zero positions checked: {len(rows)} | Issues: {bad}</div>

    <table>
      <thead>
        <tr>
          <th>Token</th>
          <th>Symbol</th>
          <th>Dropcopy</th>
          <th>EOD</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        {''.join(html_rows)}
      </tbody>
    </table>

    <div class="summary {status_class}">Result: {esc(result_text)} | Issues: {bad}</div>

    <div class="meta">
      EOD file: {esc(EOD_FILE)}<br>
      Dropcopy file: {esc(DROPCOPY_FILE)}<br>
      Note: zero quantities are skipped on both EOD and Dropcopy sides to avoid false MISSING/0 issues.
    </div>
  </div>
</body>
</html>
"""


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    try:
        eod_raw = read_remote(EOD_FILE)
        drop_raw = read_remote(DROPCOPY_FILE)
    except Exception as e:
        err = f"ERROR reading input files: {e}\nEOD file: {EOD_FILE}\nDropcopy file: {DROPCOPY_FILE}"
        print(err)
        subject = f"[FAILED] EOD vs Dropcopy Validation Report - {dt} - FILE ERROR"
        safe_send_mail(subject, err, None)
        return 2

    ok_date, date_error = validate_dropcopy_date(drop_raw, dt)
    if not ok_date:
        print(date_error)
        subject = f"[FAILED] EOD vs Dropcopy Validation Report - {dt} - STALE DROPCOPY"
        safe_send_mail(subject, date_error, None)
        return 2

    eod = load_eod_positions(eod_raw)
    drop = load_dropcopy_positions(drop_raw)

    bad, rows = compare_positions(eod, drop)

    text_report = render_text_report(rows, bad, dt)
    html_report = render_html_report(rows, bad, dt)

    print(text_report)

    subject_status = "SUCCESS" if bad == 0 else "FAILED"
    from datetime import datetime
    subject = f"[{subject_status}] EOD vs Dropcopy Validation Report - {dt} - Issues: {bad} - Test {datetime.now().strftime('%H:%M:%S')}"

    safe_send_mail(subject, text_report, html_report)

    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
