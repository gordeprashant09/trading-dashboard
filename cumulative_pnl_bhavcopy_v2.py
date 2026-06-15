#!/usr/bin/env python3

import os
import csv
import argparse
import subprocess
from datetime import datetime, timedelta

COLO_HOST = "192.168.71.200"
COLO_USER = "Data_colo"

SNAP_DIR = "/data/Dashboard/snapshots"
EOD_DIR = "/data/Dashboard/Eod"
BHAV_DIR = "/home/report/devstudio/Prashant/Bhavcopy"


def run(cmd):
    return subprocess.check_output(
        cmd,
        shell=True,
        text=True,
        stderr=subprocess.DEVNULL,
    )


def read_colo_file(path):
    return run(f"ssh {COLO_USER}@{COLO_HOST} 'cat {path} 2>/dev/null'")


def trading_dates(start, end):
    d = datetime.strptime(start, "%Y%m%d").date()
    e = datetime.strptime(end, "%Y%m%d").date()
    out = []

    while d <= e:
        if d.weekday() < 5:
            out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)

    return out


def load_bhavcopy(dt):
    path = os.path.join(BHAV_DIR, f"BhavCopy_{dt}_FUT.csv")
    out = {}

    if not os.path.exists(path):
        print(f"WARN {dt}: missing bhavcopy file {path}")
        return out

    with open(path, "r") as f:
        for r in csv.DictReader(f):
            contract = r["contract"].strip().upper()
            out[contract] = {
                "close": float(r["close"]),
                "settlement_price": float(r.get("settlement_price") or r["close"]),
                "lot_size": int(float(r["lot_size"])),
            }

    return out


def load_snapshot(dt):
    path = f"{SNAP_DIR}/dashboard_snapshot_{dt}.csv"

    try:
        raw = read_colo_file(path)
    except Exception:
        print(f"SKIP {dt}: cannot read snapshot {path}")
        return []

    if not raw.strip():
        print(f"SKIP {dt}: snapshot missing/empty {path}")
        return []

    return list(csv.DictReader(raw.splitlines()))


def load_eod(dt):
    path = f"{EOD_DIR}/eod_positions_{dt}.csv"
    out = {}

    try:
        raw = read_colo_file(path)
    except Exception:
        print(f"WARN {dt}: cannot read EOD {path}")
        return out

    if not raw.strip():
        print(f"WARN {dt}: EOD missing/empty {path}")
        return out

    for r in csv.DictReader(raw.splitlines()):
        contract = r["symbol"].strip().upper()
        out[contract] = {
            "token": str(r["token"]).strip(),
            "name": r.get("name", "").strip().upper(),
            "qty_overnight": float(r["qty_overnight"]),
            "prev_close": float(r["prev_close"]),
        }

    return out


def calc_dashboard_pnl(row, today_bhav_close, prev_bhav_close):
    qty_overnight = float(row["qty_overnight"] or 0)
    qty_buy = float(row["qty_today_buy"] or 0)
    qty_sell = float(row["qty_today_sell"] or 0)
    buy_avg = float(row["buy_avg"] or 0)
    sell_avg = float(row["sell_avg"] or 0)

    # Corrected prices
    ltp = today_bhav_close
    prev_close = prev_bhav_close

    # Carry PnL from previous BhavCopy close to today's BhavCopy close
    carry = qty_overnight * (ltp - prev_close)

    # Intraday matched and open legs
    matched_qty = min(qty_buy, qty_sell)
    open_buy_qty = qty_buy - matched_qty
    open_sell_qty = qty_sell - matched_qty

    realized = matched_qty * (sell_avg - buy_avg) if matched_qty > 0 else 0.0
    unreal_buy = open_buy_qty * (ltp - buy_avg) if open_buy_qty > 0 else 0.0
    unreal_sell = open_sell_qty * (sell_avg - ltp) if open_sell_qty > 0 else 0.0

    # Overnight long square-off today
    if qty_overnight > 0 and open_sell_qty > 0:
        close_qty = min(qty_overnight, open_sell_qty)
        realized += close_qty * (sell_avg - prev_close)
        carry -= close_qty * (ltp - prev_close)
        remaining_sell = open_sell_qty - close_qty
        unreal_sell = remaining_sell * (sell_avg - ltp) if remaining_sell > 0 else 0.0

    # Overnight short square-off today
    elif qty_overnight < 0 and open_buy_qty > 0:
        close_qty = min(abs(qty_overnight), open_buy_qty)
        realized += close_qty * (prev_close - buy_avg)
        carry += close_qty * (ltp - prev_close)
        remaining_buy = open_buy_qty - close_qty
        unreal_buy = remaining_buy * (ltp - buy_avg) if remaining_buy > 0 else 0.0

    day = realized + unreal_buy + unreal_sell

    # Trading cost from today's trades only
    buy_val = qty_buy * (buy_avg if buy_avg else ltp)
    sell_val = qty_sell * (sell_avg if sell_avg else ltp)

    buy_cost = (buy_val / 1e7) * 1018
    sell_cost = (sell_val / 1e7) * 5818
    expenses = buy_cost + sell_cost

    net = carry + day - expenses

    return {
        "carry_pnl": carry,
        "realized": realized,
        "unrealized": unreal_buy + unreal_sell,
        "day_pnl": day,
        "expenses": expenses,
        "net_pnl": net,
        "mtm": carry + day,
    }


def process_day(dt, prev_dt, baseline_date):
    today_bhav = load_bhavcopy(dt)
    prev_bhav = load_bhavcopy(prev_dt) if prev_dt else {}

    snap = load_snapshot(dt)
    eod = load_eod(dt)

    rows = []

    for r in snap:
        contract = r["expiry_label"].strip().upper()
        symbol = r["sym"].strip().upper()

        lot_size = int(float(r["lot_size"] or 1))
        qty_overnight = float(r["qty_overnight"] or 0)
        qty_buy = float(r["qty_today_buy"] or 0)
        qty_sell = float(r["qty_today_sell"] or 0)
        open_qty = float(r["open_qty"] or 0)

        # Ignore fully inactive rows
        if qty_overnight == 0 and qty_buy == 0 and qty_sell == 0 and open_qty == 0:
            continue

        status = []

        # Today's mark price: current day BhavCopy FUT close
        if contract in today_bhav:
            today_close = today_bhav[contract]["close"]
            bhav_lot = today_bhav[contract]["lot_size"]
            today_price_source = "TODAY_BHAVCOPY_FUT_CLOSE"
        else:
            today_close = float(r["ltp"] or 0)
            bhav_lot = lot_size
            today_price_source = "TODAY_SNAPSHOT_LTP_FALLBACK"
            status.append("TODAY_BHAV_MISSING")

        # Previous close: previous trading day BhavCopy FUT close
        if dt == baseline_date:
            prev_close = float(r["prev_close"] or 0)
            prev_price_source = "BASELINE_SNAPSHOT_PREV_CLOSE"
            status.append("BASELINE_DAY")
        elif contract in prev_bhav:
            prev_close = prev_bhav[contract]["close"]
            prev_price_source = "PREV_BHAVCOPY_FUT_CLOSE"
        else:
            prev_close = float(r["prev_close"] or 0)
            prev_price_source = "PREV_SNAPSHOT_PREV_CLOSE_FALLBACK"
            status.append("PREV_BHAV_MISSING")

        if bhav_lot != lot_size:
            status.append(f"LOT_MISMATCH_BHAV_{bhav_lot}_SNAP_{lot_size}")

        # Validate EOD qty against dashboard snapshot open qty
        e = eod.get(contract, {})
        eod_qty = float(e.get("qty_overnight", 0.0))
        eod_diff = eod_qty - open_qty

        if dt == baseline_date:
            status.append("NO_EOD_QTY_CHECK_FOR_BASELINE")
        else:
            if abs(eod_diff) > 0.0001:
                status.append("EOD_QTY_MISMATCH")
            else:
                status.append("EOD_QTY_OK")

        pnl = calc_dashboard_pnl(
            row=r,
            today_bhav_close=today_close,
            prev_bhav_close=prev_close,
        )

        snapshot_net = float(r["net_pnl"] or 0)

        rows.append({
            "date": dt,
            "symbol": symbol,
            "contract": contract,
            "token": e.get("token", ""),

            "lot_size": lot_size,
            "bhav_lot_size": bhav_lot,

            "qty_overnight": qty_overnight,
            "qty_today_buy": qty_buy,
            "buy_avg": float(r["buy_avg"] or 0),
            "qty_today_sell": qty_sell,
            "sell_avg": float(r["sell_avg"] or 0),

            "open_qty": open_qty,
            "open_lots": open_qty / lot_size if lot_size else 0.0,

            "eod_qty": eod_qty,
            "eod_lots": eod_qty / lot_size if lot_size else 0.0,
            "eod_minus_open_qty": eod_diff,

            "snapshot_ltp": float(r["ltp"] or 0),
            "snapshot_prev_close": float(r["prev_close"] or 0),

            "today_bhav_close_used": today_close,
            "prev_bhav_close_used": prev_close,
            "today_price_source": today_price_source,
            "prev_price_source": prev_price_source,

            "carry_pnl": round(pnl["carry_pnl"], 2),
            "realized": round(pnl["realized"], 2),
            "unrealized": round(pnl["unrealized"], 2),
            "day_pnl": round(pnl["day_pnl"], 2),
            "mtm": round(pnl["mtm"], 2),
            "expenses": round(pnl["expenses"], 2),
            "net_pnl": round(pnl["net_pnl"], 2),

            "snapshot_net_pnl": snapshot_net,
            "diff_vs_snapshot": round(pnl["net_pnl"] - snapshot_net, 2),

            "status": "|".join(status),
        })

    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument(
        "--out",
        default=None,
        help="Output CSV path. Default writes in current directory.",
    )
    args = ap.parse_args()

    dates = trading_dates(args.start, args.end)
    out = args.out or f"cumulative_pnl_bhavcopy_v2_{args.start}_{args.end}.csv"

    all_rows = []
    cumulative = 0.0

    for idx, dt in enumerate(dates):
        prev_dt = dates[idx - 1] if idx > 0 else None

        rows = process_day(
            dt=dt,
            prev_dt=prev_dt,
            baseline_date=args.start,
        )

        day_net = sum(x["net_pnl"] for x in rows)
        cumulative += day_net

        issue_count = 0
        for x in rows:
            x["day_total_mtm"] = round(sum(r["mtm"] for r in rows), 2)
            x["day_total_net_pnl"] = round(day_net, 2)
            x["cumulative_pnl"] = round(cumulative, 2)

            if (
                "EOD_QTY_MISMATCH" in x["status"]
                or "TODAY_BHAV_MISSING" in x["status"]
                or "PREV_BHAV_MISSING" in x["status"]
                or "LOT_MISMATCH" in x["status"]
            ):
                issue_count += 1

        print(
            f"{dt}: rows={len(rows)} "
            f"mtm={sum(x['mtm'] for x in rows):,.2f} "
            f"day_net={day_net:,.2f} "
            f"cumulative={cumulative:,.2f} "
            f"issues={issue_count}"
        )

        all_rows.extend(rows)

    fields = [
        "date", "symbol", "contract", "token",
        "lot_size", "bhav_lot_size",

        "qty_overnight",
        "qty_today_buy", "buy_avg",
        "qty_today_sell", "sell_avg",

        "open_qty", "open_lots",
        "eod_qty", "eod_lots", "eod_minus_open_qty",

        "snapshot_ltp", "snapshot_prev_close",
        "today_bhav_close_used", "prev_bhav_close_used",
        "today_price_source", "prev_price_source",

        "carry_pnl", "realized", "unrealized",
        "day_pnl", "mtm", "expenses", "net_pnl",

        "snapshot_net_pnl", "diff_vs_snapshot",

        "day_total_mtm",
        "day_total_net_pnl",
        "cumulative_pnl",

        "status",
    ]

    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(all_rows)

    print()
    print(f"WROTE: {out}")
    print("No existing production file modified.")


if __name__ == "__main__":
    main()
