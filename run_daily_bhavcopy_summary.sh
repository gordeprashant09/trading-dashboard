#!/bin/bash
set -euo pipefail

BASE=/home/report/devstudio/Prashant/Live_Dashboard/Prod
PY=/home/report/devstudio/Prashant/Live_Dashboard/venv/bin/python3
COLO=Data_colo@192.168.71.200
REMOTE_DIR=/data/Dashboard/Summary

DT=${1:-$(date +%Y%m%d)}
MODE=${2:-final}

cd "$BASE"

PREV_DT=$($PY - <<PY
from datetime import datetime, timedelta
d = datetime.strptime("$DT", "%Y%m%d").date() - timedelta(days=1)
while d.weekday() >= 5:
    d -= timedelta(days=1)
print(d.strftime("%Y%m%d"))
PY
)

RAW_OUT="daily_bhavcopy_raw_${DT}_${MODE}.csv"
DAILY_ONE="Summary_daily_${DT}_${MODE}.csv"
SYMBOL_ONE="Summary_symbol_pnl_${DT}_${MODE}.csv"

echo "DT=$DT PREV_DT=$PREV_DT MODE=$MODE"

$PY cumulative_pnl_bhavcopy_v2.py \
  --start "$PREV_DT" \
  --end "$DT" \
  --out "$RAW_OUT"

# Final mode must be clean. Provisional mode may have TODAY_BHAV_MISSING.
if [ "$MODE" = "final" ]; then
  if grep -E 'EOD_QTY_MISMATCH|TODAY_BHAV_MISSING|PREV_BHAV_MISSING|LOT_MISMATCH' "$RAW_OUT"; then
    echo "ERROR: final file has validation issues. Not updating Summary_daily.csv"
    exit 2
  fi
fi

$PY - <<PY
import pandas as pd

dt = int("$DT")
raw = "$RAW_OUT"

df = pd.read_csv(raw)
df = df[df["date"] == dt].copy()

if df.empty:
    raise SystemExit(f"No rows found for {dt}")

df["eod_pos_inr"] = df["open_qty"] * df["today_bhav_close_used"]
df["gross_pos_inr"] = df["eod_pos_inr"].abs()
df["buy_tv"] = df["qty_today_buy"] * df["buy_avg"]
df["sell_tv"] = df["qty_today_sell"] * df["sell_avg"]
df["tv"] = df["buy_tv"] + df["sell_tv"]

daily = df.groupby("date").agg(
    Gross_Position=("gross_pos_inr", "sum"),
    Net_Position=("eod_pos_inr", "sum"),
    MTM=("mtm", "sum"),
    Net_PNL=("net_pnl", "sum"),
    TV=("tv", "sum"),
    Trading_Cost=("expenses", "sum"),
).reset_index()

daily["Max_Margin"] = daily["Gross_Position"]
daily["EOD_Margin"] = daily["Gross_Position"]

daily = daily.rename(columns={"date": "Date"})
daily = daily[[
    "Date",
    "Gross_Position",
    "Net_Position",
    "MTM",
    "Net_PNL",
    "TV",
    "Trading_Cost",
    "Max_Margin",
    "EOD_Margin",
]].round(2)

sym = df.groupby(["date", "symbol"]).agg(
    Net_PNL=("net_pnl", "sum"),
    EOD_Pos_Lot=("open_lots", "sum"),
    EOD_Pos_INR=("eod_pos_inr", "sum"),
).reset_index()

sym = sym.rename(columns={"date": "Date", "symbol": "Symbol"})
sym = sym[[
    "Date",
    "Symbol",
    "Net_PNL",
    "EOD_Pos_Lot",
    "EOD_Pos_INR",
]].round(2)

daily.to_csv("$DAILY_ONE", index=False)
sym.to_csv("$SYMBOL_ONE", index=False)

print(daily.to_string(index=False))
print()
print("Daily Net_PNL:", round(float(daily["Net_PNL"].sum()), 2))
PY

# Both final and provisional_update update the main files.
# final overwrites provisional row for same date after BhavCopy arrives.
if [ "$MODE" = "final" ] || [ "$MODE" = "provisional_update" ]; then
  scp "$COLO:$REMOTE_DIR/Summary_daily.csv"         Summary_daily.current.csv         2>/dev/null || true
  scp "$COLO:$REMOTE_DIR/Summary_symbol_pnl.csv"    Summary_symbol_pnl.current.csv    2>/dev/null || true

  # ── Backup before overwrite (provisional = 15:50, final = 21:45) ──
  BKP_TS=$(date +%Y%m%d_%H%M)
  if [ -f Summary_daily.current.csv ]; then
    cp Summary_daily.current.csv         "Summary_daily_bkp_${BKP_TS}.csv"
    echo "Backup: Summary_daily_bkp_${BKP_TS}.csv"
  fi
  if [ -f Summary_symbol_pnl.current.csv ]; then
    cp Summary_symbol_pnl.current.csv    "Summary_symbol_pnl_bkp_${BKP_TS}.csv"
    echo "Backup: Summary_symbol_pnl_bkp_${BKP_TS}.csv"
  fi

  $PY - <<PY
import pandas as pd
from pathlib import Path

dt = int("$DT")

def update_file(current_path, one_path, out_path, key_cols):
    one = pd.read_csv(one_path)

    if Path(current_path).exists():
        cur = pd.read_csv(current_path)
        cur = cur[cur["Date"] != dt]
        out = pd.concat([cur, one], ignore_index=True)
    else:
        out = one

    out = out.sort_values(key_cols)
    out.to_csv(out_path, index=False)

update_file(
    "Summary_daily.current.csv",
    "$DAILY_ONE",
    "Summary_daily.csv",
    ["Date"],
)

update_file(
    "Summary_symbol_pnl.current.csv",
    "$SYMBOL_ONE",
    "Summary_symbol_pnl.csv",
    ["Date", "Symbol"],
)
PY

  scp Summary_daily.csv Summary_symbol_pnl.csv "$COLO:$REMOTE_DIR/"

  if [ "$MODE" = "final" ]; then
    echo "Updated FINAL Summary_daily.csv and Summary_symbol_pnl.csv on colo"
  else
    echo "Updated PROVISIONAL Summary_daily.csv and Summary_symbol_pnl.csv on colo"
  fi
else
  scp "$RAW_OUT" "$DAILY_ONE" "$SYMBOL_ONE" "$COLO:$REMOTE_DIR/"
  echo "Wrote files only. Summary_daily.csv not updated."
fi
