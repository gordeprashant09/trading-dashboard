"""
slippage_dashboard.py  v6
=========================
Live Slippage Dashboard — uses st_aggrid exactly like the position book.
Same dark theme, same fonts, same cell styles, same filter/sort icons.

Run:
    pip install streamlit paramiko redis streamlit-autorefresh streamlit-aggrid pandas
    streamlit run slippage_dashboard.py
"""
from __future__ import annotations

import os
import re
import json
import math
import logging
from collections import defaultdict
from datetime import datetime, date
from typing import Optional

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
SSH_HOST       = os.getenv("SSH_HOST",             "192.168.71.200")
SSH_PORT       = int(os.getenv("SSH_PORT",         "22"))
SSH_USER       = os.getenv("SSH_USER",             "Data_colo")
SSH_PASS       = os.getenv("SSH_PASS",             "Datacolo@2026")
REMOTE_LOG_DIR = os.getenv("REMOTE_LOG_DIR",       "/data/logs")

REDIS_HOST     = os.getenv("REDIS_HOST",           "localhost")
REDIS_PORT     = int(os.getenv("REDIS_PORT",       "6379"))
REDIS_DB       = int(os.getenv("REDIS_DB",         "1"))
DASH_REDIS_KEY = "dashboard:positions:latest2"

REFRESH_SECONDS = int(os.getenv("SLIP_REFRESH_SECONDS", "10"))

# ── EOD CSV storage ───────────────────────────────────────────
CSV_DIR      = os.getenv("SLIP_CSV_DIR",
               "/home/report/devstudio/Prashant/Live_Dashboard/Slippage/csv")
EOD_SAVE_TIME = "15:32"   # HH:MM IST — save once after market close

log = logging.getLogger("slippage_dashboard")

# ─────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Slippage Dashboard",
    page_icon="📉",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st_autorefresh(interval=REFRESH_SECONDS * 1000, key="slip_refresh")

# ─────────────────────────────────────────────────────────────
# GLOBAL CSS — same as trading_dashboard
# ─────────────────────────────────────────────────────────────
st.html("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap');

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1rem !important; padding-bottom: 0.5rem !important; }

[data-testid="stMetricValue"] {
    font-size: 1.3rem !important; font-weight: 600;
    font-family: 'JetBrains Mono', monospace !important;
}
[data-testid="stMetricLabel"] {
    font-size: 10px !important; text-transform: uppercase;
    letter-spacing: .08em; color: #555c6e !important;
}
[data-testid="stMetric"] {
    background: #13151a; border: 1px solid #1e2230;
    border-radius: 6px; padding: 10px 14px !important;
}
.section-hdr {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 10px; color: #454c5e;
    text-transform: uppercase; letter-spacing: .1em;
    margin: 0 0 6px 2px;
}
.slip-foot {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 10px;
    color: #7a8294;
    margin-top: 8px;
    line-height: 1.45;
    white-space: nowrap;
}
</style>
""")

# ─────────────────────────────────────────────────────────────
# REGEX
# ─────────────────────────────────────────────────────────────
TIME_RE = re.compile(
    r"^\s*(\d\d:\d\d:\d\d):(\d+)\s+:(?P<tag>[^:]+)::EXECUTION_STRATEGY_LIVE\s+(?P<body>.*)$"
)
BOOK_RE = re.compile(
    r"symbol=\[(?P<symbol>[^\]]+)\]\s+top_bid=(?P<bid>\d+)\s+top_ask=(?P<ask>\d+)\s+mid=(?P<mid>\d+)\s+"
    r"working_price=(?P<working>\d+)\s+desired_lots=(?P<desired>-?\d+(?:\.\d+)?)\s+"
    r"current_lots=(?P<current>-?\d+)\s+delta_lots=(?P<delta>-?\d+)"
)
SEND_RE = re.compile(r"send symbol=\[(?P<symbol>[^\]]+)\]\s+order=\[(?P<order>[^\]]+)\]")
FILL_RE = re.compile(
    r"trade_response position symbol=\[(?P<symbol>[^\]]+)\]\s+side=\[(?P<side>\d+)\]\s+"
    r"fill_qty=\[(?P<qty>\d+)\]\s+fill_price=\[(?P<price>\d+)\]\s+local_pos=\[(?P<pos>-?\d+)\]\s+"
    r"lot_qty=\[(?P<lot_qty>\d+)\]\s+current_lots=\[(?P<current>-?\d+)\]\s+"
    r"desired_lots_raw=\[(?P<desired_raw>-?\d+(?:\.\d+)?)\]\s+desired_lots=\[(?P<desired>-?\d+)\]\s+"
    r"delta_lots=\[(?P<delta>-?\d+)\]"
)

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def today_str() -> str:
    return date.today().strftime("%Y%m%d")


def get_ssh_client():
    import paramiko
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, port=SSH_PORT,
                   username=SSH_USER, password=SSH_PASS, timeout=10)
    return client


def find_today_log(ssh_client) -> Optional[str]:
    dt = today_str()
    _, stdout, _ = ssh_client.exec_command(
        f"ls {REMOTE_LOG_DIR}/Strategy-June2_algo_1_{dt}.log 2>/dev/null | head -1"
    )
    path = stdout.read().decode().strip()
    return path if path else None


# ─────────────────────────────────────────────────────────────
# EXECUTION SLIPPAGE PARSER
# ─────────────────────────────────────────────────────────────
def parse_execution_slippage(log_lines: list[str]) -> dict[str, dict]:
    book_by_symbol: dict[str, dict] = {}
    order_by_id:    dict[str, dict] = {}
    sym_fills:      dict[str, list] = defaultdict(list)

    for raw_line in log_lines:
        line = raw_line.rstrip("\n")
        tm = TIME_RE.match(line)
        if not tm:
            continue
        tag  = tm.group("tag")
        body = tm.group("body")

        if tag == "operator()":
            bm = BOOK_RE.search(body)
            if bm and "top_bid=" in body:
                sym = bm.group("symbol")
                book_by_symbol[sym] = {
                    "bid": int(bm.group("bid")),
                    "ask": int(bm.group("ask")),
                    "mid": int(bm.group("mid")),
                }
            continue

        if tag == "send_order":
            sm = SEND_RE.search(body)
            if not sm:
                continue
            fields = [p.strip() for p in sm.group("order").split(",")]
            if len(fields) < 9:
                continue
            sym      = sm.group("symbol")
            order_id = fields[7]
            side     = int(fields[5])
            price    = int(fields[2])
            qty      = int(fields[3])
            if order_id not in order_by_id:
                book = book_by_symbol.get(sym)
                if book is None:
                    continue
                order_by_id[order_id] = {
                    "symbol":  sym, "side": side,
                    "ref_mid": book["mid"], "price": price,
                    "prices": [price],
                    "qty": qty, "last_at": tm.group(1),
                }
            else:
                o = order_by_id[order_id]
                o["price"] = price
                o["prices"].append(price)
                o["qty"] = qty; o["last_at"] = tm.group(1)
            continue

        if tag == "update_position_from_fill":
            fm = FILL_RE.search(body)
            if not fm:
                continue
            sym        = fm.group("symbol")
            fill_price = int(fm.group("price"))
            qty        = int(fm.group("qty"))
            side       = int(fm.group("side"))
            sym_orders = [o for o in order_by_id.values() if o["symbol"] == sym]
            if not sym_orders:
                continue
            candidates = [o for o in sym_orders if o["side"] == side and fill_price in o.get("prices", [o["price"]])]
            if not candidates:
                candidates = [o for o in sym_orders if o["side"] == side]
            if not candidates:
                continue
            order = max(candidates, key=lambda o: o["last_at"])
            ref_mid = order["ref_mid"]
            if ref_mid <= 0:
                continue
            direction = 1 if side == 1 else -1
            slip_bps  = -(direction * (fill_price - ref_mid) / ref_mid) * 10000.0
            sym_fills[sym].append((qty, slip_bps))

    result: dict[str, dict] = {}
    for sym, fills in sym_fills.items():
        total_qty = sum(q for q, _ in fills)
        if total_qty > 0:
            wavg = sum(q * s for q, s in fills) / total_qty
            result[sym] = {"slip_bps": round(wavg, 2), "fills": len(fills)}
    return result


# ─────────────────────────────────────────────────────────────
# TOP-WINDOW SLIPPAGE  (Redis)
# ─────────────────────────────────────────────────────────────
def fetch_top_window_slippage() -> dict[str, float]:
    try:
        import redis as _redis
        r = _redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                         decode_responses=True, socket_timeout=2.0)
        raw = r.get(DASH_REDIS_KEY)
        if not raw:
            return {}
        result: dict[str, float] = {}
        for stock in json.loads(raw).get("positions", []):
            sym = stock.get("sym", "")
            if not sym:
                continue
            pairs = [
                (e.get("slippage"), e.get("buy_tv", 0) + e.get("sell_tv", 0))
                for e in stock.get("expiries", [])
                if e.get("slippage") is not None
            ]
            if not pairs:
                continue
            total_w = sum(w for _, w in pairs)
            wavg = (sum(s * w for s, w in pairs) / total_w if total_w > 0
                    else sum(s for s, _ in pairs) / len(pairs))
            result[sym] = round(wavg * 10000, 2)
        return result
    except Exception as e:
        log.warning("Redis fetch failed: %s", e)
        return {}


# ─────────────────────────────────────────────────────────────
# LOG READER
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=REFRESH_SECONDS)
def read_log_lines_cached(date_str: str) -> tuple[list[str], int]:
    try:
        client = get_ssh_client()
        path   = find_today_log(client)
        if not path:
            client.close()
            return [], 0
        _, stdout, _ = client.exec_command(f"cat {path}")
        content = stdout.read().decode("utf-8", errors="replace")
        client.close()
        lines = content.splitlines()
        return lines, len(lines)
    except Exception as e:
        log.warning("Log read failed: %s", e)
        return [], 0


# ─────────────────────────────────────────────────────────────
# LOAD HISTORICAL SESSION CSVs — seed slip_history on startup
# Reads all slippage_session_YYYYMMDD.csv files in CSV_DIR
# (excluding today's) and seeds st.session_state["slip_history"]
# with their tw_slip_cur / exec_slip_cur values, weighted by
# their saved snapshot counts (tw_snapshots / exec_snapshots).
# This makes "Avg" a running average across all trading days
# (including today's live snapshots) rather than resetting to
# Current on every Streamlit restart.
# ─────────────────────────────────────────────────────────────
def load_historical_slip_history(today_yyyymmdd: str) -> dict[str, dict]:
    import csv as _csv
    import glob as _glob

    hist: dict[str, dict] = {}
    if not os.path.isdir(CSV_DIR):
        return hist

    pattern = os.path.join(CSV_DIR, "slippage_session_*.csv")
    files = sorted(_glob.glob(pattern))

    for fpath in files:
        fname = os.path.basename(fpath)
        # slippage_session_YYYYMMDD.csv
        date_part = fname.replace("slippage_session_", "").replace(".csv", "")
        if date_part == today_yyyymmdd:
            continue  # skip today's own file if present

        try:
            with open(fpath, "r", newline="") as f:
                reader = _csv.DictReader(f)
                for row in reader:
                    sym = row.get("symbol")
                    if not sym:
                        continue
                    if sym not in hist:
                        hist[sym] = {"tw": [], "ex": []}

                    try:
                        tw_avg = float(row.get("tw_session_avg", "") or "nan")
                        tw_n   = int(float(row.get("tw_snapshots", "0") or "0"))
                        if tw_n > 0 and tw_avg == tw_avg:  # not NaN
                            hist[sym]["tw"].extend([tw_avg] * tw_n)
                    except (ValueError, TypeError):
                        pass

                    try:
                        ex_avg = float(row.get("exec_session_avg", "") or "nan")
                        ex_n   = int(float(row.get("exec_snapshots", "0") or "0"))
                        if ex_n > 0 and ex_avg == ex_avg:  # not NaN
                            hist[sym]["ex"].extend([ex_avg] * ex_n)
                    except (ValueError, TypeError):
                        pass
        except Exception as e:
            log.warning("Failed to load historical CSV %s: %s", fpath, e)

    return hist


# ─────────────────────────────────────────────────────────────
# SESSION RUNNING AVERAGE
# ─────────────────────────────────────────────────────────────
def update_running_avgs(
    top_win: dict[str, float],
    exec_slip: dict[str, dict],
    log_line_count: int,
) -> dict[str, dict]:
    last_lines = st.session_state.get("slip_last_lines", -1)
    new_data   = (log_line_count > last_lines)
    st.session_state["slip_last_lines"] = log_line_count

    if "slip_history" not in st.session_state:
        st.session_state["slip_history"] = load_historical_slip_history(today_str())
    hist = st.session_state["slip_history"]

    all_syms = set(top_win.keys()) | set(exec_slip.keys())
    for sym in all_syms:
        if sym not in hist:
            hist[sym] = {"tw": [], "ex": []}
        if new_data:
            if sym in top_win:
                hist[sym]["tw"].append(top_win[sym])
            if sym in exec_slip:
                hist[sym]["ex"].append(exec_slip[sym]["slip_bps"])

    rows = {}
    for sym in all_syms:
        h       = hist[sym]
        tw_cur  = top_win.get(sym)
        ex_cur  = exec_slip[sym]["slip_bps"] if sym in exec_slip else None
        tw_ravg = round(sum(h["tw"]) / len(h["tw"]), 2) if h["tw"] else tw_cur
        ex_ravg = round(sum(h["ex"]) / len(h["ex"]), 2) if h["ex"] else ex_cur
        miss    = round(tw_cur - ex_cur, 2) if (tw_cur is not None and ex_cur is not None) else None
        rows[sym] = {
            "tw_cur":  tw_cur,  "tw_ravg": tw_ravg,
            "ex_cur":  ex_cur,  "ex_ravg": ex_ravg,
            "miss":    miss,
            "fills":   exec_slip.get(sym, {}).get("fills", 0),
            "tw_n":    len(h["tw"]), "ex_n": len(h["ex"]),
        }
    return rows


# ─────────────────────────────────────────────────────────────
# REFERENCE SLIPPAGE (bp)
# Converted from % to bp (x100) and negated — slippage is a cost.
# e.g. 1.6817% → -168.17 bp
# ─────────────────────────────────────────────────────────────
_REF_RAW_PCT = {
    "HDFCBANK": 1.6817,  "ICICIBANK": 1.9927,  "RELIANCE": 1.5308,
    "SBIN": 2.2953,      "INFY": 1.6408,       "AXISBANK": 2.5486,
    "LT": 2.3590,        "MCX": 3.1074,        "BSE": 3.2880,
    "VEDL": 3.7175,      "KOTAKBANK": 3.2663,  "TCS": 1.3406,
    "BHARTIARTL": 1.9156,"SHRIRAMFIN": 2.9131, "M&M": 2.6625,
    "DIXON": 3.0419,     "BHEL": 4.8417,       "NATIONALUM": 2.9826,
    "ETERNAL": 2.9627,   "BAJFINANCE": 2.2806, "HINDALCO": 3.1449,
    "INDIGO": 2.2220,    "TATASTEEL": 3.4205,  "BEL": 3.0356,
    "MARUTI": 2.2881,    "PNB": 3.7881,        "ASHOKLEY": 3.4241,
    "CANBK": 4.1820,     "POLYCAB": 3.3349,    "WIPRO": 2.3455,
}
REFERENCE_SLIPPAGE_PCT = {k: round(-v, 2) for k, v in _REF_RAW_PCT.items()}

# ─────────────────────────────────────────────────────────────
# BUILD DATAFRAME FOR AGGRID
# ─────────────────────────────────────────────────────────────
# Required visible layout:
# Symbol | Reference | Current: TW Slip, Exec Slip | Avg: TW Session Avg, EXEC Slip | Slip Diff | Fills
COL_SYMBOL    = "Symbol"
COL_REFERENCE = "Reference (bp)"
COL_TW_CUR    = "TW Slip (BP)"
COL_EX_CUR    = "Exec Slip (BP)"
COL_TW_AVG    = "TW Session Avg (bp)"
COL_EX_AVG    = "Exec Session Avg (bp)"
COL_MISS      = "Slip Diff (Bp)"
COL_FILLS     = "Fills"


def build_dataframe(rows: dict) -> pd.DataFrame:
    records = []
    for sym in sorted(rows.keys()):
        r = rows[sym]
        records.append({
            COL_SYMBOL: sym,
            COL_REFERENCE: REFERENCE_SLIPPAGE_PCT.get(sym.upper()),
            COL_TW_CUR:  r["tw_cur"],
            COL_EX_CUR:  r["ex_cur"],
            COL_TW_AVG:  r["tw_ravg"],
            COL_EX_AVG:  r["ex_ravg"],
            COL_MISS:    r["miss"],
            COL_FILLS:   r["fills"] if r["fills"] > 0 else None,
        })

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=[
            COL_SYMBOL, COL_REFERENCE, COL_TW_CUR, COL_EX_CUR,
            COL_TW_AVG, COL_EX_AVG, COL_MISS, COL_FILLS,
        ])

    # Force numeric columns to float so AgGrid sorts correctly.
    for c in [COL_REFERENCE, COL_TW_CUR, COL_EX_CUR, COL_TW_AVG, COL_EX_AVG, COL_MISS, COL_FILLS]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df[[
        COL_SYMBOL,
        COL_REFERENCE,
        COL_TW_CUR,
        COL_EX_CUR,
        COL_TW_AVG,
        COL_EX_AVG,
        COL_MISS,
        COL_FILLS,
    ]]


# ─────────────────────────────────────────────────────────────
# AGGRID RENDERER
# ─────────────────────────────────────────────────────────────
def render_slippage_aggrid(df: pd.DataFrame):
    try:
        from st_aggrid import AgGrid, JsCode
    except ImportError:
        st.error("Install streamlit-aggrid: pip install streamlit-aggrid")
        return

    value_formatter = JsCode("""
    function(params) {
      if (params.value === null || params.value === undefined || isNaN(params.value)) return '—';
      let v = Number(params.value);
      let sign = v > 0 ? '+' : '';
      return sign + v.toFixed(2);
    }
    """)

    reference_formatter = JsCode("""
    function(params) {
      if (params.value === null || params.value === undefined || isNaN(params.value)) return '—';
      let v = Number(params.value);
      let sign = v > 0 ? '+' : '';
      return sign + v.toFixed(2);
    }
    """)

    fills_formatter = JsCode("""
    function(params) {
      if (params.value === null || params.value === undefined || isNaN(params.value)) return '—';
      return Number(params.value).toFixed(0);
    }
    """)

    # Cell style — same colors as position book.
    # Slip columns: positive = green, negative = red.
    # Slip Diff: positive = red, negative = green.
    cell_style = JsCode("""
    function(params) {
      const field = params.colDef.field;

      if (field === 'Symbol') {
        return {
          backgroundColor: '#1a1d26',
          color: '#d4d8e8',
          fontWeight: '700',
          fontFamily: 'JetBrains Mono, monospace',
          fontSize: '12px'
        };
      }

      let s = {
        backgroundColor: '#1a1d26',
        color: '#c8cdd8',
        fontFamily: 'JetBrains Mono, monospace',
        fontSize: '11px'
      };

      if (params.value !== null && params.value !== undefined && !isNaN(Number(params.value))) {
        let v = Number(params.value);

        if (['TW Slip (BP)', 'Exec Slip (BP)', 'TW Session Avg (bp)', 'Exec Session Avg (bp)'].includes(field)) {
          if      (v >  0.05) s.color = '#2eca8a';
          else if (v < -0.05) s.color = '#f05252';
          else                s.color = '#555c6e';
        }

        if (field === 'Slip Diff (Bp)') {
          if      (v >  0.10) s.color = '#f05252';
          else if (v < -0.10) s.color = '#2eca8a';
          else                s.color = '#555c6e';
        }

        if (field === 'Reference') {
          s.color = '#e0e4f0';
          s.fontWeight = '700';
        }

        if (field === 'Fills') {
          s.color = '#8892a4';
        }
      }

      return s;
    }
    """)

    default_filter = {
        "filter": "agSetColumnFilter",
        "sortable": True,
        "resizable": True,
        "suppressMenu": False,
        "menuTabs": ["filterMenuTab"],
    }

    column_defs = [
        {
            "headerName": "Symbol",
            "field": COL_SYMBOL,
            "pinned": "left",
            "width": 140,
            "minWidth": 125,
            "cellStyle": cell_style,
            **default_filter,
        },
        {
            "headerName": "Reference",
            "field": COL_REFERENCE,
            "width": 115,
            "minWidth": 105,
            "type": "numericColumn",
            "valueFormatter": reference_formatter,
            "cellStyle": cell_style,
            **default_filter,
        },
        {
            "headerName": "Current",
            "headerClass": "slip-group-current",
            "children": [
                {
                    "headerName": "TW Slip (BP)",
                    "field": COL_TW_CUR,
                    "width": 140,
                    "minWidth": 120,
                    "type": "numericColumn",
                    "valueFormatter": value_formatter,
                    "cellStyle": cell_style,
                    **default_filter,
                },
                {
                    "headerName": "Exec Slip (BP)",
                    "field": COL_EX_CUR,
                    "width": 140,
                    "minWidth": 120,
                    "type": "numericColumn",
                    "valueFormatter": value_formatter,
                    "cellStyle": cell_style,
                    **default_filter,
                },
            ],
        },
        {
            "headerName": "Avg",
            "headerClass": "slip-group-avg",
            "children": [
                {
                    "headerName": "TW Session Avg (bp)",
                    "field": COL_TW_AVG,
                    "width": 175,
                    "minWidth": 150,
                    "type": "numericColumn",
                    "valueFormatter": value_formatter,
                    "cellStyle": cell_style,
                    **default_filter,
                },
                {
                    "headerName": "Exec Session Avg (bp)",
                    "field": COL_EX_AVG,
                     "width": 190,
                    "minWidth": 165,
                    "type": "numericColumn",
                    "valueFormatter": value_formatter,
                    "cellStyle": cell_style,
                    **default_filter,
                },
            ],
        },
        {
            "headerName": "Slip Diff (Bp)",
            "field": COL_MISS,
            "width": 150,
            "minWidth": 125,
            "type": "numericColumn",
            "valueFormatter": value_formatter,
            "cellStyle": cell_style,
            **default_filter,
        },
        {
            "headerName": "Fills",
            "field": COL_FILLS,
            "width": 80,
            "minWidth": 70,
            "type": "numericColumn",
            "valueFormatter": fills_formatter,
            "cellStyle": cell_style,
            **default_filter,
        },
    ]

    grid_options = {
        "columnDefs": column_defs,
        "defaultColDef": {
            "sortable": True,
            "filter": "agSetColumnFilter",
            "resizable": True,
            "floatingFilter": False,
            "suppressMenu": False,
            "menuTabs": ["filterMenuTab"],
            "wrapHeaderText": False,
            "autoHeaderHeight": False,
        },
        "rowHeight": 27,
        "headerHeight": 34,
        "groupHeaderHeight": 38,
        "floatingFiltersHeight": 0,
        "suppressRowHoverHighlight": False,
        "enableCellTextSelection": True,
        "suppressHorizontalScroll": False,
        "animateRows": False,
        "onGridReady": JsCode("""
        function(params) {
          params.api.sizeColumnsToFit();
        }
        """),
        "onGridSizeChanged": JsCode("""
        function(params) {
          params.api.sizeColumnsToFit();
        }
        """),
    }

    custom_css = {
        ".ag-root-wrapper": {"background-color": "#181c2a !important", "border": "1px solid #2a2f45 !important"},
        ".ag-root": {"background-color": "#181c2a !important"},
        ".ag-body": {"background-color": "#181c2a !important"},
        ".ag-body-viewport": {"background-color": "#181c2a !important"},
        ".ag-center-cols-viewport": {"background-color": "#181c2a !important"},
        ".ag-center-cols-container": {"background-color": "#181c2a !important"},
        ".ag-header": {"background-color": "#111520 !important", "border-bottom": "1px solid #2a2f45 !important"},
        ".ag-header-row": {"background-color": "#111520 !important"},
        ".ag-header-group-cell": {
            "background-color": "#111520 !important",
            "text-align": "center !important",
            "border-bottom": "1px solid #2a2f45 !important",
            "padding-left": "0px !important",
            "padding-right": "0px !important",
        },
        ".ag-header-group-cell-label": {
            "justify-content": "center !important",
            "align-items": "center !important",
            "width": "100% !important",
            "height": "100% !important",
            "padding": "0 !important",
        },
        ".ag-header-group-text": {
            "color": "#ffffff !important",
            "font-size": "13px !important",
            "font-weight": "800 !important",
            "font-family": "IBM Plex Sans, sans-serif !important",
            "letter-spacing": "0.02em !important",
            "text-align": "center !important",
            "width": "100% !important",
            "display": "block !important",
        },
        ".slip-group-current .ag-header-group-cell-label": {
            "justify-content": "center !important",
            "text-align": "center !important",
        },
        ".slip-group-avg .ag-header-group-cell-label": {
            "justify-content": "center !important",
            "text-align": "center !important",
        },
        ".ag-header-cell": {"background-color": "#111520 !important", "padding-left": "8px !important", "padding-right": "4px !important"},
        ".ag-header-cell-text": {
            "color": "#9aa3bc !important",
            "font-size": "10px !important",
            "font-weight": "700 !important",
            "font-family": "IBM Plex Sans, sans-serif !important",
            "text-transform": "uppercase !important",
            "letter-spacing": "0.06em !important",
        },
        ".ag-header-icon": {"color": "#6b7590 !important"},
        ".ag-header-cell-menu-button": {"color": "#6b7590 !important", "opacity": "1 !important"},
        ".ag-row": {"background-color": "#1e2238 !important", "border-bottom": "1px solid #262b42 !important"},
        ".ag-row-even": {"background-color": "#1e2238 !important"},
        ".ag-row-odd": {"background-color": "#1a1e32 !important"},
        ".ag-row-hover": {"background-color": "#262c45 !important"},
        ".ag-cell": {
            "color": "#c8cdd8 !important",
            "font-family": "JetBrains Mono, monospace !important",
            "font-size": "11px !important",
            "line-height": "27px !important",
            "padding-left": "8px !important",
            "padding-right": "8px !important",
            "border-right": "none !important",
        },
        ".ag-pinned-left-header": {"background-color": "#111520 !important", "border-right": "1px solid #2a2f45 !important"},
        ".ag-pinned-left-cols-container": {"background-color": "#1e2238 !important", "border-right": "1px solid #2a2f45 !important"},
        ".ag-pinned-left-cols-container .ag-cell": {"background-color": "#1e2238 !important", "color": "#e0e4f0 !important", "font-weight": "700 !important"},
        ".ag-menu": {"background-color": "#1a1e2e !important", "color": "#c0c6d4 !important", "border": "1px solid #2a2f45 !important"},
        ".ag-filter-body-wrapper": {"background-color": "#1a1e2e !important", "color": "#c0c6d4 !important"},
        ".ag-set-filter-list": {"background-color": "#1a1e2e !important", "color": "#c0c6d4 !important"},
        ".ag-input-field-input": {"background-color": "#111520 !important", "color": "#c0c6d4 !important", "border": "1px solid #2a2f45 !important"},
        ".ag-sort-indicator-icon": {"color": "#6b7590 !important"},
    }

    height = min(620, max(220, 72 + 27 * len(df)))

    AgGrid(
        df,
        gridOptions=grid_options,
        theme="balham-dark",
        height=height,
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        enable_enterprise_modules=True,
        custom_css=custom_css,
        key="slippage_aggrid",
    )


# ─────────────────────────────────────────────────────────────
# EOD CSV SAVE
# ─────────────────────────────────────────────────────────────
def save_eod_csv(rows: dict, trade_date: str) -> tuple[bool, str]:
    """
    Save one row per symbol to:
        {CSV_DIR}/slippage_session_{YYYYMMDD}.csv

    Columns:
        date, symbol,
        tw_slip_cur, tw_session_avg,
        exec_slip_cur, exec_session_avg,
        slip_diff, fills, tw_snapshots, exec_snapshots

    Creates CSV_DIR if it doesn't exist.
    Overwrites the file for today (safe to call multiple times — last write wins).
    """
    import csv as _csv
    try:
        os.makedirs(CSV_DIR, exist_ok=True)
        filepath = os.path.join(CSV_DIR, f"slippage_session_{trade_date}.csv")
        fieldnames = [
            "date", "symbol",
            "tw_slip_cur", "tw_session_avg",
            "exec_slip_cur", "exec_session_avg",
            "slip_diff", "fills",
            "tw_snapshots", "exec_snapshots",
        ]
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = _csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for sym in sorted(rows.keys()):
                r = rows[sym]
                writer.writerow({
                    "date":             trade_date,
                    "symbol":           sym,
                    "tw_slip_cur":      r["tw_cur"]   if r["tw_cur"]   is not None else "",
                    "tw_session_avg":   r["tw_ravg"]  if r["tw_ravg"]  is not None else "",
                    "exec_slip_cur":    r["ex_cur"]   if r["ex_cur"]   is not None else "",
                    "exec_session_avg": r["ex_ravg"]  if r["ex_ravg"]  is not None else "",
                    "slip_diff":        r["miss"]      if r["miss"]     is not None else "",
                    "fills":            r["fills"],
                    "tw_snapshots":     r["tw_n"],
                    "exec_snapshots":   r["ex_n"],
                })
        return True, filepath
    except Exception as e:
        return False, str(e)

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    today = today_str()

    if st.session_state.get("slip_date") != today:
        st.session_state["slip_date"]       = today
        st.session_state["slip_history"]    = load_historical_slip_history(today)
        st.session_state["slip_last_lines"] = -1

    log_lines, line_count = read_log_lines_cached(today)
    exec_slip  = parse_execution_slippage(log_lines)
    top_win    = fetch_top_window_slippage()
    rows       = update_running_avgs(top_win, exec_slip, line_count)

    now          = datetime.now()
    trade_date   = now.strftime("%d %b %Y")
    now_hms      = now.strftime("%H:%M:%S")
    today_yyyymmdd = now.strftime("%Y%m%d")

    # ── EOD save: trigger once after 15:30, only if rows exist ───
    eod_key    = f"slip_eod_saved_{today_yyyymmdd}"
    eod_status = st.session_state.get(eod_key, None)
    now_hm     = now.strftime("%H:%M")
    if rows and now_hm >= EOD_SAVE_TIME and eod_status is None:
        ok, result = save_eod_csv(rows, today_yyyymmdd)
        st.session_state[eod_key] = ("ok", result) if ok else ("err", result)
        eod_status = st.session_state[eod_key]

    # ── Top bar — identical layout to position dashboard ─────────
    col_title, col_time = st.columns([5, 1])

    with col_title:
        st.html(
            "<div style='font-family:IBM Plex Sans,sans-serif;"
            "font-size:17px;font-weight:600;color:#c8cdd8;"
            "letter-spacing:.02em;padding-top:6px'>"
            "📉 Slippage Dashboard</div>"
        )

    with col_time:
        eod_html = ""
        if eod_status:
            state, detail = eod_status
            if state == "ok":
                fname = os.path.basename(detail)
                eod_html = f"<br><span style='color:#2eca8a;font-size:9px'>💾 EOD saved: {fname}</span>"
            else:
                eod_html = f"<br><span style='color:#f05252;font-size:9px'>⚠ EOD save failed</span>"
        st.html(
            f"<div style='padding-top:10px;font-size:10px;"
            f"font-family:JetBrains Mono,monospace;color:#454c5e;"
            f"text-align:right'>"
            f"<span style='color:#6b7385;font-size:11px;font-weight:600'>{trade_date}</span><br>"
            f"🕐 {now_hms}<br>"
            f"<span style='color:#2eca8a'>● LIVE</span>"
            f"{eod_html}</div>"
        )

    st.html("<div style='margin:10px 0 6px'></div>")
    st.html("<div class='section-hdr'>Slippage — Top-Window vs Execution</div>")

    # ── AgGrid table ──────────────────────────────────────────────
    df = build_dataframe(rows)
    render_slippage_aggrid(df)

    # ── Footer ────────────────────────────────────────────────────
    st.html(
        "<div class='slip-foot'>"
        "TW Slip: Redis SlippageEngine — book mid at 5-min window, traded-val weighted.&nbsp;&nbsp;|&nbsp;&nbsp;"
        "Exec Slip: live log fills, mid frozen at order creation.&nbsp;&nbsp;|&nbsp;&nbsp;"
        "Session Avg updates only when log grows.&nbsp;&nbsp;|&nbsp;&nbsp;"
        "Slip Diff = TW Slip (Current) − Exec Slip (Current)."
        "</div>"
    )


if __name__ == "__main__":
    main()
