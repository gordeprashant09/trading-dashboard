"""
trading_dashboard.py
====================
Streamlit Trading Dashboard

Run:
    pip install streamlit pymongo pandas
    streamlit run trading_dashboard.py

To go live:
    1. In load_data(): swap DUMMY_DATA for load_data_from_mongo()
    2. Set MONGO_URI, MONGO_DB, MONGO_COLL env vars
"""

from __future__ import annotations
import os, time, math
from datetime import datetime, date
from typing import Optional
import streamlit as st

# ── Page config ───────────────────────────────────────────
st.set_page_config(page_title="Trading Dashboard", page_icon="chart_with_upwards_trend",
                   layout="wide", initial_sidebar_state="collapsed")

# ── Config ────────────────────────────────────────────────
MONGO_URI         = os.getenv("MONGO_URI",       "mongodb://localhost:27017/")
MONGO_DB          = os.getenv("MONGO_DB",        "dropcopy")
MONGO_COLL_TRADES = os.getenv("MONGO_COLL",      "trades")
REDIS_HOST        = os.getenv("REDIS_HOST",      "localhost")
REDIS_PORT        = int(os.getenv("REDIS_PORT",  "6379"))
LTP_HASH_KEY      = os.getenv("LTP_HASH_KEY",    "last_price")
EXPENSE_PER_CR    = float(os.getenv("EXPENSE_PER_CR", "10000"))
REFRESH_SECONDS   = int(os.getenv("REFRESH_SECONDS",  "10"))

# ── Dummy data ────────────────────────────────────────────
# Replace with load_data_from_mongo() when live.
# Fields per expiry:
#   qty_overnight  : signed prev-EOD qty (+buy / -sell)
#   prev_close     : prev day closing price
#   qty_today_buy  : today absolute buy qty
#   qty_today_sell : today absolute sell qty
#   buy_avg        : today avg buy price  (0 if no buys)
#   sell_avg       : today avg sell price (0 if no sells)
#   ltp            : last traded price
DUMMY_DATA = [
    {"sym": "IDEA", "lot_size": 7000, "expiries": [
        {"label": "IDEA20260529", "qty_overnight": 35000, "prev_close": 9.70,
         "qty_today_buy": 14000, "qty_today_sell": 7000,
         "buy_avg": 9.85, "sell_avg": 10.20, "ltp": 10.45},
        {"label": "IDEA20260626", "qty_overnight": -7000, "prev_close": 9.90,
         "qty_today_buy": 0, "qty_today_sell": 14000,
         "buy_avg": 0, "sell_avg": 9.75, "ltp": 10.45},
    ]},
    {"sym": "HDFC", "lot_size": 550, "expiries": [
        {"label": "HDFC20260529", "qty_overnight": 2200, "prev_close": 1810,
         "qty_today_buy": 1650, "qty_today_sell": 550,
         "buy_avg": 1820, "sell_avg": 1865, "ltp": 1882},
        {"label": "HDFC20260626", "qty_overnight": -550, "prev_close": 1825,
         "qty_today_buy": 0, "qty_today_sell": 550,
         "buy_avg": 0, "sell_avg": 1840, "ltp": 1882},
    ]},
    {"sym": "RELIANCE", "lot_size": 250, "expiries": [
        {"label": "RELIANCE20260529", "qty_overnight": 1000, "prev_close": 2895,
         "qty_today_buy": 750, "qty_today_sell": 250,
         "buy_avg": 2910, "sell_avg": 2960, "ltp": 2975},
        {"label": "RELIANCE20260626", "qty_overnight": 500, "prev_close": 2905,
         "qty_today_buy": 500, "qty_today_sell": 0,
         "buy_avg": 2940, "sell_avg": 0, "ltp": 2975},
    ]},
    {"sym": "NIFTY", "lot_size": 75, "expiries": [
        {"label": "NIFTY20260515", "qty_overnight": -450, "prev_close": 24150,
         "qty_today_buy": 0, "qty_today_sell": 150,
         "buy_avg": 0, "sell_avg": 24350, "ltp": 24280},
        {"label": "NIFTY20260529", "qty_overnight": 225, "prev_close": 24100,
         "qty_today_buy": 150, "qty_today_sell": 0,
         "buy_avg": 24150, "sell_avg": 0, "ltp": 24280},
    ]},
]

# ── Data loaders ──────────────────────────────────────────

def load_data() -> list[dict]:
    data = DUMMY_DATA               # <-- SWAP: comment for live
    # data = load_data_from_mongo() # <-- SWAP: uncomment for live
    return data


def load_data_from_mongo() -> list[dict]:
    from pymongo import MongoClient
    import datetime as dt
    client = MongoClient(MONGO_URI)
    coll   = client[MONGO_DB][MONGO_COLL_TRADES]
    today_start = datetime.combine(date.today(), dt.time.min)
    pipeline = [
        {"$match": {"stored_at": {"$gte": today_start}}},
        {"$group": {
            "_id": {"symbol": "$symbol", "side": "$buy_sell"},
            "total_qty":   {"$sum": "$quantity"},
            "total_value": {"$sum": {"$multiply": ["$quantity", "$price"]}},
        }},
    ]
    rows = list(coll.aggregate(pipeline))
    sym_map: dict = {}
    for row in rows:
        sym  = row["_id"]["symbol"]
        side = row["_id"]["side"]
        qty  = row["total_qty"]
        avg  = row["total_value"] / qty if qty else 0.0
        sym_map.setdefault(sym, {"qty_today_buy": 0, "buy_avg": 0,
                                  "qty_today_sell": 0, "sell_avg": 0})
        if side == "B":
            sym_map[sym]["qty_today_buy"] = qty
            sym_map[sym]["buy_avg"]       = avg
        else:
            sym_map[sym]["qty_today_sell"] = qty
            sym_map[sym]["sell_avg"]       = avg
    ltp_map = get_ltp_from_redis()
    result = []
    for sym, vals in sym_map.items():
        result.append({
            "sym": sym, "lot_size": 1,
            "expiries": [{"label": sym, "qty_overnight": 0, "prev_close": 0,
                          "qty_today_buy": vals["qty_today_buy"],
                          "qty_today_sell": vals["qty_today_sell"],
                          "buy_avg": vals["buy_avg"], "sell_avg": vals["sell_avg"],
                          "ltp": ltp_map.get(sym, vals["buy_avg"] or vals["sell_avg"])}]
        })
    return result


def get_ltp_from_redis() -> dict:
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                        decode_responses=True, socket_timeout=1.0)
        raw = r.hgetall(LTP_HASH_KEY) or {}
        return {k: float(v) for k, v in raw.items() if v}
    except Exception:
        return {}

# ── PnL engine ────────────────────────────────────────────
# Mirrors risk_worker.py:
#   carry = compute_carry_pnl_from_prev()
#   day   = compute_day_pnl_from_trades()
#   net   = carry + day - expenses

def calc_expiry(e: dict, lot_size: int) -> dict:
    net_today  = e["qty_today_buy"] - e["qty_today_sell"]
    open_qty   = e["qty_overnight"] + net_today
    lots       = open_qty / lot_size if lot_size > 0 else None
    carry      = e["qty_overnight"] * (e["ltp"] - e["prev_close"])
    day_buy    = e["qty_today_buy"]  * (e["ltp"] - e["buy_avg"])   if e["qty_today_buy"]  > 0 else 0
    day_sell   = e["qty_today_sell"] * (e["sell_avg"] - e["ltp"])  if e["qty_today_sell"] > 0 else 0
    traded_val = (e["qty_today_buy"]  * (e["buy_avg"]  or e["ltp"])) + \
                 (e["qty_today_sell"] * (e["sell_avg"] or e["ltp"]))
    expenses   = (traded_val / 1e7) * EXPENSE_PER_CR
    net        = carry + day_buy + day_sell - expenses
    return {"label": e["label"], "ltp": e["ltp"], "lots": lots,
            "open_qty": open_qty, "net_exp": open_qty * e["ltp"],
            "traded_val": traded_val, "net": net}


def build_rows(data: list[dict]) -> tuple[list[dict], dict]:
    rows = []
    kpis = {"net_exp": 0.0, "traded_val": 0.0, "net": 0.0}
    for st_item in data:
        sym      = st_item["sym"]
        lot_size = st_item["lot_size"]
        exp_res  = [calc_expiry(e, lot_size) for e in st_item["expiries"]]
        total_oq = sum(r["open_qty"]   for r in exp_res)
        s_ne     = sum(r["net_exp"]    for r in exp_res)
        s_tv     = sum(r["traded_val"] for r in exp_res)
        s_net    = sum(r["net"]        for r in exp_res)
        rows.append({"is_stock": True, "sym": sym, "lot_size": lot_size,
                     "label": sym, "lots": total_oq / lot_size if lot_size > 0 else None,
                     "open_qty": total_oq, "net_exp": s_ne,
                     "traded_val": s_tv, "net": s_net, "ltp": None})
        for r in exp_res:
            rows.append({**r, "is_stock": False, "sym": sym, "lot_size": lot_size})
        kpis["net_exp"]    += s_ne
        kpis["traded_val"] += s_tv
        kpis["net"]        += s_net
    return rows, kpis

# ── Formatters ────────────────────────────────────────────

def fmt_inr(n: Optional[float], show_sign: bool = False) -> str:
    if n is None or (isinstance(n, float) and (math.isnan(n) or math.isinf(n))):
        return "&mdash;"
    n   = float(n)
    a   = abs(n)
    sgn = "+" if (show_sign and n >= 0) else ""
    if a >= 1e7: return f"{sgn}{n/1e7:.2f} Cr"
    if a >= 1e5: return f"{sgn}{n/1e5:.2f} L"
    return f"{sgn}{n:,.0f}"


def fmt_lots(v: Optional[float]) -> str:
    if v is None: return "&mdash;"
    r = round(v, 1)
    return f"+{r}" if r > 0 else str(r)


def vcol(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)): return "#565c6e"
    return "#565c6e" if abs(v) < 1 else ("#2ecc8a" if v > 0 else "#f05252")

# ── Table HTML ────────────────────────────────────────────

TABLE_CSS = """
<style>
.dt{width:100%;border-collapse:collapse;font-size:12px;font-family:monospace}
.dt th{text-align:right;font-size:10px;font-weight:600;color:#565c6e;
       text-transform:uppercase;letter-spacing:.06em;
       padding:6px 10px;border-bottom:1px solid #2a2d35}
.dt th.L{text-align:left}
.dt td{padding:5px 10px;text-align:right;border-bottom:1px solid #1e2027;color:#e8eaf0}
.dt td.L{text-align:left}
.dt tr.sr{background:#1e2027;font-weight:600}
.dt tr.er{background:#16181c}
.dt tr.er td.L{padding-left:28px;color:#8b91a0;font-size:11px}
.lsz{font-size:9px;color:#565c6e;margin-left:5px;font-weight:400}
.ltplbl{font-size:10px;color:#565c6e;margin-left:6px}
</style>
"""


def render_table(rows: list[dict], expand_all: bool, expanded_syms: set = None) -> str:
    if expanded_syms is None: expanded_syms = set()
    html = TABLE_CSS
    html += """<table class="dt"><thead><tr>
      <th class="L" style="width:32%">Symbol / Expiry</th>
      <th style="width:10%">Lots</th>
      <th style="width:19%">Net Exp.</th>
      <th style="width:19%">Traded Val</th>
      <th style="width:20%">Net PnL</th>
    </tr></thead><tbody>"""

    for row in rows:
        lc  = vcol(row["lots"])
        ec  = vcol(row["net_exp"])
        nc  = vcol(row["net"])
        lts = fmt_lots(row["lots"])

        if row["is_stock"]:
            lsz = f"{row['lot_size']:,}"
            sym = row["sym"]
            html += (
                f'<tr class="sr">'
                f'<td class="L">&gt; {sym} <span class="lsz">lot {lsz}</span></td>'
                f'<td style="color:{lc}">{lts}</td>'
                f'<td style="color:{ec}">{fmt_inr(row["net_exp"])}</td>'
                f'<td>{fmt_inr(row["traded_val"])}</td>'
                f'<td style="color:{nc}">{fmt_inr(row["net"], show_sign=True)}</td>'
                f'</tr>'
            )
        else:
            if not expand_all and row.get("sym","") not in expanded_syms:
                continue
            ltp_str = f"{row['ltp']:,.2f}" if row["ltp"] else "&mdash;"
            html += (
                f'<tr class="er">'
                f'<td class="L">{row["label"]}'
                f'<span class="ltplbl">LTP {ltp_str}</span></td>'
                f'<td style="color:{lc}">{lts}</td>'
                f'<td style="color:{ec}">{fmt_inr(row["net_exp"])}</td>'
                f'<td>{fmt_inr(row["traded_val"])}</td>'
                f'<td style="color:{nc}">{fmt_inr(row["net"], show_sign=True)}</td>'
                f'</tr>'
            )

    html += "</tbody></table>"
    return html

def render_stock_row(row: dict) -> str:
    lc  = vcol(row["lots"])
    ec  = vcol(row["net_exp"])
    nc  = vcol(row["net"])
    lts = fmt_lots(row["lots"])
    lsz = f"{row['lot_size']:,}"
    return TABLE_CSS + f"""
    <table class="dt" style="margin:0;border:none">
    <tbody><tr class="sr">
      <td class="L" style="width:32%">{row['sym']} <span class="lsz">lot {lsz}</span></td>
      <td style="width:10%;color:{lc};text-align:right">{lts}</td>
      <td style="width:19%;color:{ec};text-align:right">{fmt_inr(row["net_exp"])}</td>
      <td style="width:19%;text-align:right">{fmt_inr(row["traded_val"])}</td>
      <td style="width:20%;color:{nc};text-align:right">{fmt_inr(row["net"], show_sign=True)}</td>
    </tr></tbody></table>"""


def render_expiry_rows(exp_rows: list[dict]) -> str:
    html = TABLE_CSS + '<table class="dt" style="margin:0"><tbody>'
    for row in exp_rows:
        lc      = vcol(row["lots"])
        ec      = vcol(row["net_exp"])
        nc      = vcol(row["net"])
        lts     = fmt_lots(row["lots"])
        ltp_str = f"{row['ltp']:,.2f}" if row["ltp"] else "&mdash;"
        html += (
            f'<tr class="er">'
            f'<td class="L" style="width:32%;padding-left:28px;color:#8b91a0;font-size:11px">'
            f'{row["label"]}<span class="ltplbl"> LTP {ltp_str}</span></td>'
            f'<td style="width:10%;color:{lc};text-align:right">{lts}</td>'
            f'<td style="width:19%;color:{ec};text-align:right">{fmt_inr(row["net_exp"])}</td>'
            f'<td style="width:19%;text-align:right">{fmt_inr(row["traded_val"])}</td>'
            f'<td style="width:20%;color:{nc};text-align:right">{fmt_inr(row["net"], show_sign=True)}</td>'
            f'</tr>'
        )
    html += "</tbody></table>"
    return html


# ── Global CSS ────────────────────────────────────────────

st.html("""
<style>
#MainMenu, footer, header{visibility:hidden}
.block-container{padding-top:1rem !important;padding-bottom:1rem !important}
[data-testid="stMetricValue"]{font-size:1.6rem !important;font-weight:600}
[data-testid="stMetricLabel"]{font-size:10px !important;text-transform:uppercase;
  letter-spacing:.06em;color:#888 !important}

/* small toggle button beside each stock row */
div[data-testid="stColumn"]:first-child div[data-testid="stButton"] button {
    height: 32px !important;
    min-height: 0 !important;
    padding: 0 6px !important;
    font-size: 10px !important;
    font-family: monospace !important;
    background: #1e2027 !important;
    border: 1px solid #2a2d35 !important;
    color: #565c6e !important;
    border-radius: 3px !important;
    width: 100% !important;
}
div[data-testid="stColumn"]:first-child div[data-testid="stButton"] button:hover {
    color: #e8eaf0 !important;
    border-color: #363a44 !important;
}
</style>
""")

# ── Main ──────────────────────────────────────────────────

def main():
    if "expand_all" not in st.session_state:
        st.session_state.expand_all = False
    if "expanded_syms" not in st.session_state:
        st.session_state.expanded_syms = set()

    # Top bar
    col_title, col_time, col_btn = st.columns([4, 2, 1])
    with col_title:
        st.markdown("### Trading Dashboard")
    with col_time:
        st.html(
            f"<div style='padding-top:14px;font-size:11px;color:#888'>"
            f"  {datetime.now().strftime('%H:%M:%S')} &nbsp;"
            f"<span style='color:#e8a825'>DUMMY DATA</span></div>")
    with col_btn:
        lbl = "Collapse all" if st.session_state.expand_all else "Expand all"
        if st.button(lbl, use_container_width=True):
            st.session_state.expand_all = not st.session_state.expand_all
            if not st.session_state.expand_all:
                st.session_state.expanded_syms = set()
            st.rerun()

    # Load + compute
    data       = load_data()
    rows, kpis = build_rows(data)

    # KPI strip
    k1, k2, k3 = st.columns(3)
    k1.metric("Net Exposure", fmt_inr(kpis["net_exp"]).replace("&mdash;", "-"))
    k2.metric("Traded Value", fmt_inr(kpis["traded_val"]).replace("&mdash;", "-"))
    k3.metric("Net PnL",      fmt_inr(kpis["net"], show_sign=True).replace("&mdash;", "-"))

    st.divider()

    st.html(
        "<div style='font-size:10px;color:#565c6e;letter-spacing:.08em;"
        "text-transform:uppercase;margin-bottom:6px'>Position book &#8212; intraday</div>")

    # Table header
    st.html(TABLE_CSS + """<table class="dt"><thead><tr>
      <th class="L" style="width:5%"></th>
      <th class="L" style="width:27%">Symbol / Expiry</th>
      <th style="width:10%">Lots</th>
      <th style="width:19%">Net Exp.</th>
      <th style="width:19%">Traded Val</th>
      <th style="width:20%">Net PnL</th>
    </tr></thead></table>""")

    # Per-stock rows — button to toggle + html for the row values
    stock_rows = [r for r in rows if r["is_stock"]]
    for srow in stock_rows:
        sym     = srow["sym"]
        is_open = st.session_state.expand_all or (sym in st.session_state.expanded_syms)
        exp_rows = [r for r in rows if not r["is_stock"] and r["sym"] == sym]

        c_btn, c_row = st.columns([0.04, 0.96])
        with c_btn:
            arrow = "v" if is_open else ">"
            if st.button(arrow, key=f"tog_{sym}", use_container_width=True):
                if sym in st.session_state.expanded_syms:
                    st.session_state.expanded_syms.discard(sym)
                else:
                    st.session_state.expanded_syms.add(sym)
                st.rerun()
        with c_row:
            st.html(render_stock_row(srow))

        if is_open:
            st.html(render_expiry_rows(exp_rows))


if __name__ == "__main__":
    main()
