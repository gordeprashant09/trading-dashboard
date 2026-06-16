"""
dashboard_charts.py
====================
Streamlit chart dashboard — MTM vs NIFTY, Net Exposure vs NIFTY,
Symbol-wise MTM vs NIFTY.

Run:
    streamlit run dashboard_charts.py --server.port 8503

Data: reads from Redis dashboard:chart:YYYYMMDD (written by chart_collector.py)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from dropcopy_pnl_tab import show_dropcopy_pnl_tab
st.set_page_config(
    page_title="Dashboard Charts",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Config ────────────────────────────────────────────────────────
REDIS_HOST  = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT  = int(os.getenv("REDIS_PORT", "6379"))
DASH_DB     = int(os.getenv("REDIS_DB", "1"))
CHART_KEY   = "dashboard:chart:"
IST         = ZoneInfo("Asia/Kolkata")

# ── CSS ───────────────────────────────────────────────────────────
st.html("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;600&family=JetBrains+Mono&display=swap');
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1rem !important; }
[data-testid="stMetricValue"] {
    font-size: 1.2rem !important; font-weight: 600;
    font-family: 'JetBrains Mono', monospace !important;
}
[data-testid="stMetric"] {
    background: #13151a; border: 1px solid #1e2230;
    border-radius: 6px; padding: 10px 14px !important;
}
</style>
""")


# ══════════════════════════════════════════════════════════════════
# DATA LOADER
# ══════════════════════════════════════════════════════════════════

@st.cache_data(ttl=30)
def load_snapshots(date_str: str) -> list[dict]:
    """Load time-series snapshots from Redis for a given date."""
    try:
        import redis as _redis
        r = _redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                         db=DASH_DB, decode_responses=True, socket_timeout=2)
        raw = r.get(f"{CHART_KEY}{date_str}")
        if not raw:
            return []
        return json.loads(raw)
    except Exception:
        return []



def load_day_high_low(date_str: str) -> dict:
    """Load Position Book day high/low from same Redis keys used by main dashboard."""
    out = {}
    try:
        import redis as _redis
        r = _redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                         db=DASH_DB, decode_responses=True, socket_timeout=2)

        for name, key in [
            ("high", f"dashboard:day_high:{date_str}"),
            ("low",  f"dashboard:day_low:{date_str}"),
        ]:
            raw = r.get(key)
            if not raw:
                continue

            try:
                import json
                val = json.loads(raw)
                if isinstance(val, dict):
                    out[name] = float(val.get("pnl", val.get("value", 0)))
                    out[name + "_time"] = str(val.get("time", ""))
                else:
                    out[name] = float(val)
            except Exception:
                out[name] = float(raw)

    except Exception:
        pass

    return out


def get_available_dates() -> list[str]:
    """Get dates that have chart data in Redis."""
    try:
        import redis as _redis
        r = _redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                         db=DASH_DB, decode_responses=True, socket_timeout=2)
        keys = r.keys(f"{CHART_KEY}*")
        dates = sorted([k.replace(CHART_KEY, "") for k in keys], reverse=True)
        return dates
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════
# CHART BUILDERS
# ══════════════════════════════════════════════════════════════════

def build_dual_axis_chart(snaps: list[dict], y1_key: str, y1_label: str,
                           y1_color: str = "#2eca8a", high_low: dict | None = None) -> dict:
    """Build Plotly dual-axis chart config."""
    times      = [s["time"] for s in snaps]
    y1_vals    = [s.get(y1_key, 0) for s in snaps]  # in ₹
    nifty_vals = [s.get("nifty", 0) for s in snaps]

    extra_traces = []
    if y1_key == "net_pnl" and y1_vals:
        high_low = high_low or {}

        def nearest_x(target_time: str, fallback_idx: int) -> str:
            if target_time and target_time in times:
                return target_time
            return times[fallback_idx]

        hi_val = high_low.get("high")
        lo_val = high_low.get("low")
        hi_time = high_low.get("high_time", "")
        lo_time = high_low.get("low_time", "")

        if hi_val is None:
            hi_idx = max(range(len(y1_vals)), key=lambda i: y1_vals[i])
            hi_val = y1_vals[hi_idx]
            hi_x = times[hi_idx]
        else:
            hi_idx = min(range(len(y1_vals)), key=lambda i: abs(y1_vals[i] - hi_val))
            hi_x = nearest_x(hi_time, hi_idx)

        if lo_val is None:
            lo_idx = min(range(len(y1_vals)), key=lambda i: y1_vals[i])
            lo_val = y1_vals[lo_idx]
            lo_x = times[lo_idx]
        else:
            lo_idx = min(range(len(y1_vals)), key=lambda i: abs(y1_vals[i] - lo_val))
            lo_x = nearest_x(lo_time, lo_idx)

        extra_traces = [
            {
                "x": [hi_x],
                "y": [hi_val],
                "type": "scatter",
                "mode": "markers+text",
                "name": f"Day High {hi_val:,.0f}",
                "marker": {"size": 10, "symbol": "triangle-up", "color": "#00d084"},
                "text": [f"High {hi_val:,.0f}"],
                "textposition": "top center",
                "yaxis": "y1",
            },
            {
                "x": [lo_x],
                "y": [lo_val],
                "type": "scatter",
                "mode": "markers+text",
                "name": f"Day Low {lo_val:,.0f}",
                "marker": {"size": 10, "symbol": "triangle-down", "color": "#f05252"},
                "text": [f"Low {lo_val:,.0f}"],
                "textposition": "bottom center",
                "yaxis": "y1",
            },
        ]

    return {
        "data": [
            {
                "x": times, "y": y1_vals,
                "type": "scatter", "mode": "lines",
                "name": y1_label,
                "line": {"color": y1_color, "width": 2},
                "yaxis": "y1",
                "hovertemplate": "%{x}<br>%{y:,.0f}<extra></extra>",
            },
            {
                "x": times, "y": nifty_vals,
                "type": "scatter", "mode": "lines",
                "name": "NIFTY",
                "line": {"color": "#e8a825", "width": 1.5, "dash": "dot"},
                "yaxis": "y2",
                "hovertemplate": "%{y:,.0f}<extra>NIFTY</extra>",
            },
            *extra_traces,
        ],
        "layout": {
            "paper_bgcolor": "#0f1117",
            "plot_bgcolor":  "#13151a",
            "font": {"color": "#7a8294", "family": "JetBrains Mono"},
            "margin": {"t": 30, "b": 40, "l": 60, "r": 60},
            "legend": {"x": 0, "y": 1, "bgcolor": "rgba(0,0,0,0)",
                       "font": {"size": 11}},
            "xaxis": {
                "gridcolor": "#1e2230", "tickfont": {"size": 10},
                "title": "Time (IST)",
            },
            "yaxis": {
                "title": f"{y1_label} (L)",
                "gridcolor": "#1e2230", "tickfont": {"size": 10},
                "title": {"font": {"color": y1_color}},
                "tickfont":  {"color": y1_color},
            },
            "yaxis2": {
                "title": "NIFTY",
                "overlaying": "y", "side": "right",
                "exponentformat": "none", "tickformat": ",.0f", "gridcolor": "#1e2230", "tickfont": {"size": 10},
                "title": {"font": {"color": "#e8a825"}},
                "tickfont":  {"color": "#e8a825"},
                "showgrid": False,
            },
            "hovermode": "x unified",
        }
    }


def build_normalized_chart(snaps: list[dict], y1_key: str, y1_label: str,
                            y1_color: str = "#2eca8a") -> dict:
    """
    Normalized % change chart — both Net PnL and NIFTY rebased to 0% at the
    first snapshot. This lets you directly compare direction & magnitude.

    Net PnL % change  = (pnl_now - pnl_first) / abs(pnl_first) * 100
    NIFTY % change    = (nifty_now - nifty_first) / nifty_first * 100

    Interpretation:
      Lines moving together  → PnL positively correlated with NIFTY
      Lines diverging        → PnL is moving independently (alpha / hedge)
      PnL rising, NIFTY flat → pure alpha
    """
    times      = [s["time"] for s in snaps]
    y1_vals    = [s.get(y1_key, 0) for s in snaps]
    nifty_vals = [s.get("nifty", 0) for s in snaps]

    # Rebase from first non-zero value
    pnl_base   = next((v for v in y1_vals    if v != 0), None)
    nifty_base = next((v for v in nifty_vals if v != 0), None)

    def pct(vals, base):
        if base is None or base == 0:
            return [0.0] * len(vals)
        return [round((v - base) / abs(base) * 100, 3) for v in vals]

    pnl_pct   = pct(y1_vals,    pnl_base)
    nifty_pct = pct(nifty_vals, nifty_base)

    # Correlation label for chart title
    if len(pnl_pct) > 1:
        import statistics
        try:
            n = len(pnl_pct)
            mean_p = sum(pnl_pct)   / n
            mean_n = sum(nifty_pct) / n
            cov    = sum((p - mean_p) * (q - mean_n)
                         for p, q in zip(pnl_pct, nifty_pct)) / n
            std_p  = statistics.stdev(pnl_pct)   or 1e-9
            std_n  = statistics.stdev(nifty_pct) or 1e-9
            corr   = round(cov / (std_p * std_n), 2)
            corr_label = f"  |  Correlation: {corr:+.2f}"
        except Exception:
            corr_label = ""
    else:
        corr_label = ""

    return {
        "data": [
            {
                "x": times, "y": pnl_pct,
                "type": "scatter", "mode": "lines",
                "name": f"{y1_label} % chg",
                "line": {"color": y1_color, "width": 2},
                "yaxis": "y1",
                "hovertemplate": "%{x}<br>" + y1_label + ": %{y:+.2f}%<extra></extra>",
            },
            {
                "x": times, "y": nifty_pct,
                "type": "scatter", "mode": "lines",
                "name": "NIFTY % chg",
                "line": {"color": "#e8a825", "width": 1.5, "dash": "dot"},
                "yaxis": "y1",
                "hovertemplate": "%{x}<br>NIFTY: %{y:+.2f}%<extra></extra>",
            },
            # Zero baseline
            {
                "x": [times[0], times[-1]] if times else [],
                "y": [0, 0],
                "type": "scatter", "mode": "lines",
                "name": "baseline",
                "line": {"color": "rgba(255,255,255,0.1)", "width": 1, "dash": "dash"},
                "yaxis": "y1",
                "showlegend": False,
                "hoverinfo": "skip",
            },
        ],
        "layout": {
            "paper_bgcolor": "#0f1117",
            "plot_bgcolor":  "#13151a",
            "font": {"color": "#7a8294", "family": "JetBrains Mono"},
            "margin": {"t": 30, "b": 40, "l": 60, "r": 20},
            "legend": {"x": 0, "y": 1, "bgcolor": "rgba(0,0,0,0)",
                       "font": {"size": 11}},
            "xaxis": {
                "gridcolor": "#1e2230", "tickfont": {"size": 10},
                "title": "Time (IST)",
            },
            "yaxis": {
                "title": f"% Change from open{corr_label}",
                "gridcolor": "#1e2230", "tickfont": {"size": 10},
                "ticksuffix": "%",
                "zeroline": False,
            },
            "hovermode": "x unified",
        }
    }



def _first_nonzero(vals):
    for v in vals:
        try:
            fv = float(v)
            if fv != 0:
                return fv
        except Exception:
            continue
    return None


def _pct_change(vals, base=None):
    if base is None:
        base = _first_nonzero(vals)
    if base is None or base == 0:
        return [0.0 for _ in vals]
    out = []
    for v in vals:
        try:
            out.append(round((float(v) - base) / abs(base) * 100, 3))
        except Exception:
            out.append(0.0)
    return out


def _corr_label(a, b):
    try:
        import statistics
        if len(a) < 3 or len(b) < 3:
            return ""
        n = min(len(a), len(b))
        a = a[:n]
        b = b[:n]
        ma = sum(a) / n
        mb = sum(b) / n
        cov = sum((x - ma) * (y - mb) for x, y in zip(a, b)) / n
        sa = statistics.stdev(a) or 1e-9
        sb = statistics.stdev(b) or 1e-9
        return f" | Corr: {cov / (sa * sb):+.2f}"
    except Exception:
        return ""


def _normalized_layout(title, corr_label=""):
    return {
        "paper_bgcolor": "#0f1117",
        "plot_bgcolor":  "#13151a",
        "font": {"color": "#7a8294", "family": "JetBrains Mono"},
        "margin": {"t": 30, "b": 40, "l": 60, "r": 20},
        "legend": {"x": 0, "y": 1, "bgcolor": "rgba(0,0,0,0)", "font": {"size": 11}},
        "xaxis": {"gridcolor": "#1e2230", "tickfont": {"size": 10}, "title": "Time (IST)"},
        "yaxis": {"title": f"{title}{corr_label}", "gridcolor": "#1e2230",
                  "tickfont": {"size": 10}, "ticksuffix": "%", "zeroline": False},
        "hovermode": "x unified",
    }


def build_normalized_symbol_chart(snaps: list[dict], symbol: str) -> dict:
    """Symbol MTM and NIFTY normalized to % change from first non-zero snapshot."""
    times = [s["time"] for s in snaps]
    sym_vals = [s.get("symbols", {}).get(symbol, {}).get("net_pnl", 0) for s in snaps]
    nifty_vals = [s.get("nifty", 0) for s in snaps]

    sym_pct = _pct_change(sym_vals)
    nifty_pct = _pct_change(nifty_vals)
    corr = _corr_label(sym_pct, nifty_pct)

    last_raw = sym_vals[-1] if sym_vals else 0
    line_color = "#2eca8a" if last_raw >= 0 else "#f05252"

    return {
        "data": [
            {"x": times, "y": sym_pct, "type": "scatter", "mode": "lines",
             "name": f"{symbol} MTM % chg", "line": {"color": line_color, "width": 2},
             "hovertemplate": "%{x}<br>" + symbol + ": %{y:+.2f}%<extra></extra>"},
            {"x": times, "y": nifty_pct, "type": "scatter", "mode": "lines",
             "name": "NIFTY % chg", "line": {"color": "#e8a825", "width": 1.5, "dash": "dot"},
             "hovertemplate": "%{x}<br>NIFTY: %{y:+.2f}%<extra></extra>"},
            {"x": [times[0], times[-1]] if times else [], "y": [0, 0],
             "type": "scatter", "mode": "lines", "name": "baseline",
             "line": {"color": "rgba(255,255,255,0.1)", "width": 1, "dash": "dash"},
             "showlegend": False, "hoverinfo": "skip"},
        ],
        "layout": _normalized_layout("% Change from first snapshot", corr),
    }


def build_normalized_multi_symbol_chart(snaps: list[dict], symbols: list[str]) -> dict:
    """Combined selected symbols normalized to % change, with NIFTY also normalized."""
    times = [s["time"] for s in snaps]
    nifty_vals = [s.get("nifty", 0) for s in snaps]
    nifty_pct = _pct_change(nifty_vals)

    colors = ["#2eca8a", "#4a9eff", "#f05252", "#e8a825",
              "#a855f7", "#06b6d4", "#f97316", "#84cc16"]

    traces = []
    for idx, sym in enumerate(symbols):
        vals = [s.get("symbols", {}).get(sym, {}).get("net_pnl", 0) for s in snaps]
        pct = _pct_change(vals)
        traces.append({
            "x": times, "y": pct, "type": "scatter", "mode": "lines",
            "name": f"{sym} % chg",
            "line": {"color": colors[idx % len(colors)], "width": 1.7},
            "hovertemplate": "%{x}<br>" + sym + ": %{y:+.2f}%<extra></extra>",
        })

    traces.append({
        "x": times, "y": nifty_pct, "type": "scatter", "mode": "lines",
        "name": "NIFTY % chg",
        "line": {"color": "#e8a825", "width": 2, "dash": "dot"},
        "hovertemplate": "%{x}<br>NIFTY: %{y:+.2f}%<extra></extra>",
    })

    if times:
        traces.append({
            "x": [times[0], times[-1]], "y": [0, 0],
            "type": "scatter", "mode": "lines", "name": "baseline",
            "line": {"color": "rgba(255,255,255,0.1)", "width": 1, "dash": "dash"},
            "showlegend": False, "hoverinfo": "skip",
        })

    return {"data": traces, "layout": _normalized_layout("% Change from first snapshot")}

def build_symbol_chart(snaps: list[dict], symbol: str) -> dict:
    """Build symbol-wise MTM vs NIFTY chart."""
    times      = [s["time"] for s in snaps]
    sym_pnl    = [s.get("symbols", {}).get(symbol, {}).get("net_pnl", 0)
                  for s in snaps]
    nifty_vals = [s.get("nifty", 0) for s in snaps]

    # Color based on last value
    last_pnl  = sym_pnl[-1] if sym_pnl else 0
    line_color = "#2eca8a" if last_pnl >= 0 else "#f05252"

    return {
        "data": [
            {
                "x": times, "y": sym_pnl,
                "type": "scatter", "mode": "lines",
                "name": f"{symbol} MTM",
                "line": {"color": line_color, "width": 2},
                "yaxis": "y1",
                "hovertemplate": "%{x}<br>%{y:,.0f}<extra></extra>",
                "fill": "tozeroy",
                "fillcolor": f"rgba({'46,202,138' if last_pnl >= 0 else '240,82,82'},0.1)",
            },
            {
                "x": times, "y": nifty_vals,
                "type": "scatter", "mode": "lines",
                "name": "NIFTY",
                "line": {"color": "#e8a825", "width": 1.5, "dash": "dot"},
                "yaxis": "y2",
                "hovertemplate": "%{y:,.0f}<extra>NIFTY</extra>",
            },
        ],
        "layout": {
            "paper_bgcolor": "#0f1117",
            "plot_bgcolor":  "#13151a",
            "font": {"color": "#7a8294", "family": "JetBrains Mono"},
            "margin": {"t": 30, "b": 40, "l": 60, "r": 60},
            "legend": {"x": 0, "y": 1, "bgcolor": "rgba(0,0,0,0)",
                       "font": {"size": 11}},
            "xaxis": {"gridcolor": "#1e2230", "tickfont": {"size": 10}},
            "yaxis": {
                "title": f"{symbol} MTM (L)",
                "gridcolor": "#1e2230",
                "title": {"font": {"color": line_color}},
                "tickfont":  {"color": line_color},
            },
            "yaxis2": {
                "title": "NIFTY",
                "overlaying": "y", "side": "right",
                "exponentformat": "none", "tickformat": ",.0f", "gridcolor": "#1e2230",
                "title": {"font": {"color": "#e8a825"}},
                "tickfont":  {"color": "#e8a825"},
                "showgrid": False,
            },
            "hovermode": "x unified",
        }
    }


# ══════════════════════════════════════════════════════════════════
# MAIN APP
# ══════════════════════════════════════════════════════════════════

def main():
    st_autorefresh(interval=30000, key="chart_refresh")  # refresh every 30s

    # ── Header ────────────────────────────────────────────────────
    col_title, col_date, col_info = st.columns([3, 1, 1])

    with col_title:
        st.html("<div style='font-family:IBM Plex Sans;font-size:17px;"
                "font-weight:600;color:#c8cdd8;padding-top:6px'>"
                "📈 Trading Dashboard — Charts</div>")

    # ── Date selector ─────────────────────────────────────────────
    available = get_available_dates()
    today     = date.today().strftime("%Y%m%d")

    with col_date:
        if available:
            sel_date = st.selectbox("Date", available,
                                    index=0 if today in available else 0,
                                    label_visibility="collapsed")
        else:
            sel_date = today
            st.info("No chart data yet — collector not started")

    # ── Load snapshots ────────────────────────────────────────────
    snaps = load_snapshots(sel_date)
    day_high_low = load_day_high_low(sel_date)

    with col_info:
        st.html(
            f"<div style='text-align:right;font-size:11px;"
            f"font-family:JetBrains Mono;color:#555c6e;padding-top:8px'>"
            f"🕐 {datetime.now(IST).strftime('%H:%M:%S')}<br>"
            f"Snapshots: {len(snaps)}</div>"
        )

    if not snaps:
        st.html("""
        <div style='text-align:center;padding:60px;color:#555c6e;
                    font-family:IBM Plex Sans;font-size:14px'>
            <div style='font-size:40px;margin-bottom:16px'>📊</div>
            <div style='font-size:16px;color:#7a8294'>No chart data available</div>
            <div style='margin-top:8px;font-size:12px'>
                Start the collector:<br>
                <code style='color:#4a9eff'>python3 dashboard_chart_collector.py</code>
            </div>
        </div>
        """)
        return

    # ── Latest snapshot KPIs ─────────────────────────────────────
    latest = snaps[-1]
    k1, k2, k3, k4, k5 = st.columns(5)
    def fmt(v): return f"+₹{v:,.0f}" if v >= 0 else f"₹{v:,.0f}"
    k1.metric("Net PnL",    fmt(latest["net_pnl"]))
    k2.metric("Net Exp",    fmt(latest["net_exp"]))
    k3.metric("Carry PnL",  fmt(latest["carry_pnl"]))
    k4.metric("NIFTY",      f"{latest['nifty']:,.0f}")
    k5.metric("Snapshots",  f"{len(snaps)} pts")

    st.html("<div style='margin:8px 0'></div>")

    # ── Tabs ─────────────────────────────────────────────────
    tab1, tab2 = st.tabs(["📈 MTM vs NIFTY", "📊 Daily PnL (Dropcopy)"])

    with tab1:

        # ── Chart mode toggle ─────────────────────────────────────────
        col_lbl, col_tog = st.columns([5, 1])
        with col_lbl:
            st.html("<div style='font-size:11px;color:#555c6e;text-transform:uppercase;"
                    "letter-spacing:.1em;margin-bottom:4px'>MTM (Net PnL) vs NIFTY</div>")
        with col_tog:
            normalized = st.toggle(
                "Normalize all charts %", value=False, key="norm_toggle",
                help=(
                    "OFF — Dual axis: Net PnL (₹) left, NIFTY (pts) right. "
                    "Visual overlap but units differ.\n\n"
                    "ON — Both rebased to % change from first snapshot. "
                    "Directly comparable: same direction = correlated, "
                    "diverging = PnL moving independently of NIFTY. "
                    "Correlation shown in Y-axis label."
                )
            )

        # ── Chart 1: MTM vs NIFTY ─────────────────────────────────────
        if normalized:
            st.html(
                "<div style='font-size:10px;color:#4a9eff;margin:-4px 0 6px;'>"
                "📐 Normalized: all chart lines rebased to 0% from first non-zero snapshot. "
                "Correlation shown in Y-axis label. "
                "<span style='color:#2eca8a'>Green close to orange = NIFTY-driven PnL.</span>"
                "</div>"
            )
            chart1 = build_normalized_chart(snaps, "net_pnl", "Net PnL", "#2eca8a")
        else:
            chart1 = build_dual_axis_chart(snaps, "net_pnl", "Net PnL", "#2eca8a", day_high_low)
        st.plotly_chart(chart1, width="stretch", config={"displayModeBar": False})

        # ── Chart 2: Net Exposure vs NIFTY ────────────────────────────
        st.html("<div style='font-size:11px;color:#555c6e;text-transform:uppercase;"
                "letter-spacing:.1em;margin-bottom:4px'>Net Exposure vs NIFTY</div>")
        if normalized:
            chart2 = build_normalized_chart(snaps, "net_exp", "Net Exposure", "#4a9eff")
        else:
            chart2 = build_dual_axis_chart(snaps, "net_exp", "Net Exposure", "#4a9eff")
        st.plotly_chart(chart2, width="stretch", config={"displayModeBar": False})

        # ── Chart 3: Symbol-wise MTM vs NIFTY ────────────────────────
        st.html("<div style='font-size:11px;color:#555c6e;text-transform:uppercase;"
                "letter-spacing:.1em;margin-bottom:4px'>Symbol-wise MTM vs NIFTY</div>")

        # Get all symbols from snapshots
        all_syms = sorted(set(
            sym for s in snaps for sym in s.get("symbols", {}).keys()
        ))

        if all_syms:
            # Symbol selector — multi select for comparison
            col_sel, col_mode = st.columns([3, 1])
            with col_sel:
                sel_syms = st.multiselect(
                    "Select symbols", all_syms,
                    default=all_syms[:4],
                    label_visibility="collapsed"
                )
            with col_mode:
                grid_mode = st.toggle("Grid view", value=True)

            if sel_syms:
                if grid_mode:
                    # Grid — 2 charts per row
                    for i in range(0, len(sel_syms), 2):
                        cols = st.columns(2)
                        for j, sym in enumerate(sel_syms[i:i+2]):
                            with cols[j]:
                                st.html(f"<div style='font-size:11px;color:#7a8294;"
                                        f"font-weight:600;margin-bottom:2px'>{sym}</div>")
                                chart = build_normalized_symbol_chart(snaps, sym) if normalized else build_symbol_chart(snaps, sym)
                                st.plotly_chart(chart, width="stretch",
                                               config={"displayModeBar": False})
                else:
                    # Single combined chart
                    times = [s["time"] for s in snaps]
                    nifty = [s.get("nifty", 0) for s in snaps]
                    traces = []
                    colors = ["#2eca8a","#4a9eff","#f05252","#e8a825",
                              "#a855f7","#06b6d4","#f97316","#84cc16"]
                    for idx, sym in enumerate(sel_syms):
                        pnl = [s.get("symbols",{}).get(sym,{}).get("net_pnl",0)
                               for s in snaps]
                        traces.append({
                            "x": times, "y": pnl,
                            "type": "scatter", "mode": "lines",
                            "name": sym,
                            "line": {"color": colors[idx % len(colors)], "width": 1.5},
                            "yaxis": "y1",
                "hovertemplate": "%{x}<br>%{y:,.0f}<extra></extra>",
                        })
                    traces.append({
                        "x": times, "y": nifty,
                        "type": "scatter", "mode": "lines",
                        "name": "NIFTY",
                        "line": {"color": "#e8a825", "width": 1.5, "dash": "dot"},
                        "yaxis": "y2",
                "hovertemplate": "%{y:,.0f}<extra>NIFTY</extra>",
                    })
                    combined = build_normalized_multi_symbol_chart(snaps, sel_syms) if normalized else {
                        "data": traces,
                        "layout": {
                            "paper_bgcolor": "#0f1117",
                            "plot_bgcolor":  "#13151a",
                            "font": {"color": "#7a8294", "family": "JetBrains Mono"},
                            "margin": {"t": 30, "b": 40, "l": 60, "r": 60},
                            "legend": {"x": 0, "y": 1, "bgcolor": "rgba(0,0,0,0)"},
                            "xaxis": {"gridcolor": "#1e2230"},
                            "yaxis": {"title": "MTM (₹)", "tickformat": ",.0f", "exponentformat": "none", "tickformat": ",.0f",
                "tickformat": ",.0f",
                "tickprefix": "₹", "gridcolor": "#1e2230"},
                            "yaxis2_SKIP": {
                                "title": "NIFTY", "overlaying": "y", "side": "right",
                                "showgrid": False,
                                "title": {"font": {"color": "#e8a825"}},
                                "tickfont":  {"color": "#e8a825"},
                            },
                            "hovermode": "x unified",
                        }
                    }
                    st.plotly_chart(combined, width="stretch",
                                   config={"displayModeBar": False})



    with tab2:
        show_dropcopy_pnl_tab()

if __name__ == "__main__":
    main()
