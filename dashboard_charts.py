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
                           y1_color: str = "#2eca8a") -> dict:
    """Build Plotly dual-axis chart config."""
    times      = [s["time"] for s in snaps]
    y1_vals    = [s.get(y1_key, 0) / 1e5 for s in snaps]  # convert to L
    nifty_vals = [s.get("nifty", 0) for s in snaps]

    return {
        "data": [
            {
                "x": times, "y": y1_vals,
                "type": "scatter", "mode": "lines",
                "name": y1_label,
                "line": {"color": y1_color, "width": 2},
                "yaxis": "y1",
            },
            {
                "x": times, "y": nifty_vals,
                "type": "scatter", "mode": "lines",
                "name": "NIFTY",
                "line": {"color": "#e8a825", "width": 1.5, "dash": "dot"},
                "yaxis": "y2",
            },
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
                "gridcolor": "#1e2230", "tickfont": {"size": 10},
                "title": {"font": {"color": "#e8a825"}},
                "tickfont":  {"color": "#e8a825"},
                "showgrid": False,
            },
            "hovermode": "x unified",
        }
    }


def build_symbol_chart(snaps: list[dict], symbol: str) -> dict:
    """Build symbol-wise MTM vs NIFTY chart."""
    times      = [s["time"] for s in snaps]
    sym_pnl    = [s.get("symbols", {}).get(symbol, {}).get("net_pnl", 0) / 1e5
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
                "fill": "tozeroy",
                "fillcolor": f"rgba({'46,202,138' if last_pnl >= 0 else '240,82,82'},0.1)",
            },
            {
                "x": times, "y": nifty_vals,
                "type": "scatter", "mode": "lines",
                "name": "NIFTY",
                "line": {"color": "#e8a825", "width": 1.5, "dash": "dot"},
                "yaxis": "y2",
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
                "gridcolor": "#1e2230",
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
    def fmt(v): return f"+{v/1e5:.2f} L" if v >= 0 else f"{v/1e5:.2f} L"
    k1.metric("Net PnL",    fmt(latest["net_pnl"]))
    k2.metric("Net Exp",    fmt(latest["net_exp"]))
    k3.metric("Carry PnL",  fmt(latest["carry_pnl"]))
    k4.metric("NIFTY",      f"{latest['nifty']:,.0f}")
    k5.metric("Snapshots",  f"{len(snaps)} pts")

    st.html("<div style='margin:8px 0'></div>")

    # ── Tabs ─────────────────────────────────────────────────
    tab1, tab2 = st.tabs(["📈 MTM vs NIFTY", "📊 Daily PnL (Dropcopy)"])

    with tab1:

        # ── Chart 1: MTM vs NIFTY ─────────────────────────────────────
        st.html("<div style='font-size:11px;color:#555c6e;text-transform:uppercase;"
                "letter-spacing:.1em;margin-bottom:4px'>MTM (Net PnL) vs NIFTY</div>")
        chart1 = build_dual_axis_chart(snaps, "net_pnl", "Net PnL", "#2eca8a")
        st.plotly_chart(chart1, use_container_width=True, config={"displayModeBar": False})

        # ── Chart 2: Net Exposure vs NIFTY ────────────────────────────
        st.html("<div style='font-size:11px;color:#555c6e;text-transform:uppercase;"
                "letter-spacing:.1em;margin-bottom:4px'>Net Exposure vs NIFTY</div>")
        chart2 = build_dual_axis_chart(snaps, "net_exp", "Net Exposure", "#4a9eff")
        st.plotly_chart(chart2, use_container_width=True, config={"displayModeBar": False})

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
                                chart = build_symbol_chart(snaps, sym)
                                st.plotly_chart(chart, use_container_width=True,
                                               config={"displayModeBar": False})
                else:
                    # Single combined chart
                    times = [s["time"] for s in snaps]
                    nifty = [s.get("nifty", 0) for s in snaps]
                    traces = []
                    colors = ["#2eca8a","#4a9eff","#f05252","#e8a825",
                              "#a855f7","#06b6d4","#f97316","#84cc16"]
                    for idx, sym in enumerate(sel_syms):
                        pnl = [s.get("symbols",{}).get(sym,{}).get("net_pnl",0)/1e5
                               for s in snaps]
                        traces.append({
                            "x": times, "y": pnl,
                            "type": "scatter", "mode": "lines",
                            "name": sym,
                            "line": {"color": colors[idx % len(colors)], "width": 1.5},
                            "yaxis": "y1",
                        })
                    traces.append({
                        "x": times, "y": nifty,
                        "type": "scatter", "mode": "lines",
                        "name": "NIFTY",
                        "line": {"color": "#e8a825", "width": 1.5, "dash": "dot"},
                        "yaxis": "y2",
                    })
                    combined = {
                        "data": traces,
                        "layout": {
                            "paper_bgcolor": "#0f1117",
                            "plot_bgcolor":  "#13151a",
                            "font": {"color": "#7a8294", "family": "JetBrains Mono"},
                            "margin": {"t": 30, "b": 40, "l": 60, "r": 60},
                            "legend": {"x": 0, "y": 1, "bgcolor": "rgba(0,0,0,0)"},
                            "xaxis": {"gridcolor": "#1e2230"},
                            "yaxis": {"title": "MTM (L)", "gridcolor": "#1e2230"},
                            "yaxis2": {
                                "title": "NIFTY", "overlaying": "y", "side": "right",
                                "showgrid": False,
                                "title": {"font": {"color": "#e8a825"}},
                                "tickfont":  {"color": "#e8a825"},
                            },
                            "hovermode": "x unified",
                        }
                    }
                    st.plotly_chart(combined, use_container_width=True,
                                   config={"displayModeBar": False})



    with tab2:
        show_dropcopy_pnl_tab()

if __name__ == "__main__":
    main()
