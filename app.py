"""
app.py — Unified Trading Dashboard
====================================
Single Streamlit entry point that merges:
  1. 📊 Position Book  (trading_dashboard_colo_prod_final.py)
  2. 📈 Charts         (dashboard_charts.py)
  3. 📅 Daily PnL      (dropcopy_pnl_tab.py)
  4. 📉 Slippage       (slippage_dashboard_final.py)
  5. 🏷️ Symbol PnL     (symbol_pnl_tab.py)
  6. 🧮 Stock Fut Margin (stock_future_margin_tab.py)   ← NEW

Run:
    streamlit run app.py --server.port 8502

Keep these files in the SAME folder:
    app.py
    trading_dashboard_colo_prod_final.py
    dashboard_charts.py
    dropcopy_pnl_tab.py
    symbol_pnl_tab.py                       ← NEW
    dashboard_worker_prod.py        (used internally by trading_dashboard)
    dashboard_chart_collector.py    (cron job — run separately, NOT by this app)
    dropcopy_summary_writer.py      (cron job — run separately at 15:31)
"""

from __future__ import annotations

import importlib.util
import sys
import os
import streamlit as st

# ─────────────────────────────────────────────────────────────────
# PAGE CONFIG  — must be the very first Streamlit call
# ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Trading Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────
# SHARED CSS
# ─────────────────────────────────────────────────────────────────
st.html("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap');

#MainMenu, footer, header { visibility: hidden; }

.block-container {
    padding-top: 0.4rem !important;
    padding-bottom: 0.5rem !important;
    max-width: 100% !important;
}

[data-testid="stMetricValue"] {
    font-size: 1.3rem !important;
    font-weight: 600;
    font-family: 'JetBrains Mono', monospace !important;
}
[data-testid="stMetricLabel"] {
    font-size: 10px !important;
    text-transform: uppercase;
    letter-spacing: .08em;
    color: #555c6e !important;
}
[data-testid="stMetric"] {
    background: #13151a;
    border: 1px solid #1e2230;
    border-radius: 6px;
    padding: 10px 14px !important;
}

/* ── Tab strip ── */
[data-testid="stTabs"] [data-baseweb="tab-list"] {
    gap: 2px;
    border-bottom: 1px solid #1e2230 !important;
    padding-bottom: 0;
    background: #0f1117;
}
[data-testid="stTabs"] [data-baseweb="tab"] {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 12px;
    font-weight: 600;
    color: #555c6e;
    padding: 8px 20px;
    border-radius: 4px 4px 0 0;
    background: transparent;
}
[data-testid="stTabs"] [aria-selected="true"] {
    color: #c8cdd8 !important;
    border-bottom: 2px solid #2eca8a !important;
    background: #13151a !important;
}

[data-testid="stTabsContent"] {
    padding-top: 0 !important;
}
</style>
""")

# ─────────────────────────────────────────────────────────────────
# FIVE TABS
# ─────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊  Position Book",
    "📈  MTM vs NIFTY",
    "📅  Daily PnL (Dropcopy)",
    "📉  Slippage",
    "🏷️  Symbol PnL",
    "🧮  Stock Fut Margin",        # ← NEW
])


# ══════════════════════════════════════════════════════════════════
# MODULE LOADER
# ══════════════════════════════════════════════════════════════════

def _load_module(mod_name: str):
    _orig_set_page_config = st.set_page_config
    _orig_html            = st.html

    st.set_page_config = lambda *a, **kw: None

    _count = {"n": 0}
    def _guarded_html(*a, **kw):
        _count["n"] += 1
        if _count["n"] == 1:
            return None
        return _orig_html(*a, **kw)
    st.html = _guarded_html

    try:
        if mod_name in sys.modules:
            del sys.modules[mod_name]

        spec = importlib.util.spec_from_file_location(
            mod_name,
            os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{mod_name}.py"),
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        spec.loader.exec_module(mod)
    finally:
        st.set_page_config = _orig_set_page_config
        st.html            = _orig_html

    return mod


# ── Tab 1 — Position Book ─────────────────────────────────────────
with tab1:
    try:
        pb_mod = _load_module("trading_dashboard_colo_prod_final")
        pb_mod.main()
    except Exception as e:
        st.error(f"Position Book failed to load: {e}")
        st.exception(e)


# ── Tab 2 — MTM vs NIFTY Charts ───────────────────────────────────
with tab2:
    try:
        ch_mod = _load_module("dashboard_charts")
        from dropcopy_pnl_tab import show_dropcopy_pnl_tab as _real_dct
        ch_mod.show_dropcopy_pnl_tab = lambda: st.info(
            "📅 Daily PnL is in the **Daily PnL (Dropcopy)** tab →"
        )
        ch_mod.main()
        ch_mod.show_dropcopy_pnl_tab = _real_dct
    except Exception as e:
        st.error(f"Charts failed to load: {e}")
        st.exception(e)


# ── Tab 3 — Daily PnL Dropcopy ────────────────────────────────────
with tab3:
    try:
        from dropcopy_pnl_tab import show_dropcopy_pnl_tab
        show_dropcopy_pnl_tab()
    except Exception as e:
        st.error(f"Daily PnL tab failed to load: {e}")
        st.exception(e)


# ── Tab 4 — Slippage ──────────────────────────────────────────────
with tab4:
    try:
        slip_mod = _load_module("slippage_dashboard_final")
        slip_mod.main()
    except Exception as e:
        st.error(f"Slippage tab failed to load: {e}")
        st.exception(e)


# ── Tab 5 — Symbol PnL ────────────────────────────────────────────
with tab5:
    try:
        from symbol_pnl_tab import show_symbol_pnl_tab
        show_symbol_pnl_tab()
    except Exception as e:
        st.error(f"Symbol PnL tab failed to load: {e}")
        st.exception(e)

# ── Tab 6 — NSE Stock Future Margin ───────────────────────────────
with tab6:
    try:
        from stock_future_margin_tab import show_stock_future_margin_tab
        show_stock_future_margin_tab()
    except Exception as e:
        st.error(f"Stock Future Margin tab failed to load: {e}")
        st.exception(e)

