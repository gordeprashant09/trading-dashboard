#!/usr/bin/env python3
from __future__ import annotations

import json
from typing import Any

import pandas as pd
import plotly.express as px
import redis
import streamlit as st


REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

USERNAME = "stock_futures_dashboard"
SNAPSHOT_KEY = "margin:stock_futures:snapshot:latest"
OUTPUT_KEY = f"margin:outputs:latest:{USERNAME}"


def _redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def _load_json(r: redis.Redis, key: str) -> dict[str, Any]:
    raw = r.get(key)
    if not raw:
        return {}
    return json.loads(raw)


def _money(v: Any) -> str:
    try:
        return f"₹{float(v):,.0f}"
    except Exception:
        return "₹0"


def _num(v: Any) -> str:
    try:
        return f"{float(v):,.0f}"
    except Exception:
        return "0"


def _build_symbol_df(snapshot: dict[str, Any], output: dict[str, Any]) -> pd.DataFrame:
    rows = snapshot.get("rows") or []
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).copy()

    for c in ["qty_units", "lot_size", "future_ltp", "spot_ltp"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    if "underlying" not in df.columns:
        return pd.DataFrame()

    df["position_value"] = df["qty_units"].abs() * df["future_ltp"]
    df["signed_value"] = df["qty_units"] * df["future_ltp"]

    total_position_value = float(df["position_value"].sum() or 0.0)

    result = output.get("result") or {}
    total_margin = float(result.get("grand_total_broker_style") or 0.0)

    # Allocation approximation:
    # Current worker returns portfolio-level margin only.
    # Until per-symbol SPAN rows are emitted, allocate total margin by abs position value.
    per_symbol = output.get("per_symbol_margin") or {}

    df["span_margin"] = df["underlying"].map(
        lambda x: float((per_symbol.get(str(x).upper()) or {}).get("span_margin") or 0.0)
    )
    df["exposure_margin"] = df["underlying"].map(
        lambda x: float((per_symbol.get(str(x).upper()) or {}).get("exposure_margin") or 0.0)
    )
    df["margin"] = df["underlying"].map(
        lambda x: float((per_symbol.get(str(x).upper()) or {}).get("total_margin") or 0.0)
    )

    # Fallback only if worker has not yet published per_symbol_margin
    if df["margin"].sum() <= 0 and total_position_value > 0 and total_margin > 0:
        span_margin = float(result.get("span_broker_style") or result.get("scan_risk_total") or 0.0)
        exposure_margin = float(result.get("exposure_total") or 0.0)
        df["weight"] = df["position_value"] / total_position_value
        df["span_margin"] = df["weight"] * span_margin
        df["exposure_margin"] = df["weight"] * exposure_margin
        df["margin"] = df["span_margin"] + df["exposure_margin"]

    df["margin_pct"] = df["margin"] / total_margin * 100 if total_margin else 0.0
    df["margin_to_value_pct"] = df["margin"] / df["position_value"].replace(0, pd.NA) * 100
    df["margin_to_value_pct"] = df["margin_to_value_pct"].fillna(0.0)

    return df



def _cr(v: Any) -> float:
    try:
        return float(v or 0.0) / 10000000.0
    except Exception:
        return 0.0


def _money_cr(v: Any) -> str:
    return f"₹{_cr(v):,.2f} Cr"


def _metric_card(label: str, value: str, icon: str = "", accent: str = "#4cc9f0") -> None:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-icon" style="color:{accent};">{icon}</div>
            <div>
                <div class="metric-title">{label}</div>
                <div class="metric-value">{value}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _add_trader_css() -> None:
    st.markdown("""
    <style>
    .stApp {
        background: #07111f;
        color: #f8fafc;
    }
    .block-container {
        padding-top: 1.2rem;
        padding-left: 1.2rem;
        padding-right: 1.2rem;
        max-width: 100%;
    }
    .metric-card {
        min-height: 76px;
        background: linear-gradient(135deg, #101d2f 0%, #14243a 100%);
        border: 1px solid #304c6d;
        border-radius: 12px;
        padding: 14px 16px;
        display: flex;
        align-items: center;
        gap: 14px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.20);
    }
    .metric-icon {
        font-size: 28px;
        line-height: 1;
        min-width: 34px;
        text-align: center;
    }
    .metric-title {
        font-size: 12px;
        color: #aebbd0;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        font-weight: 700;
    }
    .metric-value {
        font-size: 25px;
        color: #ffffff;
        font-weight: 900;
        margin-top: 4px;
        white-space: nowrap;
    }
    .section-card {
        background: #0d1828;
        border: 1px solid #304c6d;
        border-radius: 12px;
        padding: 12px;
        margin-top: 8px;
    }
    h3, h4 {
        color: #ffffff !important;
        font-weight: 900 !important;
    }
    [data-testid="stDataFrame"] {
        border: 1px solid #304c6d;
        border-radius: 10px;
        overflow: hidden;
    }
    [data-testid="stDataFrame"] div {
        color: #f8fafc;
    }
    thead tr th {
        background-color: #12365b !important;
        color: #ffffff !important;
        font-weight: 800 !important;
    }
    tbody tr:nth-child(even) {
        background-color: #0d1828 !important;
    }
    tbody tr:nth-child(odd) {
        background-color: #101d2f !important;
    }
    tbody tr:hover {
        background-color: #1b3554 !important;
    }
    </style>
    """, unsafe_allow_html=True)


def show_stock_future_margin_tab() -> None:
    st.markdown("### 📊 NSE Stock Futures Margin")
    _add_trader_css()

    r = _redis()
    snapshot = _load_json(r, SNAPSHOT_KEY)
    output = _load_json(r, OUTPUT_KEY)

    if not snapshot:
        st.warning("No stock-futures margin snapshot found. Run collector first.")
        st.code("python3 stock_future_margin_collector.py", language="bash")
        return

    result = output.get("result") or {}
    span_date = output.get("span_date") or snapshot.get("span_date") or ""
    minmax_key = f"margin:outputs:minmax:{USERNAME}:{span_date}" if span_date else ""
    minmax = r.hgetall(minmax_key) if minmax_key else {}

    total_margin = float(result.get("grand_total_broker_style") or 0.0)

    df = _build_symbol_df(snapshot, output)
    if df.empty:
        st.info("No stock future rows found.")
        return

    gross_exposure = float(df["position_value"].sum()) if "position_value" in df else 0.0
    net_long = float(df.loc[df["signed_value"] > 0, "signed_value"].sum()) if "signed_value" in df else 0.0
    net_short = abs(float(df.loc[df["signed_value"] < 0, "signed_value"].sum())) if "signed_value" in df else 0.0

    top_row = df.sort_values("margin", ascending=False).head(1)
    top_symbol = "-"
    top_text = "₹0.00 Cr"
    if not top_row.empty:
        top_symbol = str(top_row.iloc[0]["underlying"])
        top_text = f"{top_symbol} / {_money_cr(top_row.iloc[0]['margin'])}"

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        _metric_card("Total Margin", _money_cr(total_margin), "💼", "#35a7ff")
    with c2:
        _metric_card("Positions", _num(snapshot.get("positions_count")), "👥", "#ff8c00")
    with c3:
        _metric_card("Gross Exposure", _money_cr(gross_exposure), "📈", "#ff8c00")
    with c4:
        _metric_card("Long Exposure", _money_cr(net_long), "↗", "#22c55e")
    with c5:
        _metric_card("Short Exposure", _money_cr(net_short), "↘", "#ef4444")

    c6, c7, c8, c9 = st.columns(4)
    with c6:
        _metric_card("Max Today", _money_cr(minmax.get("max_total")), "↗", "#22c55e")
    with c7:
        _metric_card("Min Today", _money_cr(minmax.get("min_total")), "↘", "#ef4444")
    with c8:
        _metric_card("Avg Today", _money_cr(minmax.get("avg_total")), "〽", "#35a7ff")
    with c9:
        _metric_card("Highest Margin", top_text, "🏷", "#f59e0b")

    st.markdown("#### Top Margin Consumers")

    top = df.sort_values("margin", ascending=False).head(10).copy()
    top["margin_cr"] = top["margin"] / 10000000.0
    top["label"] = top["underlying"].astype(str)
    top = top.sort_values("margin_cr", ascending=True)

    left, right = st.columns([1.35, 1.0])

    with left:
        fig_bar = px.bar(
            top,
            x="margin_cr",
            y="label",
            orientation="h",
            text=top["margin_cr"].map(lambda x: f"₹{x:.2f} Cr"),
            labels={"margin_cr": "Margin (₹ Cr)", "label": ""},
            height=360,
        )
        fig_bar.update_traces(textposition="outside", cliponaxis=False)
        fig_bar.update_layout(
            margin=dict(l=20, r=80, t=10, b=30),
            paper_bgcolor="#0d1828",
            plot_bgcolor="#0d1828",
            font=dict(color="#f8fafc"),
            xaxis=dict(gridcolor="#26364c", zeroline=False),
            yaxis=dict(gridcolor="#0d1828"),
            showlegend=False,
        )
        st.plotly_chart(fig_bar, width="stretch")

    with right:
        donut = df.sort_values("margin", ascending=False).head(10).copy()
        donut["margin_cr"] = donut["margin"] / 10000000.0
        fig_donut = px.pie(
            donut,
            names="underlying",
            values="margin_cr",
            hole=0.55,
            height=360,
        )
        fig_donut.update_traces(
            textinfo="percent",
            hovertemplate="%{label}<br>₹%{value:.2f} Cr<br>%{percent}<extra></extra>",
        )
        fig_donut.update_layout(
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="#0d1828",
            plot_bgcolor="#0d1828",
            font=dict(color="#f8fafc"),
            legend=dict(orientation="v", y=0.5),
            annotations=[
                dict(
                    text=f"Total<br>₹{total_margin/10000000.0:.2f} Cr",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(size=18, color="#ffffff"),
                )
            ],
        )
        st.plotly_chart(fig_donut, width="stretch")

    st.markdown("#### Margin by Symbol")

    view = df.sort_values("margin", ascending=False).copy()

    # Keep clean trader-facing columns
    keep_cols = [
        "underlying",
        "tradingsymbol",
        "expiry",
        "qty_units",
        "future_ltp",
        "spot_ltp",
        "position_value",
        "span_margin",
        "exposure_margin",
        "margin",
        "margin_pct",
        "margin_to_value_pct",
    ]
    view = view[[c for c in keep_cols if c in view.columns]]

    # Date display if expiry is YYYYMMDD
    if "expiry" in view.columns:
        view["expiry"] = view["expiry"].astype(str)

    def _pct_badge(v):
        try:
            x = float(v)
        except Exception:
            return ""
        if x >= 15:
            return f"🔴 {x:.2f}%"
        if x >= 5:
            return f"🟠 {x:.2f}%"
        return f"🟢 {x:.2f}%"

    if "margin_pct" in view.columns:
        view["margin_pct_badge"] = view["margin_pct"].map(_pct_badge)
        view = view.drop(columns=["margin_pct"])

    ordered = [
        "underlying",
        "tradingsymbol",
        "expiry",
        "qty_units",
        "future_ltp",
        "spot_ltp",
        "position_value",
        "span_margin",
        "exposure_margin",
        "margin",
        "margin_pct_badge",
        "margin_to_value_pct",
    ]
    view = view[[c for c in ordered if c in view.columns]]

    st.dataframe(
        view,
        width="stretch",
        hide_index=True,
        column_config={
            "underlying": "Symbol",
            "tradingsymbol": "Future Contract",
            "expiry": "Expiry",
            "qty_units": st.column_config.NumberColumn("Qty", format="%.0f"),
            "future_ltp": st.column_config.NumberColumn("Future LTP", format="%.2f"),
            "spot_ltp": st.column_config.NumberColumn("Spot/Fallback", format="%.2f"),
            "position_value": st.column_config.NumberColumn("Position Value (₹)", format="₹%.0f"),
            "span_margin": st.column_config.NumberColumn("SPAN Margin (₹)", format="₹%.0f"),
            "exposure_margin": st.column_config.NumberColumn("Exposure Margin (₹)", format="₹%.0f"),
            "margin": st.column_config.NumberColumn("Margin (₹)", format="₹%.0f"),
            "margin_pct_badge": "Margin %",
            "margin_to_value_pct": st.column_config.NumberColumn("Margin / Value", format="%.2f%%"),
        },
    )


def main() -> None:
    show_stock_future_margin_tab()


if __name__ == "__main__":
    main()
