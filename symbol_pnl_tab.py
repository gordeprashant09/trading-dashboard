"""
symbol_pnl_tab.py — Symbol-wise cumulative PnL tab
====================================================
Reads  /data/Dashboard/Summary/Summary_symbol_pnl.csv  via SSH.

Layout (top → bottom):
  1. KPI cards
  2. Symbol PnL Table  — SYMBOL | TOTAL  (sorted desc)
  3. Total PnL by Symbol bar chart
"""

from __future__ import annotations
import io, logging, os
import paramiko
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

log = logging.getLogger(__name__)

SSH_HOST       = os.getenv("SSH_HOST",  "192.168.71.200")
SSH_PORT       = int(os.getenv("SSH_PORT", "22"))
SSH_USER       = os.getenv("SSH_USER",  "Data_colo")
SSH_PASS       = os.getenv("SSH_PASS",  "Datacolo@2026")
SYMBOL_PNL_CSV = "/data/Dashboard/Summary/Summary_symbol_pnl.csv"


@st.cache_data(ttl=300)
def load_symbol_pnl() -> pd.DataFrame:
    try:
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        c.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER,
                  password=SSH_PASS, timeout=10)
        _, o, _ = c.exec_command(f"cat {SYMBOL_PNL_CSV} 2>/dev/null")
        raw = o.read().decode("utf-8", errors="replace")
        c.close()

        if not raw.strip():
            return pd.DataFrame()

        df = pd.read_csv(io.StringIO(raw))
        df["Date"]     = df["Date"].astype(str).str[:8]
        df["DateLabel"] = (
            pd.to_datetime(df["Date"], format="%Y%m%d")
            .dt.strftime("%Y-%m-%d")
        )
        df["Net_PNL"] = pd.to_numeric(df["Net_PNL"], errors="coerce").fillna(0)
        df["Symbol"]  = df["Symbol"].astype(str).str.strip()
        return df.sort_values(["Symbol", "DateLabel"]).reset_index(drop=True)

    except Exception as e:
        log.error("load_symbol_pnl failed: %s", e)
        return pd.DataFrame()


def show_symbol_pnl_tab():
    st.markdown("### 🏷️ Symbol-wise Cumulative PnL")

    df = load_symbol_pnl()
    if df.empty:
        st.warning(
            "No data found at `Summary_symbol_pnl.csv`. "
            "Check SSH connection or run the summary writer."
        )
        return

    # ── Aggregation ───────────────────────────────────────────────
    tot = (
        df.groupby("Symbol", as_index=False)["Net_PNL"]
        .sum()
        .rename(columns={"Net_PNL": "Total_PNL"})
        .sort_values("Total_PNL", ascending=False)
        .reset_index(drop=True)
    )
    grand_total = tot["Total_PNL"].sum()
    winners     = (tot["Total_PNL"] > 0).sum()
    losers      = (tot["Total_PNL"] < 0).sum()
    best_sym    = tot.iloc[0]
    worst_sym   = tot.iloc[-1]

    # ── Win/Loss day counts per symbol ─────────────────────────────
    win_lose = (
        df.assign(
            win_day  = (df["Net_PNL"] > 0).astype(int),
            lose_day = (df["Net_PNL"] < 0).astype(int),
        )
        .groupby("Symbol", as_index=False)
        .agg(Win_Days=("win_day", "sum"), Lose_Days=("lose_day", "sum"))
    )
    tot = tot.merge(win_lose, on="Symbol", how="left")

    # ── KPI cards ─────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Grand Total PnL",           f"₹{grand_total:+,.0f}")
    c2.metric("Winners 🟢",                f"{winners} symbols")
    c3.metric("Losers 🔴",                 f"{losers} symbols")
    c4.metric(f"Best: {best_sym['Symbol']}",  f"₹{best_sym['Total_PNL']:+,.0f}")
    c5.metric(f"Worst: {worst_sym['Symbol']}", f"₹{worst_sym['Total_PNL']:+,.0f}")

    # ── Total PnL chart + aligned Win/Loss table ─────────────────
    st.markdown("#### Total PnL by Symbol (all days combined)")

    # Keep ONE ordering for both the bar chart and the win/loss values.
    # Plotly horizontal bars render the first y-value at the bottom, so
    # ascending PnL gives: worst at bottom, best at top.
    plot_df = tot.sort_values("Total_PNL", ascending=True).reset_index(drop=True)
    bar_colors = ["#00d084" if v >= 0 else "#ff4b4b" for v in plot_df["Total_PNL"]]
    row_height = 28
    fig_height = max(520, 95 + len(plot_df) * row_height)

    from plotly.subplots import make_subplots

    fig_bar = make_subplots(
        rows=1,
        cols=2,
        shared_yaxes=True,
        horizontal_spacing=0.025,
        column_widths=[0.82, 0.18],
        specs=[[{"type": "bar"}, {"type": "scatter"}]],
    )

    # Left side: PnL bar chart
    fig_bar.add_trace(
        go.Bar(
            x=plot_df["Total_PNL"],
            y=plot_df["Symbol"],
            orientation="h",
            marker_color=bar_colors,
            text=[f"₹{v:+,.0f}" for v in plot_df["Total_PNL"]],
            textposition="outside",
            textfont=dict(size=11, color="#dce3ef"),
            hovertemplate="%{y}: ₹%{x:+,.0f}<extra></extra>",
            showlegend=False,
        ),
        row=1,
        col=1,
    )

    # Right side: table values aligned to the same y-axis/symbol rows.
    fig_bar.add_trace(
        go.Scatter(
            x=[0.35] * len(plot_df),
            y=plot_df["Symbol"],
            mode="text",
            text=[str(int(v)) for v in plot_df["Win_Days"]],
            textfont=dict(color="#dce3ef", size=12),
            hoverinfo="skip",
            showlegend=False,
        ),
        row=1,
        col=2,
    )
    fig_bar.add_trace(
        go.Scatter(
            x=[0.75] * len(plot_df),
            y=plot_df["Symbol"],
            mode="text",
            text=[str(int(v)) for v in plot_df["Lose_Days"]],
            textfont=dict(color="#dce3ef", size=12),
            hoverinfo="skip",
            showlegend=False,
        ),
        row=1,
        col=2,
    )

    fig_bar.add_vline(
        x=0,
        line_dash="dash",
        line_color="rgba(255,255,255,0.25)",
        row=1,
        col=1,
    )

    # Add right-table header labels.
    fig_bar.add_annotation(
        x=0.35,
        y=1.035,
        xref="x2",
        yref="paper",
        text="<b>Win_Days</b>",
        showarrow=False,
        font=dict(color="#9aa4b2", size=12),
    )
    fig_bar.add_annotation(
        x=0.75,
        y=1.035,
        xref="x2",
        yref="paper",
        text="<b>Lose_Days</b>",
        showarrow=False,
        font=dict(color="#9aa4b2", size=12),
    )

    # Right-side table background + row guide lines.
    # This keeps the previous chart layout, but makes the Win/Lose values
    # easier to visually track against the matching symbol/bar row.
    n_rows = max(1, len(plot_df))

    # Overall table panel background.
    fig_bar.add_shape(
        type="rect",
        xref="x2 domain",
        yref="paper",
        x0=0,
        x1=1,
        y0=0,
        y1=1,
        fillcolor="rgba(22, 27, 36, 0.38)",
        line=dict(color="rgba(255,255,255,0.07)", width=1),
        layer="below",
    )

    # Alternating row bands. Plotly category rows map evenly in paper space
    # because both subplots share the same y-axis categories.
    for i in range(n_rows):
        y0 = i / n_rows
        y1 = (i + 1) / n_rows
        if i % 2 == 0:
            fig_bar.add_shape(
                type="rect",
                xref="x2 domain",
                yref="paper",
                x0=0,
                x1=1,
                y0=y0,
                y1=y1,
                fillcolor="rgba(255,255,255,0.018)",
                line=dict(width=0),
                layer="below",
            )

    # Horizontal row separators.
    for i in range(n_rows + 1):
        y_paper = i / n_rows
        fig_bar.add_shape(
            type="line",
            xref="x2 domain",
            yref="paper",
            x0=0,
            x1=1,
            y0=y_paper,
            y1=y_paper,
            line=dict(color="rgba(255,255,255,0.055)", width=1),
            layer="below",
        )

    # Column divider + outer borders for the small table.
    for x_line, width, color in [
        (0.0, 1, "rgba(255,255,255,0.07)"),
        (0.55, 1, "rgba(255,255,255,0.10)"),
        (1.0, 1, "rgba(255,255,255,0.07)"),
    ]:
        fig_bar.add_shape(
            type="line",
            xref="x2 domain",
            yref="paper",
            x0=x_line,
            x1=x_line,
            y0=0,
            y1=1,
            line=dict(color=color, width=width),
            layer="below",
        )

    fig_bar.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=fig_height,
        margin=dict(l=10, r=20, t=35, b=35),
        bargap=0.25,
    )

    fig_bar.update_xaxes(
        title_text="Net PnL (₹)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
        zeroline=False,
        row=1,
        col=1,
    )
    fig_bar.update_yaxes(
        showgrid=False,
        tickfont=dict(size=11, color="#e8edf7"),
        row=1,
        col=1,
    )

    # Right table axis: no chart scale shown; symbols align via shared y-axis.
    fig_bar.update_xaxes(
        range=[0, 1],
        visible=False,
        fixedrange=True,
        row=1,
        col=2,
    )
    fig_bar.update_yaxes(
        showticklabels=False,
        showgrid=False,
        row=1,
        col=2,
    )

    st.plotly_chart(fig_bar, use_container_width=True)
