"""
dropcopy_pnl_tab.py — Daily PnL tab for dashboard_charts.py (port 8503)
"""
import io, logging, os
import paramiko
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

log = logging.getLogger(__name__)

SSH_HOST  = os.getenv("SSH_HOST",  "192.168.71.200")
SSH_PORT  = int(os.getenv("SSH_PORT", "22"))
SSH_USER  = os.getenv("SSH_USER",  "Data_colo")
SSH_PASS  = os.getenv("SSH_PASS",  "Datacolo@2026")
DAILY_CSV = "/data/Dashboard/Summary/Summary_daily.csv"


@st.cache_data(ttl=300)
def load_daily_csv() -> pd.DataFrame:
    try:
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        c.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER,
                  password=SSH_PASS, timeout=10)
        _, o, _ = c.exec_command(f"cat {DAILY_CSV} 2>/dev/null")
        raw = o.read().decode("utf-8", errors="replace")
        c.close()
        if not raw.strip():
            return pd.DataFrame()

        df = pd.read_csv(io.StringIO(raw))

        # Use string date for display — avoids Plotly timestamp issue
        df["Date"] = df["Date"].astype(str).str[:8]
        df["DateLabel"] = pd.to_datetime(df["Date"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
        df = df.sort_values("DateLabel").reset_index(drop=True)

        # Net_PNL fallback
        if "Net_PNL" not in df.columns:
            df["Net_PNL"] = df.get("MTM", pd.Series([0]*len(df))) - df.get("Trading_Cost", pd.Series([0]*len(df)))

        df["Cumul_Net_PNL"]   = df["Net_PNL"].cumsum()
        df["Daily_MTM_Move"]  = df["MTM"].diff().fillna(df["MTM"])
        return df

    except Exception as e:
        log.error("load_daily_csv failed: %s", e)
        return pd.DataFrame()


def show_dropcopy_pnl_tab():
    st.markdown("### 📊 Daily PnL — Dropcopy Summary")

    df = load_daily_csv()
    if df.empty:
        st.warning("No data yet. Run dropcopy_summary_writer.py at 15:31.")
        return

    latest = df.iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest Net PnL",     f"₹{latest['Net_PNL']:+,.0f}")
    c2.metric("Cumulative Net PnL", f"₹{latest['Cumul_Net_PNL']:+,.0f}")
    c3.metric("Latest MTM",         f"₹{latest['MTM']:+,.0f}")
    c4.metric("Days",               f"{len(df)}")

    dates = df["DateLabel"].tolist()

    # ── Cumulative Net PnL line chart ─────────────────────────────
    st.markdown("#### Cumulative Net PnL")
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=dates, y=df["Cumul_Net_PNL"].tolist(),
        mode="lines+markers",
        name="Cumulative Net PnL",
        line=dict(color="#00d084", width=2),
        marker=dict(size=8),
    ))
    fig1.add_hline(y=0, line_dash="dash",
                   line_color="rgba(255,255,255,0.2)")
    fig1.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=350,
        margin=dict(l=10, r=10, t=10, b=40),
        xaxis=dict(title="Date", type="category",
                   tickangle=-30, showgrid=False),
        yaxis=dict(title="₹", showgrid=True,
                   gridcolor="rgba(255,255,255,0.05)"),
        hovermode="x unified",
    )
    st.plotly_chart(fig1, use_container_width=True)

    # ── Daily MTM Move bar chart ──────────────────────────────────
    st.markdown("#### Daily MTM Move")
    colors = ["#00d084" if v >= 0 else "#ff4b4b"
              for v in df["Daily_MTM_Move"].tolist()]
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=dates, y=df["Daily_MTM_Move"].tolist(),
        marker_color=colors, name="MTM Move",
    ))
    fig2.add_hline(y=0, line_dash="dash",
                   line_color="rgba(255,255,255,0.2)")
    fig2.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=250,
        margin=dict(l=10, r=10, t=10, b=40),
        xaxis=dict(title="Date", type="category",
                   tickangle=-30, showgrid=False),
        yaxis=dict(title="₹", showgrid=True,
                   gridcolor="rgba(255,255,255,0.05)"),
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ── Table ─────────────────────────────────────────────────────
    st.markdown("#### Day-wise PnL Table")
    disp = df[["DateLabel", "MTM", "Net_PNL",
               "Cumul_Net_PNL", "Daily_MTM_Move",
               "TV", "Trading_Cost"]].copy()
    disp = disp.rename(columns={
        "DateLabel":      "Date",
        "MTM":            "MTM (₹)",
        "Net_PNL":        "Net PnL (₹)",
        "Cumul_Net_PNL":  "Cumul Net PnL (₹)",
        "Daily_MTM_Move": "Daily MTM Move (₹)",
        "TV":             "Traded Val (₹)",
        "Trading_Cost":   "Expenses (₹)",
    })

    def cpnl(v):
        return f"color: {'#00d084' if v >= 0 else '#ff4b4b'}"

    st.dataframe(
        disp.style
            .map(cpnl, subset=["Net PnL (₹)", "Cumul Net PnL (₹)",
                                "MTM (₹)", "Daily MTM Move (₹)"])
            .format({
                "MTM (₹)":            "₹{:+,.0f}",
                "Net PnL (₹)":        "₹{:+,.0f}",
                "Cumul Net PnL (₹)":  "₹{:+,.0f}",
                "Daily MTM Move (₹)": "₹{:+,.0f}",
                "Traded Val (₹)":     "₹{:,.0f}",
                "Expenses (₹)":       "₹{:,.0f}",
            }),
        use_container_width=True,
        height=min(400, 60 + len(disp) * 35),
    )
