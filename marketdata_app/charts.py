"""Plotly chart helpers — flat·minimal, Hangang clay accent."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

ACCENT = "#c96442"
UP = "#2f7a3a"
DOWN = "#b14a3a"
INK = "#111111"
MUTED = "#888888"
EDGE = "#d6d6d0"
SOFT = "#f4f4f1"


def _base_layout(title: str = "", height: int = 320) -> dict:
    return dict(
        title=dict(text=title, x=0, font=dict(size=13, color=INK)) if title else None,
        margin=dict(l=40, r=20, t=30 if title else 10, b=30),
        height=height,
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(showgrid=False, color=MUTED, linecolor=EDGE),
        yaxis=dict(gridcolor=SOFT, color=MUTED, linecolor=EDGE, zeroline=False),
        font=dict(family="ui-sans-serif, system-ui, sans-serif", size=11, color="#333"),
        hoverlabel=dict(bgcolor="white", bordercolor=EDGE, font_size=11),
    )


def price_chart(df: pd.DataFrame, *, name: str = "", height: int = 360) -> go.Figure:
    """일별 종가 라인 + 거래대금 막대 (sub-axis)."""
    fig = go.Figure()
    if df.empty:
        fig.update_layout(**_base_layout(name, height))
        return fig

    fig.add_trace(go.Scatter(
        x=df["date"], y=df["close"], mode="lines",
        line=dict(color=ACCENT, width=1.6),
        name="Close",
        hovertemplate="%{x|%Y-%m-%d}<br>Close: %{y:,.0f}<extra></extra>",
    ))
    if "value" in df.columns:
        fig.add_trace(go.Bar(
            x=df["date"], y=df["value"], yaxis="y2",
            marker_color=EDGE, opacity=0.6,
            name="Trading value",
            hovertemplate="%{x|%Y-%m-%d}<br>Value: %{y:,.0f} KRW<extra></extra>",
        ))

    layout = _base_layout(name, height)
    layout["yaxis"]["title"] = "Close (KRW)"
    layout["yaxis2"] = dict(
        overlaying="y", side="right", showgrid=False,
        color=MUTED, linecolor=EDGE, title="Value",
    )
    layout["showlegend"] = False
    fig.update_layout(**layout)
    return fig


def foreign_ownership_chart(df: pd.DataFrame, *, height: int = 240) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(**_base_layout("Foreign ownership %", height))
        return fig

    fig.add_trace(go.Scatter(
        x=df["date"], y=df["foreign_pct"], mode="lines",
        line=dict(color=ACCENT, width=1.6),
        fill="tozeroy", fillcolor="rgba(201,100,66,0.08)",
        hovertemplate="%{x|%Y-%m-%d}<br>Foreign: %{y:.2f}%<extra></extra>",
    ))
    layout = _base_layout("Foreign ownership %", height)
    layout["yaxis"]["ticksuffix"] = "%"
    layout["showlegend"] = False
    fig.update_layout(**layout)
    return fig


def candlestick(df: pd.DataFrame, *, name: str = "", height: int = 360) -> go.Figure:
    fig = go.Figure()
    if df.empty:
        fig.update_layout(**_base_layout(name, height))
        return fig
    fig.add_trace(go.Candlestick(
        x=df["date"],
        open=df["open"], high=df["high"], low=df["low"], close=df["close"],
        increasing_line_color=UP, decreasing_line_color=DOWN,
        increasing_fillcolor=UP, decreasing_fillcolor=DOWN,
        showlegend=False,
        name=name or "OHLC",
    ))
    layout = _base_layout(name, height)
    layout["xaxis"]["rangeslider"] = dict(visible=False)
    fig.update_layout(**layout)
    return fig


def investor_flow_chart(df: pd.DataFrame, *, height: int = 260) -> go.Figure:
    """투자자별 거래대금 누적 막대."""
    fig = go.Figure()
    if df.empty:
        fig.update_layout(**_base_layout("Investor flow (net value)", height))
        return fig

    # pykrx columns: 외국인합계, 기관합계, 개인 (or similar)
    candidate_cols = {
        "외국인합계": ("Foreign", ACCENT),
        "기관합계": ("Institutional", INK),
        "개인": ("Retail", MUTED),
    }
    for col, (label, color) in candidate_cols.items():
        if col in df.columns:
            fig.add_trace(go.Bar(
                x=df["date"], y=df[col], name=label,
                marker_color=color, opacity=0.85,
            ))
    layout = _base_layout("Investor flow (net value, KRW)", height)
    layout["barmode"] = "relative"
    layout["legend"] = dict(orientation="h", y=-0.2, x=0)
    fig.update_layout(**layout)
    return fig
