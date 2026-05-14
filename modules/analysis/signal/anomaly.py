"""
Anomaly Detection — Rolling Z-score / IQR 기반 이상치 탐지
  - 매출 / 거래건수 각각 탐지
  - rolling window 기반 z-score (전역 평균 대신 국소 기준선)
  - 이상 이벤트 로그 생성
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from modules.analysis.guides import render_guide
from modules.common.foundation import _parse_dates
from modules.common.core.audit import compute_module_audit, check_sample_size_sanity, check_anomaly_rate_sanity
from modules.common.core.result import enrich_result


# ── 색상 ─────────────────────────────────────────────────────────────────────
C_BLUE  = "#1e40af"
C_GREEN = "#16a34a"
C_RED   = "#dc2626"
C_AMBER = "#d97706"
C_GRAY  = "#6b7280"

# 심각도 기준: (min_abs_z, label, text_color, bg_color)
SEVERITY_LEVELS = [
    (5.0, "🔴 CRITICAL", "#7f1d1d", "#fee2e2"),
    (4.0, "🟠 HIGH",     "#7c2d12", "#ffedd5"),
    (3.0, "🟡 MEDIUM",   "#713f12", "#fef9c3"),
    (0.0, "🔵 LOW",      "#1e3a8a", "#dbeafe"),
]


def _severity(z: float) -> tuple[str, str, str]:
    """(label, text_color, bg_color)"""
    az = abs(z) if not np.isnan(z) else 0.0
    for threshold, label, tc, bg in SEVERITY_LEVELS:
        if az >= threshold:
            return label, tc, bg
    return "🔵 LOW", "#1e3a8a", "#dbeafe"


# ══════════════════════════════════════════════════════════════════════════════
# 계산 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _rolling_zscore(s: pd.Series, window: int) -> tuple[pd.Series, pd.Series, pd.Series]:
    """(z_score, rolling_mean, rolling_std)"""
    mp = min(3, window)
    rm = s.rolling(window, min_periods=mp).mean()
    rs = s.rolling(window, min_periods=mp).std(ddof=1).replace(0, np.nan)
    z  = (s - rm) / rs
    return z.round(2), rm, rs


def _rolling_iqr_mask(s: pd.Series, window: int) -> pd.Series:
    """Rolling IQR 기반 이상치 마스크 (pandas rolling quantile 사용)."""
    mp = min(3, window)
    q1 = s.rolling(window, min_periods=mp).quantile(0.25)
    q3 = s.rolling(window, min_periods=mp).quantile(0.75)
    iqr = (q3 - q1).replace(0, np.nan)
    return (s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)


def _flag_group(grp: pd.DataFrame, col: str, prefix: str,
                method: str, threshold: float, window: int) -> pd.DataFrame:
    """
    단일 메트릭(col)에 대해 anomaly 플래그 컬럼을 추가.

    추가되는 컬럼:
        {prefix}_z       rolling z-score
        {prefix}_rmean   rolling mean (기준선)
        {prefix}_rstd    rolling std
        {prefix}_spike   bool — 급등
        {prefix}_drop    bool — 급락
        {prefix}_flag    bool — 급등 or 급락
        {prefix}_type    str  — "급등 🚨" / "급락 ⚠️" / ""
    """
    grp = grp.copy()
    s   = grp[col]

    z, rm, rs = _rolling_zscore(s, window)
    iqr_mask  = _rolling_iqr_mask(s, window)

    if method == "Z-score":
        mask = z.abs() > threshold
    elif method == "IQR":
        mask = iqr_mask
    else:
        mask = (z.abs() > threshold) | iqr_mask

    mask = mask.fillna(False)

    grp[f"{prefix}_z"]     = z
    grp[f"{prefix}_rmean"] = rm.round(0)
    grp[f"{prefix}_rstd"]  = rs.round(0)
    grp[f"{prefix}_flag"]  = mask
    grp[f"{prefix}_spike"] = mask & (s >= rm)
    grp[f"{prefix}_drop"]  = mask & (s < rm)
    grp[f"{prefix}_type"]  = np.where(
        grp[f"{prefix}_spike"], "급등 🚨",
        np.where(grp[f"{prefix}_drop"], "급락 ⚠️", "")
    )
    return grp


def _build_events(grp: pd.DataFrame, date_col: str,
                  metrics: list[tuple[str, str, str]],  # (col, prefix, metric_name)
                  company: str) -> list[dict]:
    """이상 이벤트 row 리스트 생성."""
    events = []
    for _, row in grp.iterrows():
        for col, prefix, metric_name in metrics:
            if not row.get(f"{prefix}_flag", False):
                continue
            val  = row[col]
            mean = row.get(f"{prefix}_rmean", np.nan)
            z    = row.get(f"{prefix}_z", np.nan)
            dev  = (val - mean) / mean * 100 if pd.notna(mean) and mean != 0 else np.nan
            sev_label, sev_tc, sev_bg = _severity(z if pd.notna(z) else 0.0)
            events.append({
                "date":          row[date_col],
                "company":       company,
                "metric":        metric_name,
                "type":          row.get(f"{prefix}_type", "이상"),
                "actual":        round(val, 0),
                "expected":      round(mean, 0) if pd.notna(mean) else None,
                "deviation_pct": round(dev, 1)  if pd.notna(dev)  else None,
                "z_score":       round(z, 2)    if pd.notna(z)    else None,
                "severity":      sev_label,
                "_tc":           sev_tc,
                "_bg":           sev_bg,
            })
    return events


# ══════════════════════════════════════════════════════════════════════════════
# 메인 계산 함수
# ══════════════════════════════════════════════════════════════════════════════

def run_anomaly_detection(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    """
    Rolling Z-score / IQR 기반 이상치 탐지 (매출 + 거래건수).

    Returns
    -------
    {
      "agg_df"   : pd.DataFrame  # 집계 + 플래그 컬럼
      "event_df" : pd.DataFrame  # 이상 이벤트 목록
      "name_col" : str
      "date_col" : str
      "sales_col": str
      "has_tx"   : bool
      "n_anomaly": int
      "threshold": float
      "window"   : int
    }
    """
    date_col  = role_map.get("transaction_date")
    sales_col = role_map.get("sales_amount")
    name_col  = role_map.get("company_name") or role_map.get("brand_name")
    tx_col    = role_map.get("number_of_tx")

    agg_unit  = params.get("agg_unit", "일")
    method    = params.get("method", "Z-score")
    threshold = float(params.get("threshold", 2.5))
    window    = int(params.get("window", 6))

    FREQ_MAP = {"일": "D", "주": "W", "월": "ME"}
    freq = FREQ_MAP.get(agg_unit, "D")

    # ── 전처리 ────────────────────────────────────────────────────────────────
    n_original = len(df)
    df = df.copy()
    df[date_col]  = _parse_dates(df[date_col])
    df[sales_col] = pd.to_numeric(df[sales_col], errors="coerce")
    df = df.dropna(subset=[date_col, sales_col])
    n_valid   = len(df)
    _date_min = str(df[date_col].min().date()) if n_valid > 0 else None
    _date_max = str(df[date_col].max().date()) if n_valid > 0 else None
    df["__row__"] = 1
    df = df.set_index(date_col)

    agg_dict: dict = {sales_col: "sum", "__row__": "count"}
    if tx_col:
        df[tx_col] = pd.to_numeric(df[tx_col], errors="coerce")
        agg_dict[tx_col] = "sum"

    # ── 집계 ──────────────────────────────────────────────────────────────────
    if name_col:
        agg = df.groupby([name_col, pd.Grouper(freq=freq)]).agg(agg_dict).reset_index()
    else:
        agg = df.groupby(pd.Grouper(freq=freq)).agg(agg_dict).reset_index()
        agg["__all__"] = "전체"
        name_col = "__all__"

    has_tx = bool(tx_col and tx_col in agg.columns)
    agg["tx_count"] = agg[tx_col] if has_tx else agg["__row__"]
    agg = agg.sort_values([name_col, date_col]).reset_index(drop=True)

    # ── 회사별 이상치 탐지 ────────────────────────────────────────────────────
    metrics = [(sales_col, "s", "매출")]
    metrics.append(("tx_count", "t", "거래건수"))  # 항상 포함, 회사 없으면 행 수

    frames: list[pd.DataFrame] = []
    all_events: list[dict] = []

    for co, grp in agg.groupby(name_col, sort=False):
        grp = grp.copy().sort_values(date_col).reset_index(drop=True)

        if len(grp) < 3:
            grp["is_anomaly"] = False
            frames.append(grp)
            continue

        for col, prefix, _ in metrics:
            grp = _flag_group(grp, col, prefix, method, threshold, window)

        grp["is_anomaly"] = grp["s_flag"] | grp["t_flag"]

        all_events.extend(_build_events(grp, date_col, metrics, str(co)))
        frames.append(grp)

    agg_out = pd.concat(frames, ignore_index=True)

    event_df = (
        pd.DataFrame(all_events)
        .sort_values(["date", "company"], ascending=[False, True])
        .reset_index(drop=True)
        if all_events else pd.DataFrame()
    )

    n_anomaly   = int(agg_out["is_anomaly"].sum())
    n_total_agg = len(agg_out)

    bs  = check_sample_size_sanity(n_total_agg, min_required=12)
    bs += check_anomaly_rate_sanity(n_anomaly, n_total_agg)

    audit, conf = compute_module_audit(
        n_original=n_original,
        n_valid=n_valid,
        role_map=role_map,
        used_roles=["transaction_date", "sales_amount", "company_name", "number_of_tx"],
        date_min=_date_min,
        date_max=_date_max,
        formula=f"Rolling {method} (window={window}, threshold=±{threshold}σ)",
        agg_unit=agg_unit,
        window=window,
        n_computable=n_total_agg - n_anomaly,
        n_periods=n_total_agg,
        business_checks=bs,
    )

    result = {
        "status":    "success",
        "message":   f"이상 탐지 완료 ({n_anomaly}/{n_total_agg} 이상)",
        "data":      agg_out,
        "metrics":   {"n_anomaly": n_anomaly, "n_total": n_total_agg, "threshold": threshold},
        "agg_df":    agg_out,
        "event_df":  event_df,
        "name_col":  name_col,
        "date_col":  date_col,
        "sales_col": sales_col,
        "has_tx":    has_tx,
        "n_anomaly": n_anomaly,
        "threshold": threshold,
        "window":    window,
    }
    return enrich_result(result, audit, conf)


# ══════════════════════════════════════════════════════════════════════════════
# 렌더링 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _timeline_chart(sub: pd.DataFrame, date_col: str,
                    val_col: str, prefix: str,
                    threshold: float, title: str) -> go.Figure:
    """실제값 + rolling band + 이상치 마커 차트.

    방어: spike/drop/flag 컬럼이 비-bool dtype이어도 안전하게 처리.
    """
    rm = sub[f"{prefix}_rmean"]
    rs = sub[f"{prefix}_rstd"]
    # 명시적 bool 변환 — pandas fancy indexing 오해 방지
    spike_mask = sub[f"{prefix}_spike"].fillna(False).astype(bool).to_numpy()
    drop_mask  = sub[f"{prefix}_drop"].fillna(False).astype(bool).to_numpy()
    spk = sub.loc[spike_mask] if len(spike_mask) == len(sub) else sub.iloc[0:0]
    drp = sub.loc[drop_mask]  if len(drop_mask)  == len(sub) else sub.iloc[0:0]

    fig = go.Figure()

    # 정상 범위 밴드
    upper = rm + threshold * rs
    lower = (rm - threshold * rs).clip(lower=0)
    fig.add_scatter(
        x=pd.concat([sub[date_col], sub[date_col].iloc[::-1]]),
        y=pd.concat([upper, lower.iloc[::-1]]),
        fill="toself", fillcolor="rgba(22,163,74,0.08)",
        line=dict(color="rgba(0,0,0,0)"),
        name=f"정상 범위 (±{threshold}σ)", showlegend=True,
    )

    # Rolling mean (기준선)
    fig.add_scatter(
        x=sub[date_col], y=rm,
        mode="lines", name="Rolling 기준선",
        line=dict(color=C_GRAY, width=1.5, dash="dash"),
    )

    # 실제값 라인
    fig.add_scatter(
        x=sub[date_col], y=sub[val_col],
        mode="lines", name="실제값",
        line=dict(color=C_BLUE, width=2),
    )

    # 정상 점 — flag bool 캐스팅 + 반전
    flag_mask = sub[f"{prefix}_flag"].fillna(False).astype(bool).to_numpy()
    normal_mask = ~flag_mask
    normal = sub.loc[normal_mask] if len(normal_mask) == len(sub) else sub.iloc[0:0]
    fig.add_scatter(
        x=normal[date_col], y=normal[val_col],
        mode="markers", name="정상",
        marker=dict(color=C_BLUE, size=5, opacity=0.5),
    )

    # 급등
    if not spk.empty:
        fig.add_scatter(
            x=spk[date_col], y=spk[val_col],
            mode="markers+text", name="급등 🚨",
            marker=dict(color=C_RED, size=14, symbol="triangle-up"),
            text=[f"Z={v:.1f}" for v in spk[f"{prefix}_z"].fillna(0)],
            textposition="top center",
        )

    # 급락
    if not drp.empty:
        fig.add_scatter(
            x=drp[date_col], y=drp[val_col],
            mode="markers+text", name="급락 ⚠️",
            marker=dict(color=C_AMBER, size=14, symbol="triangle-down"),
            text=[f"Z={v:.1f}" for v in drp[f"{prefix}_z"].fillna(0)],
            textposition="bottom center",
        )

    fig.update_layout(
        title=title,
        xaxis_title="기간", yaxis_title=val_col,
        hovermode="x unified", legend=dict(orientation="h"),
        margin=dict(t=44, b=0),
    )
    return fig


def _event_card_html(row: pd.Series) -> str:
    """이상 이벤트 하나를 HTML 카드로 렌더링."""
    tc, bg  = row["_tc"], row["_bg"]
    dt      = pd.Timestamp(row["date"]).strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"])[:10]
    dev_s   = f"{row['deviation_pct']:+.1f}%" if pd.notna(row.get("deviation_pct")) else "—"
    z_s     = f"Z = {row['z_score']:+.2f}" if pd.notna(row.get("z_score")) else ""
    exp_s   = f"{row['expected']:,.0f}" if pd.notna(row.get("expected")) else "—"
    act_s   = f"{row['actual']:,.0f}"
    co_s    = f" | {row['company']}" if row.get("company") != "전체" else ""

    return (
        f"<div style='background:{bg};border-left:4px solid {tc};"
        f"border-radius:6px;padding:9px 14px;margin:5px 0'>"
        f"<div style='display:flex;justify-content:space-between;align-items:center'>"
        f"<span style='font-weight:700;color:{tc};font-size:13px'>"
        f"{row['severity']} — {row['type']}</span>"
        f"<span style='font-size:11px;color:{C_GRAY}'>{dt}{co_s}</span>"
        f"</div>"
        f"<div style='font-size:12px;color:#374151;margin-top:4px'>"
        f"<b>{row['metric']}</b> &nbsp;|&nbsp; "
        f"실제 <b>{act_s}</b> &nbsp; 기준선 {exp_s} &nbsp; "
        f"편차 <b style='color:{tc}'>{dev_s}</b> &nbsp; {z_s}"
        f"</div>"
        f"</div>"
    )


# ══════════════════════════════════════════════════════════════════════════════
# 메인 렌더러
# ══════════════════════════════════════════════════════════════════════════════

def _render(result: dict):
    render_guide("anomaly")
    # 실패/경고 결과는 일찍 처리 — KeyError 방지
    if not isinstance(result, dict) or result.get("status") == "failed":
        st.error(result.get("message", "Anomaly 분석 실패") if isinstance(result, dict) else "결과 없음")
        return
    if "agg_df" not in result:
        st.info(result.get("message", "Anomaly 결과 데이터 부족"))
        return
    agg_df    = result["agg_df"]
    event_df  = result.get("event_df", pd.DataFrame())
    date_col  = result.get("date_col")
    sales_col = result.get("sales_col")
    name_col  = result.get("name_col")
    n_anomaly = result.get("n_anomaly", 0)
    threshold = result.get("threshold", 2.5)
    window    = result.get("window", 6)

    companies = sorted(agg_df[name_col].unique())

    # ── 전체 배너 ─────────────────────────────────────────────────────────────
    n_crit = (event_df["severity"] == "🔴 CRITICAL").sum() if not event_df.empty else 0
    n_high = (event_df["severity"] == "🟠 HIGH").sum()     if not event_df.empty else 0

    if n_anomaly > 0:
        parts = [f"**{n_anomaly}**개 이상 이벤트"]
        if n_crit: parts.append(f"CRITICAL {n_crit}건")
        if n_high: parts.append(f"HIGH {n_high}건")
        st.error(f"🚨 {' · '.join(parts)} 감지")
    else:
        st.success("✅ 이상치가 감지되지 않았습니다.")

    st.caption(f"Rolling 윈도우: {window}기간 | 임계값: ±{threshold}σ")

    # ── 회사 선택 ─────────────────────────────────────────────────────────────
    if len(companies) > 1:
        sel = st.selectbox("분석 대상 회사", companies, key="an_co_sel")
    else:
        sel = companies[0]

    sub  = agg_df[agg_df[name_col] == sel].sort_values(date_col).reset_index(drop=True)
    n_co = int(sub["is_anomaly"].sum())

    # ── 요약 메트릭 ────────────────────────────────────────────────────────────
    st.markdown(f"### {sel} — Anomaly Detection")

    co_events = event_df[event_df["company"] == sel] if not event_df.empty else pd.DataFrame()
    n_spike = int(sub["s_spike"].sum()) if "s_spike" in sub.columns else 0
    n_drop  = int(sub["s_drop"].sum())  if "s_drop"  in sub.columns else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("전체 포인트",   len(sub))
    c2.metric("이상 이벤트",   n_co)
    c3.metric("이상치 비율",   f"{n_co/len(sub)*100:.1f}%" if len(sub) else "—")
    c4.metric("급등 🚨",      n_spike)
    c5.metric("급락 ⚠️",      n_drop)

    # ── 탭 ────────────────────────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["📈 매출 이상치", "🔢 거래건수 이상치", "📋 이벤트 로그"])

    # ── TAB 1: 매출 타임라인 ──────────────────────────────────────────────────
    with tab1:
        if "s_flag" in sub.columns:
            st.plotly_chart(
                _timeline_chart(sub, date_col, sales_col, "s",
                                threshold, f"{sel} — 매출 이상치 탐지"), key="anomaly_1"
            )

            with st.expander("매출 이상치 데이터 테이블"):
                show_cols = [date_col, sales_col, "s_rmean", "s_z", "s_type"]
                show = sub[[c for c in show_cols if c in sub.columns]].copy()
                show = show.rename(columns={
                    date_col: "날짜", sales_col: "매출액",
                    "s_rmean": "Rolling 기준선", "s_z": "Z-score", "s_type": "유형",
                })
                # 명시적 bool 캐스팅 — int/NaN 등 비-bool 값이 있어도 안전 (fancy
                # indexing 오해 방지 → KeyError "Index([-1,-1...])" 차단)
                mask = sub["s_flag"].fillna(False).astype(bool).to_numpy()
                if len(mask) == len(show):
                    show_anm = show[mask].sort_values("날짜", ascending=False)
                else:
                    show_anm = show.iloc[0:0]
                if show_anm.empty:
                    st.info("매출 이상치가 없습니다.")
                else:
                    st.dataframe(show_anm, hide_index=True)
        else:
            st.info("데이터가 부족합니다.")

    # ── TAB 2: 거래건수 타임라인 ──────────────────────────────────────────────
    with tab2:
        if "t_flag" in sub.columns and "tx_count" in sub.columns:
            st.plotly_chart(
                _timeline_chart(sub, date_col, "tx_count", "t",
                                threshold, f"{sel} — 거래건수 이상치 탐지"), key="anomaly_2"
            )

            with st.expander("거래건수 이상치 데이터 테이블"):
                show_cols = [date_col, "tx_count", "t_rmean", "t_z", "t_type"]
                show = sub[[c for c in show_cols if c in sub.columns]].copy()
                show = show.rename(columns={
                    date_col: "날짜", "tx_count": "거래건수",
                    "t_rmean": "Rolling 기준선", "t_z": "Z-score", "t_type": "유형",
                })
                mask = sub["t_flag"].fillna(False).astype(bool).to_numpy()
                if len(mask) == len(show):
                    show_anm = show[mask].sort_values("날짜", ascending=False)
                else:
                    show_anm = show.iloc[0:0]
                if show_anm.empty:
                    st.info("거래건수 이상치가 없습니다.")
                else:
                    st.dataframe(show_anm, hide_index=True)
        else:
            st.info("거래건수 데이터가 없습니다.")

    # ── TAB 3: 이벤트 로그 ────────────────────────────────────────────────────
    with tab3:
        if co_events.empty:
            st.info(f"{sel}: 이상 이벤트가 없습니다.")
        else:
            # 심각도 필터
            sev_options = ["전체"] + list(
                dict.fromkeys(co_events["severity"].tolist())
            )
            sel_sev = st.selectbox("심각도 필터", sev_options, key="an_sev_filter")
            metric_options = ["전체"] + sorted(co_events["metric"].unique().tolist())
            sel_metric = st.radio("메트릭 필터", metric_options, horizontal=True,
                                  key="an_metric_filter")

            filtered = co_events.copy()
            if sel_sev != "전체":
                filtered = filtered[filtered["severity"] == sel_sev]
            if sel_metric != "전체":
                filtered = filtered[filtered["metric"] == sel_metric]

            st.caption(f"{len(filtered)}건 표시")

            for _, row in filtered.iterrows():
                st.markdown(_event_card_html(row), unsafe_allow_html=True)

            with st.expander("이벤트 로그 테이블 (다운로드용)"):
                dl_cols = ["date", "company", "metric", "type", "severity",
                           "actual", "expected", "deviation_pct", "z_score"]
                dl = filtered[[c for c in dl_cols if c in filtered.columns]].copy()
                dl = dl.rename(columns={
                    "date": "날짜", "company": "회사", "metric": "메트릭",
                    "type": "유형", "severity": "심각도",
                    "actual": "실제값", "expected": "기준선",
                    "deviation_pct": "편차(%)", "z_score": "Z-score",
                })
                st.dataframe(dl, hide_index=True)

    # ── 회사별 비교 ────────────────────────────────────────────────────────────
    if len(companies) <= 1:
        return

    st.divider()
    st.markdown("### 🏢 회사별 이상치 현황")

    rows = []
    for co in companies:
        co_df = agg_df[agg_df[name_col] == co]
        n     = int(co_df["is_anomaly"].sum())
        total = len(co_df)
        ev    = event_df[event_df["company"] == co] if not event_df.empty else pd.DataFrame()
        rows.append({
            "회사":           co,
            "전체 포인트":    total,
            "이상 이벤트":    n,
            "이상치 비율(%)": round(n / total * 100, 1) if total else 0,
            "급등 🚨":       int(co_df.get("s_spike", pd.Series(dtype=bool)).sum()),
            "급락 ⚠️":       int(co_df.get("s_drop",  pd.Series(dtype=bool)).sum()),
            "CRITICAL":      int((ev["severity"] == "🔴 CRITICAL").sum()) if not ev.empty else 0,
        })

    summary_df = pd.DataFrame(rows).sort_values("이상 이벤트", ascending=False)
    st.dataframe(summary_df, hide_index=True)

    # 이상치 건수 수평 막대
    fig_cmp = go.Figure(go.Bar(
        x=summary_df["이상 이벤트"],
        y=summary_df["회사"],
        orientation="h",
        marker_color=[C_RED if n > 0 else C_GRAY for n in summary_df["이상 이벤트"]],
        text=summary_df["이상 이벤트"],
        textposition="outside",
    ))
    fig_cmp.update_layout(
        title="회사별 이상 이벤트 건수",
        xaxis_title="건수", margin=dict(t=40, b=0),
    )
    st.plotly_chart(fig_cmp, key="anomaly_3")
