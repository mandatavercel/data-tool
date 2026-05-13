"""Factor Research — POS 매출 features × forward stock returns.

Cross-sectional Rank IC + Sector neutralization + Quintile backtest의
헤지펀드 표준 factor 검증 화면.

run_factor_research → result dict
_render             → Tier 1 dashboard
"""
from __future__ import annotations
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from modules.analysis.guides     import render_guide
from modules.common.core.audit import (
    compute_module_audit, check_growth_sanity, check_sample_size_sanity,
)
from modules.common.core.result import enrich_result

from modules.analysis.factor.panel      import build_pit_panel
from modules.analysis.factor.features   import build_features, FEATURE_DEFS, available_features
from modules.analysis.factor.targets    import build_forward_returns, join_signals_with_targets, HORIZONS
from modules.analysis.factor.ic         import cross_sectional_rank_ic, lag_decay_ics, time_series_ic
from modules.analysis.factor.neutralize import neutralize
from modules.analysis.factor.backtest   import quintile_backtest
from modules.analysis.factor.sector     import fetch_sector_master

# yfinance fetcher 재사용
from modules.analysis.signal.market     import (
    _fetch_daily_ohlcv, _fetch_daily_ohlcv_with_error,
    _fetch_benchmark_daily, _to_krx_code,
)

# DART corp_code 매핑 재사용
from modules.analysis.signal.earnings import _fetch_corp_code_map


_FETCH_MAX_WORKERS = 8


def _yfinance_health_check() -> tuple[bool, str]:
    """yfinance import 가능 + 버전 확인 + 헬스체크 1회."""
    try:
        import yfinance as yf
    except ImportError:
        return False, "yfinance 미설치 — `pip install yfinance` 필요"

    try:
        ver = yf.__version__
    except Exception:
        ver = "unknown"

    # 헬스체크: 삼성전자 1주 데이터
    try:
        tk = yf.Ticker("005930.KS")
        h = tk.history(period="5d")
        if h is None or h.empty:
            return False, (
                f"yfinance(v{ver}) 호출 자체는 성공하나 빈 데이터 반환. "
                "Yahoo Finance API 차단 또는 IP rate limit 의심. "
                "권장: `pip install --upgrade yfinance` 후 재시도, "
                "또는 다른 네트워크에서 시도."
            )
        return True, f"yfinance v{ver} OK"
    except Exception as e:
        return False, f"yfinance(v{ver}) 호출 실패: {type(e).__name__}: {str(e)[:80]}"


def _build_daily_prices(
    stock_codes: list[str], start: str, end: str,
) -> tuple[pd.DataFrame, list[dict], str]:
    """전체 universe의 daily prices를 병렬 fetch + 통합.

    Returns:
        DataFrame[stock_code, date, adj_close]
        list[dict]  — 실패 회사 진단 (stock_code, reason)
        str         — 헬스체크 메시지 + 첫 종목 디버그 정보
    """
    health_ok, health_msg = _yfinance_health_check()
    if not health_ok:
        failures = [{"stock_code": c, "reason": "yfinance 환경 문제 (아래 메시지 참고)"} for c in stock_codes]
        return pd.DataFrame(columns=["stock_code", "date", "adj_close"]), failures, health_msg

    # 첫 종목은 디버그 모드로 — 실제 에러 캡처
    first_code = stock_codes[0] if stock_codes else None
    first_debug_msg = ""
    if first_code:
        df_first, err_first = _fetch_daily_ohlcv_with_error(first_code, start, end)
        if df_first.empty:
            first_debug_msg = f" / 첫 종목({first_code}) 실패 trace: {err_first}"

    out = []
    failures: list[dict] = []
    # yfinance 1.3.0은 동시성에 더 민감 — 안전하게 4
    n_workers = min(4, max(1, len(stock_codes)))
    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futures = {
            ex.submit(_fetch_daily_ohlcv, code, start, end): code
            for code in stock_codes
        }
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                df = fut.result()
                if df.empty:
                    failures.append({
                        "stock_code": code,
                        "reason": "yfinance 빈 응답 (4경로 모두 실패)",
                    })
                    continue
                df = df[["date", "adj_close"]].copy()
                df["stock_code"] = code
                out.append(df)
            except Exception as e:
                failures.append({"stock_code": code, "reason": f"{type(e).__name__}: {str(e)[:60]}"})

    diag_msg = health_msg + first_debug_msg

    if not out:
        return pd.DataFrame(columns=["stock_code", "date", "adj_close"]), failures, diag_msg
    merged = pd.concat(out, ignore_index=True).sort_values(["stock_code", "date"]).reset_index(drop=True)
    return merged, failures, diag_msg


# ── Runner ────────────────────────────────────────────────────────────────────

def run_factor_research(df: pd.DataFrame, role_map: dict, params: dict) -> dict:
    """POS factor research 백테스트.

    params (선택):
      dart_api_key       : DART API key (sector master용)
      available_lag_days : 매출월 종료 후 가용까지 영업일 (default 5)
    """
    sales_col   = role_map.get("sales_amount")
    date_col    = role_map.get("transaction_date")
    company_col = role_map.get("company_name")
    # stock_code / security_code 둘 다 후보 — _to_krx_code 변환 성공하는 쪽 사용
    # (사용자가 stock_code에 13자리 법인등록번호를 매핑해도 ISIN으로 fallback)
    _stock_candidates = [
        role_map[r] for r in ("stock_code", "security_code")
        if role_map.get(r) and role_map[r] in df.columns
    ]
    stock_col   = _stock_candidates[0] if _stock_candidates else None
    tx_col      = role_map.get("number_of_tx")

    if not (sales_col and date_col and stock_col):
        return {
            "status":  "failed",
            "message": "transaction_date / sales_amount / stock_code(또는 security_code) 역할 필수",
            "data":    None,
            "metrics": {},
        }

    n_original = len(df)
    available_lag = int(params.get("available_lag_days", 5))
    dart_api_key  = params.get("dart_api_key", "").strip()

    # 후보 컬럼들을 시도해서 KRX 6자리 변환 성공률 가장 높은 컬럼 선택
    df0 = df.copy()
    best_col = None
    best_n   = 0
    for cand in _stock_candidates:
        codes = df0[cand].astype(str).map(_to_krx_code)
        n_ok  = int(codes.notna().sum())
        if n_ok > best_n:
            best_n   = n_ok
            best_col = cand
    if best_col is None or best_n == 0:
        sample_vals = []
        for cand in _stock_candidates:
            try:
                sample_vals.extend(str(v) for v in df0[cand].dropna().unique()[:2])
            except Exception:
                pass
        return {
            "status":  "failed",
            "message": (
                f"매핑된 종목코드 컬럼({', '.join(_stock_candidates)})에서 KRX 6자리를 추출하지 못함. "
                f"샘플값: {', '.join(sample_vals[:4])}. "
                f"ISIN(KR+10자리) 또는 6자리 종목코드 필요 — 법인등록번호(13자리)는 매핑 해제."
            ),
            "data": None, "metrics": {},
        }
    stock_col = best_col
    df0["_stock"] = df0[stock_col].astype(str).map(_to_krx_code)
    df0 = df0.dropna(subset=["_stock"])
    df0[stock_col] = df0["_stock"]
    df0 = df0.drop(columns=["_stock"])

    n_valid = len(df0)
    warnings: list[str] = []

    # ── 1) PIT panel + features ─────────────────────────────────────────────
    panel = build_pit_panel(
        df0, sales_col=sales_col, date_col=date_col,
        company_col=company_col or stock_col, stock_col=stock_col,
        tx_col=tx_col, available_lag_days=available_lag,
    )
    if panel.empty:
        # 진단: 어디서 0건이 됐는지
        n_after_extract = len(df0)
        diag = (
            f"PIT panel 생성 실패 — 종목코드 추출 후 {n_after_extract}행이 남았으나 "
            f"sales_month 집계 시 0건. "
            "원인 가능성: ① sales_amount 컬럼이 모두 NaN/0 ② transaction_date 파싱 실패 "
            "③ 같은 (stock_code, month) 조합이 없음. "
            "Step 2 매핑·Step 3 검증에서 매출·날짜 컬럼이 정상인지 확인하세요."
        )
        return {"status": "failed", "message": diag,
                "data": None, "metrics": {}}

    panel = build_features(panel)
    feat_list = available_features(panel)
    if not feat_list:
        warnings.append("Feature 생성 실패 — 매출 데이터 12개월 이상 필요 (YoY 계산용)")

    # ── 2) Sector master ─────────────────────────────────────────────────────
    stock_codes = sorted(panel["stock_code"].dropna().unique().tolist())
    corp_code_map: dict[str, str] = {}
    if dart_api_key:
        with st.spinner("DART corp_code 매핑 중..."):
            try:
                name_map, stock_map = _fetch_corp_code_map(dart_api_key)
                for code in stock_codes:
                    if code in stock_map:
                        corp_code_map[code] = stock_map[code]
            except Exception as e:
                err_msg = str(e)
                if any(k in err_msg for k in ("ConnectionError", "Max retries", "timed out",
                                                "Connection reset", "URLError", "getaddrinfo")):
                    warnings.append(
                        "DART 접속 차단됨 — sector 분류는 pykrx만 사용 (induty_code 없음). "
                        "Factor 분석은 정상 진행, sector-neutral은 제한적."
                    )
                else:
                    warnings.append(f"DART corp 매핑 실패: {err_msg[:200]}")

    with st.spinner(f"섹터 master 구축 중... ({len(stock_codes)}개사)"):
        sector_master = fetch_sector_master(
            tuple(stock_codes),
            dart_api_key=dart_api_key or None,
            corp_code_map=tuple(corp_code_map.items()) if corp_code_map else None,
        )

    n_classified = int((sector_master["sector_gics"] != "Unclassified").sum()) if not sector_master.empty else 0

    # ── 3) Daily stock prices + benchmark ────────────────────────────────────
    sd = panel["signal_date"].min().strftime("%Y-%m-%d")
    ed = (panel["signal_date"].max() + pd.Timedelta(days=200)).strftime("%Y-%m-%d")
    with st.spinner(f"주가 OHLCV 수집 중... ({len(stock_codes)}개 종목)"):
        daily_prices, fetch_failures, yf_health_msg = _build_daily_prices(stock_codes, sd, ed)
        bench_daily  = _fetch_benchmark_daily(sd, ed)

    n_fetched = daily_prices["stock_code"].nunique() if not daily_prices.empty else 0
    n_failed  = len(fetch_failures)

    if daily_prices.empty:
        return {
            "status":  "failed",
            "message": (
                f"주가 데이터 수집 전부 실패 ({n_failed}/{len(stock_codes)}종목). "
                f"원인: {yf_health_msg}"
            ),
            "data":    None,
            "metrics": {},
            "_fetch_failures": fetch_failures,
            "_yf_health_msg":  yf_health_msg,
        }
    if n_failed:
        warnings.append(
            f"yfinance 부분 실패: {n_failed}/{len(stock_codes)}종목 "
            f"(상장폐지·미상장·네트워크 오류 등)"
        )

    bench_for_target = (
        bench_daily[["date", "adj_close"]]
        if not bench_daily.empty else None
    )

    # ── 4) Forward returns + PIT join ────────────────────────────────────────
    fwd_returns = build_forward_returns(
        daily_prices, bench_prices=bench_for_target, sector_master=sector_master,
    )
    panel_full = join_signals_with_targets(panel, fwd_returns)
    # sector master 병합 — sector_master 비어있거나 컬럼 누락 모두 방어
    have_sector = (
        not sector_master.empty
        and {"stock_code", "sector_gics"}.issubset(sector_master.columns)
    )
    if have_sector:
        merge_cols = ["stock_code", "sector_gics"]
        for c in ("market_cap", "market"):
            if c in sector_master.columns:
                merge_cols.append(c)
        panel_full = panel_full.merge(
            sector_master[merge_cols].drop_duplicates("stock_code"),
            on="stock_code", how="left",
        )
    # 누락 컬럼은 기본값으로 채움 (panel_full이 비어 있어도 안전)
    if "sector_gics" not in panel_full.columns:
        panel_full["sector_gics"] = "Unclassified"
    if "market_cap" not in panel_full.columns:
        panel_full["market_cap"] = np.nan
    if "market" not in panel_full.columns:
        panel_full["market"] = ""

    if panel_full.empty:
        warnings.append("PIT 매칭 후 데이터 없음 — signal_date 이후 주가가 충분치 않음")

    # ── 5) Default neutralize: sales_yoy → winsorize + sector_z ─────────────
    for f in feat_list:
        if f in panel_full.columns:
            panel_full = neutralize(
                panel_full, feature=f,
                methods=("winsorize", "sector_z"),
                out_col=f"{f}_n",
            )

    # ── 6) Lag decay table — 모든 feature × horizon × target_kind ───────────
    lag_decay = []
    for f in feat_list:
        col = f"{f}_n" if f"{f}_n" in panel_full.columns else f
        for tk in ("raw", "excess", "sector_rel"):
            tbl = lag_decay_ics(panel_full, feature=col, target_kind=tk)
            if tbl.empty: continue
            tbl["feature"]     = f
            tbl["target_kind"] = tk
            lag_decay.append(tbl)
    lag_decay_df = pd.concat(lag_decay, ignore_index=True) if lag_decay else pd.DataFrame()

    # ── 7) 메트릭 (default feature = sales_yoy, horizon = 1m, target = sector_rel) ──
    default_feat   = "sales_yoy" if "sales_yoy" in feat_list else (feat_list[0] if feat_list else None)
    default_target = "fwd_sector_rel_1m" if "fwd_sector_rel_1m" in panel_full.columns else "fwd_1m"

    primary_ic = (
        cross_sectional_rank_ic(panel_full, f"{default_feat}_n", default_target)
        if default_feat else {"ic_mean": 0, "ic_std": 0, "ic_t": 0, "icir": 0,
                              "hit_rate": 0, "n_periods": 0, "ic_series": pd.DataFrame()}
    )

    primary_bt = (
        quintile_backtest(panel_full, f"{default_feat}_n", target=default_target,
                          sector_neutral=True)
        if default_feat else {"sharpe": 0, "sharpe_after": 0, "max_dd": 0,
                              "annual_ret": 0, "hit_rate": 0, "turnover": 0,
                              "n_periods": 0, "portfolio": pd.DataFrame()}
    )

    n_panel_obs = len(panel_full)
    n_dates     = panel_full["signal_date"].nunique() if not panel_full.empty else 0
    n_stocks    = panel_full["stock_code"].nunique() if not panel_full.empty else 0

    metrics = {
        "n_panel_obs":   n_panel_obs,
        "n_dates":       n_dates,
        "n_stocks":      n_stocks,
        "n_classified":  n_classified,
        "default_feat":  default_feat or "(없음)",
        "ic_mean":       round(primary_ic["ic_mean"], 4),
        "ic_t":          round(primary_ic["ic_t"], 2),
        "icir":          round(primary_ic["icir"], 3),
        "hit_rate":      round(primary_ic["hit_rate"], 3),
        "ls_sharpe":     primary_bt["sharpe"],
        "ls_sharpe_ac":  primary_bt["sharpe_after"],
        "ls_annual":     primary_bt["annual_ret"],
        "ls_max_dd":     primary_bt["max_dd"],
        "ls_turnover":   primary_bt["turnover"],
    }

    status  = "warning" if warnings else "success"
    message = " | ".join(warnings) if warnings else (
        f"Factor research 완료 · {n_stocks}개 종목 × {n_dates}개 시점, "
        f"{default_feat} CS Rank IC = {primary_ic['ic_mean']:.3f} (t={primary_ic['ic_t']:.2f})"
    )

    bs  = check_sample_size_sanity(n_dates, min_required=24)
    bs += check_growth_sanity(panel["sales_yoy"].dropna() if "sales_yoy" in panel.columns else None)

    audit, conf = compute_module_audit(
        n_original=n_original, n_valid=n_valid,
        role_map=role_map,
        used_roles=["transaction_date", "sales_amount", "stock_code",
                    "company_name", "number_of_tx"],
        date_min=str(panel["sales_month"].min()) if not panel.empty else None,
        date_max=str(panel["sales_month"].max()) if not panel.empty else None,
        formula="POS YoY × forward returns · CS Rank IC + Quintile backtest",
        agg_unit="월", n_computable=n_panel_obs, n_periods=n_dates,
        business_checks=bs,
    )

    return enrich_result({
        "status":            status,
        "message":           message,
        "data":              panel_full,
        "metrics":           metrics,
        "_panel":            panel_full,
        "_sector_master":    sector_master,
        "_feat_list":        feat_list,
        "_lag_decay":        lag_decay_df,
        "_primary_ic":       primary_ic,
        "_primary_bt":       primary_bt,
        "_default_feat":     default_feat,
        "_default_target":   default_target,
        "_fetch_failures":   fetch_failures,
        "_n_fetched":        n_fetched,
        "_n_failed":         n_failed,
    }, audit, conf)


# ── Renderer ──────────────────────────────────────────────────────────────────

_TARGET_KIND_LABEL = {
    "raw":        "Raw return",
    "excess":     "Market-adjusted (vs KOSPI)",
    "sector_rel": "Sector-relative",
}


def _strip_html(items: list[tuple]) -> str:
    inner = "".join(
        f"<div><span style='color:#94a3b8'>{lbl}</span> "
        f"<b style='color:{col}'>{val}</b></div>"
        for lbl, val, col in items
    )
    return (
        f"<div style='font-family:\"SF Mono\",Menlo,Consolas,monospace;font-size:13px;"
        f"background:#0f172a;color:#e2e8f0;padding:10px 16px;border-radius:4px;"
        f"display:flex;flex-wrap:wrap;gap:22px;align-items:center;line-height:1.4;"
        f"margin-bottom:10px'>{inner}</div>"
    )


def _explain_box(title: str, body: str, color: str = "#1e40af") -> None:
    st.markdown(
        f"<div style='background:#f8fafc;border-left:3px solid {color};padding:12px 16px;"
        f"font-size:13px;line-height:1.7;color:#334155;margin:4px 0 24px 0;border-radius:4px'>"
        f"<b style='color:#0f172a'>{title}</b><br>{body}</div>",
        unsafe_allow_html=True,
    )


def _render(result: dict):
    render_guide("market_signal")  # 임시로 동일 가이드 사용 — 추후 factor 가이드 분리

    if result["status"] == "failed":
        st.error(result["message"])
        return
    if result["status"] == "warning":
        st.warning(result["message"])

    m            = result["metrics"]
    panel        = result.get("_panel", pd.DataFrame())
    feat_list    = result.get("_feat_list", [])
    lag_decay_df = result.get("_lag_decay", pd.DataFrame())
    sector_master = result.get("_sector_master", pd.DataFrame())

    if panel.empty or not feat_list:
        st.error("분석 가능한 panel 데이터가 없습니다 (최소 12개월 매출 + 주가 매칭 필요).")
        return

    # ── Selectors ─────────────────────────────────────────────────────────────
    sel1, sel2, sel3 = st.columns([2, 1, 1])
    with sel1:
        feat_options = [(f, FEATURE_DEFS[f][0]) for f in feat_list if f in FEATURE_DEFS]
        feat_labels  = [lbl for _, lbl in feat_options]
        feat_codes   = [c for c, _ in feat_options]
        sel_feat_label = st.selectbox(
            "Feature", feat_labels, key="fr_feat_sel",
            index=feat_codes.index(m["default_feat"]) if m["default_feat"] in feat_codes else 0,
        )
        sel_feat = feat_codes[feat_labels.index(sel_feat_label)]
    with sel2:
        sel_horizon = st.selectbox("Horizon", list(HORIZONS.keys()), index=0, key="fr_h_sel")
    with sel3:
        sel_kind = st.selectbox(
            "Target", list(_TARGET_KIND_LABEL.keys()),
            format_func=lambda k: _TARGET_KIND_LABEL[k], index=2, key="fr_kind_sel",
        )

    feat_col   = f"{sel_feat}_n" if f"{sel_feat}_n" in panel.columns else sel_feat
    target_col = {"raw": f"fwd_{sel_horizon}",
                  "excess": f"fwd_excess_{sel_horizon}",
                  "sector_rel": f"fwd_sector_rel_{sel_horizon}"}[sel_kind]

    if target_col not in panel.columns:
        st.warning(f"{_TARGET_KIND_LABEL[sel_kind]} target이 가용하지 않습니다.")
        return

    # ── 단일 헤더 strip — 핵심 지표 ──────────────────────────────────────────
    ic_result = cross_sectional_rank_ic(panel, feat_col, target_col)
    bt_result = quintile_backtest(panel, feat_col, target=target_col, sector_neutral=True)

    ic_mean = ic_result["ic_mean"]
    ic_color = "#dc2626" if abs(ic_mean) < 0.02 else "#d97706" if abs(ic_mean) < 0.05 else "#16a34a"

    strip_items = [
        ("UNIVERSE",  f"{m['n_stocks']} stocks",                         "#fff"),
        ("PERIODS",   f"{ic_result['n_periods']}",                       "#fff"),
        ("IC̄",        f"{ic_mean:+.4f}",                                 ic_color),
        ("IC σ",      f"{ic_result['ic_std']:.4f}",                      "#fff"),
        ("ICIR",      f"{ic_result['icir']:+.2f}",                       "#fff"),
        ("|t|",       f"{abs(ic_result['ic_t']):.2f}",                   "#fff"),
        ("HIT",       f"{ic_result['hit_rate']*100:.0f}%",               "#fff"),
        ("LS Sharpe", f"{bt_result['sharpe']:.2f}",                      "#fff"),
        ("LS AnnRet", f"{bt_result['annual_ret']:+.1f}%",                "#fff"),
        ("MaxDD",     f"{bt_result['max_dd']:+.1f}%",                    "#fff"),
        ("Turnover",  f"{bt_result['turnover']*100:.0f}%",               "#fff"),
    ]
    st.markdown(_strip_html(strip_items), unsafe_allow_html=True)
    st.caption(
        f"**{FEATURE_DEFS[sel_feat][0]}** × **{_TARGET_KIND_LABEL[sel_kind]} {sel_horizon}** · "
        f"{FEATURE_DEFS[sel_feat][1]}"
    )

    # ── Chart 1: IC time series + Rolling 12m IC ─────────────────────────────
    ics = ic_result.get("ic_series", pd.DataFrame())
    if not ics.empty:
        fig_ic = go.Figure()
        fig_ic.add_trace(go.Bar(
            x=ics["date"], y=ics["ic"],
            marker_color=["#16a34a" if v >= 0 else "#dc2626" for v in ics["ic"]],
            name="IC (period)", opacity=0.55,
            hovertemplate="%{x|%Y-%m-%d}<br>IC=%{y:.3f}<extra></extra>",
        ))
        if "ic_rolling12" in ics.columns:
            fig_ic.add_trace(go.Scatter(
                x=ics["date"], y=ics["ic_rolling12"],
                mode="lines", name="Rolling 12M",
                line=dict(color="#1e40af", width=2.5),
                hovertemplate="%{x|%Y-%m-%d}<br>R12 IC=%{y:.3f}<extra></extra>",
            ))
        fig_ic.add_hline(y=0, line_color="#64748b", line_width=1)
        fig_ic.update_layout(
            title=dict(text="Cross-sectional Rank IC — 시간별 추이", font=dict(size=14)),
            height=340, plot_bgcolor="#fff",
            margin=dict(t=50, b=40, l=10, r=10),
            xaxis=dict(showgrid=False, tickformat="%Y-%m", nticks=12, type="date"),
            yaxis=dict(title="IC", gridcolor="#e2e8f0"),
            legend=dict(orientation="h", yanchor="top", y=-0.15),
            hovermode="x unified",
        )
        st.plotly_chart(fig_ic, key="fr_ic_ts", use_container_width=True)
        _explain_box(
            "차트 설명 — Cross-sectional Rank IC",
            "매 signal_date마다 universe 회사들을 feature로 정렬한 rank와 forward return rank의 "
            "Spearman 상관계수. <b>각 막대 = 그 시점의 단면 IC</b>, 파란 라인 = 12개월 롤링 평균.<br>"
            "<b>해석:</b> 0 위에서 안정적이면 factor가 시장 전반에 걸쳐 작동. "
            "0을 자주 가로지르면 일관된 alpha가 아님. ICIR ≥ 0.5면 양호, ≥ 1.0이면 우수."
        )

    # ── Chart 2: Lag Decay (horizon × target_kind) ───────────────────────────
    if not lag_decay_df.empty:
        sub = lag_decay_df[
            (lag_decay_df["feature"] == sel_feat) &
            (lag_decay_df["target_kind"] == sel_kind)
        ].copy()
        if not sub.empty:
            sub["horizon_d"] = sub["horizon"].map(HORIZONS)
            sub = sub.sort_values("horizon_d")
            fig_dec = go.Figure(go.Bar(
                x=sub["horizon"], y=sub["ic_mean"],
                marker_color=["#1e40af" if abs(v) >= 0.02 else "#cbd5e1"
                              for v in sub["ic_mean"]],
                text=[f"{v:+.3f}" for v in sub["ic_mean"]],
                textposition="outside",
                hovertemplate=(
                    "%{x}<br>IC̄ %{y:.3f}<br>"
                    "|t|=%{customdata[0]:.2f}<br>"
                    "ICIR=%{customdata[1]:+.2f}<br>"
                    "Periods=%{customdata[2]}<extra></extra>"
                ),
                customdata=sub[["ic_t", "icir", "n_periods"]].values,
            ))
            fig_dec.add_hline(y=0, line_color="#64748b", line_width=1)
            fig_dec.update_layout(
                title=dict(
                    text=f"Lag Decay — {sel_feat} × {_TARGET_KIND_LABEL[sel_kind]}",
                    font=dict(size=14),
                ),
                height=300, plot_bgcolor="#fff",
                margin=dict(t=50, b=40, l=10, r=10),
                xaxis=dict(title="Horizon"),
                yaxis=dict(title="IC̄ (mean)", gridcolor="#e2e8f0"),
            )
            st.plotly_chart(fig_dec, key="fr_lag_decay", use_container_width=True)
            _explain_box(
                "차트 설명 — Lag Decay Curve",
                f"같은 feature ({sel_feat})의 IC가 horizon에 따라 어떻게 감쇠하는지. "
                "1m → 3m → 6m로 갈수록 IC가 줄어드는 게 자연스러운 alpha decay 패턴.<br>"
                "<b>해석:</b> 짧은 horizon이 강하면 단기 trading 시그널, 긴 horizon이 강하면 "
                "포지션 빌딩용. 모든 horizon에서 |IC| < 0.02면 factor 무효."
            )

    # ── Chart 3: Quintile cumulative returns ─────────────────────────────────
    portfolio = bt_result.get("portfolio", pd.DataFrame())
    if not portfolio.empty and "cum_LS" in portfolio.columns:
        fig_q = go.Figure()
        # 분위별 누적 (해석용)
        q_cols = sorted([c for c in portfolio.columns if c.startswith("q") and c != "qLS"])
        cum_qs = portfolio[q_cols].cumsum() if q_cols else pd.DataFrame()
        palette = ["#dc2626", "#f59e0b", "#94a3b8", "#84cc16", "#16a34a"]
        for i, qc in enumerate(q_cols):
            fig_q.add_trace(go.Scatter(
                x=portfolio["signal_date"], y=cum_qs[qc],
                mode="lines", name=f"{qc.upper()}",
                line=dict(color=palette[i % len(palette)], width=1.5),
            ))
        fig_q.add_trace(go.Scatter(
            x=portfolio["signal_date"], y=portfolio["cum_LS"],
            mode="lines", name="L/S (Q5−Q1)",
            line=dict(color="#1e40af", width=2.8),
        ))
        if "cum_LS_after_cost" in portfolio.columns:
            fig_q.add_trace(go.Scatter(
                x=portfolio["signal_date"], y=portfolio["cum_LS_after_cost"],
                mode="lines", name=f"L/S after cost ({bt_result['cost_bps']:.0f}bps)",
                line=dict(color="#7c3aed", width=2, dash="dash"),
            ))
        fig_q.add_hline(y=0, line_color="#94a3b8", line_width=1)
        fig_q.update_layout(
            title=dict(text="Quintile Cumulative Return — Sector-Neutral Long-Short",
                       font=dict(size=14)),
            height=380, plot_bgcolor="#fff",
            margin=dict(t=50, b=40, l=10, r=10),
            xaxis=dict(showgrid=False, tickformat="%Y-%m", nticks=12, type="date"),
            yaxis=dict(title="Cumulative return (sum %)", gridcolor="#e2e8f0"),
            legend=dict(orientation="h", yanchor="top", y=-0.15),
            hovermode="x unified",
        )
        st.plotly_chart(fig_q, key="fr_quintile", use_container_width=True)
        _explain_box(
            "차트 설명 — Quintile Long-Short",
            f"매 시점 feature 분위로 종목을 5등분 → Q5(상위 20%) 매수 + Q1(하위 20%) 매도 = "
            f"sector-neutral long-short 포트폴리오. <b>cost_bps {bt_result['cost_bps']:.0f}</b> "
            "왕복 거래비용 차감 후 곡선이 점선.<br>"
            "<b>해석:</b> 우상향 + cost 차감 후에도 양수 ≈ 실현 가능한 alpha. "
            "Sharpe ≥ 0.5 양호, ≥ 1 우수. Q5/Q1 spread가 클수록 monotonic factor."
        )

    # ════════════════════════════════════════════════════════════════════════
    # 회사별 Drill-down — 단일 회사 관점에서 feature ↔ forward return
    # ════════════════════════════════════════════════════════════════════════
    st.divider()
    st.markdown("### 회사별 Drill-down")

    co_options = sorted(panel["stock_code"].dropna().unique().tolist()) if "stock_code" in panel.columns else []
    if not co_options:
        st.info("드릴다운 가능한 회사가 없습니다.")
    else:
        # 회사명 매핑 (있으면 함께 표시)
        if "company" in panel.columns:
            name_map = panel.dropna(subset=["stock_code", "company"]).drop_duplicates("stock_code").set_index("stock_code")["company"].to_dict()
            display_options = [f"{c} · {name_map.get(c, '')}" for c in co_options]
        else:
            name_map = {}
            display_options = co_options

        sel_idx = st.selectbox(
            "회사 선택", range(len(co_options)),
            format_func=lambda i: display_options[i],
            key="fr_drill_sel",
        )
        sel_co = co_options[sel_idx]
        sel_name = name_map.get(sel_co, "")

        co_panel = panel[panel["stock_code"] == sel_co].sort_values("signal_date").reset_index(drop=True)

        if feat_col not in co_panel.columns or target_col not in co_panel.columns:
            st.warning(f"이 회사의 {feat_col} 또는 {target_col} 데이터가 없습니다.")
        else:
            sub = co_panel[["signal_date", feat_col, target_col]].dropna().reset_index(drop=True)
            n_obs = len(sub)
            if n_obs < 4:
                st.warning(f"이 회사의 유효 관측치 {n_obs}개 — 최소 4개 필요.")
            else:
                # TS IC + t-stat + hit
                from scipy.stats import spearmanr, pearsonr
                ts_ic, p_val = spearmanr(sub[feat_col], sub[target_col])
                ts_ic = float(ts_ic) if ts_ic is not None and not (isinstance(ts_ic, float) and math.isnan(ts_ic)) else 0.0
                ts_t  = ts_ic * math.sqrt(max(n_obs - 2, 1)) / math.sqrt(max(1 - ts_ic*ts_ic, 1e-9)) if abs(ts_ic) < 1 else 0.0
                # Hit rate (방향 일치율)
                mask = sub[feat_col].notna() & sub[target_col].notna()
                hit  = float((np.sign(sub[feat_col][mask]) == np.sign(sub[target_col][mask])).mean() * 100) if mask.any() else 0.0
                feat_avg = float(sub[feat_col].mean())
                ret_avg  = float(sub[target_col].mean())

                ic_color = "#dc2626" if abs(ts_ic) < 0.15 else "#d97706" if abs(ts_ic) < 0.3 else "#16a34a"
                co_strip = "".join(
                    f"<div><span style='color:#94a3b8'>{lbl}</span> "
                    f"<b style='color:{col}'>{val}</b></div>"
                    for lbl, val, col in [
                        ("STOCK",      f"{sel_co}",                  "#fff"),
                        ("NAME",       (sel_name or '—')[:18],       "#fff"),
                        ("N OBS",      f"{n_obs}",                   "#fff"),
                        ("TS IC",      f"{ts_ic:+.3f}",              ic_color),
                        ("|t|",        f"{abs(ts_t):.2f}",           "#fff"),
                        ("HIT",        f"{hit:.0f}%",                "#fff"),
                        (f"{sel_feat[:10]} 평균", f"{feat_avg:+.1f}",     "#fff"),
                        (f"{sel_horizon} 평균",   f"{ret_avg:+.1f}%",     "#fff"),
                    ]
                )
                st.markdown(
                    f"<div style='font-family:\"SF Mono\",Menlo,Consolas,monospace;font-size:13px;"
                    f"background:#0f172a;color:#e2e8f0;padding:10px 16px;border-radius:4px;"
                    f"display:flex;flex-wrap:wrap;gap:22px;align-items:center;line-height:1.4;"
                    f"margin-bottom:10px'>{co_strip}</div>",
                    unsafe_allow_html=True,
                )

                # 차트 1 — feature와 forward return 이중축 시계열
                fig_ts = go.Figure()
                fig_ts.add_trace(go.Bar(
                    x=sub["signal_date"], y=sub[feat_col],
                    name=FEATURE_DEFS.get(sel_feat, (sel_feat, ""))[0],
                    marker_color=["#3b82f6" if v >= 0 else "#94a3b8" for v in sub[feat_col]],
                    opacity=0.7, yaxis="y",
                ))
                fig_ts.add_trace(go.Scatter(
                    x=sub["signal_date"], y=sub[target_col],
                    name=f"{_TARGET_KIND_LABEL[sel_kind]} {sel_horizon}",
                    line=dict(color="#dc2626", width=2.2), yaxis="y2",
                    mode="lines+markers",
                ))
                fig_ts.update_layout(
                    title=dict(text=f"{sel_co} {sel_name} — Feature vs Forward Return", font=dict(size=14)),
                    height=340, plot_bgcolor="#fff",
                    margin=dict(t=50, b=40, l=10, r=10),
                    xaxis=dict(showgrid=False, tickformat="%Y-%m", nticks=10, type="date"),
                    yaxis=dict(title=FEATURE_DEFS.get(sel_feat, (sel_feat, ""))[0],
                               showgrid=True, gridcolor="#e2e8f0"),
                    yaxis2=dict(title=f"Forward Return {sel_horizon}",
                                overlaying="y", side="right", showgrid=False),
                    legend=dict(orientation="h", yanchor="top", y=-0.15),
                    hovermode="x unified",
                )
                st.plotly_chart(fig_ts, key="fr_drill_ts", use_container_width=True)
                _explain_box(
                    "차트 설명 — 이 회사의 시계열",
                    "왼쪽 축은 매출 feature(파/회색 막대), 오른쪽 축은 해당 horizon의 미래 수익률(빨간 라인). "
                    "두 시리즈가 같이 움직이면 (둘 다 양수 또는 둘 다 음수) TS IC가 양수, 반대면 음수.",
                )

                # 차트 2 — scatter + 회귀선
                fig_sc = go.Figure()
                fig_sc.add_trace(go.Scatter(
                    x=sub[feat_col], y=sub[target_col],
                    mode="markers",
                    marker=dict(size=8, color="#3b82f6",
                                line=dict(color="#1e40af", width=1)),
                    text=[d.strftime("%Y-%m-%d") for d in sub["signal_date"]],
                    hovertemplate="%{text}<br>feature=%{x:.2f}<br>return=%{y:.2f}%<extra></extra>",
                    name="관측치",
                ))
                # 단순 1차 회귀선
                if n_obs >= 3:
                    x_arr = sub[feat_col].values.astype(float)
                    y_arr = sub[target_col].values.astype(float)
                    if np.std(x_arr) > 1e-9:
                        slope, intercept = np.polyfit(x_arr, y_arr, 1)
                        x_line = np.array([x_arr.min(), x_arr.max()])
                        y_line = slope * x_line + intercept
                        fig_sc.add_trace(go.Scatter(
                            x=x_line, y=y_line, mode="lines",
                            name=f"회귀선 (slope={slope:.2f})",
                            line=dict(color="#dc2626", width=2, dash="dash"),
                        ))
                fig_sc.add_hline(y=0, line_color="#94a3b8", line_width=1)
                fig_sc.add_vline(x=0, line_color="#94a3b8", line_width=1)
                fig_sc.update_layout(
                    title=dict(
                        text=f"Feature × Forward Return Scatter (TS IC = {ts_ic:+.3f})",
                        font=dict(size=14),
                    ),
                    height=380, plot_bgcolor="#fff",
                    margin=dict(t=50, b=40, l=10, r=10),
                    xaxis=dict(title=FEATURE_DEFS.get(sel_feat, (sel_feat, ""))[0],
                               showgrid=True, gridcolor="#e2e8f0", zeroline=False),
                    yaxis=dict(title=f"Forward Return {sel_horizon} (%)",
                               showgrid=True, gridcolor="#e2e8f0", zeroline=False),
                    legend=dict(orientation="h", yanchor="top", y=-0.15),
                )
                st.plotly_chart(fig_sc, key="fr_drill_scatter", use_container_width=True)
                _explain_box(
                    "차트 설명 — Feature × Forward Return Scatter",
                    "각 점이 한 시점의 (feature 값, 그 시점 이후 수익률). "
                    "<b>점들이 우상향이면 TS IC > 0</b> (feature가 클수록 미래 수익률이 높음). "
                    "회귀선의 기울기가 가파를수록 강한 관계, 점들이 흩어져 있으면 약한 관계.",
                )

    # ── Sector × horizon IC heatmap ───────────────────────────────────────────
    if not lag_decay_df.empty and "sector_gics" in panel.columns:
        with st.expander("📊 Sector × Horizon IC Heatmap", expanded=False):
            sectors = sorted(panel["sector_gics"].dropna().unique().tolist())
            horizons = list(HORIZONS.keys())
            mat = []
            for sec in sectors:
                sub = panel[panel["sector_gics"] == sec]
                row = []
                for h in horizons:
                    target = {"raw": f"fwd_{h}", "excess": f"fwd_excess_{h}",
                              "sector_rel": f"fwd_sector_rel_{h}"}[sel_kind]
                    if target not in sub.columns:
                        row.append(float("nan")); continue
                    r = cross_sectional_rank_ic(sub, feat_col, target, min_stocks=3)
                    row.append(r["ic_mean"])
                mat.append(row)
            if mat and any(any(np.isfinite(v) for v in r) for r in mat):
                fig_hm = go.Figure(go.Heatmap(
                    z=mat, x=horizons, y=sectors,
                    colorscale=[[0, "#b91c1c"], [0.5, "#f8fafc"], [1, "#15803d"]],
                    zmin=-0.15, zmax=0.15,
                    text=[[f"{v:+.3f}" if np.isfinite(v) else "" for v in row] for row in mat],
                    texttemplate="%{text}",
                    textfont=dict(size=11, family="SF Mono, Menlo, monospace"),
                    colorbar=dict(title="IC̄", thickness=12),
                ))
                fig_hm.update_layout(
                    title=dict(text=f"Sector × Horizon IC ({_TARGET_KIND_LABEL[sel_kind]})",
                               font=dict(size=13)),
                    height=max(280, len(sectors) * 32 + 80),
                    plot_bgcolor="#fff",
                    margin=dict(t=40, b=30, l=10, r=10),
                )
                st.plotly_chart(fig_hm, key="fr_sector_hm", use_container_width=True)
                _explain_box(
                    "의미",
                    "행=섹터, 열=horizon. 셀=그 섹터 내부의 cross-sectional Rank IC. "
                    "특정 섹터에서만 강한 신호가 나오면 universe 전체로는 못 쓰지만 "
                    "섹터-specific 전략으로 분리 가능.",
                    color="#94a3b8",
                )

    # ── Per-stock TS IC (진단용) ─────────────────────────────────────────────
    with st.expander("🔍 Per-stock TS IC — 진단용", expanded=False):
        ts = time_series_ic(panel, feat_col, target_col)
        if not ts.empty:
            ts_show = ts.head(50).copy()
            ts_show["ts_ic"]   = ts_show["ts_ic"].round(3)
            ts_show["p_value"] = ts_show["p_value"].round(3)
            st.dataframe(ts_show, hide_index=True, use_container_width=True, height=380)
            _explain_box(
                "의미",
                "각 회사의 시계열 상관 (TS IC) — Cross-sectional IC와 별개의 진단 지표. "
                "CS IC는 약한데 TS IC가 강한 회사가 많으면 그 회사들이 universe 평균과 "
                "역방향으로 움직이는 idiosyncratic 신호일 가능성. "
                "factor 검증의 메인 지표는 위쪽 CS IC.",
                color="#94a3b8",
            )

    # ── Sector master table ──────────────────────────────────────────────────
    with st.expander(f"🗂 Sector Master — 종목 {len(sector_master)}개", expanded=False):
        if not sector_master.empty:
            st.dataframe(
                sector_master[["stock_code", "name", "market", "market_cap",
                              "induty_code", "sector_gics", "source"]],
                hide_index=True, use_container_width=True,
            )

    # ── 주가 fetch 실패 진단 ─────────────────────────────────────────────────
    fetch_failures = result.get("_fetch_failures", [])
    n_fetched = result.get("_n_fetched", 0)
    n_failed  = result.get("_n_failed", 0)
    if fetch_failures:
        with st.expander(
            f"⚠️ 주가 fetch 실패 종목 ({n_failed}/{n_fetched + n_failed}개) — 원인",
            expanded=(n_fetched == 0),
        ):
            st.dataframe(
                pd.DataFrame(fetch_failures),
                hide_index=True, use_container_width=True,
            )
            st.caption(
                "**가능한 원인**: 상장폐지/미상장 (yfinance에 데이터 없음), "
                "ISIN→6자리 변환 실패, KOSDAQ→KOSPI 잘못된 매핑, "
                "yfinance 일시적 rate limit, 네트워크 차단(VPN/방화벽). "
                "다시 실행하면 캐시된 성공 종목은 즉시 반환되고 실패 종목만 재시도합니다."
            )
