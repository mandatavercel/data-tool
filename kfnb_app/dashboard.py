"""
kfnb_app/dashboard.py — 분석 완료 후 정교한 투자 대시보드.

KPI · 모멘텀 · 점유율/ASP · 테마 · Use-case · (알파) · 투자적합성 진단 탭.
'이 데이터가 글로벌 투자기관에 정말 좋은가/부족한 점'을 솔직히 드러낸다.
Streamlit + plotly (지연 import). 호출: render(st.session_state)
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from kfnb_app.mapping import coverage as cov_mod
from kfnb_app.standardization import tagging
from kfnb_app.insight import assessment as assess

_SCORE_COLOR = {"High": "#16a34a", "Med": "#d97706", "Low": "#dc2626"}


def _kpi(title, value, sub="", accent="#1f3864", bg="#eef2ff"):
    return (f"<div style='background:{bg};border-radius:10px;padding:14px 16px;height:100%'>"
            f"<div style='font-size:11px;color:#64748b;text-transform:uppercase;"
            f"letter-spacing:.5px;font-weight:600;margin-bottom:6px'>{title}</div>"
            f"<div style='font-size:22px;font-weight:700;color:{accent};margin-bottom:2px'>{value}</div>"
            f"<div style='font-size:11px;color:#64748b'>{sub}</div></div>")


def render(SS):
    import plotly.graph_objects as go

    prof = SS.get("profile") or {}
    sku = SS.get("sku_master")
    mp = SS.get("monthly_panel")
    ac = SS.get("annual_company")
    uc = SS.get("use_cases")
    label = SS.get("label", "K-F&B")
    if sku is None or mp is None:
        st.info("먼저 ①~⑥ 단계를 완료하세요 (패널까지 생성 필요).")
        return

    cov = cov_mod.coverage_by_sales(sku)
    a = assess.build_assessment(
        profile=prof, sku_master=sku, monthly_panel=mp, coverage=cov,
        qc_result=SS.get("qc_result", {}), source_name=SS.get("src_name", ""),
        alpha_returns=SS.get("alpha_returns"), pit_panel=SS.get("pit_panel"),
        sector_label=label)
    SS["assessment"] = a

    st.subheader(f"📊 {label} 투자 대시보드")
    s = prof.get("summary", {})
    cols = st.columns(5)
    kpis = [("기간", s.get("period", "—").replace(" ~ ", "~"), f"{a['kpis']['years']:.1f}년"),
            ("SKU", f"{s.get('skus', 0):,}", f"브랜드 {s.get('brands', 0)}"),
            ("상장 매핑(매출)", f"{cov.get('listed_coverage_pct', 0):.0f}%", "POS→티커"),
            ("고신뢰 매핑", f"{cov.get('high_confidence_pct', 0):.0f}%", "verified"),
            ("투자적합 등급", a["grade"].split(" ")[0], f"score {a['score']}/3")]
    for c, (t, v, sub) in zip(cols, kpis):
        c.markdown(_kpi(t, v, sub), unsafe_allow_html=True)
    st.caption("")

    tabs = st.tabs(["📈 모멘텀", "🔀 점유율·ASP", "🌶 테마", "🎯 Use-case",
                    "🔬 알파", "🧭 투자 적합성·한계", "🧾 결론·DDQ"])

    # ── 모멘텀 ────────────────────────────────────────────────────────────
    with tabs[0]:
        if ac is not None and not ac.empty:
            piv = ac.pivot_table(index="yr", columns="company_kr",
                                 values="sales_amt", aggfunc="sum").fillna(0) / 1e8
            fig = go.Figure()
            for co in piv.columns:
                fig.add_bar(name=co, x=[str(y) for y in piv.index], y=piv[co])
            fig.update_layout(barmode="group", height=380, title="연도별 회사 매출 (억원)",
                              legend_orientation="h", margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True, key="dash_ann")
        st.markdown("**브랜드 월별 매출 추세 (상위 6)**")
        top = (mp.groupby("brand_kr")["sales_amt"].sum().sort_values(ascending=False)
               .head(6).index.tolist())
        fig2 = go.Figure()
        for b in top:
            d = mp[mp.brand_kr == b].sort_values("ym")
            fig2.add_scatter(x=d["ym"].astype(str), y=d["sales_amt"] / 1e8,
                             mode="lines", name=b)
        fig2.update_layout(height=380, legend_orientation="h", margin=dict(t=20, b=10),
                           yaxis_title="억원")
        st.plotly_chart(fig2, use_container_width=True, key="dash_brand")

    # ── 점유율·ASP ────────────────────────────────────────────────────────
    with tabs[1]:
        sh = mp.groupby(["ym", "company_kr"])["sales_amt"].sum().reset_index()
        tot = sh.groupby("ym")["sales_amt"].transform("sum")
        sh["share"] = sh["sales_amt"] / tot * 100
        piv = sh.pivot_table(index="ym", columns="company_kr", values="share").fillna(0)
        fig = go.Figure()
        for co in piv.columns:
            fig.add_scatter(x=piv.index.astype(str), y=piv[co], stackgroup="one",
                            name=co, mode="lines")
        fig.update_layout(height=360, title="회사 점유율 추이 (%)",
                          legend_orientation="h", margin=dict(t=40, b=10))
        st.plotly_chart(fig, use_container_width=True, key="dash_share")
        if "asp_won" in mp:
            fig2 = go.Figure()
            for b in top:
                d = mp[mp.brand_kr == b].sort_values("ym")
                fig2.add_scatter(x=d["ym"].astype(str), y=d["asp_won"], mode="lines", name=b)
            fig2.update_layout(height=340, title="브랜드 ASP 추이 (원)",
                               legend_orientation="h", margin=dict(t=40, b=10))
            st.plotly_chart(fig2, use_container_width=True, key="dash_asp")

    # ── 테마 ──────────────────────────────────────────────────────────────
    with tabs[2]:
        cvg = tagging.theme_coverage(sku)
        if cvg:
            items = sorted(((k, v) for k, v in cvg.items()), key=lambda x: -x[1])
            fig = go.Figure(go.Bar(x=[v for _, v in items], y=[k for k, _ in items],
                                   orientation="h", marker_color="#6366f1"))
            fig.update_layout(height=360, title="투자 테마별 매출 비중 (%)",
                              margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True, key="dash_theme")
        if "package_format" in sku:
            pk = sku.groupby("package_format")["sales_amt"].sum().sort_values(ascending=False)
            fig2 = go.Figure(go.Pie(labels=pk.index, values=pk.values, hole=.45))
            fig2.update_layout(height=320, title="포장형태 매출 믹스", margin=dict(t=40, b=10))
            st.plotly_chart(fig2, use_container_width=True, key="dash_pkg")

    # ── Use-case ──────────────────────────────────────────────────────────
    with tabs[3]:
        if uc is not None and not uc.empty:
            color = {"momentum": "#16a34a", "new_hit": "#2563eb",
                     "share_shift": "#a855f7", "asp_premium": "#d97706"}
            fig = go.Figure(go.Bar(
                x=uc["value"], y=uc["entity_kr"] + " · " + uc["usecase_type"],
                orientation="h",
                marker_color=[color.get(t, "#64748b") for t in uc["usecase_type"]]))
            fig.update_layout(height=460, title="발굴된 투자 시그널 (value)",
                              margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True, key="dash_uc")
            st.dataframe(uc[["rank", "usecase_type", "entity_kr", "ticker",
                             "value", "confidence", "thesis_ko"]],
                         hide_index=True, use_container_width=True, height=240)
        else:
            st.info("Use-case 시그널이 없습니다.")

    # ── 알파 (탭에서 직접 실행) ───────────────────────────────────────────
    with tabs[4]:
        import importlib.util as _ilu
        from kfnb_app import config as _cfg
        has_px = _ilu.find_spec("pykrx") is not None or _ilu.find_spec("yfinance") is not None
        st.caption("POS 신호 vs 주가/공시매출 — walk-forward OOS + FDR. 주가는 pykrx(키 불필요), "
                   "공시매출 선행성은 DART 키 필요.")
        if not has_px:
            st.warning("⚠️ 주가 모듈(pykrx) 미설치 — 주가 상관 분석 불가.")
            if st.button("📥 주가 모듈 설치 (pykrx + yfinance)", key="dash_install_px"):
                from kfnb_app.utils.pkg import pip_install
                with st.spinner("pip install pykrx yfinance … (1~3분)"):
                    ok, log = pip_install(["pykrx", "yfinance"])
                if ok:
                    st.success("✅ 설치 완료 — 페이지 새로고침 후 '알파 리서치 실행'")
                else:
                    st.error("설치 실패 — 터미널: `python3 -m pip install --break-system-packages pykrx yfinance`")
                    st.code(log[-800:])
        try:
            _dk = st.secrets.get("DART_API_KEY", "")
        except Exception:
            _dk = ""
        dart_key = st.text_input("DART_API_KEY (공시매출 선행성용, 선택)", value=_dk,
                                 type="password", key="dash_dart")
        if st.button("🔬 알파 리서치 실행", type="primary", key="dash_run_alpha",
                     disabled=not (has_px or dart_key)):
            from kfnb_app.insight import alpha as _al
            from kfnb_app.ingest import prices as _px, disclosures as _dis
            codes = sorted({str(c) for c in mp.get("krx_code", []) if str(c)})
            nmap = {r.krx_code: r.company_en_official
                    for r in _cfg.COMPANY_MAP.values() if r.krx_code}
            with st.spinner("주가/공시 수집 + lead-lag(OOS·FDR) 분석 중…"):
                pxdf, pnote = _px.monthly_prices(codes)
                SS["alpha_prices"] = pxdf      # 시각화용 주가 시계열 보존
                SS["alpha_returns"] = _al.research_vs_returns(mp, pxdf)
                rev, rnote = _dis.quarterly_revenue(codes, dart_key)
                SS["alpha_revenue"] = _al.research_vs_revenue(mp, rev)
                SS["alpha_report"] = _al.alpha_report(SS["alpha_returns"],
                                                      SS["alpha_revenue"], nmap, label)
                SS["_alpha_notes"] = f"주가: {pnote} · 공시: {rnote}"
            st.rerun()
        if SS.get("_alpha_notes"):
            st.caption(SS["_alpha_notes"])
        ar = SS.get("alpha_returns")
        nmap = {r.krx_code: r.company_en_official
                for r in _cfg.COMPANY_MAP.values() if r.krx_code}
        if ar is not None and not ar.empty:
            view = ar.copy()
            view["label"] = (view["krx_code"].map(lambda c: nmap.get(c, c))
                             + " · " + view["signal"])
            # ① in-sample vs OOS 상관 — 과적합 가시화
            fig = go.Figure()
            fig.add_bar(name="in-sample (full)", x=view["label"], y=view["full_corr"],
                        marker_color="#94a3b8")
            fig.add_bar(name="out-of-sample", x=view["label"],
                        y=view["oos_corr"].fillna(0),
                        marker_color=["#16a34a" if s else "#f87171"
                                      for s in view["significant"]])
            fig.update_layout(barmode="group", height=380, margin=dict(t=46, b=80),
                              title="POS→주가 상관: in-sample vs OOS (초록=FDR 유의)",
                              yaxis_title="Spearman corr", legend_orientation="h")
            fig.update_xaxes(tickangle=-35)
            st.plotly_chart(fig, use_container_width=True, key="dash_alpha_bar")
            n_sig = int(view["significant"].sum())
            st.caption(f"FDR(q<0.10) 유의 시그널 **{n_sig}/{len(view)}건**. "
                       "in-sample 대비 OOS가 무너지면(부호 반전·0 수렴) 과적합 신호입니다.")

            # ② 종목·시그널 선택 → lead-lag 프로파일 + POS vs 주가 시계열
            pit = SS.get("pit_panel")
            px = SS.get("alpha_prices")
            if pit is not None and not pit.empty and px is not None and not px.empty:
                from kfnb_app.insight import alpha as _al
                opts = view["label"].tolist()
                sel = st.selectbox("상세 보기 (종목·시그널)", opts, key="dash_alpha_sel")
                row = view[view["label"] == sel].iloc[0]
                code, sgn = row["krx_code"], row["signal"]
                sig = (pit[(pit.krx_code == code) & (pit.signal == sgn)]
                       .set_index("ym")["value"].sort_index())
                ret = px[px.krx_code == code].set_index("ym")["ret"].sort_index()
                cc1, cc2 = st.columns(2)
                with cc1:
                    ll = _al.leadlag(sig, ret, 6)
                    if ll:
                        fig2 = go.Figure(go.Bar(
                            x=[f"+{l}m" for l, _, _ in ll], y=[c for _, c, _ in ll],
                            marker_color="#6366f1"))
                        fig2.update_layout(height=300, title="Lead-lag 프로파일 (corr by 선행개월)",
                                           yaxis_title="corr", margin=dict(t=46, b=10))
                        st.plotly_chart(fig2, use_container_width=True, key="dash_alpha_ll")
                with cc2:
                    fig3 = go.Figure()
                    fig3.add_scatter(x=sig.index.astype(str), y=sig.values,
                                     name=f"POS {sgn}", line=dict(color="#2563eb"))
                    fig3.add_scatter(x=ret.index.astype(str), y=(ret * 100).values,
                                     name="주가 월수익률(%)", yaxis="y2",
                                     line=dict(color="#dc2626"))
                    fig3.update_layout(height=300, title="POS 신호 vs 주가수익률",
                                       margin=dict(t=46, b=10), legend_orientation="h",
                                       yaxis2=dict(overlaying="y", side="right"))
                    st.plotly_chart(fig3, use_container_width=True, key="dash_alpha_ts")

            rv = SS.get("alpha_revenue")
            if rv is not None and not rv.empty:
                st.markdown("**공시매출 선행성 (POS 분기매출 → 공시매출)**")
                rvv = rv.copy()
                rvv["label"] = rvv["krx_code"].map(lambda c: nmap.get(c, c))
                figr = go.Figure(go.Bar(
                    x=rvv["label"], y=rvv["best_lead_m"],
                    marker_color=["#16a34a" if s else "#cbd5e1" for s in rvv["significant"]],
                    text=[f"corr {c:+.2f}" for c in rvv["corr"]]))
                figr.update_layout(height=300, title="POS가 공시매출을 선행하는 개월수 (초록=유의)",
                                   yaxis_title="선행 개월", margin=dict(t=46, b=10))
                st.plotly_chart(figr, use_container_width=True, key="dash_alpha_rev")
                st.dataframe(rv, hide_index=True, use_container_width=True)
            with st.expander("📄 알파 리포트 / 시그널 표"):
                st.markdown(SS.get("alpha_report", ""))
                st.dataframe(ar, hide_index=True, use_container_width=True)
        elif SS.get("_alpha_notes"):
            st.info("주가 데이터를 가져오지 못했거나 유의 시그널이 없습니다. (위 상태 메시지 참고)")

    # ── 투자 적합성·한계 (핵심) ───────────────────────────────────────────
    with tabs[5]:
        st.markdown(f"### 종합 등급: {a['grade']}  ·  score {a['score']}/3.0")
        st.info(a["verdict"])
        st.markdown("#### 스코어카드")
        sc = pd.DataFrame(a["scorecard"])
        def _row_style(r):
            c = _SCORE_COLOR.get(r["score"], "#64748b")
            return [f"background-color:{c}22"] * len(r)
        st.dataframe(sc.rename(columns={"dimension": "항목", "score": "등급",
                     "value": "값", "rationale": "근거"}),
                     hide_index=True, use_container_width=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### ⚠️ 한계 (솔직한 약점)")
            for x in a["limitations"]:
                st.markdown(f"- {x}")
        with c2:
            st.markdown("#### 🚀 institutional-grade 로 가는 길")
            for x in a["recommendations"]:
                st.markdown(f"- {x}")

    # ── 결론 · DDQ (거짓 없는 결론 + 투자기관 Q&A) ─────────────────────────
    with tabs[6]:
        from kfnb_app.insight import conclusion as _concl, investor_qa as _qa
        spec = SS.get("data_spec")
        c = _concl.build_conclusion(
            spec=spec, profile=prof, coverage=cov, sku_master=sku,
            monthly_panel=mp, pit_panel=SS.get("pit_panel"),
            alpha_returns=SS.get("alpha_returns"), sector_label=label)
        st.markdown(f"#### 결론 ({c['backtest_met']}/{c['backtest_total']} 백테스트 조건 충족)")
        st.info(c["verdict"])
        cc1, cc2 = st.columns(2)
        with cc1:
            st.markdown("##### ✅ 말할 수 있는 것")
            for x in c["can_say"]:
                st.markdown(f"- {x}")
            st.markdown("##### ❓ 확인 필요")
            for x in c["unknowns"]:
                st.markdown(f"- {x}")
        with cc2:
            st.markdown("##### ❌ 말할 수 없는 것")
            for x in c["cannot_say"]:
                st.markdown(f"- {x}")
        st.markdown("##### 백테스트 필요충분조건")
        st.dataframe(pd.DataFrame(c["backtest_checks"]).rename(
            columns={"condition": "조건", "status": "상태", "note": "비고"}),
            hide_index=True, use_container_width=True)
        st.markdown("#### 투자기관 DDQ (Q&A)")
        qa = _qa.build_qa(spec=spec, profile=prof, coverage=cov, sku_master=sku,
                          monthly_panel=mp, pit_panel=SS.get("pit_panel"),
                          alpha_returns=SS.get("alpha_returns"),
                          qc_result=SS.get("qc_result", {}), sector_label=label)
        st.dataframe(pd.DataFrame(qa).rename(columns={"category": "분류",
                     "q": "질문", "a": "답변"}), hide_index=True,
                     use_container_width=True, height=400)
