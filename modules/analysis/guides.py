"""
Contextual guides for every step and analysis module.
Call render_guide(key) at the top of each step/render function.
"""
import streamlit as st

# ── shared style helpers ───────────────────────────────────────────────────────

def _term(name: str, definition: str):
    st.markdown(
        f"<div style='margin:4px 0;font-size:13px'>"
        f"<span style='font-weight:600;color:#1e40af'>{name}</span>"
        f"<span style='color:#6b7280'> — </span>{definition}</div>",
        unsafe_allow_html=True,
    )


def _section(title: str, body: str):
    st.markdown(f"**{title}**")
    st.markdown(f"<div style='font-size:13px;color:#374151;line-height:1.6'>{body}</div>",
                unsafe_allow_html=True)
    st.write("")


def _tip(text: str):
    st.markdown(
        f"<div style='background:#eff6ff;border-left:3px solid #3b82f6;padding:8px 12px;"
        f"border-radius:0 6px 6px 0;font-size:12px;color:#1e40af;margin:6px 0'>"
        f"💡 {text}</div>",
        unsafe_allow_html=True,
    )


def _warn(text: str):
    st.markdown(
        f"<div style='background:#fffbeb;border-left:3px solid #f59e0b;padding:8px 12px;"
        f"border-radius:0 6px 6px 0;font-size:12px;color:#92400e;margin:6px 0'>"
        f"⚠️ {text}</div>",
        unsafe_allow_html=True,
    )


# ── render entry point ─────────────────────────────────────────────────────────

def render_guide(key: str, expanded: bool = False):
    _RENDERERS = {
        "step1":            _guide_step1,
        "step2":            _guide_step2,
        "step3":            _guide_step3,
        "step4":            _guide_step4,
        "growth":           _guide_growth,
        "demand":           _guide_demand,
        "anomaly":          _guide_anomaly,
        "brand":            _guide_brand,
        "sku":              _guide_sku,
        "category":         _guide_category,
        "market_signal":    _guide_market_signal,
        "earnings_intel":   _guide_earnings,
        "alpha_validation": _guide_alpha,
    }
    fn = _RENDERERS.get(key)
    if fn is None:
        return
    label = _TITLES.get(key, "분석 가이드")
    with st.expander(f"📖 {label}", expanded=expanded):
        fn()


_TITLES = {
    "step1":            "데이터 업로드 가이드",
    "step2":            "Schema Intelligence 가이드",
    "step3":            "데이터 검증 가이드",
    "step4":            "분석 설정 가이드",
    "growth":           "Growth Analytics 가이드",
    "demand":           "Demand Intelligence 가이드",
    "anomaly":          "Anomaly Detection 가이드",
    "brand":            "Brand Intelligence 가이드",
    "sku":              "SKU Intelligence 가이드",
    "category":         "Category Intelligence 가이드",
    "market_signal":    "Market Signal 가이드",
    "earnings_intel":   "Earnings Intelligence 가이드",
    "alpha_validation": "Alpha Validation 가이드",
}


# ══════════════════════════════════════════════════════════════════════════════
# STEP GUIDES
# ══════════════════════════════════════════════════════════════════════════════

def _guide_step1():
    c1, c2 = st.columns(2)
    with c1:
        _section("이 단계의 목적",
            "원천 POS / 거래 데이터를 업로드합니다. 업로드된 데이터는 이후 모든 분석의 기초가 됩니다.")
        st.markdown("**지원 형식**")
        st.markdown("""
- `xlsx` — Excel 파일 (단일 시트 권장)
- `csv` — UTF-8 / EUC-KR 인코딩
""")
        _tip("행 수가 10만 건을 넘으면 처리 시간이 늘어납니다. 필요 시 기간 필터링 후 업로드하세요.")
    with c2:
        _section("권장 데이터 구조",
            "한 행이 하나의 거래 또는 하나의 제품·날짜 조합인 '롱 포맷'이 최적입니다.")
        st.markdown("""
| 컬럼 예시 | 역할 |
|-----------|------|
| `날짜` / `거래일시` | 거래 일자 |
| `매출금액` / `판매액` | 매출 |
| `회사명` / `브랜드` | 회사 구분 |
| `상품코드` / `SKU` | 상품 구분 |
| `종목코드` / `ISIN` | 주식 연동 |
""")
        _warn("같은 날짜·상품이 여러 행으로 나뉜 경우에도 시스템이 자동 집계합니다.")


def _guide_step2():
    c1, c2 = st.columns(2)
    with c1:
        _section("이 단계의 목적",
            "각 컬럼이 어떤 의미를 갖는지(역할) 지정합니다. "
            "시스템이 컬럼명을 기반으로 역할을 자동 추론하지만, "
            "반드시 확인 후 필요하면 직접 수정하세요.")
        st.markdown("**필수 역할 (최소 조건)**")
        st.markdown("""
- **transaction_date** — 분석의 시간축. 없으면 어떤 분석도 불가
- **sales_amount** — 모든 매출 기반 지표의 기준값
""")
    with c2:
        _section("역할별 설명", "")
        roles = [
            ("transaction_date",  "거래 발생 일자 (YYYY-MM-DD 또는 YYYYMMDD)"),
            ("sales_amount",      "거래금액 / 매출액 (숫자)"),
            ("company_name",      "회사 · 브랜드명. 회사별 비교 분석에 필요"),
            ("product_name",      "상품명. SKU / 카테고리 분석에 필요"),
            ("product_code",      "상품 고유 코드 (SKU ID)"),
            ("category",          "상품 카테고리. 카테고리 분석에 필요"),
            ("quantity",          "판매 수량. ATV 계산에 활용"),
            ("stock_code",        "종목코드 또는 ISIN. Market Signal 연동에 필요"),
            ("security_code",     "stock_code 대체 인식 (ISIN 포함)"),
        ]
        for r, d in roles:
            _term(r, d)
    _tip("역할 중복 지정은 불가합니다. 한 컬럼에 하나의 역할만 부여하세요.")


def _guide_step3():
    c1, c2 = st.columns(2)
    with c1:
        _section("이 단계의 목적",
            "업로드된 데이터의 품질을 점검합니다. "
            "결측값 · 이상 날짜 · 음수 매출 등을 감지하고 "
            "분석 가능 여부를 판단합니다.")
        st.markdown("**품질 점수(Quality Score) 기준**")
        st.markdown("""
| 점수 | 의미 |
|------|------|
| 80–100 | 양호. 전체 분석 가능 |
| 60–79 | 일부 경고. 분석은 가능하나 결과 해석 주의 |
| 0–59 | 품질 낮음. 데이터 정제 후 재업로드 권장 |
""")
    with c2:
        _section("점검 항목", "")
        checks = [
            ("날짜 파싱 성공률", "인식 가능한 날짜 형식으로 변환된 비율"),
            ("매출 결측률",      "sales_amount 컬럼의 null/빈값 비율"),
            ("음수 매출 비율",   "매출액이 음수인 행의 비율 (환불 포함 가능)"),
            ("기간 커버리지",    "데이터의 최소 날짜 ~ 최대 날짜 범위"),
            ("중복 행 비율",     "날짜+상품+회사가 동일한 중복 행 비율"),
        ]
        for name, desc in checks:
            _term(name, desc)
    _warn("음수 매출은 반품 처리일 수 있습니다. 제거 여부는 분석 목적에 따라 결정하세요.")


def _guide_step4():
    c1, c2 = st.columns(2)
    with c1:
        _section("이 단계의 목적",
            "어떤 분석을 실행할지 선택하고 파라미터를 설정합니다. "
            "시스템이 역할 매핑 결과를 기반으로 각 모듈의 실행 가능 여부를 자동 판단합니다.")
        st.markdown("**실행 가능 상태**")
        st.markdown("""
| 상태 | 의미 |
|------|------|
| 🟢 완전 실행 | 필수·선택 역할 모두 충족 |
| 🟡 제한 실행 | 필수 역할만 있음. 일부 기능 제한 |
| 🔴 실행 불가 | 필수 역할 없음. 결과가 failed로 반환됨 |
""")
    with c2:
        _section("모듈 분류", "")
        st.markdown("**Intelligence Hub** — 소비 데이터 자체 분석")
        hub = [
            ("Growth Analytics",    "매출 성장률 (MoM · YoY · CAGR) 분석"),
            ("Demand Intelligence", "거래량 · 객단가 기반 수요 신호 분석"),
            ("Brand Intelligence",  "브랜드별 점유율 및 추이 분석"),
            ("SKU Intelligence",    "상품 단위 매출 분포 및 집중도 분석"),
            ("Category Intelligence","카테고리별 성과 및 구조 분석"),
        ]
        for name, desc in hub:
            _term(name, desc)
        st.write("")
        st.markdown("**Signal Layer** — 외부 시장 데이터 연동 분석")
        sig = [
            ("Anomaly Detection",     "시계열 이상 패턴 탐지"),
            ("Market Signal",         "POS 매출 → 주가 선행 신호 분석"),
            ("Earnings Intelligence", "DART 공시 실적과 POS 데이터 비교"),
            ("Alpha Validation",      "전체 모듈 결과를 종합한 투자 신호 점수"),
        ]
        for name, desc in sig:
            _term(name, desc)


# ══════════════════════════════════════════════════════════════════════════════
# MODULE GUIDES
# ══════════════════════════════════════════════════════════════════════════════

def _guide_growth():
    c1, c2 = st.columns(2)
    with c1:
        _section("분석 목적",
            "월별·분기별 매출의 성장 속도와 방향성을 측정합니다. "
            "소비재·리테일 데이터에서 가장 기본이 되는 분석입니다.")
        _section("핵심 지표", "")
        terms = [
            ("MoM (Month-over-Month)",
             "전월 대비 성장률. 단기 모멘텀 측정. 계절성에 민감하므로 단독 해석 주의"),
            ("YoY (Year-over-Year)",
             "전년 동월 대비 성장률. 계절성을 자동 제거하므로 소비재에서 가장 신뢰도 높은 지표"),
            ("CAGR (Compound Annual Growth Rate)",
             "연평균 복합성장률. 전체 기간의 성장 속도를 단일 값으로 표현"),
            ("3M 이동평균",
             "최근 3개월 평균. 단기 노이즈를 제거하고 추세를 명확히 시각화"),
            ("12M 이동평균",
             "최근 12개월 평균. 장기 추세선. 이 선 위에 있으면 성장 국면"),
        ]
        for name, desc in terms:
            _term(name, desc)
    with c2:
        _section("결과 해석 방법", "")
        st.markdown("""
**YoY Growth 탭**
- 막대가 초록: 전년 대비 성장 / 빨간: 역성장
- 3M 평활선이 우상향 → 모멘텀 지속

**Raw Sales 탭**
- 12M 이동평균이 상승하는 구간 = 구조적 성장
- CAGR이 업종 평균 대비 높으면 outperformer

**MoM Momentum 탭**
- ΔMoM(가속도) > 0 연속 → 성장 가속화 국면
- ΔMoM < 0 연속 → 성장 둔화. 추세 전환 가능성 점검 필요
""")
        _tip("YoY > 0이더라도 ΔMoM이 2개월 연속 음수면 성장 속도가 꺾이고 있다는 신호입니다.")
        _warn("데이터 기간이 13개월 미만이면 YoY 계산이 불가합니다.")


def _guide_demand():
    c1, c2 = st.columns(2)
    with c1:
        _section("분석 목적",
            "단순 매출액이 아닌 '거래 건수'와 '객단가' 두 축으로 수요를 분해합니다. "
            "매출 증가가 '더 많이 사는 것'인지 '더 비싸게 사는 것'인지 구분할 수 있습니다.")
        _section("핵심 지표", "")
        terms = [
            ("Demand Score",
             "거래량(Transaction Count)과 ATV를 결합한 0–100 수요 강도 지수. "
             "50 기준으로 초과 시 수요 양호, 미만 시 수요 약화"),
            ("ATV (Average Transaction Value)",
             "거래당 평균 금액. 객단가 상승 = 소비자의 지불 의향 증가 또는 고단가 상품 판매 증가"),
            ("Transaction Count",
             "단위 기간(월) 내 발생한 거래 건수. 소비 빈도를 나타냄"),
            ("Volume × Value Decomposition",
             "매출 변화를 '거래량 효과'와 '단가 효과'로 분리. "
             "둘 다 오르면 가장 강한 수요 신호"),
        ]
        for name, desc in terms:
            _term(name, desc)
    with c2:
        _section("결과 해석 방법", "")
        st.markdown("""
| 거래량 | ATV | 의미 |
|--------|-----|------|
| ↑ | ↑ | 🟢 강한 수요 — 구매량·단가 동시 상승 |
| ↑ | ↓ | 🟡 볼륨 성장 — 저단가 상품 확대 또는 할인 판매 |
| ↓ | ↑ | 🟡 프리미엄화 — 구매 건수 줄고 단가 상승 |
| ↓ | ↓ | 🔴 수요 약화 — 전반적 소비 감소 |
""")
        _tip("거래량이 떨어지더라도 ATV가 급등하면 프리미엄 전환으로 해석할 수 있습니다. "
             "카테고리 맥락을 반드시 함께 확인하세요.")


def _guide_anomaly():
    c1, c2 = st.columns(2)
    with c1:
        _section("분석 목적",
            "정상 패턴에서 벗어난 시점을 자동 탐지합니다. "
            "프로모션 효과, 공급망 이슈, 외부 충격 등 비정상적 매출 변동을 찾아냅니다.")
        _section("탐지 방법", "")
        terms = [
            ("Z-Score",
             "평균과 표준편차를 이용해 얼마나 벗어났는지 측정. "
             "|Z| > 2 이면 이상치로 판정 (약 5% 수준). 분포가 정규에 가까울수록 정확"),
            ("IQR (Interquartile Range)",
             "중앙 50% 데이터 범위의 1.5배를 벗어난 값을 이상치로 판정. "
             "Z-Score보다 극단값에 강건(Robust)"),
            ("Rolling Baseline",
             "12개월 이동평균을 기준선으로 하고 ±2σ 범위를 벗어난 시점 탐지. "
             "추세 변화가 있는 데이터에 적합"),
            ("이상치율 (Anomaly Rate)",
             "전체 기간 중 이상치로 판정된 월의 비율. "
             "낮을수록 안정적 소비 패턴"),
        ]
        for name, desc in terms:
            _term(name, desc)
    with c2:
        _section("결과 해석 방법", "")
        st.markdown("""
**양방향 이상치**
- 🔴 상향 이상치: 프로모션, 특수 이벤트, 채널 오픈
- 🔵 하향 이상치: 공급 부족, 경쟁 심화, 데이터 누락 가능성

**Alpha Score에서의 역할**
- Safety Score (0–25점) 구성요소
- 이상치율이 낮을수록 Safety Score 높음
- 안정적 소비 패턴 = 예측 가능성 높음 = 투자 신뢰도 ↑
""")
        _warn("이상치가 많아도 반드시 '나쁜' 데이터가 아닙니다. "
              "판촉 활동이나 시장 확장으로 인한 긍정적 이상치일 수 있습니다.")
        _tip("이상치 탐지는 Safety Score에 반영됩니다. Alpha Validation 탭에서 종합 점수를 확인하세요.")


def _guide_brand():
    c1, c2 = st.columns(2)
    with c1:
        _section("분석 목적",
            "브랜드별 매출 점유율과 성장 추이를 분석합니다. "
            "특정 브랜드가 시장 내에서 지위를 강화하고 있는지, 약화되고 있는지를 파악합니다.")
        _section("핵심 지표", "")
        terms = [
            ("Share of Wallet (SoW)",
             "전체 매출에서 특정 브랜드가 차지하는 비율. 브랜드 경쟁력의 핵심 지표"),
            ("Brand Momentum",
             "브랜드 점유율의 최근 변화 속도. 점유율 자체가 낮더라도 빠르게 오르면 성장 기업"),
            ("Brand Concentration",
             "상위 N개 브랜드의 집중도. 높을수록 과점 시장"),
        ]
        for name, desc in terms:
            _term(name, desc)
    with c2:
        _section("결과 해석 방법", "")
        st.markdown("""
- **점유율 상승 + 매출 성장**: 가장 이상적인 시나리오
- **점유율 유지 + 카테고리 전체 성장**: 시장 성장에 편승
- **점유율 하락 + 매출 성장**: 경쟁 심화. 시장은 크지만 경쟁 불리
- **점유율 하락 + 매출 감소**: 🚨 위험 신호
""")
        _tip("점유율 분석에는 최소 2개 이상의 브랜드 데이터가 필요합니다.")


def _guide_sku():
    c1, c2 = st.columns(2)
    with c1:
        _section("분석 목적",
            "개별 상품(SKU) 단위로 매출 기여도와 집중도를 분석합니다. "
            "어떤 상품이 실제 매출을 이끄는지, 장기 성장주가 무엇인지 파악합니다.")
        _section("핵심 지표", "")
        terms = [
            ("파레토 분석 (80/20 Rule)",
             "상위 20%의 SKU가 전체 매출의 80%를 차지하는 경향. "
             "실제 비율이 이 기준에서 얼마나 벗어나는지 측정"),
            ("SKU Velocity",
             "단위 기간당 판매 빈도 및 금액. 빠를수록 회전율 높은 핵심 SKU"),
            ("Long-tail SKU",
             "매출 기여도는 낮지만 다양성 제공. 과도하게 많으면 재고 비효율 유발"),
            ("SKU 집중도 (HHI)",
             "허핀달-허쉬만 지수. 1에 가까울수록 소수 SKU 의존도 높음. 리스크 지표"),
        ]
        for name, desc in terms:
            _term(name, desc)
    with c2:
        _section("결과 해석 방법", "")
        st.markdown("""
**매출 집중도 분석**
- Core SKU(상위 20%): 수익성 방어의 핵심
- Growth SKU: 매출 증가율 상위, 아직 절대량은 작음
- Tail SKU: 정리 대상 또는 니치 마켓 서비스용

**투자 관점**
- Core SKU 성장 = 기존 제품 경쟁력 확인
- Growth SKU 부상 = 신제품/혁신 성공 가능성
- Tail SKU 급증 = 포트폴리오 복잡도 증가 (운영 리스크)
""")
        _warn("SKU 코드가 없으면 상품명으로 대체 분석하므로 동일 상품이 다른 이름으로 집계될 수 있습니다.")


def _guide_category():
    c1, c2 = st.columns(2)
    with c1:
        _section("분석 목적",
            "카테고리별 매출 구조와 성장 동학을 분석합니다. "
            "회사 전체의 매출 믹스 변화와 카테고리 전략 적합성을 평가합니다.")
        _section("핵심 지표", "")
        terms = [
            ("Category Mix",
             "전체 매출에서 각 카테고리가 차지하는 비율. 믹스 변화가 전략 변화를 반영"),
            ("Category Growth Rate",
             "카테고리별 성장률. 전체 매출 성장률 대비 초과 성장하는 카테고리 발견"),
            ("Category Contribution",
             "전체 성장에서 해당 카테고리가 기여한 퍼센트포인트(pp). "
             "성장률이 높아도 규모가 작으면 기여도는 낮음"),
        ]
        for name, desc in terms:
            _term(name, desc)
    with c2:
        _section("결과 해석 방법", "")
        st.markdown("""
**Mix Shift 분석**
- 고마진 카테고리 비중 증가 → 수익성 개선 가능
- 저마진 카테고리 비중 증가 → 외형 성장이지만 수익 압박

**카테고리 포트폴리오**
- 성장 카테고리 집중 → 공격적 전략
- 다수 카테고리 균형 → 안정적이지만 성장 동력 약할 수 있음
""")
        _tip("카테고리 컬럼이 없으면 이 분석은 실행 불가입니다. Schema Intelligence에서 category 역할을 지정하세요.")


def _guide_market_signal():
    c1, c2 = st.columns(2)
    with c1:
        _section("분석 목적",
            "거래 데이터를 시장 기대 변화의 선행 신호로 평가. "
            "퀀트·헤지펀드 표준 지표(IC, IR, t-stat)로 cross-sectional 신호 품질 측정.")
        _section("표준 지표", "")
        terms = [
            ("IC (Information Coefficient)",
             "회사별 매출 변화와 주가 수익률 간 Pearson r. "
             "전체 평균(IC̄)은 universe 평균 신호 강도"),
            ("σ(IC)",
             "Cross-sectional IC 표준편차. 회사 간 편차"),
            ("IR (Information Ratio)",
             "IC̄ / σ(IC). 신호 일관성. > 0.5 양호, > 1 우수"),
            ("|t|̄",
             "각 회사 r의 t-statistic 평균. > 2면 통계적으로 유의 (p < 0.05)"),
            ("HIT",
             "방향성 일치율 평균. 50% = random, 60%+ = 의미"),
            ("VOL-CONF",
             "거래량 동조 회사 비율 (|vol corr| ≥ 0.3). "
             "참여자가 실제로 반응 중인지 confirmation"),
            ("MED LAG",
             "전체 회사 best_lag의 중간값. 시장 평균 반응 속도"),
            ("Grade A/B/C/D",
             "Score ≥ 70 / 50 / 30 / < 30. "
             "Score = IC*35 + Hit*25 + Persist*20 + Vol*10 + Stability*10"),
        ]
        for name, desc in terms:
            _term(name, desc)
    with c2:
        _section("화면 구성", "")
        st.markdown("""
**Header strip** — UNIVERSE / IC̄ / σ(IC) / IR / |t|̄ / HIT / VOL-CONF / MED LAG / A·B 한 줄

**Main panel (좌)** — IC by Lag decay curve
**Main panel (우)** — Top Long/Short candidates 테이블 (r, lag, t, hit, score)

**Lag Correlation Matrix** — |IC| 상위 12개사 회사×lag heatmap (전체 토글)

**Drill-down (회사 선택)**
- 헤더 strip — TICKER / IC / LAG / |t| / HIT / PERSIST / VOL r / N / SCORE
- 좌: Lag-adjusted overlay (매출을 best_lag만큼 미래 이동, 주가와 정렬)
- 우: Lag scan 막대
- 하단 expander 3개 — Volume / Event Study / Rolling IC
""")
        _section("빈도 가이드", "")
        st.markdown("""
- **Daily** 1–30일 lag — 단기 알파
- **Weekly** 1–12주 lag — 표준·추천
- **Monthly** 1–12개월 lag — 장기 트렌드
""")
        _warn("yfinance OHLCV 6시간 캐싱. 종목코드 부재·오류 시 연동 실패.")
        _tip("number_of_tx 매핑 시 거래건수↔거래량 lag corr 추가. "
             "ISIN(KR…) 또는 6자리 KRX 코드 지원.")


def _guide_earnings():
    c1, c2 = st.columns(2)
    with c1:
        _section("분석 목적",
            "금융감독원 전자공시(DART)의 분기 실적과 POS 데이터를 비교해 "
            "소비 데이터의 선행성을 수치로 검증합니다.")
        _section("DART 연동 원리", "")
        st.markdown("""
1. API Key 입력 → 전체 기업 코드 맵 다운로드
2. 데이터의 회사명/종목코드 → DART 법인코드 자동 매핑
3. 분기 재무제표(매출액) 수집
4. POS 분기 집계 vs DART 분기 매출 비교
""")
        _section("핵심 지표", "")
        terms = [
            ("QoQ (Quarter-over-Quarter)",
             "직전 분기 대비 성장률. 분기 실적의 단기 모멘텀"),
            ("YoY (Year-over-Year)",
             "전년 동 분기 대비 성장률. 계절성 제거"),
            ("Coverage Ratio",
             "POS 매출 / DART 공시 매출. 100% 초과 시 채널 이외 매출 포함, "
             "낮으면 POS가 일부 채널만 커버"),
            ("방향 일치율 (Direction Match Rate)",
             "POS QoQ 방향과 DART QoQ 방향이 같은 분기 비율. "
             "높을수록 POS가 전사 실적을 잘 대표"),
            ("선행일 (Lead Days)",
             "공시 의무일 기준 POS 데이터가 몇 일 먼저 정보를 갖는지. "
             "Q1/Q2/Q3: 45일, Q4: 90일 기준"),
        ]
        for name, desc in terms:
            _term(name, desc)
    with c2:
        _section("결과 해석 방법", "")
        st.markdown("""
**선행 신호 분석 탭**
- 방향 일치율 ≥ 70%: 🟢 POS가 실적 방향을 잘 예측
- 방향 일치율 50–70%: 🟡 부분적 예측력
- 방향 일치율 < 50%: 🔴 POS와 공시 실적 괴리 큼

**산점도 해석**
- 1분면(우상향): POS ↑ & DART ↑ — 일치
- 3분면(좌하향): POS ↓ & DART ↓ — 일치
- 2·4분면: 방향 불일치 — 채널 믹스 차이 또는 데이터 오류 점검 필요

**분기별 색상**
- 🟢 초록: 방향 일치 / 🔴 빨간: 방향 불일치
""")
        _warn("DART 매핑은 회사명 또는 종목코드 기반으로 이루어집니다. "
              "영문·약어 사용 시 매핑 실패율이 높아질 수 있습니다. "
              "수동 매핑(파라미터 입력)으로 보완하세요.")
        _tip("분기 데이터는 최소 4분기(1년) 이상이어야 YoY 계산이 가능합니다.")


def _guide_alpha():
    c1, c2 = st.columns(2)
    with c1:
        _section("분석 목적",
            "모든 분석 모듈의 결과를 하나의 종합 점수(Alpha Score)로 집계합니다. "
            "POS 데이터가 투자 신호로서 얼마나 강력한지를 0–100점으로 표현합니다.")
        _section("Alpha Score 구성 (최대 100점)", "")
        st.markdown("""
| 구성요소 | 최대 | 근거 |
|----------|------|------|
| Growth Score | 40점 | MoM + YoY 성장률 크기 |
| Demand Score | 35점 | Demand Intelligence의 수요 지수 |
| Safety Score | 25점 | 이상치율의 역수 (낮을수록 안정) |
| Market Bonus | 10점 | Market Signal 최적 상관계수 > 0.5 시 추가 |
""")
        _section("신뢰도 배수 (Confidence Multiplier)", "")
        st.markdown("""
핵심 모듈(Growth·Demand·Anomaly)이 몇 개 실행되었는지에 따라 점수 조정:

| 실행 모듈 수 | 신뢰도 배수 |
|-------------|------------|
| 3개 모두 | × 1.00 (100%) |
| 2개 | × 0.82 (82%) |
| 1개 | × 0.65 (65%) |
| 0개 | × 0.50 (50%) |
""")
    with c2:
        _section("신호 등급 해석", "")
        st.markdown("""
| Alpha Score | 등급 | 의미 |
|-------------|------|------|
| 75–100 | 🟢 강한 알파 신호 | 소비 선행 데이터가 긍정적 방향성 제시 |
| 55–74  | 🟡 중립 신호 | 일부 긍정 지표. 추가 검증 권장 |
| 0–54   | 🔴 약한 신호 | 소비 지표 부진 또는 데이터 불충분 |
""")
        _section("탭별 가이드", "")
        st.markdown("""
- **점수 분해**: Growth/Demand/Safety/Bonus 구성요소 막대 + 레이더 차트
- **모듈 상태**: 각 분석 모듈의 실행 결과 요약
- **해석**: 점수 구성요소별 인사이트 텍스트
""")
        _warn("Alpha Score는 POS 데이터 기반의 소비 신호 강도를 측정합니다. "
              "투자 의사결정의 단독 근거로 사용하지 마세요.")
        _tip("Market Signal의 상관계수가 0.5를 넘으면 최대 10점 보너스가 추가됩니다. "
             "Market Signal을 함께 실행하면 Alpha Score 정확도가 높아집니다.")
