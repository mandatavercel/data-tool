"""
kfnb_app/insight/investor_qa.py — 글로벌 투자기관 DDQ(실사 질의응답).

바이사이드(퀀트/펀더멘탈/PM/데이터팀)가 alt-data 도입 전 묻는 질문을 망라하고,
DataSpec + 측정된 사실로 **거짓 없이** 답한다. 모르면 'CONFIRM(확인 필요)'.
선택적으로 LLM(anthropic) 호출로 답변을 정교화하거나 누락 질문을 보강한다(graceful).
"""
from __future__ import annotations

import pandas as pd

CONFIRM = "❓ 확인 필요"


def _g(spec, k, d="unknown"):
    return getattr(spec, k, d) if spec else d


def build_qa(*, spec, profile: dict, coverage: dict, sku_master: pd.DataFrame,
             monthly_panel: pd.DataFrame, pit_panel: pd.DataFrame | None = None,
             alpha_returns: pd.DataFrame | None = None,
             qc_result: dict | None = None, sector_label: str = "K-F&B") -> list[dict]:
    s = profile.get("summary", {})
    amt = _g(spec, "amount_basis"); qty = _g(spec, "qty_basis")
    pop = _g(spec, "population"); cad = _g(spec, "release_cadence")
    rest = _g(spec, "restatement"); chan = _g(spec, "channel_scope", "") or CONFIRM
    lag = _g(spec, "release_lag_days", 15)
    n_listed = int(sku_master.loc[sku_master.get("listed", False) == True,  # noqa: E712
                                  "company_kr"].nunique()) if sku_master is not None and "listed" in sku_master else 0
    has_pit = pit_panel is not None and not pit_panel.empty
    has_alpha = alpha_returns is not None and not alpha_returns.empty
    cov_v = coverage.get("sku_verified_pct", "?")
    cov_listed = coverage.get("listed_coverage_pct", "?")

    def amt_txt():
        return {"vat_incl_retail": "VAT 포함 소매가(소비자 결제금액)",
                "vat_excl": "VAT 제외/정가 기준", "discounted_net": "할인 후 실결제",
                "unknown": CONFIRM}.get(amt, amt)

    def pop_txt():
        return {"census": "전점(census)", "sample": "표본(panel)",
                "multi_channel": "다채널 포함", "unknown": CONFIRM}.get(pop, pop)

    Q = []
    def add(cat, q, a):
        Q.append({"category": cat, "q": q, "a": a})

    # A. 데이터 정체성/정의
    add("Provenance", "이 데이터는 정확히 무엇을 측정하나?",
        f"{chan} 채널의 {sector_label} POS 거래를 바코드(SKU) 단위 일별 집계. "
        f"기간 {s.get('period','?')}, SKU {s.get('skus','?')}개, 상장사 {n_listed}곳 매핑.")
    add("Provenance", "매출(SALE_AMT)의 정의는? VAT/할인/반품 처리?",
        f"{amt_txt()}." + (" 회사 순매출(ex-VAT)과 직접 등가 아님." if amt == "vat_incl_retail" else ""))
    add("Provenance", "수량(SALE_QTY)의 단위는? (ASP 정합성)",
        {"selling_unit": "판매단위(바코드 1=1) → ASP=금액/수량 유효",
         "each": "낱개 환산 → ASP 재정의 필요", "unknown": CONFIRM}.get(qty, qty))
    add("Provenance", "통화/기준 시점 통일?", f"{_g(spec,'currency','KRW')}, 일별 거래일 기준.")

    # B. 커버리지/대표성
    add("Coverage", "모집단은 전점인가 표본인가?", pop_txt()
        + (" — 표본이면 패널 드리프트/대표성 보정 필요." if pop == "sample" else ""))
    add("Coverage", "이 채널이 카테고리 전체 소매에서 차지하는 비중은?",
        "본 데이터로 산출 불가(채널 외 매출 미관측). " + (
            "단일/부분 채널이므로 '시장점유율'이 아니라 '채널 내 점유율'만 가능." if pop != "multi_channel" else CONFIRM))
    add("Coverage", "지역 커버리지?", f"{s.get('regions','?')}개 시도(region_en 제공).")
    add("Coverage", "점포 수/동일점(SSS) 정보가 있나?",
        "없음 — 매출 증가가 수요인지 입점확대인지 분해 불가. (B유형 필드 필요)")
    add("Coverage", "프로모션/정상가 정보가 있나?",
        "없음 — 진성수요 vs 행사효과 분리 불가.")

    # C. PIT/딜리버리
    add("Point-in-Time", "거래일과 데이터 입수일(available_date)의 차이는?",
        f"available_date = 기준일 + {lag}일(명세값). 컬럼으로 제공." + (
            "" if cad != "unknown" else f" 실제 입수주기 {CONFIRM}."))
    add("Point-in-Time", "look-ahead 없이 그 시점 값을 재현할 수 있나?",
        "예 — walk-forward 시그널 패널(인과적)로 제공, 절단 동치 검증됨." if has_pit
        else "현재 패널 없음.")
    add("Point-in-Time", "데이터가 사후 정정(restatement)되나?",
        {"none": "정정 없음(원수치 불변).", "revised": "정정 있음 — 정정이력 보존 필요(현재 미보존).",
         "unknown": CONFIRM}.get(rest, rest))
    add("Point-in-Time", "갱신 주기/지연(latency)은?", f"주기={cad if cad!='unknown' else CONFIRM}, 지연≈{lag}일.")
    add("Point-in-Time", "히스토리 길이는?", f"{s.get('period','?')}.")

    # D. 엔티티 매핑
    add("Mapping", "POS를 상장 종목에 어떻게 연결하나?",
        f"회사→KRX/ISIN/Bloomberg/GICS 마스터 매핑. 상장 매출 커버리지 {cov_listed}%.")
    add("Mapping", "브랜드/SKU 영문 매핑 방법과 신뢰도는?",
        f"상위 매출 브랜드는 사람이 검수한 표준 영문명(verified), 롱테일은 규칙/로마자 "
        f"표준화+confidence flag. 검수완료 매출 {cov_v}%.")
    add("Mapping", "ID는 이름이 바뀌어도 안정적인가?",
        "예 — company=ISIN, brand=brand_id, SKU=barcode(GTIN). alias 제공.")
    add("Mapping", "커버리지를 어떻게 정의하나? (게임 가능성)",
        "매출기준. verified(검수)와 named(로마자 포함)를 분리 표기 — 'named 100%'를 커버리지로 "
        "과대주장하지 않음.")

    # E. 방법론/시그널
    add("Methodology", "ASP/점유율/모멘텀 시그널의 정의는?",
        "ASP=금액/수량, 점유율=매핑 유니버스 내 비중(시장점유율 아님), 모멘텀=YoY/MoM. "
        "전부 PIT 패널로 시계열 제공.")
    add("Methodology", "신제품 히트는 어떻게 탐지하나?",
        "출시 후 회전속도(월평균 매출)+코호트 비교(개선중). 초기 파이프필 편향 주의.")
    add("Methodology", "프로모션/믹스 보정은?",
        "현재 미보정 — ASP 상승이 가격인상인지 믹스변화인지 단정 불가로 표기.")

    # F. 백테스트 타당성
    add("Backtest", "생존편향/패널구성 변화는?",
        f"{CONFIRM} — 점포 유니버스 변동·중단 SKU 보존 여부 확인 필요(동일패널 검증 권장).")
    add("Backtest", "알파 통계는 out-of-sample인가?",
        "현재 알파 리포트는 탐색적(in-sample·best-lag·다중검정 미보정). walk-forward IC/FDR "
        "보정 전엔 가설생성용으로만 사용 권고.")
    add("Backtest", "횡단면 알파가 가능한가?",
        f"상장 {n_listed}곳 — {'가능' if n_listed>=15 else '불가(소수 종목, 개별 관찰 수준)'}.")
    add("Backtest", "용량(capacity)/회전(turnover)/거래비용 분석은?",
        f"{CONFIRM} — 데이터 상품 범위 밖, 전략단에서 평가 필요.")

    # G. 예측 증거
    add("Evidence", "공시매출과의 상관/선행성 증거는?",
        "POS 분기성장 vs 공시매출 lead-lag 분석 제공(DART 연동 시)." if has_alpha
        else f"{CONFIRM} — 주가/DART 연동 실행 시 산출.")
    add("Evidence", "주가 수익률과의 상관(IC)은?",
        "탐색적 상관 제공(주가 연동 시). in-sample 한계 명시." if has_alpha else f"{CONFIRM}.")

    # H. 한계/리스크/컴플라이언스
    add("Risk", "수출 주도 종목의 실적을 반영하나?",
        "아니오 — 국내 POS 한정. 해외매출 비중 큰 종목(예: 삼양식품)은 과소반영.")
    add("Risk", "개인정보(PII)/내부정보(MNPI) 이슈는?",
        "집계 데이터(개인 식별 불가). 단, 특정 기업 매출 정밀추정의 MNPI 오인 소지는 고객 컴플라이언스와 협의.")
    add("Risk", "이 데이터의 가장 큰 약점은?",
        "단일/부분 채널 + 프로모션·점포수 부재 + 수출 사각. → 전체 실적 nowcasting 단독 사용 부적합.")

    # I. 상업/운영
    add("Commercial", "납품 형식/주기/이력 깊이?",
        f"CSV 마스터 번들(daily+masters+QC+docs). 주기 {cad if cad!='unknown' else CONFIRM}, "
        f"이력 {s.get('period','?')}.")
    add("Commercial", "라이선스/재판매/독점 여부?", f"{CONFIRM} — 원천사 계약 조건 확인 필요.")
    return Q


def qa_markdown(qa: list[dict], sector_label: str = "K-F&B",
                llm_section: str = "") -> str:
    L = [f"# {sector_label} — Investor DDQ (글로벌 투자기관 Q&A)", "",
         "> 바이사이드 실사 질문을 망라하고 DataSpec+측정사실로 답합니다. "
         "'확인 필요'는 원천사/운영 확인이 필요한 항목입니다(거짓 없이 표기).", ""]
    cats: dict[str, list] = {}
    for x in qa:
        cats.setdefault(x["category"], []).append(x)
    for cat, items in cats.items():
        L.append(f"## {cat}")
        for x in items:
            L.append(f"**Q. {x['q']}**")
            L.append(f"A. {x['a']}")
            L.append("")
    if llm_section:
        L += ["---", "## (LLM 보강) 추가 질의응답", llm_section]
    return "\n".join(L)


def llm_enhance(qa: list[dict], context: dict, api_key: str,
                model: str = "claude-3-5-sonnet-20241022") -> str:
    """anthropic 호출로 누락 질문 보강 + 답변 정교화. 키/SDK 없으면 ''."""
    if not api_key:
        return ""
    try:
        import anthropic
    except Exception:
        return ""
    try:
        client = anthropic.Anthropic(api_key=api_key)
        base = "\n".join(f"- Q:{x['q']} A:{x['a']}" for x in qa)
        prompt = (
            "당신은 alt-data 벤더의 실사 담당입니다. 아래는 한국 편의점 POS 기반 K-F&B "
            "데이터에 대한 기존 Q&A와 사실(context)입니다. 글로벌 바이사이드가 추가로 "
            "물을 만한 질문 5~8개를 더 만들고, **데이터로 확인되지 않는 내용은 절대 지어내지 말고 "
            "'확인 필요'로** 답하세요. 한국어, Q/A 형식.\n\n"
            f"[CONTEXT]\n{context}\n\n[기존 Q&A]\n{base}")
        msg = client.messages.create(
            model=model, max_tokens=1500,
            messages=[{"role": "user", "content": prompt}])
        return msg.content[0].text
    except Exception as e:
        return f"(LLM 보강 실패: {type(e).__name__})"
