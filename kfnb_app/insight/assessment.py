"""
kfnb_app/insight/assessment.py — 글로벌 투자기관 관점의 데이터 적합성 진단.

"이 데이터가 정말 투자기관에 좋은가? 부족한 점은?"을 솔직하게 드러낸다.
아티팩트(프로파일·커버리지·SKU마스터·패널·QC·알파)로부터 스코어카드 +
한계 + 업그레이드 권고 + 종합 등급을 산출한다. 순수 pandas (테스트 가능).
"""
from __future__ import annotations

import pandas as pd

_SCORE = {"High": 3, "Med": 2, "Low": 1}
_GRADE = [(2.5, "B+ — 조기신호용 유효 (Useful early-read)"),
          (2.0, "B0 — 보조지표 수준 (Supplementary)"),
          (1.5, "C+ — 제한적 (Limited)"),
          (0.0, "C — 단일채널 참고용 (Reference only)")]


def _dim(name, score, value, why):
    return {"dimension": name, "score": score, "value": value, "rationale": why}


def build_assessment(*, profile: dict, sku_master: pd.DataFrame,
                     monthly_panel: pd.DataFrame, coverage: dict,
                     qc_result: dict, source_name: str = "",
                     alpha_returns: pd.DataFrame | None = None,
                     pit_panel: pd.DataFrame | None = None,
                     spec=None,
                     sector_label: str = "K-F&B") -> dict:
    s = profile.get("summary", {})
    years = _years(s.get("period", ""))
    pop = getattr(spec, "population", "unknown") if spec else "unknown"
    rest = getattr(spec, "restatement", "unknown") if spec else "unknown"
    qty_basis = getattr(spec, "qty_basis", "unknown") if spec else "unknown"
    # 채널 단일 여부: 명세 우선, 없으면 파일명 폴백
    if pop == "multi_channel":
        single_channel = False
    elif pop in ("census", "sample"):
        single_channel = True
    else:
        single_channel = "CU" in str(source_name).upper() or "편의점" in str(source_name)

    # 편의점 즉시소비 편향 (용기면 vs 봉지면)
    cup_share = _cup_share(sku_master)

    dims = []
    # 1) 히스토리
    dims.append(_dim(
        "히스토리 길이", "High" if years >= 5 else "Med" if years >= 3 else "Low",
        f"{years:.1f}년 일별",
        "백테스트에 충분" if years >= 5 else "다소 짧음"))
    # 2) 입자도 (바코드/SKU)
    bc = profile.get("quality", {}).get("barcode_ok_pct", 0)
    dims.append(_dim(
        "입자도(바코드/SKU)", "High" if bc >= 99 else "Med",
        f"바코드 {bc:.0f}% · SKU {s.get('skus', 0)}",
        "EAN-13 단위 식별 가능 — 강점"))
    # 3) 티커 매핑 (매출기준)
    lc = coverage.get("listed_coverage_pct", 0)
    dims.append(_dim(
        "상장사 매핑(매출%)", "High" if lc >= 95 else "Med" if lc >= 70 else "Low",
        f"{lc:.0f}%",
        "POS→상장사 연결 강함" if lc >= 95 else "비상장 비중 있음"))
    # 4) 채널 폭 (핵심 한계)
    dims.append(_dim(
        "채널 커버리지", "Low" if single_channel else "Med",
        "편의점 단일체인(CU)" if single_channel else "다채널",
        "마트·온라인·슈퍼 미포함 → 전체 소매 대표성 제한"))
    # 5) 수요 vs 프로모션 분리
    dims.append(_dim(
        "프로모션/가격 분리", "Low", "행사·정상가 필드 없음",
        "진성수요와 행사효과 분리 불가 (B유형 필드 필요)"))
    # 6) 동일점 vs 입점확대
    dims.append(_dim(
        "동일점(SSS) 분해", "Low", "판매 점포수 없음",
        "매출 증가가 수요인지 입점확대인지 구분 불가"))
    # 7) 수출 모멘텀 포착
    dims.append(_dim(
        "수출 모멘텀 포착", "Low", "국내 POS only",
        "해외매출 비중 큰 종목(예: 삼양식품 ~80% 수출)의 실적을 과소반영"))
    # 8) 패널 대표성
    dims.append(_dim(
        "패널 대표성", "Med", f"용기면 매출비중 {cup_share:.0f}%",
        "편의점 즉시소비(컵) 편향 — 가정용(봉지/멀티팩)은 마트 데이터 보완 필요"))
    # 9) 예측력 검증
    validated = alpha_returns is not None and not alpha_returns.empty
    dims.append(_dim(
        "예측력 검증", "Med" if validated else "Low",
        "주가/공시 상관(탐색적) 완료" if validated else "미검증(주가/DART 필요)",
        "walk-forward·다중검정 보정 후에만 알파로 채택"))
    # 10) PIT/백테스트 정합성
    has_pit = pit_panel is not None and not pit_panel.empty
    dims.append(_dim(
        "PIT/백테스트 정합성", "High" if has_pit else "Low",
        "available_date + walk-forward 패널 제공" if has_pit else "없음",
        "데이터 입수시점 명시·인과적 시계열 → look-ahead 없이 백테스트 가능"
        if has_pit else "available_date 부재 → PIT 백테스트 불가"))

    avg = sum(_SCORE[d["score"]] for d in dims) / len(dims)
    grade = next(g for thr, g in _GRADE if avg >= thr)

    limitations = [
        "단일 채널(CU 편의점) — 마트/SSM/온라인 미포함으로 카테고리 전체 점유율·실적 대표성 제한",
        "프로모션·정상가·판매점포수 부재 — 진성수요 vs 행사, 동일점 vs 입점확대 분리 불가",
        "국내 POS 한정 — 수출 주도 종목(삼양식품 등)의 해외 모멘텀 미포착",
        "편의점 특성상 용기면·1인 소비 편향 (가정용 봉지/멀티팩 과소)",
        f"롱테일 SKU는 규칙 기반 표준화(needs_review {_needs(qc_result)}건) — 상위 매출은 verified",
    ]
    recommendations = [
        "원천사(B유형) 추가 확보: promo_flag·정상가/실판매가·판매점포수 → 진성수요/동일점 지표화",
        "멀티채널 결합: 대형마트·온라인(쿠팡 등)·SSM POS로 전체 소매 커버리지 확대",
        "수출/관세 데이터 결합으로 해외 모멘텀 보완 (특히 삼양식품)",
        "DART 공시매출·주가와 lead-lag 검증으로 nowcasting 신뢰도(IC) 제시",
        "패널 대표성 가중치(채널·연령) 보정 및 커버리지 메타데이터 동봉",
    ]
    verdict = (
        f"{sector_label} CU POS 데이터는 **국내 편의점 채널의 라면/주류 브랜드 모멘텀을 "
        "조기에 읽는 alt-data 로는 유효**합니다(바코드 입자도·6년 히스토리·상장사 매핑이 강점). "
        "다만 **단일 채널·프로모션 미보정·수출 사각** 때문에 현재 단독으로는 전체 실적 "
        "nowcasting 의 institutional-grade 라 보긴 어렵습니다. 위 업그레이드(B유형·멀티채널·"
        "수출데이터·검증)를 더하면 등급이 크게 상승합니다.")

    return {
        "grade": grade, "score": round(avg, 2), "scorecard": dims,
        "limitations": limitations, "recommendations": recommendations,
        "verdict": verdict,
        "kpis": {"years": years, "skus": s.get("skus", 0),
                 "listed_cov": lc, "high_conf": coverage.get("high_confidence_pct", 0),
                 "single_channel": single_channel},
    }


def assessment_markdown(a: dict, sector_label: str = "K-F&B") -> str:
    lines = [f"# {sector_label} — Investor Readiness Assessment", "",
             f"**Overall grade: {a['grade']}** (score {a['score']}/3.0)", "",
             a["verdict"], "", "## Scorecard"]
    lines.append("| Dimension | Score | Value | Note |")
    lines.append("|---|---|---|---|")
    for d in a["scorecard"]:
        lines.append(f"| {d['dimension']} | {d['score']} | {d['value']} | {d['rationale']} |")
    lines += ["", "## Known Limitations"]
    lines += [f"- {x}" for x in a["limitations"]]
    lines += ["", "## What would make it institutional-grade"]
    lines += [f"- {x}" for x in a["recommendations"]]
    return "\n".join(lines)


# ── helpers ─────────────────────────────────────────────────────────────────
def _years(period: str) -> float:
    try:
        a, b = period.split(" ~ ")
        ya, yb = int(a[:4]), int(b[:4])
        ma, mb = int(a[5:7]), int(b[5:7])
        return (yb * 12 + mb - ya * 12 - ma) / 12
    except Exception:
        return 0.0


def _cup_share(sku_master: pd.DataFrame) -> float:
    if "package_format" not in sku_master or "sales_amt" not in sku_master:
        return 0.0
    amt = pd.to_numeric(sku_master["sales_amt"], errors="coerce")
    tot = amt.sum()
    if not tot:
        return 0.0
    cup = amt[sku_master["package_format"].astype(str).str.contains("Cup|Bowl|Can", na=False)].sum()
    return cup / tot * 100


def _needs(qc_result: dict) -> int:
    t = (qc_result or {}).get("tables", {})
    um = t.get("unmapped_items")
    return len(um) if um is not None else 0
