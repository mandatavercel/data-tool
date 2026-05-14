"""
파생 메트릭 (Derived Metrics) — 매핑된 역할 조합으로 자동 계산되는 부가 지표.

활용 패턴:
    from modules.common.derived_metrics import suggest_derived, compute_derived

    # Step 2 — 매핑 완료 후 사용자에게 어떤 분석이 가능한지 미리보기
    hints = suggest_derived(role_map)
    # → ['ARPU (1인당 매출)', '결제 빈도 (1인당 결제 횟수)', ...]

    # 각 분석 모듈 안에서 ad-hoc 파생 컬럼 추가
    df = compute_derived(df, role_map)
    # → df에 'arpu', 'tx_per_user' 등 자동 생성 (해당 컬럼이 있을 때만)

설계 원칙:
    - 새 역할이 추가돼도 자동으로 가능 분석 늘어남
    - 분석 모듈이 derived 컬럼을 자유롭게 활용
    - "거래건수" vs "이용자수" 같은 의미 구분을 분석 단에서 살림
"""
from __future__ import annotations

import pandas as pd


# 파생 메트릭 정의 — (이름, 필요한 역할, 한국어 설명, 계산식 라벨)
# 'requires'는 role_map에 모두 매핑되어 있을 때만 활성
DERIVED_METRICS = [
    {
        "key":         "arpu",
        "name":        "💵 ARPU (1인당 매출)",
        "requires":    ["sales_amount", "active_users"],
        "formula":     "sales_amount / active_users",
        "describes":   "1명의 활성 이용자가 평균적으로 얼마나 결제했나. "
                       "ARPU↑면 기존 고객의 결제력 증가 = 충성도·프리미엄화.",
        "example":     "월매출 920만원 / 이용자 208명 = ARPU 44,231원",
    },
    {
        "key":         "tx_per_user",
        "name":        "🔁 결제 빈도 (1인당 결제 횟수)",
        "requires":    ["sales_count", "active_users"],
        "formula":     "sales_count / active_users",
        "describes":   "1명의 활성 이용자가 평균적으로 몇 번 결제했나. "
                       "↑면 재방문 빈도 증가, ↓면 1회성 사용자 비중 증가.",
        "example":     "거래 547건 / 이용자 208명 = 1인당 2.63회 결제",
    },
    {
        "key":         "atv",
        "name":        "🎯 ATV (객단가 — 1결제당 금액)",
        "requires":    ["sales_amount", "sales_count"],
        "formula":     "sales_amount / sales_count",
        "describes":   "1회 결제당 평균 금액. "
                       "↑면 프리미엄화·번들 효과, ↓면 저가 상품 증가.",
        "example":     "월매출 920만원 / 거래 547건 = ATV 16,815원",
    },
    {
        "key":         "user_growth",
        "name":        "📈 이용자 성장률",
        "requires":    ["active_users", "transaction_date"],
        "formula":     "active_users MoM/QoQ growth",
        "describes":   "활성 이용자 수 성장 추세. "
                       "💡 매출 성장과 비교하면 'driver 분리' 가능: "
                       "매출 +20% & 유저 +20% → 신규 유입 / "
                       "매출 +20% & 유저 +5% → 기존 고객 강화(ARPU↑).",
        "example":     "10월 208명 → 11월 225명 = +8.2% MoM",
    },
    {
        "key":         "penetration_driver",
        "name":        "🔬 성장 동인 분해 (사용자 vs ARPU)",
        "requires":    ["sales_amount", "active_users", "transaction_date"],
        "formula":     "Δ매출 = Δ유저수 × ARPU + 유저수 × ΔARPU",
        "describes":   "매출 성장이 '신규 유입(volume)' 때문인지 "
                       "'기존 고객 강화(price)' 때문인지 정량 분해.",
        "example":     "매출 +20% 중 +15%는 유저 확대, +5%는 ARPU 상승",
    },
]


def suggest_derived(role_map: dict[str, str]) -> list[dict]:
    """role_map(역할→컬럼명) 기준으로 활성화 가능한 파생 메트릭 리스트.

    Returns: list of dicts with name/describes/example/requires/active_now (bool)
    """
    mapped_roles = {r for r, c in role_map.items() if c}
    out = []
    for m in DERIVED_METRICS:
        active = all(req in mapped_roles for req in m["requires"])
        out.append({**m, "active_now": active})
    return out


def compute_derived(df: pd.DataFrame, role_map: dict[str, str]) -> pd.DataFrame:
    """df에 파생 컬럼을 추가해서 반환 (해당 역할이 매핑된 경우만).

    DataFrame 그대로 복사해서 반환 — 원본 안 건드림. 분석 모듈이 df를 받자마자
    이 함수로 한 번 흘려보내면 ARPU 등 파생 컬럼이 자동 추가됨.
    """
    out = df.copy()

    def col(role: str) -> str | None:
        c = role_map.get(role)
        return c if c and c in out.columns else None

    amt = col("sales_amount")
    cnt = col("sales_count")
    usr = col("active_users")

    # ARPU = 매출 / 이용자수
    if amt and usr:
        with pd.option_context("mode.use_inf_as_na", True):
            try:
                out["arpu"] = pd.to_numeric(out[amt], errors="coerce") / \
                              pd.to_numeric(out[usr], errors="coerce")
            except Exception:
                pass

    # 결제 빈도 = 거래건수 / 이용자수
    if cnt and usr:
        try:
            out["tx_per_user"] = pd.to_numeric(out[cnt], errors="coerce") / \
                                  pd.to_numeric(out[usr], errors="coerce")
        except Exception:
            pass

    # ATV = 매출 / 거래건수
    if amt and cnt:
        try:
            out["atv"] = pd.to_numeric(out[amt], errors="coerce") / \
                         pd.to_numeric(out[cnt], errors="coerce")
        except Exception:
            pass

    return out
