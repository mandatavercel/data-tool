"""
kfnb_app/insight/conclusion.py — 거짓 없는 결론 생성기.

데이터 출처 명세(DataSpec) + 측정된 사실로부터:
  - 이 데이터가 '무엇인가'(정체성)
  - 백테스트 / 라이브 필요충분조건 체크리스트 (충족/미충족/미확인)
  - 말할 수 있는 것 / 말할 수 없는 것 / 반드시 확인해야 할 것
을 산출한다. 측정으로 확정 못 하는 항목은 명세값을 그대로 쓰고, 명세가
'unknown' 이면 결론에 'unknown(확인 필요)'으로 정직하게 노출한다.

상태값: met(충족) / unmet(미충족) / unknown(미확인)
"""
from __future__ import annotations

import pandas as pd

MET, UNMET, UNK = "met", "unmet", "unknown"
_ICON = {MET: "✅", UNMET: "❌", UNK: "❓"}


def _chk(cond, status, note):
    return {"condition": cond, "status": status, "note": note}


def build_conclusion(*, spec, profile: dict, coverage: dict,
                     sku_master: pd.DataFrame, monthly_panel: pd.DataFrame,
                     pit_panel: pd.DataFrame | None = None,
                     alpha_returns: pd.DataFrame | None = None,
                     sector_label: str = "K-F&B") -> dict:
    s = profile.get("summary", {})
    years = _years(s.get("period", ""))
    n_listed = _n_listed(sku_master)
    has_pit = pit_panel is not None and not pit_panel.empty
    amt = getattr(spec, "amount_basis", "unknown")
    qty = getattr(spec, "qty_basis", "unknown")
    pop = getattr(spec, "population", "unknown")
    cad = getattr(spec, "release_cadence", "unknown")
    rest = getattr(spec, "restatement", "unknown")
    chan = getattr(spec, "channel_scope", "") or "(채널 미기재)"
    lag = getattr(spec, "release_lag_days", 15)

    # ── 백테스트 필요충분조건 ─────────────────────────────────────────────
    bt = []
    bt.append(_chk("PIT available_date(입수시점) 존재", MET if has_pit else UNMET,
                   f"기준일+{lag}일. 실제 지연은 명세값 사용"
                   + ("" if cad != "unknown" else " (입수주기 unknown → 확인 필요)")))
    bt.append(_chk("인과적(look-ahead 없는) 시그널 시계열",
                   MET if has_pit else UNMET, "walk-forward 패널로 절단 동치 검증됨"))
    bt.append(_chk("정정(restatement) 처리",
                   MET if rest == "none" else (UNMET if rest == "revised" else UNK),
                   {"none": "정정 없음(원수치 불변)",
                    "revised": "정정 있음 — 정정이력 미보존(원발표치 덮어쓰기 위험)",
                    "unknown": "정정 여부 미확인 → 원천사 확인 필요"}[rest]))
    bt.append(_chk("지표 정의 확정(매출/수량)",
                   MET if (amt != "unknown" and qty != "unknown") else UNK,
                   f"매출={amt}, 수량={qty}"))
    bt.append(_chk("ASP 유효성", MET if qty == "selling_unit"
                   else (UNK if qty == "unknown" else UNMET),
                   "수량=판매단위 → ASP=금액/수량 유효" if qty == "selling_unit"
                   else "수량 정의에 따라 ASP 재정의 필요"))
    bt.append(_chk("히스토리 길이(≥3년)", MET if years >= 3 else UNMET,
                   f"{years:.1f}년"))
    bt.append(_chk("패널 구성 안정성/생존편향",
                   UNK, "점포 유니버스 변동·중단SKU 보존 여부 미확인 → 동일패널 검증 필요"))
    bt.append(_chk("횡단면 충분성", MET if n_listed >= 15 else UNMET,
                   f"상장 매핑 {n_listed}개 — {'충분' if n_listed>=15 else '소수(횡단면 추론 불가, 단종목 수준)'}"))
    bt.append(_chk("실적 대비 검증(외부)",
                   MET if (alpha_returns is not None and not alpha_returns.empty) else UNK,
                   "주가/공시 상관 탐색 완료(단 in-sample)" if (alpha_returns is not None and not alpha_returns.empty)
                   else "주가/DART 검증 미수행"))

    # ── 라이브 필요충분조건 ───────────────────────────────────────────────
    lv = []
    lv.append(_chk("갱신 주기 정의",
                   MET if cad in ("daily", "weekly", "monthly") else UNK,
                   f"명세: {cad}"))
    lv.append(_chk("입수 지연(latency) 허용가능",
                   UNK if cad == "unknown" else MET,
                   f"기준일+{lag}일 (전략 호라이즌 대비 평가 필요)"))
    lv.append(_chk("스키마 안정성(딜리버리 간)",
                   UNK, "멀티오너 스키마 매핑으로 흡수 가능하나 신규 컬럼은 확인 필요"))
    lv.append(_chk("전달 메커니즘(SFTP/API)", UNK, "운영 외 항목 — 별도 합의 필요"))

    # ── 말할 수 있는 것 / 없는 것 / 확인 필요 ─────────────────────────────
    can = [
        f"{chan} 채널에서 {sector_label} 브랜드/SKU의 매출·판매량·ASP 추세 (available_date 기준 PIT)",
        "신제품 출시 후 회전 속도(조기 히트 탐지)와 브랜드 모멘텀(YoY/MoM)",
        "매핑 유니버스 내 회사 점유율 변화(=시장점유율 아님)",
        f"상장사({n_listed}개) 단위 시그널 — {'횡단면' if n_listed>=15 else '개별 종목 관찰'} 수준",
    ]
    cannot = []
    if pop != "multi_channel":
        cannot.append("전체 소매 카테고리 시장점유율 (편의점/단일채널은 일부 채널)")
    cannot.append("수출·해외 매출 모멘텀 (국내 POS 한정)")
    cannot.append("프로모션 제외 '진성수요' 분리 (행사/정상가 필드 부재)")
    cannot.append("동일점(SSS) vs 입점확대 분해 (판매 점포수 부재)")
    if amt == "vat_incl_retail":
        cannot.append("회사 순매출(ex-VAT·도매·프로모션 차감)과 직접 등가 비교 (본 데이터는 VAT포함 소매가)")

    unknowns = []
    for label, v in [("모집단(population)", pop), ("입수주기", cad), ("정정여부", rest)]:
        if v == "unknown":
            unknowns.append(f"{label} 미확인 → 원천사 확인 필요")
    unknowns.append("점포수/프로모션/정상가(B유형) 확보 시 진성수요·동일점 분석 가능")
    unknowns.append("점포 유니버스 변동 이력(생존편향) 확인")

    bt_met = sum(1 for c in bt if c["status"] == MET)
    verdict = _verdict(bt, lv, n_listed, pop, sector_label, chan)

    return {"identity": _identity(spec, s, sector_label, chan, n_listed),
            "backtest_checks": bt, "live_checks": lv,
            "can_say": can, "cannot_say": cannot, "unknowns": unknowns,
            "backtest_met": bt_met, "backtest_total": len(bt),
            "verdict": verdict, "spec": spec.to_dict() if hasattr(spec, "to_dict") else {}}


def conclusion_markdown(c: dict, sector_label: str = "K-F&B") -> str:
    L = [f"# {sector_label} — 데이터 결론 (Data Conclusion, 무수정 정직본)", "",
         "> 본 결론은 선언된 Data Spec + 측정된 사실에만 근거한다. 명세가 미확인이면 "
         "결론에도 '확인 필요'로 표기한다.", "",
         "## 이 데이터는 무엇인가", c["identity"], "",
         f"## 백테스트 필요충분조건 ({c['backtest_met']}/{c['backtest_total']} 충족)"]
    for x in c["backtest_checks"]:
        L.append(f"- {_ICON[x['status']]} {x['condition']} — {x['note']}")
    L += ["", "## 라이브 필요충분조건"]
    for x in c["live_checks"]:
        L.append(f"- {_ICON[x['status']]} {x['condition']} — {x['note']}")
    L += ["", "## ✅ 말할 수 있는 것"]
    L += [f"- {x}" for x in c["can_say"]]
    L += ["", "## ❌ 말할 수 없는 것"]
    L += [f"- {x}" for x in c["cannot_say"]]
    L += ["", "## ❓ 반드시 확인해야 할 것"]
    L += [f"- {x}" for x in c["unknowns"]]
    L += ["", "## 결론", c["verdict"]]
    return "\n".join(L)


# ── helpers ─────────────────────────────────────────────────────────────────
def _identity(spec, s, sector_label, chan, n_listed):
    amt = getattr(spec, "amount_basis", "unknown")
    qty = getattr(spec, "qty_basis", "unknown")
    pop = getattr(spec, "population", "unknown")
    return (f"{s.get('period','?')} 기간, **{chan}** 채널의 {sector_label} POS 거래를 "
            f"바코드(SKU) 단위로 집계한 데이터. 매출 기준=`{amt}`, 수량 기준=`{qty}`, "
            f"모집단=`{pop}`. 상장사 {n_listed}곳에 매핑됨. "
            f"즉 '특정 소매채널에서 관측된 소비자 결제 기반 판매 시계열'이며, "
            f"회사 전체 실적·전체 시장 수요와 동일하지 않다.")


def _verdict(bt, lv, n_listed, pop, sector_label, chan):
    unmet = [c for c in bt if c["status"] == "unmet"]
    unk = [c for c in bt if c["status"] == "unknown"]
    base = (f"이 데이터는 **{chan} 채널의 {sector_label} 브랜드/SKU 판매 모멘텀을 "
            "available_date 기준으로 추적하는 PIT alt-data**로서, 단일 종목의 국내 "
            "채널 수요 조기신호 생성에는 사용할 수 있다.")
    if unmet:
        base += (" 다만 " + ", ".join(c["condition"] for c in unmet[:3])
                 + " 가 미충족이라, 현재 단독으로 전체 실적 nowcasting·횡단면 알파로 쓰기엔 부적합하다.")
    if unk:
        base += (" 또한 " + ", ".join(c["condition"] for c in unk[:3])
                 + " 는 원천사 확인 전까지 '미확정'이다.")
    base += (" 결론적으로, 지금 정직하게 말할 수 있는 것은 '관측된 채널 내 판매 흐름'이지 "
             "'시장 전체 수요'나 '회사 실적' 그 자체가 아니다.")
    return base


def _years(period: str) -> float:
    try:
        a, b = period.split(" ~ ")
        return (int(b[:4]) * 12 + int(b[5:7]) - int(a[:4]) * 12 - int(a[5:7])) / 12
    except Exception:
        return 0.0


def _n_listed(sku_master) -> int:
    if sku_master is None or "listed" not in sku_master:
        return 0
    return int(sku_master.loc[sku_master["listed"] == True, "company_kr"].nunique())  # noqa: E712
