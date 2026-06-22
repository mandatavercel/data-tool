"""
kfnb_app/cli.py — 헤드리스 실행기.

사용법:
    python -m kfnb_app.cli <input.csv> <output.xlsx> [--sector 면류]
                           [--brand 불닭볶음면] [--label K-Food] [--non-strict]

각 단계의 검증 결과를 콘솔에 출력하고, 게이트 통과 시 최종 xlsx 를 생성한다.
"""
from __future__ import annotations

import argparse
import sys

from .pipeline import STAGE_LABELS, run_pipeline

_SEV_ICON = {"ok": "✅", "info": "ℹ️ ", "warning": "⚠️ ",
             "error": "❌", "critical": "🛑"}


def _print_stage(res) -> None:
    label = STAGE_LABELS.get(res.name, res.name)
    print(f"\n{_SEV_ICON.get(res.severity, '·')} {label}  "
          f"[{res.severity.upper()}]")
    for c in res.validation["checks"]:
        print(f"    {_SEV_ICON.get(c['severity'], '·')} {c['label']}: {c['detail']}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="kfnb", description="K-F&B 데이터 상품 에이전트 (CLI)")
    ap.add_argument("input", help="원천 POS CSV 경로")
    ap.add_argument("output", help="생성할 xlsx 경로")
    ap.add_argument("--sector", default="면류",
                    help="cat_l2 필터 (기본 면류; 전체는 'all')")
    ap.add_argument("--brand", default="불닭볶음면",
                    help="모멘텀 추세를 뽑을 포커스 브랜드")
    ap.add_argument("--label", default="K-Food", help="상품 라벨")
    ap.add_argument("--daily", action="store_true",
                    help="daily_sales_en.csv(영문명 조인, 대용량) 포함")
    ap.add_argument("--ids", default="",
                    help="SKU에 포함할 식별자(쉼표): isin,krx_code,bbg_ticker,bloomberg,gics,gics_sector")
    ap.add_argument("--alpha", action="store_true",
                    help="알파 리서치(주가 상관·공시 선행성) 실행 — pykrx/DART 필요")
    ap.add_argument("--dart-key", default="",
                    help="DART_API_KEY (미지정 시 환경변수 DART_API_KEY 사용)")
    # Data Spec (결론·available_date 근거)
    ap.add_argument("--channel", default="", help="채널 범위 (예: 'CU 편의점')")
    ap.add_argument("--population", default="unknown",
                    choices=["unknown", "census", "sample", "multi_channel"])
    ap.add_argument("--amount-basis", default="vat_incl_retail",
                    choices=["vat_incl_retail", "vat_excl", "discounted_net", "unknown"])
    ap.add_argument("--qty-basis", default="selling_unit",
                    choices=["selling_unit", "each", "unknown"])
    ap.add_argument("--cadence", default="unknown",
                    choices=["unknown", "daily", "weekly", "monthly"])
    ap.add_argument("--lag-days", type=int, default=15, help="입수 지연(일) → available_date")
    ap.add_argument("--restatement", default="unknown",
                    choices=["unknown", "none", "revised"])
    ap.add_argument("--non-strict", action="store_true",
                    help="검증 경고/오류 무시하고 끝까지 진행")
    args = ap.parse_args(argv)

    sector = None if args.sector.lower() == "all" else args.sector
    print(f"▶ 입력: {args.input}\n▶ 섹터: {sector or '전체'}  "
          f"포커스: {args.brand}")

    import os
    from kfnb_app.config import DataSpec
    dart_key = args.dart_key or os.environ.get("DART_API_KEY", "")
    id_cols = [s.strip() for s in args.ids.split(",") if s.strip()] or None
    spec = DataSpec(amount_basis=args.amount_basis, qty_basis=args.qty_basis,
                    currency="KRW", channel_scope=args.channel,
                    population=args.population, release_cadence=args.cadence,
                    release_lag_days=args.lag_days, restatement=args.restatement)
    result = run_pipeline(
        args.input, args.output,
        sector=sector, focus_brand=args.brand,
        sector_label=args.label, include_daily=args.daily,
        id_cols=id_cols, data_spec=spec, run_alpha=args.alpha,
        dart_api_key=dart_key, strict=not args.non_strict)

    for res in result["stages"]:
        _print_stage(res)

    if not result["ok"]:
        print(f"\n🛑 파이프라인 중단 — 단계 [{result['halted_at']}] 검증 미통과. "
              f"검수 후 --non-strict 로 강제 진행 가능.")
        return 1

    exp = result.get("export", {})
    print(f"\n🎉 완료 → {exp.get('path')}  ({exp.get('sheets')} 시트)")
    print(f"   딜리버리 패키지 → {exp.get('bundle_zip')}")
    print(f"   구성: {', '.join(exp.get('files', []))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
