"""
KRX 종목 마스터 확장 헬퍼 — pykrx 기반.

mandata_kr 패키지의 hand-curated 데모셋(217개)을 KOSPI+KOSDAQ 전체로 확장.
- 기존 hand-curated 행은 그대로 보존 (덮어쓰지 않음)
- 새 종목은 local_code/name/market만 채우고 나머지 컬럼은 빈 값
- ISIN은 보통주 규칙(KR7XXXXXX003)으로 추정 (정확도 ~90%)
- 완료 후 sync_meta.json 작성 → 라이브러리가 "demo set" 표시 자동 해제

Cloud 환경(ephemeral fs)에서는 변경사항이 reboot 시 사라짐 — Export 후 git commit 권장.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


# mandata_kr CSV 위치 (security_id_app/krx_sync.py → ../korea-security-id/mandata_kr/data)
_HERE = Path(__file__).resolve().parent
_DATA_DIR = _HERE.parent / "korea-security-id" / "mandata_kr" / "data"
_MASTER_CSV = _DATA_DIR / "equity_master.csv"
_SYNC_META = _DATA_DIR / "sync_meta.json"


def _load_existing() -> tuple[list[dict], list[str]]:
    """기존 equity_master.csv 로드. (rows, columns) 반환."""
    if not _MASTER_CSV.exists():
        return [], []
    with open(_MASTER_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = list(reader.fieldnames or [])
        rows = list(reader)
    return rows, cols


def _guess_isin(local_code: str) -> str:
    """보통주 ISIN 추정. 정확하지 않을 수 있지만 lookup용으로는 충분."""
    if not local_code or len(local_code) != 6:
        return ""
    return f"KR7{local_code}003"


def patch_full_universe(
    progress_cb: Callable[[str, float], None] | None = None,
) -> dict:
    """
    pykrx로 KOSPI+KOSDAQ 종목 마스터 가져와서 equity_master.csv에 추가.

    Args:
        progress_cb: (message: str, fraction: 0.0~1.0) 콜백. 진행 표시용.

    Returns:
        {"row_count": int, "added": int, "last_synced_utc": str, "source": str}

    Raises:
        RuntimeError: pykrx 미설치 / KRX 호출 실패 등
    """
    try:
        from pykrx import stock
    except ImportError as e:
        raise RuntimeError(
            "pykrx 패키지가 필요해요. 터미널에서: "
            "`pip3 install pykrx --break-system-packages`"
        ) from e

    if not _MASTER_CSV.exists():
        raise RuntimeError(f"기본 마스터 파일이 없어요: {_MASTER_CSV}")

    # 1) 기존 데이터 로드 (hand-curated 보존)
    if progress_cb:
        progress_cb("기존 마스터 로드 중...", 0.02)
    existing_rows, cols = _load_existing()
    if not cols:
        raise RuntimeError("equity_master.csv가 비어있거나 헤더가 없어요.")

    existing_codes: set[str] = {r.get("local_code", "") for r in existing_rows if r.get("local_code")}

    # 2) pykrx로 종목 코드 가져오기 (오늘 날짜 기준)
    today = datetime.now().strftime("%Y%m%d")
    new_rows: list[dict] = []

    markets = ("KOSPI", "KOSDAQ")
    base_frac = 0.05
    for m_idx, market in enumerate(markets):
        if progress_cb:
            progress_cb(f"📡 {market} 종목 마스터 가져오는 중...", base_frac + 0.02)
        try:
            tickers = stock.get_market_ticker_list(today, market=market)
        except Exception as e:
            # 휴장일이면 어제로 재시도
            for back in range(1, 7):
                try:
                    alt_date = (datetime.now().replace(day=max(1, datetime.now().day - back))).strftime("%Y%m%d")
                    tickers = stock.get_market_ticker_list(alt_date, market=market)
                    break
                except Exception:
                    continue
            else:
                raise RuntimeError(f"{market} 종목 리스트 조회 실패: {e}") from e

        total_in_market = len(tickers)
        for i, t in enumerate(tickers):
            if t in existing_codes:
                continue  # hand-curated 우선
            try:
                name_kr = stock.get_market_ticker_name(t)
            except Exception:
                name_kr = ""

            row = {c: "" for c in cols}
            row["local_code"] = t
            row["company_name_kr"] = name_kr
            row["isin"] = _guess_isin(t)
            row["market"] = market
            new_rows.append(row)
            existing_codes.add(t)

            if progress_cb and (i % 30 == 0 or i == total_in_market - 1):
                # 시장별 진행 비율 (KOSPI 0.05~0.50, KOSDAQ 0.50~0.95)
                market_frac = (i + 1) / max(total_in_market, 1)
                overall = base_frac + (m_idx + market_frac) * (0.90 / len(markets))
                progress_cb(
                    f"📡 {market}: {i + 1:,}/{total_in_market:,}  ·  새로 추가 {len(new_rows):,}개",
                    min(overall, 0.95),
                )

    # 3) CSV 다시 쓰기
    if progress_cb:
        progress_cb("💾 CSV 저장 중...", 0.96)
    all_rows = existing_rows + new_rows
    with open(_MASTER_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(all_rows)

    # 4) sync_meta.json 작성 → 라이브러리가 "Demo set" 자동 해제
    if progress_cb:
        progress_cb("📝 sync_meta.json 작성...", 0.99)
    meta = {
        "last_synced_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "pykrx (KOSPI + KOSDAQ)",
        "row_count": len(all_rows),
        "added": len(new_rows),
        "preserved": len(existing_rows),
    }
    _SYNC_META.write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if progress_cb:
        progress_cb(f"✅ 완료 — 총 {meta['row_count']:,}개 (+{meta['added']:,}개 신규)", 1.0)

    return meta


def reset_to_demo() -> None:
    """sync_meta.json만 삭제해서 데모 상태로 되돌림 (CSV는 그대로). 디버깅용."""
    if _SYNC_META.exists():
        _SYNC_META.unlink()


def get_master_path() -> Path:
    """다운로드 버튼 등에서 사용할 CSV 절대경로."""
    return _MASTER_CSV
