"""
kfnb_app/ingest/trends.py — 외부 트렌드/뉴스 신호 어댑터 (graceful).

상품 추천 엔진(strategy.recommender)의 입력. "트렌드(예: 불닭 매운맛)를 빠르게
반영"하기 위해 외부 공개 신호를 모은다.
  - google_trends : 구글 트렌드(pytrends) 관심도·모멘텀 (키 불필요, 네트워크 필요)
  - news_volume   : 뉴스 노출량 (Naver 뉴스 API 등, 키 필요)

라이브러리/네트워크/키가 없으면 (빈 DataFrame, 사유)를 반환해 추천 파이프라인을
막지 않는다. 실제 조회는 사용자 실행 환경에서 동작(샌드박스는 비어있음).
streamlit 비의존.
"""
from __future__ import annotations

import pandas as pd

TREND_COLS = ["keyword", "trend_recent", "trend_base", "trend_momentum"]
NEWS_COLS = ["query", "news_count"]


def _empty(cols, note):
    return pd.DataFrame(columns=cols), note


def google_trends(keywords, geo: str = "KR",
                  timeframe: str = "today 12-m") -> tuple[pd.DataFrame, str]:
    """키워드 리스트 → 구글 트렌드 관심도·모멘텀. (df, note).

    trend_momentum = 최근 3구간 평균 / 직전 9구간 평균 - 1  (상승 추세 > 0).
    pytrends 미설치/네트워크 차단 시 (빈 df, 사유).
    """
    kws = [str(k).strip() for k in dict.fromkeys(keywords) if str(k).strip()]
    if not kws:
        return _empty(TREND_COLS, "키워드 없음")
    try:
        from pytrends.request import TrendReq
    except Exception:                              # noqa: BLE001
        return _empty(TREND_COLS, "pytrends 미설치 — 트렌드 자동조회 생략")
    rows = []
    try:
        py = TrendReq(hl="ko-KR", tz=540)
        # 트렌드 API는 한번에 5개 키워드 제한 → 배치
        for i in range(0, len(kws), 5):
            batch = kws[i:i + 5]
            py.build_payload(batch, timeframe=timeframe, geo=geo)
            iot = py.interest_over_time()
            if iot is None or iot.empty:
                continue
            for kw in batch:
                if kw not in iot.columns:
                    continue
                s = iot[kw].astype(float)
                recent = s.tail(3).mean()
                base = s.iloc[:-3].tail(9).mean() if len(s) > 3 else s.mean()
                mom = (recent / base - 1.0) if base else 0.0
                rows.append({"keyword": kw, "trend_recent": round(recent, 1),
                             "trend_base": round(base, 1),
                             "trend_momentum": round(mom, 3)})
    except Exception as e:                         # noqa: BLE001
        return _empty(TREND_COLS, f"트렌드 조회 실패: {type(e).__name__}")
    if not rows:
        return _empty(TREND_COLS, "트렌드 데이터 없음")
    return pd.DataFrame(rows), f"{len(rows)}개 키워드 트렌드 조회"


def news_volume(queries, naver_client_id: str = "",
                naver_client_secret: str = "") -> tuple[pd.DataFrame, str]:
    """검색어 리스트 → 뉴스 노출 건수(Naver 뉴스 API). (df, note).

    키(클라이언트 ID/Secret) 없으면 (빈 df, 사유). 네트워크는 사용자 환경.
    """
    qs = [str(q).strip() for q in dict.fromkeys(queries) if str(q).strip()]
    if not qs:
        return _empty(NEWS_COLS, "검색어 없음")
    if not (naver_client_id and naver_client_secret):
        return _empty(NEWS_COLS, "뉴스 API 키 없음 — 뉴스량 자동조회 생략")
    try:
        import requests
    except Exception:                              # noqa: BLE001
        return _empty(NEWS_COLS, "requests 미설치")
    rows = []
    headers = {"X-Naver-Client-Id": naver_client_id,
               "X-Naver-Client-Secret": naver_client_secret}
    for q in qs:
        try:
            r = requests.get("https://openapi.naver.com/v1/search/news.json",
                             params={"query": q, "display": 1}, headers=headers,
                             timeout=10)
            r.raise_for_status()
            rows.append({"query": q, "news_count": int(r.json().get("total", 0))})
        except Exception:                          # noqa: BLE001
            rows.append({"query": q, "news_count": 0})
    return pd.DataFrame(rows), f"{len(rows)}개 검색어 뉴스량 조회"
