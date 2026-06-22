"""
kfnb_app.strategy — 데이터셋 '제작 이전' 상품 기획·유니버스 관리 레이어.

데이터를 만들기 전에 "무엇을 만들 것인가"를 먼저 정하고 정기적으로 관리한다.
  - universe : F&B 섹터 회사 유니버스(20개)·대표 브랜드(5개) 하이브리드 선정 +
               선정사유 + 반기 리뷰 + 영속화 (MSCI식 정기관리)
  - packages : Basic/Professional/Premium 3-티어 상품 카탈로그 + 분석질문→상품 매핑
"""
from kfnb_app.strategy import universe, packages, recommender  # noqa: F401

__all__ = ["universe", "packages", "recommender"]
