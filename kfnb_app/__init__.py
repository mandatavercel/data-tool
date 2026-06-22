"""
kfnb_app — K-F&B 데이터 상품 에이전트 (config-driven)
=====================================================
원천 F&B POS 데이터를 글로벌 투자기관용 '투자등급' 데이터 상품으로 단계별
변환하는 에이전트. 각 단계마다 검증(validation/QC) 게이트를 통과해야 진행하며,
최종적으로 마스터 파일 묶음(딕셔너리) + QC 리포트 + 문서 패키지를 생성한다.

레이어 구조:
    ingest/          적재 + 멀티오너 스키마 매핑 + 외부소스 (dataio, schema_mapper,
                     prices[pykrx/yfinance], disclosures[DART])
    profiling/       프로파일링 (profiler)
    standardization/ 정규화·태깅·텍스트클렌징 (normalize, tagging, text_cleaning)
    mapping/         회사/브랜드/SKU 마스터링·커버리지 (company, mastering, coverage)
    insight/         use-case 발굴·알파(상관/선행성)·PIT walk-forward·투자적합성·
                     결론·투자기관 DDQ (usecase, alpha, pit, assessment, conclusion,
                     investor_qa)
    qc/              품질관리 + 리포트 (checks)
    export/          xlsx·마스터번들·문서 (workbook, bundle, docs)
    utils/           로마자 등 (romanization)
    dashboard.py     plotly 투자 대시보드
    config / configs/  YAML·CSV 외부 설정 (DataSpec 포함, 단일 진실)

진입점:
    - kfnb_app.pipeline.run_pipeline(...)  헤드리스 전체 실행
    - kfnb_app.app                          Streamlit UI (스텝형)
    - python -m kfnb_app.cli <input> <out>  CLI
"""
from __future__ import annotations

__version__ = "0.2.0"
