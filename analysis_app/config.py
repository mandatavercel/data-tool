"""
analysis_app 전역 상수 모음.

step별 모듈에서 import해 사용.
"""
from __future__ import annotations

from modules.analysis.intelligence.growth    import run_growth_analysis,    _render as _render_growth
from modules.analysis.intelligence.demand    import run_demand_analysis,    _render as _render_demand
from modules.analysis.intelligence.brand     import run_brand_analysis,     _render as _render_brand
from modules.analysis.intelligence.sku       import run_sku_analysis,       _render as _render_sku
from modules.analysis.intelligence.category  import run_category_analysis,  _render as _render_category
from modules.analysis.signal.anomaly         import run_anomaly_detection,  _render as _render_anomaly
from modules.analysis.signal.market          import run_market_signal,      _render as _render_market
from modules.analysis.signal.earnings        import run_earnings_intel,     _render as _render_earnings
from modules.analysis.signal.alpha           import run_alpha_validation,   _render as _render_alpha
from modules.analysis.signal.factor_research import run_factor_research,    _render as _render_factor


# ── 스텝 정의 ─────────────────────────────────────────────────────────────────
STEPS = {
    1: "Data Upload",
    2: "Schema Intelligence",
    3: "Data Validation",
    4: "Analysis Setup",
    5: "Results",
    6: "Signal Dashboard",
}

# ── 분석 모듈 표시 라벨 ────────────────────────────────────────────────────────
ANALYSIS_OPTIONS = {
    "growth":           "📈 Growth Analytics",
    "demand":           "🔥 Demand Intelligence",
    "brand":            "🏷 Brand Intelligence",
    "sku":              "📦 SKU Intelligence",
    "category":         "🗂 Category Intelligence",
    "anomaly":          "🚨 Anomaly Detection",
    "market_signal":    "📉 Market Signal",
    "earnings_intel":   "📊 Earnings Intelligence",
    "alpha_validation": "🎯 Alpha Validation",
    "factor_research":  "🧪 Factor Research",
}

# ── 레이어별 모듈 그룹 ─────────────────────────────────────────────────────────
MODULE_LAYERS = {
    "Intelligence Hub": ["growth", "demand", "brand", "sku", "category"],
    "Signal Layer":     ["anomaly", "market_signal", "earnings_intel", "alpha_validation"],
    "Factor Layer":     ["factor_research"],
}

# ── Runner 함수 매핑 (run_*_analysis) ──────────────────────────────────────────
RUNNERS: dict = {
    "growth":           run_growth_analysis,
    "demand":           run_demand_analysis,
    "brand":            run_brand_analysis,
    "sku":              run_sku_analysis,
    "category":         run_category_analysis,
    "anomaly":          run_anomaly_detection,
    "market_signal":    run_market_signal,
    "earnings_intel":   run_earnings_intel,
    "alpha_validation": run_alpha_validation,
    "factor_research":  run_factor_research,
}

# ── Renderer 함수 매핑 (_render) ───────────────────────────────────────────────
RENDERERS: dict = {
    "growth":           _render_growth,
    "demand":           _render_demand,
    "brand":            _render_brand,
    "sku":              _render_sku,
    "category":         _render_category,
    "anomaly":          _render_anomaly,
    "market_signal":    _render_market,
    "earnings_intel":   _render_earnings,
    "alpha_validation": _render_alpha,
    "factor_research":  _render_factor,
}
