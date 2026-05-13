"""Core computation layer — pure functions, no Streamlit, no side effects."""
from modules.common.core.metrics     import (
    calculate_growth_rate,
    calculate_mom,
    calculate_qoq,
    calculate_yoy,
    calculate_correlation,
    calculate_lag_correlation,
    calculate_tracking_ratio,
)
from modules.common.core.normalizer  import (
    normalize_date,
    normalize_numeric,
    normalize_amount_to_krw,
    normalize_score,
    infer_amount_unit,
)
from modules.common.core.validators  import (
    validate_required_columns,
    validate_numeric_values,
    validate_date_values,
    validate_sample_size,
    validate_ratio_sanity,
    validate_tracking_ratio,
)
from modules.common.core.audit import (
    compute_module_audit,
    build_input_audit,
    build_data_quality,
    build_calculation_audit,
    build_confidence,
    compute_confidence_score,
    grade_confidence,
    check_growth_sanity,
    check_tracking_ratio_sanity,
    check_correlation_sanity,
    check_anomaly_rate_sanity,
    check_sample_size_sanity,
)
from modules.common.core.result import (
    make_result,
    enrich_result,
    failed_result,
    get_confidence_grade,
    get_confidence_score,
)

__all__ = [
    "calculate_growth_rate", "calculate_mom", "calculate_qoq", "calculate_yoy",
    "calculate_correlation", "calculate_lag_correlation", "calculate_tracking_ratio",
    "normalize_date", "normalize_numeric", "normalize_amount_to_krw",
    "normalize_score", "infer_amount_unit",
    "validate_required_columns", "validate_numeric_values", "validate_date_values",
    "validate_sample_size", "validate_ratio_sanity", "validate_tracking_ratio",
    "compute_module_audit", "build_input_audit", "build_data_quality",
    "build_calculation_audit", "build_confidence", "compute_confidence_score",
    "grade_confidence", "check_growth_sanity", "check_tracking_ratio_sanity",
    "check_correlation_sanity", "check_anomaly_rate_sanity", "check_sample_size_sanity",
    "make_result", "enrich_result", "failed_result",
    "get_confidence_grade", "get_confidence_score",
]
