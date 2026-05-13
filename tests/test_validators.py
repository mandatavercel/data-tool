"""Tests for modules/core/validators.py

Golden dataset philosophy:
- Each test exercises one specific scenario
- Expected severity/label/ok are hand-verified
- No mocking — functions are pure
"""
import math
import pandas as pd

from modules.common.core.validators import (
    validate_required_columns,
    validate_numeric_values,
    validate_date_values,
    validate_sample_size,
    validate_ratio_sanity,
    validate_tracking_ratio,
)


# ── validate_required_columns ──────────────────────────────────────────────────

class TestValidateRequiredColumns:
    def test_all_present_returns_ok(self):
        role_map = {"transaction_date": "date_col", "sales_amount": "sales_col"}
        result = validate_required_columns(role_map, ["transaction_date", "sales_amount"])
        assert result["ok"] is True
        assert result["missing"] == []

    def test_missing_single_column(self):
        role_map = {"transaction_date": "date_col"}
        result = validate_required_columns(role_map, ["transaction_date", "sales_amount"])
        assert result["ok"] is False
        assert "sales_amount" in result["missing"]

    def test_missing_all_columns(self):
        result = validate_required_columns({}, ["transaction_date", "sales_amount"])
        assert result["ok"] is False
        assert len(result["missing"]) == 2

    def test_extra_columns_in_role_map_ignored(self):
        role_map = {"transaction_date": "d", "sales_amount": "s", "company_name": "c"}
        result = validate_required_columns(role_map, ["transaction_date", "sales_amount"])
        assert result["ok"] is True

    def test_empty_required_always_ok(self):
        result = validate_required_columns({}, [])
        assert result["ok"] is True

    def test_message_contains_missing_name(self):
        role_map = {"transaction_date": "d"}
        result = validate_required_columns(role_map, ["transaction_date", "sales_amount"])
        assert "sales_amount" in result["message"]


# ── validate_numeric_values ────────────────────────────────────────────────────

class TestValidateNumericValues:
    def test_clean_positive_series(self):
        s = pd.Series([100.0, 200.0, 300.0])
        result = validate_numeric_values(s)
        assert result["n_null"] == 0
        assert result["n_negative"] == 0
        assert result["n_zero"] == 0

    def test_detects_negative_values(self):
        # 2 negative out of 4
        s = pd.Series([100.0, -50.0, 200.0, -30.0])
        result = validate_numeric_values(s)
        assert result["n_negative"] == 2
        assert result["negative_pct"] == 50.0

    def test_detects_zeros(self):
        s = pd.Series([0.0, 100.0, 0.0])
        result = validate_numeric_values(s)
        assert result["n_zero"] == 2

    def test_detects_null_values(self):
        s = pd.Series([100.0, float("nan"), 300.0, float("nan")])
        result = validate_numeric_values(s)
        assert result["n_null"] == 2
        assert abs(result["null_pct"] - 50.0) < 1e-9

    def test_string_column_counts_as_null(self):
        s = pd.Series(["abc", "def"])
        result = validate_numeric_values(s)
        assert result["n_null"] == 2

    def test_iqr_outlier_detection(self):
        # [10, 10, 10, 10, 10, 10, 10, 10000] → 10000 is outlier
        s = pd.Series([10.0] * 7 + [10_000.0])
        result = validate_numeric_values(s)
        assert result["n_outlier_iqr"] >= 1

    def test_mean_and_max_correct(self):
        s = pd.Series([10.0, 20.0, 30.0])
        result = validate_numeric_values(s)
        assert abs(result["mean"] - 20.0) < 1e-9
        assert abs(result["max"] - 30.0) < 1e-9

    def test_n_total_matches_input(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        assert validate_numeric_values(s)["n_total"] == 5


# ── validate_date_values ───────────────────────────────────────────────────────

class TestValidateDateValues:
    def test_valid_dates_returns_ok(self):
        s = pd.Series(["2021-01-01", "2021-06-01", "2021-12-31"])
        result = validate_date_values(s)
        assert result["ok"] is True
        assert result["n_null"] == 0

    def test_invalid_date_string_detected(self):
        # "not_a_date" should parse to NaT
        s = pd.Series(["2021-01-01", "not_a_date", "2021-03-01"])
        result = validate_date_values(s)
        assert result["n_null"] >= 1
        assert result["ok"] is False

    def test_yyyymmdd_format_parsed(self):
        s = pd.Series(["20200101", "20201231"])
        result = validate_date_values(s)
        assert result["ok"] is True
        assert result["date_min"] == "2020-01-01"
        assert result["date_max"] == "2020-12-31"

    def test_date_range_days_correct(self):
        s = pd.Series(["2020-01-01", "2020-12-31"])
        result = validate_date_values(s)
        assert result["n_days"] == 365

    def test_all_invalid_returns_not_ok(self):
        s = pd.Series(["abc", "xyz", "???"])
        result = validate_date_values(s)
        assert result["ok"] is False
        assert result["n_null"] == 3

    def test_null_pct_calculation(self):
        s = pd.Series(["2021-01-01", "bad"])
        result = validate_date_values(s)
        assert abs(result["null_pct"] - 50.0) < 1e-9

    def test_date_min_max_strings(self):
        s = pd.Series(["2022-03-01", "2022-06-30"])
        result = validate_date_values(s)
        assert result["date_min"] == "2022-03-01"
        assert result["date_max"] == "2022-06-30"


# ── validate_sample_size ───────────────────────────────────────────────────────

class TestValidateSampleSize:
    def test_sufficient_sample_ok(self):
        result = validate_sample_size(100, min_required=10)
        assert result["ok"] is True
        assert result["severity"] == "ok"

    def test_exactly_minimum_is_ok(self):
        result = validate_sample_size(10, min_required=10)
        assert result["ok"] is True

    def test_below_minimum_warning(self):
        # 5 < 10, but ≥ max(3, 10//3)=3 → warning
        result = validate_sample_size(5, min_required=10)
        assert result["severity"] == "warning"
        assert result["ok"] is False

    def test_very_few_samples_critical(self):
        # 1 < max(3, 10//3)=3 → critical
        result = validate_sample_size(1, min_required=10)
        assert result["severity"] == "critical"
        assert result["ok"] is False

    def test_zero_samples_critical(self):
        result = validate_sample_size(0, min_required=10)
        assert result["severity"] == "critical"

    def test_message_contains_count(self):
        result = validate_sample_size(7, min_required=10)
        assert "7" in result["message"]

    def test_custom_min_required(self):
        result = validate_sample_size(50, min_required=100)
        assert result["severity"] != "ok"


# ── validate_ratio_sanity ──────────────────────────────────────────────────────

class TestValidateRatioSanity:
    """POS/DART Tracking Ratio severity classification."""

    def test_normal_ratio_ok(self):
        result = validate_ratio_sanity(75.0)
        assert result["severity"] == "ok"

    def test_ratio_100_percent_ok(self):
        result = validate_ratio_sanity(100.0)
        assert result["severity"] == "ok"

    def test_ratio_110_caution(self):
        # 100 < ratio ≤ 150 → caution
        result = validate_ratio_sanity(110.0)
        assert result["severity"] == "caution"

    def test_ratio_169_5_warning(self):
        # The HITE JINRO test case: 169.5% → warning
        result = validate_ratio_sanity(169.5)
        assert result["severity"] == "warning"

    def test_ratio_above_200_critical(self):
        result = validate_ratio_sanity(250.0)
        assert result["severity"] == "critical"

    def test_negative_ratio_critical(self):
        result = validate_ratio_sanity(-10.0)
        assert result["severity"] == "critical"

    def test_nan_returns_warning(self):
        result = validate_ratio_sanity(float("nan"))
        assert result["severity"] == "warning"

    def test_very_low_ratio_caution(self):
        # < 5% → caution (POS tracking very little of DART)
        result = validate_ratio_sanity(2.0)
        assert result["severity"] == "caution"

    def test_label_present(self):
        result = validate_ratio_sanity(80.0)
        assert isinstance(result["label"], str)
        assert len(result["label"]) > 0

    def test_message_contains_value(self):
        result = validate_ratio_sanity(80.0)
        assert "80" in result["message"]


# ── validate_tracking_ratio ────────────────────────────────────────────────────

class TestValidateTrackingRatio:
    """Aggregate tracking ratio validation across a time series."""

    def _make_tr(self, values):
        return pd.Series(values, dtype=float)

    def test_normal_ratios_ok(self):
        # All below 100% → ok
        tr = self._make_tr([50.0, 60.0, 70.0, 80.0])
        result = validate_tracking_ratio(tr)
        assert result["severity"] == "ok"
        assert result["issues"] == []

    def test_avg_above_150_critical(self):
        tr = self._make_tr([160.0, 170.0, 180.0])
        result = validate_tracking_ratio(tr)
        assert result["severity"] == "critical"
        assert len(result["issues"]) >= 1

    def test_avg_above_100_warning(self):
        tr = self._make_tr([105.0, 110.0, 115.0])
        result = validate_tracking_ratio(tr)
        assert result["severity"] in ("warning", "critical")

    def test_extreme_value_above_200(self):
        tr = self._make_tr([50.0, 60.0, 250.0])
        result = validate_tracking_ratio(tr)
        assert result["n_extreme"] == 1
        assert result["severity"] == "critical"

    def test_negative_value_is_extreme(self):
        tr = self._make_tr([80.0, 90.0, -5.0])
        result = validate_tracking_ratio(tr)
        assert result["n_extreme"] == 1

    def test_inf_converted_to_nan(self):
        tr = self._make_tr([float("inf"), 50.0, 60.0])
        result = validate_tracking_ratio(tr)
        # Inf is excluded from avg — should not raise
        assert isinstance(result["avg"], float)

    def test_empty_series_returns_ok(self):
        result = validate_tracking_ratio(pd.Series([], dtype=float))
        assert result["severity"] == "ok"
        assert math.isnan(result["avg"])

    def test_n_above_100_count(self):
        tr = self._make_tr([80.0, 110.0, 120.0, 90.0])
        result = validate_tracking_ratio(tr)
        assert result["n_above_100"] == 2

    def test_avg_computed_correctly(self):
        tr = self._make_tr([100.0, 200.0])
        result = validate_tracking_ratio(tr)
        assert abs(result["avg"] - 150.0) < 1e-9

    def test_high_std_triggers_issue(self):
        # Wide variance: [10, 200] → std ≈ 134 > 40 → issue added
        tr = self._make_tr([10.0, 200.0])
        result = validate_tracking_ratio(tr)
        assert any("표준편차" in issue for issue in result["issues"])
