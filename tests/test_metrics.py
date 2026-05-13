"""Tests for modules/core/metrics.py

Golden dataset philosophy:
- Use hand-computed expected values, not the function itself
- Cover normal cases, edge cases, and known failure modes
- No Streamlit, no file I/O, no external network calls
"""
import math
import numpy as np
import pandas as pd

from modules.common.core.metrics import (
    calculate_growth_rate,
    calculate_mom,
    calculate_qoq,
    calculate_yoy,
    calculate_correlation,
    calculate_lag_correlation,
    calculate_tracking_ratio,
)


# ── calculate_growth_rate ──────────────────────────────────────────────────────

class TestCalculateGrowthRate:
    def test_basic_increase(self):
        s = pd.Series([100.0, 120.0])
        result = calculate_growth_rate(s, periods=1)
        assert math.isnan(result.iloc[0])          # first period always NaN
        assert abs(result.iloc[1] - 20.0) < 1e-9  # (120-100)/100 * 100 = 20%

    def test_basic_decrease(self):
        s = pd.Series([200.0, 150.0])
        result = calculate_growth_rate(s, periods=1)
        assert abs(result.iloc[1] - (-25.0)) < 1e-9  # -25%

    def test_periods_4(self):
        # YoY via growth_rate with periods=4
        # [100, 100, 100, 100, 110] → 5th vs 1st = +10%
        s = pd.Series([100.0, 100.0, 100.0, 100.0, 110.0])
        result = calculate_growth_rate(s, periods=4)
        assert math.isnan(result.iloc[0])
        assert abs(result.iloc[-1] - 10.0) < 1e-9

    def test_flat_series_returns_zeros(self):
        s = pd.Series([50.0, 50.0, 50.0])
        result = calculate_growth_rate(s, periods=1)
        assert abs(result.iloc[1]) < 1e-9
        assert abs(result.iloc[2]) < 1e-9

    def test_single_element_all_nan(self):
        s = pd.Series([100.0])
        result = calculate_growth_rate(s, periods=1)
        assert math.isnan(result.iloc[0])

    def test_returns_series(self):
        s = pd.Series([10.0, 20.0])
        assert isinstance(calculate_growth_rate(s), pd.Series)


# ── calculate_mom ──────────────────────────────────────────────────────────────

class TestCalculateMom:
    """MoM = consecutive period change (periods=1)."""

    def test_100_to_120(self):
        s = pd.Series([100.0, 120.0])
        assert abs(calculate_mom(s).iloc[-1] - 20.0) < 1e-9

    def test_1000_to_800(self):
        s = pd.Series([1_000.0, 800.0])
        assert abs(calculate_mom(s).iloc[-1] - (-20.0)) < 1e-9

    def test_three_months(self):
        # Jan=100, Feb=110, Mar=99
        # MoM Feb = +10%, Mar = (99-110)/110*100 = -10%
        s = pd.Series([100.0, 110.0, 99.0])
        result = calculate_mom(s)
        assert abs(result.iloc[1] - 10.0) < 1e-9
        assert abs(result.iloc[2] - (-10.0)) < 0.01

    def test_first_element_nan(self):
        assert math.isnan(calculate_mom(pd.Series([200.0, 300.0])).iloc[0])


# ── calculate_qoq ──────────────────────────────────────────────────────────────

class TestCalculateQoq:
    """QoQ = consecutive quarter change (periods=1)."""

    def test_q1_to_q2_increase(self):
        # Q1=500, Q2=600 → +20%
        s = pd.Series([500.0, 600.0])
        assert abs(calculate_qoq(s).iloc[-1] - 20.0) < 1e-9

    def test_q1_to_q2_decrease(self):
        s = pd.Series([600.0, 500.0])
        expected = (500 - 600) / 600 * 100   # -16.666...%
        assert abs(calculate_qoq(s).iloc[-1] - expected) < 1e-6

    def test_four_quarters(self):
        s = pd.Series([100.0, 110.0, 99.0, 108.9])
        result = calculate_qoq(s)
        assert math.isnan(result.iloc[0])
        assert abs(result.iloc[1] - 10.0) < 1e-9
        assert abs(result.iloc[2] - (-10.0)) < 0.01

    def test_same_as_growth_rate_periods_1(self):
        s = pd.Series([200.0, 250.0, 300.0])
        qoq    = calculate_qoq(s)
        manual = calculate_growth_rate(s, periods=1)
        pd.testing.assert_series_equal(qoq, manual)


# ── calculate_yoy ──────────────────────────────────────────────────────────────

class TestCalculateYoy:
    """YoY with freq='Q' uses periods=4; freq='M' uses periods=12."""

    def test_quarterly_5_periods(self):
        # Quarters: Q1=100, Q2=100, Q3=100, Q4=100, Q1_next=110 → +10%
        s = pd.Series([100.0, 100.0, 100.0, 100.0, 110.0])
        result = calculate_yoy(s, freq="Q")
        assert math.isnan(result.iloc[0])
        assert abs(result.iloc[-1] - 10.0) < 1e-9

    def test_monthly_13_periods(self):
        # 12 months flat then +20%
        s = pd.Series([100.0] * 12 + [120.0])
        result = calculate_yoy(s, freq="M")
        assert math.isnan(result.iloc[0])
        assert abs(result.iloc[-1] - 20.0) < 1e-9

    def test_default_freq_is_Q(self):
        s = pd.Series([100.0, 100.0, 100.0, 100.0, 115.0])
        assert abs(calculate_yoy(s).iloc[-1] - 15.0) < 1e-9

    def test_yoy_negative(self):
        s = pd.Series([200.0, 200.0, 200.0, 200.0, 180.0])
        assert abs(calculate_yoy(s).iloc[-1] - (-10.0)) < 1e-9


# ── calculate_correlation ──────────────────────────────────────────────────────

class TestCalculateCorrelation:
    def test_perfect_positive(self):
        a = pd.Series([1.0, 2.0, 3.0, 4.0])
        b = pd.Series([1.0, 2.0, 3.0, 4.0])
        assert abs(calculate_correlation(a, b) - 1.0) < 1e-9

    def test_perfect_negative(self):
        a = pd.Series([1.0, 2.0, 3.0])
        b = pd.Series([3.0, 2.0, 1.0])
        assert abs(calculate_correlation(a, b) - (-1.0)) < 1e-9

    def test_no_correlation(self):
        # Alternating vs constant — r = 0
        a = pd.Series([1.0, -1.0, 1.0, -1.0])
        b = pd.Series([0.0,  0.0, 0.0,  0.0])
        r = calculate_correlation(a, b)
        # b has zero variance → NaN
        assert math.isnan(r)

    def test_nan_pairs_dropped(self):
        # NaN pair at index 1 should be ignored
        a = pd.Series([1.0, float("nan"), 3.0])
        b = pd.Series([1.0, 2.0,          3.0])
        r = calculate_correlation(a, b)
        assert abs(r - 1.0) < 1e-9

    def test_too_few_valid_returns_nan(self):
        a = pd.Series([1.0, float("nan")])
        b = pd.Series([2.0, float("nan")])
        assert math.isnan(calculate_correlation(a, b))

    def test_numpy_array_input(self):
        a = np.array([1.0, 2.0, 3.0])
        b = np.array([1.0, 2.0, 3.0])
        assert abs(calculate_correlation(a, b) - 1.0) < 1e-9

    def test_result_in_range(self):
        rng = np.random.default_rng(42)
        a = rng.standard_normal(50)
        b = rng.standard_normal(50)
        r = calculate_correlation(a, b)
        assert -1.0 <= r <= 1.0


# ── calculate_lag_correlation ──────────────────────────────────────────────────

class TestCalculateLagCorrelation:
    def _identical_series(self):
        return np.arange(1.0, 9.0)

    def test_returns_dataframe(self):
        a = self._identical_series()
        result = calculate_lag_correlation(a, a, max_lag=2, min_lag=0)
        assert isinstance(result, pd.DataFrame)

    def test_columns_present(self):
        a = self._identical_series()
        df = calculate_lag_correlation(a, a, max_lag=1, min_lag=0)
        assert set(["lag", "label", "r", "n"]).issubset(df.columns)

    def test_row_count(self):
        # min_lag=−2, max_lag=4 → lags −2,−1,0,1,2,3,4 = 7 rows
        a = np.arange(1.0, 11.0)
        df = calculate_lag_correlation(a, a, max_lag=4, min_lag=-2)
        assert len(df) == 7

    def test_lag0_perfect_correlation(self):
        a = np.arange(1.0, 9.0)
        df = calculate_lag_correlation(a, a, max_lag=1, min_lag=0)
        lag0 = df[df["lag"] == 0]["r"].iloc[0]
        assert abs(lag0 - 1.0) < 1e-9

    def test_lead_detected(self):
        # a = [1,2,3,4,5,6,7,8], b = a shifted right by 1 (b leads a by 1)
        a = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
        b = np.array([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
        df = calculate_lag_correlation(a, b, max_lag=2, min_lag=0)
        # at lag=1 a[:7] vs b[1:8] → perfect correlation
        r_lag1 = df[df["lag"] == 1]["r"].iloc[0]
        assert abs(r_lag1 - 1.0) < 1e-9

    def test_custom_names_in_label(self):
        a = np.arange(1.0, 6.0)
        df = calculate_lag_correlation(a, a, max_lag=1, min_lag=0,
                                       name_a="POS", name_b="DART")
        label_lag1 = df[df["lag"] == 1]["label"].iloc[0]
        assert "POS" in label_lag1

    def test_n_column_reflects_valid_pairs(self):
        # last element NaN → n at lag=0 should be len-1
        a = np.array([1.0, 2.0, 3.0, float("nan")])
        b = np.array([1.0, 2.0, 3.0, 4.0])
        df = calculate_lag_correlation(a, b, max_lag=0, min_lag=0)
        assert df[df["lag"] == 0]["n"].iloc[0] == 3


# ── calculate_tracking_ratio ───────────────────────────────────────────────────

class TestCalculateTrackingRatio:
    def test_pos_50_dart_200_gives_25(self):
        pos  = pd.Series([50.0])
        dart = pd.Series([200.0])
        result = calculate_tracking_ratio(pos, dart)
        assert abs(result.iloc[0] - 25.0) < 1e-9

    def test_pos_equals_dart_gives_100(self):
        pos  = pd.Series([300.0, 400.0])
        dart = pd.Series([300.0, 400.0])
        result = calculate_tracking_ratio(pos, dart)
        assert all(abs(v - 100.0) < 1e-6 for v in result)

    def test_pos_double_dart_gives_200(self):
        pos  = pd.Series([200.0])
        dart = pd.Series([100.0])
        assert abs(calculate_tracking_ratio(pos, dart).iloc[0] - 200.0) < 1e-9

    def test_dart_zero_returns_nan(self):
        pos  = pd.Series([100.0])
        dart = pd.Series([0.0])
        result = calculate_tracking_ratio(pos, dart)
        assert math.isnan(result.iloc[0])

    def test_multiple_periods(self):
        # Manually computed: [50/100, 110/200, 0/50] * 100 = [50, 55, 0]
        pos  = pd.Series([50.0, 110.0, 0.0])
        dart = pd.Series([100.0, 200.0, 50.0])
        result = calculate_tracking_ratio(pos, dart)
        assert abs(result.iloc[0] - 50.0) < 1e-9
        assert abs(result.iloc[1] - 55.0) < 1e-9
        assert abs(result.iloc[2] - 0.0)  < 1e-9

    def test_returns_series(self):
        pos  = pd.Series([100.0])
        dart = pd.Series([200.0])
        assert isinstance(calculate_tracking_ratio(pos, dart), pd.Series)
