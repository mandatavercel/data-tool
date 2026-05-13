"""Tests for modules/core/normalizer.py

Golden dataset philosophy:
- Concrete input/output pairs verified by hand
- Test every named unit in _UNIT_MULTIPLIERS
- Test date parser with every format the app encounters
"""
import math
import pandas as pd

from modules.common.core.normalizer import (
    normalize_date,
    normalize_numeric,
    normalize_amount_to_krw,
    normalize_score,
    infer_amount_unit,
)


# ── normalize_date ─────────────────────────────────────────────────────────────

class TestNormalizeDate:
    def test_yyyymmdd_integer_string(self):
        # Most common POS format: "20200101"
        s = pd.Series(["20200101"])
        result = normalize_date(s)
        assert result.iloc[0] == pd.Timestamp("2020-01-01")

    def test_yyyymmdd_integer_column(self):
        # Some files store as integer 20200101
        s = pd.Series([20200101, 20200201])
        result = normalize_date(s)
        assert result.iloc[0] == pd.Timestamp("2020-01-01")
        assert result.iloc[1] == pd.Timestamp("2020-02-01")

    def test_iso_format_dash(self):
        s = pd.Series(["2021-06-15"])
        result = normalize_date(s)
        assert result.iloc[0] == pd.Timestamp("2021-06-15")

    def test_iso_format_slash(self):
        s = pd.Series(["2021/06/15"])
        result = normalize_date(s)
        assert result.iloc[0] == pd.Timestamp("2021-06-15")

    def test_already_datetime(self):
        s = pd.Series(pd.to_datetime(["2022-03-10"]))
        result = normalize_date(s)
        assert result.iloc[0] == pd.Timestamp("2022-03-10")

    def test_invalid_returns_nat(self):
        s = pd.Series(["not_a_date"])
        result = normalize_date(s)
        assert pd.isna(result.iloc[0])

    def test_mixed_valid_invalid(self):
        s = pd.Series(["20200101", "bad", "20210315"])
        result = normalize_date(s)
        assert result.iloc[0] == pd.Timestamp("2020-01-01")
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == pd.Timestamp("2021-03-15")

    def test_returns_datetime_dtype(self):
        s = pd.Series(["2023-01-01"])
        result = normalize_date(s)
        assert pd.api.types.is_datetime64_any_dtype(result)


# ── normalize_numeric ──────────────────────────────────────────────────────────

class TestNormalizeNumeric:
    def test_comma_string_to_float(self):
        # Comma-separated thousands: "1,234,567" → 1234567
        # Note: pandas to_numeric does NOT strip commas natively.
        # normalize_numeric handles strings via to_numeric(errors='coerce').
        # Comma strings will be NaN — the caller must strip commas first.
        # This test confirms the behaviour is documented (NaN, not crash).
        s = pd.Series(["1,234,567"])
        result = normalize_numeric(s)
        assert math.isnan(result.iloc[0])

    def test_plain_numeric_string(self):
        s = pd.Series(["1234567"])
        result = normalize_numeric(s)
        assert abs(result.iloc[0] - 1_234_567) < 1

    def test_integer_column(self):
        s = pd.Series([100, 200, 300])
        result = normalize_numeric(s)
        assert result.tolist() == [100.0, 200.0, 300.0]

    def test_float_column(self):
        s = pd.Series([1.5, 2.5, 3.5])
        result = normalize_numeric(s)
        assert result.tolist() == [1.5, 2.5, 3.5]

    def test_non_numeric_becomes_nan(self):
        s = pd.Series(["abc", "xyz"])
        result = normalize_numeric(s)
        assert all(math.isnan(v) for v in result)

    def test_fill_value_replaces_nan(self):
        s = pd.Series([1.0, float("nan"), 3.0])
        result = normalize_numeric(s, fill_value=0.0)
        assert result.iloc[1] == 0.0

    def test_none_fill_leaves_nan(self):
        s = pd.Series([float("nan")])
        result = normalize_numeric(s)
        assert math.isnan(result.iloc[0])

    def test_returns_series(self):
        assert isinstance(normalize_numeric(pd.Series([1, 2])), pd.Series)


# ── normalize_amount_to_krw ────────────────────────────────────────────────────

class TestNormalizeAmountToKrw:
    """Golden dataset: exact unit conversions verified by hand."""

    def test_won_unit_unchanged(self):
        # 원 → multiplier=1 → no change
        s = pd.Series([500_000.0])
        result = normalize_amount_to_krw(s, "원")
        assert abs(result.iloc[0] - 500_000.0) < 1e-9

    def test_chonwon_unit(self):
        # 100 천원 = 100,000 원
        s = pd.Series([100.0])
        result = normalize_amount_to_krw(s, "천원")
        assert abs(result.iloc[0] - 100_000.0) < 1e-9

    def test_manwon_unit(self):
        # 50 만원 = 500,000 원
        s = pd.Series([50.0])
        result = normalize_amount_to_krw(s, "만원")
        assert abs(result.iloc[0] - 500_000.0) < 1e-9

    def test_baekmanwon_unit(self):
        # 100 백만원 = 100,000,000 원  (the key DART-POS mismatch case)
        s = pd.Series([100.0])
        result = normalize_amount_to_krw(s, "백만원")
        assert abs(result.iloc[0] - 100_000_000.0) < 1e-9

    def test_eokwon_unit(self):
        # 5 억원 = 500,000,000 원
        s = pd.Series([5.0])
        result = normalize_amount_to_krw(s, "억원")
        assert abs(result.iloc[0] - 500_000_000.0) < 1e-9

    def test_sibeokwon_unit(self):
        # 2 십억원 = 2,000,000,000 원
        s = pd.Series([2.0])
        result = normalize_amount_to_krw(s, "십억원")
        assert abs(result.iloc[0] - 2_000_000_000.0) < 1e-9

    def test_unknown_unit_passthrough(self):
        # Unknown unit → multiplier=1, no transformation
        s = pd.Series([999.0])
        result = normalize_amount_to_krw(s, "미지정")
        assert abs(result.iloc[0] - 999.0) < 1e-9

    def test_multiple_rows(self):
        s = pd.Series([1.0, 2.0, 3.0])
        result = normalize_amount_to_krw(s, "백만원")
        expected = [1_000_000.0, 2_000_000.0, 3_000_000.0]
        for got, exp in zip(result, expected):
            assert abs(got - exp) < 1e-9


# ── normalize_score ────────────────────────────────────────────────────────────

class TestNormalizeScore:
    def test_midpoint_returns_50(self):
        # val == lo + (hi-lo)/2 = 0 with default lo=-50, hi=50
        assert abs(normalize_score(0.0) - 50.0) < 1e-9

    def test_lo_returns_0(self):
        assert abs(normalize_score(-50.0, lo=-50.0, hi=50.0) - 0.0) < 1e-9

    def test_hi_returns_100(self):
        assert abs(normalize_score(50.0, lo=-50.0, hi=50.0) - 100.0) < 1e-9

    def test_below_lo_clamped_to_0(self):
        assert abs(normalize_score(-100.0, lo=-50.0, hi=50.0) - 0.0) < 1e-9

    def test_above_hi_clamped_to_100(self):
        assert abs(normalize_score(200.0, lo=-50.0, hi=50.0) - 100.0) < 1e-9

    def test_nan_returns_50(self):
        assert abs(normalize_score(float("nan")) - 50.0) < 1e-9

    def test_custom_range(self):
        # lo=0, hi=100: val=25 → 25%
        assert abs(normalize_score(25.0, lo=0.0, hi=100.0) - 25.0) < 1e-9


# ── infer_amount_unit ──────────────────────────────────────────────────────────

class TestInferAmountUnit:
    """
    Golden dataset: DART is in 원(KRW).
    POS/DART mean ratio determines unit mismatch.

    Ranges (from normalizer.py):
        0.1–10×    → 원   (no mismatch)
        80–120×    → 천원 (POS in K KRW)
        800–1200×  → 백만원 (POS in M KRW)
        8000–12000× → 억원 (POS in 100M KRW)
    """

    def test_no_mismatch(self):
        # ratio ≈ 1 → same unit
        result = infer_amount_unit(pos_mean=1_000.0, dart_mean=1_000.0)
        assert result["unit_type"] == "원"
        assert result["is_mismatch"] is False

    def test_chonwon_mismatch(self):
        # POS mean = 100,000 KRW expressed as 1,000 천원 vs DART mean = 1,000,000 KRW
        # ratio = 1000 / 10 = 100× → 천원 detected
        result = infer_amount_unit(pos_mean=1_000.0, dart_mean=10.0)
        assert result["unit_type"] == "천원"
        assert result["is_mismatch"] is True

    def test_baekmanwon_mismatch(self):
        # ratio ≈ 1,000× → 백만원
        result = infer_amount_unit(pos_mean=1_000.0, dart_mean=1.0)
        assert result["unit_type"] == "백만원"
        assert result["is_mismatch"] is True

    def test_eokwon_mismatch(self):
        # ratio ≈ 10,000× → 억원
        result = infer_amount_unit(pos_mean=10_000.0, dart_mean=1.0)
        assert result["unit_type"] == "억원"
        assert result["is_mismatch"] is True

    def test_dart_mean_zero_returns_unknown(self):
        result = infer_amount_unit(pos_mean=100.0, dart_mean=0.0)
        assert result["unit_type"] == "unknown"
        assert result["is_mismatch"] is False

    def test_nan_inputs_return_unknown(self):
        result = infer_amount_unit(float("nan"), float("nan"))
        assert result["unit_type"] == "unknown"

    def test_ratio_key_present(self):
        result = infer_amount_unit(pos_mean=500.0, dart_mean=500.0)
        assert "ratio" in result
        assert abs(result["ratio"] - 1.0) < 1e-9

    def test_note_is_string(self):
        result = infer_amount_unit(1000.0, 1.0)
        assert isinstance(result["note"], str)
        assert len(result["note"]) > 0
