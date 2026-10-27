import pytest

from common import airquality as aq


def test_correction_low_range_matches_us_wide_equation():
    assert aq.epa_correct(20, 50) == pytest.approx(0.524 * 20 - 0.0862 * 50 + 5.75)


def test_correction_is_linear_at_all_concentrations():
    # The U.S.-wide form has no breakpoints: same slope in clean air and in heavy smoke.
    assert aq.epa_correct(300, 40) == pytest.approx(0.524 * 300 - 0.0862 * 40 + 5.75)


@pytest.mark.parametrize("breakpoint", [30, 50, 210, 260])
@pytest.mark.parametrize("rh", [10, 50, 90])
def test_extended_variant_is_continuous_at_breakpoints(breakpoint, rh):
    eps = 1e-9
    assert aq.epa_correct_extended(breakpoint - eps, rh) == pytest.approx(aq.epa_correct_extended(breakpoint, rh), abs=1e-6)


def test_correction_never_negative():
    assert aq.epa_correct(0, 100) == 0.0


@pytest.mark.parametrize("conc, aqi", [
    (0.0, 0), (9.0, 50), (9.1, 51), (12.0, 56), (35.4, 100), (35.5, 101),
    (55.4, 150), (55.5, 151), (125.4, 200), (225.4, 300), (325.4, 500), (900, 500),
])
def test_aqi_2024_breakpoints(conc, aqi):
    assert aq.pm25_to_aqi(conc) == aqi


def test_aqi_truncates_to_one_decimal():
    # 9.09 truncates to 9.0 (Good), not up to 9.1 (Moderate)
    assert aq.pm25_to_aqi(9.09) == 50


@pytest.mark.parametrize("a, b, expected", [
    (10, 14, True),     # within 5 ug/m3
    (100, 106, True),   # 6 apart but only 6% different
    (10, 30, False),    # 20 apart and 100% different
])
def test_ab_agreement(a, b, expected):
    assert aq.ab_agree(a, b) is expected


def test_categories():
    assert aq.aqi_category(50) == "Good"
    assert aq.aqi_category(151) == "Unhealthy"
    assert aq.aqi_category(500) == "Hazardous"
