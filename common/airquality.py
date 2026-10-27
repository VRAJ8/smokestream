"""Reference (plain Python) versions of the air quality math.

transforms.py implements the same formulas as Spark column expressions, and
tests/test_transforms.py checks that the two agree. Change both together.
"""

import math

# Readings outside this range are sensor faults, not air.
PM_MIN_VALID = 0.0
PM_MAX_VALID = 1000.0

# A/B channel agreement (Barkjohn et al., 2021): a reading is dropped only when
# the two laser channels differ by more than 5 ug/m3 AND by more than 70%.
AB_ABS_LIMIT = 5.0
AB_PCT_LIMIT = 0.70

# 2024 U.S. EPA AQI breakpoints for PM2.5 (ug/m3), in effect since May 6, 2024.
AQI_BREAKPOINTS = [
    (0.0, 9.0, 0, 50),
    (9.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 125.4, 151, 200),
    (125.5, 225.4, 201, 300),
    (225.5, 325.4, 301, 500),
]
AQI_CATEGORIES = [
    (50, "Good"),
    (100, "Moderate"),
    (150, "Unhealthy for Sensitive Groups"),
    (200, "Unhealthy"),
    (300, "Very Unhealthy"),
    (500, "Hazardous"),
]


def is_valid(a, b, rh):
    """True when both channels and humidity are present and physically plausible."""
    if a is None or b is None or rh is None:
        return False
    return (PM_MIN_VALID <= a <= PM_MAX_VALID and PM_MIN_VALID <= b <= PM_MAX_VALID
            and 0.0 <= rh <= 100.0)


def ab_agree(a, b):
    diff = abs(a - b)
    mean = (a + b) / 2.0
    pct = diff / mean if mean > 0 else 0.0
    return diff <= AB_ABS_LIMIT or pct <= AB_PCT_LIMIT


def epa_correct(pa, rh):
    """EPA extended U.S.-wide PurpleAir correction (Barkjohn et al. 2021, 2022).

    pa is the mean of the A and B channel PM2.5 (cf_1) readings and rh is the
    sensor's relative humidity in percent. Above 50 ug/m3 the equation blends
    into a steeper slope and then a quadratic fit for extreme smoke. Each blend
    weight w runs from 0 to 1 across its band, so the curve is continuous.
    The result is floored at 0, since the RH term can push clean-air values
    slightly negative.
    """
    if pa < 30:
        value = 0.524 * pa - 0.0862 * rh + 5.75
    elif pa < 50:
        w = pa / 20 - 1.5
        value = (0.786 * w + 0.524 * (1 - w)) * pa - 0.0862 * rh + 5.75
    elif pa < 210:
        value = 0.786 * pa - 0.0862 * rh + 5.75
    elif pa < 260:
        w = pa / 50 - 4.2
        value = ((0.69 * w + 0.786 * (1 - w)) * pa - 0.0862 * rh * (1 - w)
                 + 2.966 * w + 5.75 * (1 - w) + 8.84e-4 * pa ** 2 * w)
    else:
        value = 2.966 + 0.69 * pa + 8.84e-4 * pa ** 2
    return max(0.0, value)


def pm25_to_aqi(conc):
    """PM2.5 concentration to AQI. EPA truncates to 0.1 ug/m3 and rounds the index."""
    c = math.floor(conc * 10) / 10
    if c < 0:
        return 0
    for c_lo, c_hi, i_lo, i_hi in AQI_BREAKPOINTS:
        if c <= c_hi:
            # Round half up, matching Spark's round().
            return int(math.floor((i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo + 0.5))
    return 500  # beyond the top of the index


def aqi_category(aqi):
    for upper, name in AQI_CATEGORIES:
        if aqi <= upper:
            return name
    return "Hazardous"
