"""Streaming summaries against exact answers on known streams."""

import random
from collections import deque

from common.stream_algos import DGIM, BloomFilter, FlajoletMartin, ReservoirSample, RotatingBloom


def test_bloom_has_no_false_negatives_and_about_the_target_false_positive_rate():
    bf = BloomFilter(10_000, fp_rate=0.01)
    for i in range(10_000):
        bf.add(("in", i))
    assert all(("in", i) in bf for i in range(10_000))
    fp = sum(("out", i) in bf for i in range(20_000)) / 20_000
    assert fp < 0.02


def test_rotating_bloom_forgets_only_after_two_periods():
    rb = RotatingBloom(100, period_s=60)
    assert not rb.seen_before("a", now=0)
    assert rb.seen_before("a", now=90)        # previous period still remembered
    rb.seen_before("b", now=155)              # rotates again; "a" was in the dropped filter
    assert not rb.seen_before("a", now=156)


def test_flajolet_martin_within_25_percent():
    for n in (50, 5000):
        fm = FlajoletMartin(seed=1)
        for i in range(n):
            fm.add(i)
            fm.add(i)  # repeats do not count
        assert abs(fm.estimate() / n - 1) < 0.25


def test_dgim_stays_within_half_of_the_exact_count():
    rng, n = random.Random(0), 500
    dg, window = DGIM(n), deque(maxlen=n)
    for t in range(5000):
        bit = rng.random() < (0.7 if (t // 700) % 2 else 0.1)
        dg.add(bit)
        window.append(bit)
        exact = sum(window)
        if exact:
            assert abs(dg.count() - exact) <= 0.5 * exact
    assert len(dg.buckets) <= 2 * (n.bit_length() + 1)


def test_reservoir_keeps_every_position_equally_often():
    counts = [0] * 100
    for seed in range(2000):
        r = ReservoirSample(10, seed)
        for i in range(100):
            r.add(i)
        for i in r.items:
            counts[i] += 1
    assert min(counts) > 120 and max(counts) < 280  # expected 200 each
