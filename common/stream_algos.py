"""Bounded-memory stream summaries that run beside the pipeline.

  BloomFilter          drop readings Kafka delivered twice (at-least-once delivery)
  RotatingBloom        the same over an unbounded stream: two filters, swapped every period
  FlajoletMartin       distinct sensors reporting per hour, without keeping their ids
  DGIM                 how many of the last N readings were elevated, in O(log^2 N) bits
  ReservoirSample      a uniform sample of elevated events for blind labeling, without
                       knowing in advance how many events the season will have

All hashing is keyed blake2b, so results are reproducible across runs and machines
(Python's hash() is salted per process).
"""

import math
import random
from hashlib import blake2b


def _hashes(key, n, salt=b""):
    """n independent 32-bit hashes of key."""
    out, i = [], 0
    while len(out) < n:
        d = blake2b(repr(key).encode(), digest_size=64, salt=salt + i.to_bytes(4, "big")).digest()
        out += [int.from_bytes(d[j:j + 4], "big") for j in range(0, 64, 4)]
        i += 1
    return out[:n]


class BloomFilter:
    """Set membership with no false negatives and a tunable false-positive rate."""

    def __init__(self, capacity, fp_rate=0.001):
        self.m = max(8, math.ceil(-capacity * math.log(fp_rate) / math.log(2) ** 2))
        self.k = max(1, round(self.m / capacity * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)

    def _positions(self, key):
        d = blake2b(repr(key).encode(), digest_size=16).digest()
        h1, h2 = int.from_bytes(d[:8], "big"), int.from_bytes(d[8:], "big") | 1
        return [(h1 + i * h2) % self.m for i in range(self.k)]  # double hashing

    def __contains__(self, key):
        return all(self.bits[p >> 3] >> (p & 7) & 1 for p in self._positions(key))

    def add(self, key):
        for p in self._positions(key):
            self.bits[p >> 3] |= 1 << (p & 7)

    @property
    def nbytes(self):
        return len(self.bits)


class RotatingBloom:
    """Duplicate detection over an unbounded stream. A key is remembered for at least
    one period and at most two, so the period must exceed the longest redelivery delay."""

    def __init__(self, capacity_per_period, period_s, fp_rate=0.001):
        self.capacity, self.period, self.fp = capacity_per_period, period_s, fp_rate
        self.current, self.previous, self.started = BloomFilter(capacity_per_period, fp_rate), None, None

    def seen_before(self, key, now):
        """True if key was (probably) seen; otherwise remember it and return False."""
        if self.started is None:
            self.started = now
        if now - self.started >= self.period:
            self.previous, self.current, self.started = self.current, BloomFilter(self.capacity, self.fp), now
        if key in self.current or (self.previous is not None and key in self.previous):
            return True
        self.current.add(key)
        return False

    @property
    def nbytes(self):
        return self.current.nbytes * 2


class FlajoletMartin:
    """Distinct-count estimate, Flajolet and Martin (1985).

    Each hash function keeps a bitmap of the trailing-zero counts it has seen; R is
    the lowest bit still unset, and 2^R / 0.77351 estimates the distinct count.
    R is averaged within groups of hash functions and the median taken across groups.
    """

    PHI = 0.77351

    def __init__(self, groups=8, per_group=8, seed=0):
        self.groups, self.per_group = groups, per_group
        self.salt = seed.to_bytes(4, "big")
        self.bitmaps = [0] * (groups * per_group)

    @staticmethod
    def _tail_zeros(x):
        return 31 if x == 0 else min(31, (x & -x).bit_length() - 1)

    def add(self, key):
        for i, h in enumerate(_hashes(key, len(self.bitmaps), self.salt)):
            self.bitmaps[i] |= 1 << self._tail_zeros(h)

    def estimate(self):
        lowest_unset = [(~b & (b + 1)).bit_length() - 1 for b in self.bitmaps]
        means = sorted(sum(lowest_unset[g * self.per_group:(g + 1) * self.per_group]) / self.per_group
                       for g in range(self.groups))
        mid = len(means) // 2
        r = means[mid] if len(means) % 2 else (means[mid - 1] + means[mid]) / 2
        return 2 ** r / self.PHI

    @property
    def nbytes(self):
        return 4 * len(self.bitmaps)  # one 32-bit bitmap per hash function


class DGIM:
    """Count of 1s among the last N bits of a stream, within 50% (r=2 buckets per size).

    Buckets hold (timestamp of their most recent 1, size); sizes are powers of two and
    at most r buckets share a size, so memory is O(log^2 N) bits instead of N.
    """

    def __init__(self, n, r=2):
        self.n, self.r, self.t, self.buckets = n, r, 0, []  # newest bucket first

    def add(self, bit):
        self.t += 1
        if self.buckets and self.buckets[-1][0] <= self.t - self.n:
            self.buckets.pop()  # oldest bucket fell out of the window
        if not bit:
            return
        self.buckets.insert(0, (self.t, 1))
        size, i = 1, 0
        while True:
            same = [j for j in range(i, len(self.buckets)) if self.buckets[j][1] == size]
            if len(same) <= self.r:
                break
            a, b = same[-2], same[-1]  # merge the two oldest of this size
            self.buckets[a] = (self.buckets[a][0], size * 2)
            del self.buckets[b]
            size, i = size * 2, a

    def count(self):
        if not self.buckets:
            return 0.0
        return sum(s for _, s in self.buckets) - self.buckets[-1][1] / 2  # half of the oldest bucket

    @property
    def nbytes(self):
        # each bucket: a timestamp mod N (log2 N bits) and a size exponent (log2 log2 N bits)
        bits = math.ceil(math.log2(self.n)) + math.ceil(math.log2(max(2, math.log2(self.n))))
        return math.ceil(len(self.buckets) * bits / 8)


class ReservoirSample:
    """Algorithm R: after n items, each one is in the sample with probability k/n."""

    def __init__(self, k, seed=None):
        self.k, self.n, self.items, self.rng = k, 0, [], random.Random(seed)

    def add(self, item):
        self.n += 1
        if len(self.items) < self.k:
            self.items.append(item)
        else:
            j = self.rng.randrange(self.n)
            if j < self.k:
                self.items[j] = item
