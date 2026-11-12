"""Streaming algorithms used in the pipeline (course rubric: Bloom filter, DGIM, reservoir sampling).

* BloomFilter     - producers use it to tell NEW / REVISED / DUPLICATE CAISO records without keeping all history.
* DGIM            - counts 'dirty' 5-minute intervals in the last N intervals using O(log^2 N) memory.
* ReservoirSample - uniform fixed-size sample of the interval stream (used for the accuracy spot-check).
"""
import hashlib
import math
import random


class BloomFilter:
    def __init__(self, capacity: int = 200_000, fp_rate: float = 0.001):
        self.m = max(8, int(-capacity * math.log(fp_rate) / (math.log(2) ** 2)))
        self.k = max(1, round(self.m / capacity * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)

    def _positions(self, item: str):
        h = hashlib.sha256(item.encode()).digest()
        h1, h2 = int.from_bytes(h[:8], "big"), int.from_bytes(h[8:16], "big") | 1
        return [(h1 + i * h2) % self.m for i in range(self.k)]   # Kirsch-Mitzenmacher double hashing

    def add(self, item: str):
        for p in self._positions(item):
            self.bits[p >> 3] |= 1 << (p & 7)

    def __contains__(self, item: str) -> bool:
        return all(self.bits[p >> 3] & (1 << (p & 7)) for p in self._positions(item))


class RecordClassifier:
    """Two Bloom filters: one on interval keys, one on (interval, content-hash)."""
    def __init__(self, **kw):
        self.intervals, self.versions = BloomFilter(**kw), BloomFilter(**kw)

    def classify(self, interval_key: str, content: str) -> str:
        version = f"{interval_key}|{hashlib.md5(content.encode()).hexdigest()}"
        if version in self.versions:
            return "duplicate"
        status = "revised" if interval_key in self.intervals else "new"
        self.intervals.add(interval_key)
        self.versions.add(version)
        return status


class DGIM:
    """Approximate count of 1-bits in the last `window` bits; error <= 50%, typically far less."""
    def __init__(self, window: int = 288):          # 288 five-minute intervals = 24 h
        self.window, self.t, self.buckets = window, 0, []   # buckets: [timestamp, size], newest first

    def update(self, bit: int):
        self.t += 1
        if self.buckets and self.buckets[-1][0] <= self.t - self.window:
            self.buckets.pop()
        if bit:
            self.buckets.insert(0, [self.t, 1])
            size = 1
            while True:
                same = [i for i, b in enumerate(self.buckets) if b[1] == size]
                if len(same) <= 2:
                    break
                i, j = same[-2], same[-1]                  # merge the two oldest buckets of this size
                self.buckets[i] = [self.buckets[i][0], size * 2]
                del self.buckets[j]
                size *= 2

    def count(self) -> int:
        if not self.buckets:
            return 0
        return sum(b[1] for b in self.buckets) - self.buckets[-1][1] // 2


class ReservoirSample:
    def __init__(self, k: int = 500, seed: int = None):
        self.k, self.n, self.items, self.rng = k, 0, [], random.Random(seed)

    def add(self, item):
        self.n += 1
        if len(self.items) < self.k:
            self.items.append(item)
        else:
            j = self.rng.randint(0, self.n - 1)
            if j < self.k:
                self.items[j] = item
