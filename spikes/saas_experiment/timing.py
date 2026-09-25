from __future__ import annotations

import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class SampleStats:
    n: int
    median_s: float
    p95_s: float
    total_s: float

    def as_dict(self) -> dict[str, float | int]:
        return {
            "n": self.n,
            "median_s": self.median_s,
            "p95_s": self.p95_s,
            "total_s": self.total_s,
        }


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def summarize(samples_s: list[float]) -> SampleStats:
    if not samples_s:
        return SampleStats(n=0, median_s=0.0, p95_s=0.0, total_s=0.0)
    ordered = sorted(samples_s)
    return SampleStats(
        n=len(samples_s),
        median_s=float(statistics.median(ordered)),
        p95_s=percentile(ordered, 0.95),
        total_s=float(sum(samples_s)),
    )


def timed(fn: Callable[[], T]) -> tuple[T, float]:
    start = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - start
