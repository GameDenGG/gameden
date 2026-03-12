import threading
from collections import defaultdict, deque

_MAX_SAMPLES = 1000

_latency_lock = threading.Lock()
_latency_samples: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=_MAX_SAMPLES))

_cache_lock = threading.Lock()
_cache_hits: dict[str, int] = defaultdict(int)
_cache_misses: dict[str, int] = defaultdict(int)


def record_latency(endpoint: str, elapsed_ms: float) -> None:
    with _latency_lock:
        _latency_samples[endpoint].append(float(elapsed_ms))


def _percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    idx = (len(sorted_values) - 1) * q
    low = int(idx)
    high = min(low + 1, len(sorted_values) - 1)
    frac = idx - low
    return sorted_values[low] * (1.0 - frac) + sorted_values[high] * frac


def get_latency_stats() -> dict[str, dict[str, float | int]]:
    with _latency_lock:
        snapshot = {k: list(v) for k, v in _latency_samples.items()}

    out: dict[str, dict[str, float | int]] = {}
    for endpoint, values in snapshot.items():
        if not values:
            continue
        ordered = sorted(values)
        count = len(values)
        out[endpoint] = {
            "count": count,
            "avg_ms": round(sum(values) / count, 2),
            "p50_ms": round(_percentile(ordered, 0.50), 2),
            "p95_ms": round(_percentile(ordered, 0.95), 2),
            "max_ms": round(max(values), 2),
        }
    return out


def record_cache_hit(endpoint: str) -> None:
    with _cache_lock:
        _cache_hits[endpoint] += 1


def record_cache_miss(endpoint: str) -> None:
    with _cache_lock:
        _cache_misses[endpoint] += 1


def get_cache_stats() -> dict[str, dict[str, float | int]]:
    with _cache_lock:
        endpoints = set(_cache_hits.keys()) | set(_cache_misses.keys())
        hits_copy = dict(_cache_hits)
        misses_copy = dict(_cache_misses)

    out: dict[str, dict[str, float | int]] = {}
    for endpoint in sorted(endpoints):
        hits = int(hits_copy.get(endpoint, 0))
        misses = int(misses_copy.get(endpoint, 0))
        total = hits + misses
        hit_rate = (hits / total) if total > 0 else 0.0
        out[endpoint] = {
            "hits": hits,
            "misses": misses,
            "hit_rate": round(hit_rate, 4),
        }
    return out