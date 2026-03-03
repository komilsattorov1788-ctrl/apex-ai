"""
APEX AI - Metrics Collector
==============================
Prometheus-style real-time metrics for the entire Data Plane.

Exposes:
  - Request rates (req/sec, req/min)
  - Cache hit/miss rates
  - Model latency percentiles (p50, p95, p99)
  - Error rates per provider
  - Token usage & billing totals
  - Governor utilization
  - System health score (0-100)
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("apex.data_plane.metrics")


# ─────────────────────────────────────────────────
# ROLLING COUNTER (thread-safe sliding window)
# ─────────────────────────────────────────────────
class RollingCounter:
    def __init__(self, window_seconds: int = 60):
        self.window = window_seconds
        self._events: deque = deque()
        self._total = 0

    def record(self, value: float = 1.0):
        now = time.time()
        self._events.append((now, value))
        self._total += value
        self._evict(now)

    def rate_per_second(self) -> float:
        now = time.time()
        self._evict(now)
        if not self._events:
            return 0.0
        window_sum = sum(v for _, v in self._events)
        return round(window_sum / self.window, 3)

    def rate_per_minute(self) -> float:
        return round(self.rate_per_second() * 60, 2)

    def window_total(self) -> float:
        now = time.time()
        self._evict(now)
        return sum(v for _, v in self._events)

    def total(self) -> float:
        return self._total

    def _evict(self, now: float):
        cutoff = now - self.window
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()


# ─────────────────────────────────────────────────
# LATENCY HISTOGRAM
# ─────────────────────────────────────────────────
class LatencyHistogram:
    def __init__(self, window_size: int = 1000):
        self._samples: deque = deque(maxlen=window_size)
        self._total_ms = 0.0
        self._count = 0

    def record(self, latency_ms: float):
        self._samples.append(latency_ms)
        self._total_ms += latency_ms
        self._count += 1

    def p50(self) -> float:
        return self._percentile(50)

    def p95(self) -> float:
        return self._percentile(95)

    def p99(self) -> float:
        return self._percentile(99)

    def avg(self) -> float:
        if not self._samples:
            return 0.0
        return round(sum(self._samples) / len(self._samples), 1)

    def _percentile(self, pct: int) -> float:
        if not self._samples:
            return 0.0
        sorted_samples = sorted(self._samples)
        idx = int(len(sorted_samples) * pct / 100)
        idx = min(idx, len(sorted_samples) - 1)
        return round(sorted_samples[idx], 1)

    def to_dict(self) -> dict:
        return {
            "p50_ms":  self.p50(),
            "p95_ms":  self.p95(),
            "p99_ms":  self.p99(),
            "avg_ms":  self.avg(),
            "samples": len(self._samples),
        }


# ─────────────────────────────────────────────────
# PER-MODEL METRICS
# ─────────────────────────────────────────────────
@dataclass
class ModelMetrics:
    model_id: str
    requests:        RollingCounter = field(default_factory=RollingCounter)
    errors:          RollingCounter = field(default_factory=RollingCounter)
    latency:         LatencyHistogram = field(default_factory=LatencyHistogram)
    tokens_in:       RollingCounter = field(default_factory=lambda: RollingCounter(3600))
    tokens_out:      RollingCounter = field(default_factory=lambda: RollingCounter(3600))
    cost_usd:        float = 0.0

    def record_request(self, latency_ms: float, tokens_in: int, tokens_out: int,
                       cost_usd: float, success: bool):
        self.requests.record()
        self.latency.record(latency_ms)
        self.tokens_in.record(tokens_in)
        self.tokens_out.record(tokens_out)
        self.cost_usd += cost_usd
        if not success:
            self.errors.record()

    def error_rate(self) -> float:
        reqs = self.requests.window_total()
        if reqs == 0:
            return 0.0
        return round(self.errors.window_total() / reqs * 100, 2)

    def to_dict(self) -> dict:
        return {
            "model_id":          self.model_id,
            "req_per_sec":       self.requests.rate_per_second(),
            "req_per_min":       self.requests.rate_per_minute(),
            "total_requests":    int(self.requests.total()),
            "error_rate_pct":    self.error_rate(),
            "latency":           self.latency.to_dict(),
            "tokens_in_per_min": round(self.tokens_in.rate_per_minute(), 0),
            "tokens_out_per_min":round(self.tokens_out.rate_per_minute(), 0),
            "total_cost_usd":    round(self.cost_usd, 6),
        }


# ─────────────────────────────────────────────────
# MAIN METRICS COLLECTOR
# ─────────────────────────────────────────────────
class MetricsCollector:
    def __init__(self):
        # Global counters
        self._requests      = RollingCounter(60)
        self._cache_hits    = RollingCounter(60)
        self._cache_misses  = RollingCounter(60)
        self._errors        = RollingCounter(60)
        self._latency       = LatencyHistogram(5000)
        self._tokens_total  = RollingCounter(3600)
        self._cost_total_usd = 0.0

        # Per-model
        self._models: dict[str, ModelMetrics] = {}
        self._lock = asyncio.Lock()
        self._start_time = time.time()

        # Request size distribution
        self._req_sizes: deque = deque(maxlen=1000)

    def _get_model_metrics(self, model_id: str) -> ModelMetrics:
        if model_id not in self._models:
            self._models[model_id] = ModelMetrics(model_id=model_id)
        return self._models[model_id]

    async def record_inference(
        self,
        model_id: str,
        latency_ms: float,
        input_tokens: int,
        output_tokens: int,
        cost_usd: float,
        cache_hit: bool,
        success: bool = True,
    ):
        async with self._lock:
            self._requests.record()
            self._latency.record(latency_ms)
            self._tokens_total.record(input_tokens + output_tokens)

            if cache_hit:
                self._cache_hits.record()
            else:
                self._cache_misses.record()

            if not success:
                self._errors.record()
            else:
                self._cost_total_usd += cost_usd

            model = self._get_model_metrics(model_id)
            model.record_request(latency_ms, input_tokens, output_tokens, cost_usd, success)

    async def record_request_size(self, message_len: int):
        async with self._lock:
            self._req_sizes.append(message_len)

    # ─────────────────────────────────────────────
    # HEALTH SCORE
    # ─────────────────────────────────────────────
    def compute_health_score(self) -> int:
        score = 100

        # Error rate penalty
        total = self._requests.window_total()
        if total > 0:
            error_rate = self._errors.window_total() / total
            score -= int(error_rate * 100)

        # Latency penalty
        p99 = self._latency.p99()
        if p99 > 5000:
            score -= 30
        elif p99 > 2000:
            score -= 15
        elif p99 > 1000:
            score -= 5

        # Cache efficiency bonus
        cache_total = self._cache_hits.window_total() + self._cache_misses.window_total()
        if cache_total > 10:
            hit_rate = self._cache_hits.window_total() / cache_total
            if hit_rate > 0.5:
                score += 5

        return max(0, min(100, score))

    # ─────────────────────────────────────────────
    # REPORT
    # ─────────────────────────────────────────────
    async def get_dashboard(self) -> dict:
        async with self._lock:
            uptime_s = time.time() - self._start_time
            cache_total = self._cache_hits.window_total() + self._cache_misses.window_total()
            cache_hit_rate = (
                round(self._cache_hits.window_total() / cache_total * 100, 1)
                if cache_total > 0 else 0.0
            )

            avg_req_size = (
                round(sum(self._req_sizes) / len(self._req_sizes), 0)
                if self._req_sizes else 0
            )

            return {
                "health_score":     self.compute_health_score(),
                "uptime_s":         round(uptime_s, 0),
                "uptime_human":     self._format_uptime(uptime_s),

                "traffic": {
                    "req_per_sec":       self._requests.rate_per_second(),
                    "req_per_min":       self._requests.rate_per_minute(),
                    "total_requests":    int(self._requests.total()),
                    "error_rate_pct":    round(
                        self._errors.window_total() / max(self._requests.window_total(), 1) * 100, 2),
                    "avg_message_chars": int(avg_req_size),
                },

                "latency": {
                    "p50_ms": self._latency.p50(),
                    "p95_ms": self._latency.p95(),
                    "p99_ms": self._latency.p99(),
                    "avg_ms": self._latency.avg(),
                },

                "cache": {
                    "hit_rate_pct":  cache_hit_rate,
                    "hits_per_min":  self._cache_hits.rate_per_minute(),
                    "misses_per_min":self._cache_misses.rate_per_minute(),
                    "total_hits":    int(self._cache_hits.total()),
                    "cost_saved_usd":round(self._cache_hits.total() * 0.005, 4),
                },

                "billing": {
                    "total_cost_usd":    round(self._cost_total_usd, 4),
                    "tokens_per_min":    round(self._tokens_total.rate_per_minute(), 0),
                    "total_tokens":      int(self._tokens_total.total()),
                    "avg_cost_per_req":  round(
                        self._cost_total_usd / max(int(self._requests.total()), 1), 6),
                },

                "models": {
                    mid: m.to_dict()
                    for mid, m in self._models.items()
                },
            }

    def _format_uptime(self, seconds: float) -> str:
        s = int(seconds)
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        if h > 0:
            return f"{h}h {m}m {sec}s"
        elif m > 0:
            return f"{m}m {sec}s"
        return f"{sec}s"

    async def get_prometheus_text(self) -> str:
        dashboard = await self.get_dashboard()
        lines = [
            "# APEX AI Metrics",
            f"apex_health_score {dashboard['health_score']}",
            f"apex_requests_total {dashboard['traffic']['total_requests']}",
            f"apex_requests_per_second {dashboard['traffic']['req_per_sec']}",
            f"apex_error_rate_pct {dashboard['traffic']['error_rate_pct']}",
            f"apex_latency_p50_ms {dashboard['latency']['p50_ms']}",
            f"apex_latency_p95_ms {dashboard['latency']['p95_ms']}",
            f"apex_latency_p99_ms {dashboard['latency']['p99_ms']}",
            f"apex_cache_hit_rate_pct {dashboard['cache']['hit_rate_pct']}",
            f"apex_cost_total_usd {dashboard['billing']['total_cost_usd']}",
            f"apex_tokens_total {dashboard['billing']['total_tokens']}",
        ]
        for mid, m in dashboard["models"].items():
            safe = mid.replace("-", "_").replace("/", "_")
            lines.append(f'apex_model_requests_total{{model="{mid}"}} {m["total_requests"]}')
            lines.append(f'apex_model_latency_p99_ms{{model="{mid}"}} {m["latency"]["p99_ms"]}')
            lines.append(f'apex_model_error_rate_pct{{model="{mid}"}} {m["error_rate_pct"]}')
        return "\n".join(lines)


# ─────────────────────────────────────────────────
# SINGLETON
# ─────────────────────────────────────────────────
_collector: Optional[MetricsCollector] = None

def get_metrics_collector() -> MetricsCollector:
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
        logger.info("[MetricsCollector] Initialized. Dashboard + Prometheus metrics ready.")
    return _collector
