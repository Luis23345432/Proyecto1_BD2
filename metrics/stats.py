from __future__ import annotations

import time
from typing import Dict, Any
from contextlib import contextmanager


class StatsManager:
    def __init__(self):
        self.counters: Dict[str, int] = {}
        self.timers: Dict[str, float] = {}  # Acumulado en segundos
        self.timer_calls: Dict[str, int] = {}  # Número de llamadas
        self._active_timers: Dict[str, float] = {}  # Timers activos (para contexto)

    def reset(self):
        self.counters.clear()
        self.timers.clear()
        self.timer_calls.clear()
        self._active_timers.clear()

    # ========================================
    # CONTADORES
    # ========================================

    def inc(self, key: str, amount: int = 1):
        self.counters[key] = self.counters.get(key, 0) + amount

    def get_counter(self, key: str) -> int:
        return self.counters.get(key, 0)

    # ========================================
    # TIMERS
    # ========================================

    @contextmanager
    def timer(self, key: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            self.timers[key] = self.timers.get(key, 0.0) + elapsed
            self.timer_calls[key] = self.timer_calls.get(key, 0) + 1

    def get_time(self, key: str) -> float:
        return self.timers.get(key, 0.0)

    def get_time_ms(self, key: str) -> float:
        return self.timers.get(key, 0.0) * 1000

    def get_avg_time_ms(self, key: str) -> float:
        total_time = self.timers.get(key, 0.0)
        calls = self.timer_calls.get(key, 0)
        if calls == 0:
            return 0.0
        return (total_time / calls) * 1000

    # ========================================
    # REPORTES
    # ========================================

    def get_stats(self) -> Dict[str, Any]:
        return {
            "counters": dict(self.counters),
            "timers": {
                k: {
                    "total_ms": round(v * 1000, 3),
                    "calls": self.timer_calls.get(k, 0),
                    "avg_ms": round(self.get_avg_time_ms(k), 3)
                }
                for k, v in self.timers.items()
            }
        }

    def get_index_stats(self, index_name: str) -> Dict[str, Any]:
        prefix = f"{index_name}."

        return {
            "operations": {
                "insert": {
                    "count": self.get_counter(f"{prefix}insert.calls"),
                    "total_ms": round(self.get_time_ms(f"{prefix}insert.time"), 3),
                    "avg_ms": round(self.get_avg_time_ms(f"{prefix}insert.time"), 3),
                },
                "search": {
                    "count": self.get_counter(f"{prefix}search.calls"),
                    "total_ms": round(self.get_time_ms(f"{prefix}search.time"), 3),
                    "avg_ms": round(self.get_avg_time_ms(f"{prefix}search.time"), 3),
                },
                "range_search": {
                    "count": self.get_counter(f"{prefix}range.calls"),
                    "total_ms": round(self.get_time_ms(f"{prefix}range.time"), 3),
                    "avg_ms": round(self.get_avg_time_ms(f"{prefix}range.time"), 3),
                },
                "delete": {
                    "count": self.get_counter(f"{prefix}delete.calls"),
                    "total_ms": round(self.get_time_ms(f"{prefix}delete.time"), 3),
                    "avg_ms": round(self.get_avg_time_ms(f"{prefix}delete.time"), 3),
                },
            },
            "disk_access": {
                "reads": self.get_counter(f"{prefix}disk.reads"),
                "writes": self.get_counter(f"{prefix}disk.writes"),
                "total": self.get_counter(f"{prefix}disk.reads") + self.get_counter(f"{prefix}disk.writes"),
            }
        }

    def print_summary(self):
        print("\n" + "=" * 60)
        print("PERFORMANCE METRICS SUMMARY")
        print("=" * 60)

        if self.counters:
            print("\nCOUNTERS:")
            for key, value in sorted(self.counters.items()):
                print(f"  {key}: {value}")

        if self.timers:
            print("\nTIMERS:")
            for key in sorted(self.timers.keys()):
                total_ms = self.get_time_ms(key)
                calls = self.timer_calls.get(key, 0)
                avg_ms = self.get_avg_time_ms(key)
                print(f"  {key}:")
                print(f"    Total: {total_ms:.3f}ms")
                print(f"    Calls: {calls}")
                print(f"    Avg: {avg_ms:.3f}ms")

        print("=" * 60 + "\n")


# Instancia global
stats = StatsManager()