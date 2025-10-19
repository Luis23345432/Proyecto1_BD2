from __future__ import annotations

import csv
import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional


class StatsManager:
    """
    Thread-safe counters and timers registry.
    - counters: name -> int
    - timers: name -> total_seconds (float)
    """

    _instance: Optional["StatsManager"] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self.counters: Dict[str, int] = {}
        self.timers: Dict[str, float] = {}
        self.metadata: Dict[str, Any] = {}
        self._mtx = threading.Lock()

    @classmethod
    def instance(cls) -> "StatsManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = StatsManager()
            return cls._instance

    # counters
    def inc(self, name: str, amount: int = 1) -> None:
        with self._mtx:
            self.counters[name] = self.counters.get(name, 0) + amount

    def add(self, name: str, amount: int) -> None:
        self.inc(name, amount)

    def get(self, name: str, default: int = 0) -> int:
        with self._mtx:
            return self.counters.get(name, default)

    # timers
    @contextmanager
    def timer(self, name: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            with self._mtx:
                self.timers[name] = self.timers.get(name, 0.0) + elapsed
                # also count occurrences for avg
                self.counters[f"{name}#count"] = self.counters.get(f"{name}#count", 0) + 1

    def add_time(self, name: str, seconds: float) -> None:
        with self._mtx:
            self.timers[name] = self.timers.get(name, 0.0) + seconds

    def snapshot(self) -> Dict[str, Any]:
        with self._mtx:
            return {
                "counters": dict(self.counters),
                "timers": dict(self.timers),
                "metadata": dict(self.metadata),
            }

    def reset(self) -> None:
        with self._mtx:
            self.counters.clear()
            self.timers.clear()

    def set_meta(self, key: str, value: Any) -> None:
        with self._mtx:
            self.metadata[key] = value

    def save_json(self, path: str) -> None:
        blob = self.snapshot()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)

    def save_csv(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        snap = self.snapshot()
        # Flatten: counters and timers into rows key,value,type
        rows = []
        for k, v in snap["counters"].items():
            rows.append({"name": k, "value": v, "type": "counter"})
        for k, v in snap["timers"].items():
            rows.append({"name": k, "value": v, "type": "timer"})
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["name", "value", "type"])
            writer.writeheader()
            writer.writerows(rows)


# convenient singleton
stats = StatsManager.instance()
