from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional

from metrics import MetricsObserver


class HashTableBase(ABC):
    """Common interface for hash table implementations used by the benchmark."""

    def __init__(self, observer: Optional[MetricsObserver] = None) -> None:
        self.observer = observer or MetricsObserver()

    @abstractmethod
    def insert(self, key: Any, value: Any) -> bool:
        """Insert a key-value pair. Return False if key already exists."""

    @abstractmethod
    def find(self, key: Any) -> Optional[Any]:
        """Find value by key. Return None if not found."""

    @abstractmethod
    def delete(self, key: Any) -> bool:
        """Delete key. Return True if deleted, else False."""

    @abstractmethod
    def __len__(self) -> int:
        """Return number of stored elements."""

    @abstractmethod
    def capacity(self) -> int:
        """Return total number of slots available for elements."""

    def load_factor(self) -> float:
        cap = self.capacity()
        return 0.0 if cap == 0 else len(self) / cap

    def reset_metrics(self) -> None:
        self.observer.reset()

    def metrics_snapshot(self) -> dict[str, int]:
        return self.observer.snapshot()
