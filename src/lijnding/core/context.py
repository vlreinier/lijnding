from __future__ import annotations
import multiprocessing
from typing import Any, Optional, Dict

class PipelineContext:
    """
    A process-safe context for sharing state between pipeline stages.
    """
    def __init__(self, manager: Optional[multiprocessing.Manager] = None):
        if manager is None:
            # This is not ideal for production, but allows for easy testing
            # of non-multiprocessing pipelines.
            self._manager = multiprocessing.Manager()
        else:
            self._manager = manager

        self._state: Dict[str, Any] = self._manager.dict()
        self._lock = self._manager.Lock()

    @property
    def state(self) -> Dict[str, Any]:
        return self._state

    def get(self, key: str, default: Any = None) -> Any:
        return self._state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._state[key] = value

    def inc(self, key: str, amount: int = 1) -> int:
        with self._lock:
            current_value = self._state.get(key, 0)
            new_value = current_value + amount
            self._state[key] = new_value
            return new_value
