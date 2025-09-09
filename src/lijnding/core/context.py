from __future__ import annotations

import multiprocessing as mp
import threading
from typing import Any, Dict, Optional
import logging

from .log import get_logger
from ..config import Config


class Context:
    """
    A dict-like context for sharing state and metrics across pipeline stages.
    """

    def __init__(
        self,
        mp_safe: bool = False,
        initial_data: Optional[Dict[str, Any]] = None,
        config: Optional[Config] = None,
        *,
        pipeline_name: Optional[str] = None,
        _from_proxies=None,
    ):
        self.logger: logging.Logger = get_logger("lijnding.context")
        self.worker_state: Dict[str, Any] = {}
        self.config = config
        self.pipeline_name = pipeline_name

        if _from_proxies:
            # Reconstruct from existing manager proxies
            self._data, self._lock = _from_proxies
            self._mp_safe = True
            return

        self._mp_safe = mp_safe
        self._manager = None
        if mp_safe:
            self._manager = mp.Manager()
            self._data = self._manager.dict()
            self._lock = self._manager.Lock()
        else:
            self._data: Dict[str, Any] = {}
            self._lock = threading.Lock()

        if initial_data:
            self.update(initial_data)

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value

    def update(self, other: Dict[str, Any]) -> None:
        with self._lock:
            for k, v in other.items():
                self._data[k] = v

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data)

    def inc(self, key: str, amount: int = 1) -> int:
        with self._lock:
            current_value = self._data.get(key, 0)
            new_value = int(current_value) + amount
            self._data[key] = new_value
            return new_value

    def __repr__(self) -> str:
        return f"Context(mp_safe={self._mp_safe}, data={self.to_dict()})"

    def on_run_start(self, pipeline):
        """Hook called at the start of a pipeline run."""
        pass

    def on_run_finish(self, pipeline, exception: Optional[Exception] = None):
        """Hook called at the end of a pipeline run."""
        pass

    def on_stage_start(self, stage, index: int):
        """Hook called at the start of each stage."""
        pass

    def on_stage_error(self, stage, exception: Exception):
        """Hook called when a stage encounters an error during processing."""
        pass

    def shutdown(self):
        """Shuts down any background processes, like a multiprocessing.Manager."""
        if self._manager:
            self.logger.debug("Shutting down multiprocessing manager for context.")
            self._manager.shutdown()
