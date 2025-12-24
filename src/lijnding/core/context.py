from __future__ import annotations

import multiprocessing as mp
from typing import Any, Dict
import logging
from logging.handlers import QueueHandler

class PipelineContext:
    def __init__(self, manager: "mp.Manager", log_queue: "mp.Queue"):
        self._state = manager.dict()
        self._stop_event = mp.Event()
        self._counter = mp.Value('i', 0)
        self._log_queue = log_queue

    @property
    def is_shutting_down(self):
        return self._stop_event.is_set()

    def signal_failure(self):
        self._stop_event.set()

    def get_logger(self, name: str) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = QueueHandler(self._log_queue)
            logger.addHandler(handler)
        logger.propagate = False
        return logger

    def increment_counter(self) -> int:
        with self._counter.get_lock():
            self._counter.value += 1
            return self._counter.value

    def get_counter(self):
        return self._counter.value
