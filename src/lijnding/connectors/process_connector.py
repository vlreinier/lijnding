from __future__ import annotations
import asyncio
import multiprocessing
from typing import Any, Optional

from .base import BaseConnector

class ProcessConnector(BaseConnector):
    """
    A connector that uses a process-safe `multiprocessing.Queue`.
    Blocking operations are run in a separate thread via `asyncio.to_thread`.
    """
    def __init__(self, maxsize: int = 64):
        self.q = multiprocessing.Queue(maxsize=maxsize)

    async def put(self, item: Optional[Any]):
        await asyncio.to_thread(self.q.put, item)

    async def get(self) -> Optional[Any]:
        return await asyncio.to_thread(self.q.get)
