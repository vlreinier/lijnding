from __future__ import annotations
import asyncio
from typing import Any, Optional

from .base import BaseConnector

class AsyncConnector(BaseConnector):
    """
    A connector that uses a native `asyncio.Queue` for communication between
    async stages.
    """
    def __init__(self, maxsize: int = 64):
        self.q = asyncio.Queue(maxsize=maxsize)

    async def put(self, item: Optional[Any]):
        await self.q.put(item)

    async def get(self) -> Optional[Any]:
        return await self.q.get()
