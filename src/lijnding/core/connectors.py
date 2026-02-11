import asyncio
import queue
import multiprocessing
from typing import Optional, Any

class BaseConnector:
    async def put(self, item: Optional[Any]):
        raise NotImplementedError

    async def get(self) -> Optional[Any]:
        raise NotImplementedError


class AsyncMemoryConnector(BaseConnector):
    def __init__(self, maxsize=64):
        self.q = asyncio.Queue(maxsize=maxsize)

    async def put(self, item):
        await self.q.put(item)

    async def get(self):
        return await self.q.get()


class ThreadSafeConnector(BaseConnector):
    def __init__(self, maxsize=64):
        self.q = queue.Queue(maxsize=maxsize)

    async def put(self, item):
        await asyncio.to_thread(self.q.put, item)

    async def get(self):
        return await asyncio.to_thread(self.q.get)


class InterProcessConnector(BaseConnector):
    def __init__(self, maxsize=64):
        self.q = multiprocessing.Queue(maxsize=maxsize)

    async def put(self, item):
        await asyncio.to_thread(self.q.put, item)

    async def get(self):
        return await asyncio.to_thread(self.q.get)
