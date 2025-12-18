from __future__ import annotations
import asyncio
from typing import Any, AsyncIterator, Iterator

class AsyncBridge:
    """
    A janus-style bridge to connect async and sync code, ensuring streaming.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop
        self._q: asyncio.Queue = asyncio.Queue(maxsize=1)
        self._sync_sentinel = object()
        self._async_sentinel = object()
        self._done = False

    async def aput(self, item: Any) -> None:
        """Coroutine to put an item into the queue from the async side."""
        await self._q.put(item)

    def get(self) -> Any:
        """Blocking method to get an item from the sync side."""
        if self._done:
            return self._sync_sentinel

        future = asyncio.run_coroutine_threadsafe(self._q.get(), self._loop)
        result = future.result()

        if result is self._async_sentinel:
            self._done = True
            return self._sync_sentinel
        return result

    def __iter__(self) -> Iterator[Any]:
        """Allows the sync side to iterate over the bridge."""
        while True:
            item = self.get()
            if item is self._sync_sentinel:
                break
            yield item

    async def close(self) -> None:
        """Signals the end of the async stream."""
        await self.aput(self._async_sentinel)

    async def drain_to_sync(self, aiterator: AsyncIterator[Any]) -> None:
        """Pulls items from an async iterator and puts them into the bridge."""
        try:
            async for item in aiterator:
                await self.aput(item)
        finally:
            await self.close()
