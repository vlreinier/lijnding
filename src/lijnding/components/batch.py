from __future__ import annotations

import asyncio
from typing import Any, Iterable, List, AsyncIterator

from ..core.stage import Stage, generator_stage


def batch(size: int = 10, timeout: float = 0) -> Stage:
    """
    Creates a stage that groups items from a stream into batches (lists).

    This is a generator stage that processes items as they arrive. A batch is
    emitted when it reaches the specified `size`.

    If a `timeout` > 0 is provided, the stage becomes async and will also emit
    a batch if `timeout` seconds have passed since the first item of the
    current batch arrived. Using a timeout requires an async-capable runner.

    Args:
        size: The maximum number of items per batch.
        timeout: The maximum time in seconds to wait for a batch to fill up.
                 If 0, no timeout is used. Using a timeout makes the stage async.

    Returns:
        A Stage configured to perform streaming batching.
    """
    if not isinstance(size, int) or size <= 0:
        raise ValueError("Batch size must be a positive integer.")
    if not isinstance(timeout, (int, float)) or timeout < 0:
        raise ValueError("Timeout must be a non-negative number.")

    # If a timeout is specified, we return an async stage.
    if timeout > 0:

        @generator_stage(name=f"batch(size={size}, timeout={timeout})", backend="async")
        async def _batch_async(
            iterable: AsyncIterator[Any],
        ) -> AsyncIterator[List[Any]]:
            """
            An async generator that batches items from an async iterator.
            This implementation uses a queue to decouple the producer from the
            consumer, which makes timeout handling robust and prevents cancellation
            of the upstream iterator.
            """
            queue = asyncio.Queue(maxsize=size)
            sentinel = object()  # Sentinel to signal end of stream

            async def producer():
                """Feeds the queue from the source iterable."""
                try:
                    async for item in iterable:
                        await queue.put(item)
                finally:
                    await queue.put(sentinel)

            producer_task = asyncio.create_task(producer())
            current_batch: List[Any] = []

            while not producer_task.done() or not queue.empty():
                try:
                    # If a batch is empty, wait forever for an item. Otherwise, use timeout.
                    current_timeout = timeout if current_batch else None
                    item = await asyncio.wait_for(queue.get(), timeout=current_timeout)

                    if item is sentinel:
                        if current_batch:
                            yield current_batch
                        await producer_task
                        return

                    current_batch.append(item)
                    if len(current_batch) >= size:
                        yield current_batch
                        current_batch = []

                except asyncio.TimeoutError:
                    if current_batch:
                        yield current_batch
                        current_batch = []
                    continue

            if not producer_task.done():
                producer_task.cancel()
                try:
                    await producer_task
                except asyncio.CancelledError:
                    pass

        return _batch_async

    # If no timeout, we return a synchronous stage.
    else:

        @generator_stage(name=f"batch(size={size})")
        def _batch_sync(iterable: Iterable[Any]) -> Iterable[List[Any]]:
            current_batch: List[Any] = []
            for item in iterable:
                current_batch.append(item)
                if len(current_batch) >= size:
                    yield current_batch
                    current_batch = []
            # Yield the last partial batch if it exists.
            if current_batch:
                yield current_batch

        return _batch_sync
