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

        @generator_stage(name=f"batch(size={size}, timeout={timeout})")
        async def _batch_async(
            iterable: AsyncIterator[Any],
        ) -> AsyncIterator[List[Any]]:
            current_batch: List[Any] = []
            while True:
                try:
                    # If batch is empty, wait forever for an item. Otherwise, use timeout.
                    current_timeout = timeout if current_batch else None
                    item = await asyncio.wait_for(
                        iterable.__anext__(), timeout=current_timeout
                    )
                    current_batch.append(item)

                    if len(current_batch) >= size:
                        yield current_batch
                        current_batch = []

                except asyncio.TimeoutError:
                    # This can only happen if current_batch was not empty.
                    yield current_batch
                    current_batch = []

                except StopAsyncIteration:
                    # The input stream is exhausted. Yield the last partial batch.
                    if current_batch:
                        yield current_batch
                    break

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
