import asyncio
import queue
import types
from typing import Any, AsyncIterator, Iterable


SENTINEL = object()


class AsyncToSyncIterator:
    """
    Wraps an async iterator into a sync iterator, allowing it to be used
    in a synchronous context (like a for loop).

    This implementation is thread-safe and is designed to be used from a
    separate thread than the one running the asyncio event loop.
    """

    def __init__(
        self, async_iterator: AsyncIterator[Any], loop: asyncio.AbstractEventLoop
    ):
        self._async_iterator = async_iterator
        self._loop = loop

    def __iter__(self):
        return self

    def __next__(self):
        """
        Fetches the next item from the async iterator. This is a blocking call.
        It submits the `__anext__` call to the event loop and waits for the result.
        """
        future = asyncio.run_coroutine_threadsafe(
            self._async_iterator.__anext__(), self._loop
        )
        try:
            return future.result()
        except StopAsyncIteration:
            # Re-raise as a standard StopIteration for the sync context
            raise StopIteration


def ensure_iterable(obj: Any) -> Iterable[Any]:
    """
    Ensures that the given object is an iterable.
    If it's a list or a generator, it's returned as is.
    If it's another type (including a tuple), it's wrapped in a list.
    `None` is treated as an empty list.
    """
    if obj is None:
        return []
    # Pass through lists and all kinds of generators
    if isinstance(obj, (list, types.GeneratorType, types.AsyncGeneratorType)):
        return obj
    # Wrap other types (including tuples) in a list to treat them as a single item
    return [obj]
