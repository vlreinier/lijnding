from __future__ import annotations

import asyncio
import threading
import time
import queue
from typing import TYPE_CHECKING, Any, AsyncIterator, AsyncIterable

from .base import (
    BaseRunner,
    _handle_route_to_pipeline,
    _handle_transform_and_retry,
)
from ..core.utils import ensure_iterable

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


class ThreadingRunner(BaseRunner):
    """
    A runner that executes sync stages concurrently in a manually managed
    pool of threads.
    """

    async def _run_itemwise(
        self,
        stage: "Stage",
        context: "Context",
        iterable: AsyncIterable[Any],
        *,
        executor=None,
    ) -> AsyncIterator[Any]:
        """Processes items concurrently in a thread pool, with structured logging."""
        stage.logger.info("stream_started", backend="threading", workers=stage.workers)
        stream_start_time = time.perf_counter()
        total_items_in = 0
        total_items_out = 0

        loop = asyncio.get_running_loop()

        # Using standard queues for thread-safe communication
        in_queue = queue.Queue(maxsize=stage.buffer_size or stage.workers)
        out_queue = queue.Queue()

        SENTINEL = object()
        threads = []

        def worker(worker_id: int):
            """Target function for each worker thread."""
            import copy

            # Each thread gets a shallow copy of the context.
            # If the context is mp-safe, the proxies are copied.
            # If not, the dict is copied.
            worker_context = copy.copy(context)
            worker_context.worker_state = {}

            logger = stage.logger.bind(worker_id=worker_id)

            if stage.hooks.on_worker_init:
                worker_context.worker_state = stage.hooks.on_worker_init(worker_context) or {}

            logger.debug("worker_started")

            while True:
                try:
                    item = in_queue.get()
                    if item is SENTINEL:
                        break

                    nonlocal total_items_in
                    total_items_in += 1
                    stage.metrics["items_in"] += 1

                    if stage.is_async:
                        # This is a limitation of this runner; it cannot run async code.
                        # This check is defensive.
                        raise TypeError("ThreadingRunner cannot execute async stages.")

                    results = stage._invoke(worker_context, item)
                    output_stream = ensure_iterable(results)

                    for res in output_stream:
                        out_queue.put(res)

                except Exception as e:
                    out_queue.put(e)

            out_queue.put(SENTINEL) # Signal that this worker is done
            logger.debug("worker_finished")
            if stage.hooks.on_worker_exit:
                stage.hooks.on_worker_exit(worker_context)


        for i in range(stage.workers):
            thread = threading.Thread(target=worker, args=(i,))
            thread.start()
            threads.append(thread)

        # Feeder coroutine to pull from async iterable and put into thread queue
        async def feeder():
            async for item in iterable:
                # This is a blocking put, so we run it in a thread to not block the event loop
                await loop.run_in_executor(None, in_queue.put, item)

            for _ in range(stage.workers):
                await loop.run_in_executor(None, in_queue.put, SENTINEL)

        feeder_task = asyncio.create_task(feeder())

        finished_workers = 0
        try:
            while finished_workers < stage.workers:
                # This is a blocking get, so run it in a thread
                result = await loop.run_in_executor(None, out_queue.get)

                if result is SENTINEL:
                    finished_workers += 1
                    continue

                if isinstance(result, Exception):
                    # TODO: Add retry logic here
                    raise result

                total_items_out += 1
                yield result
        finally:
            if not feeder_task.done():
                feeder_task.cancel()

            # Ensure all threads are cleaned up
            for thread in threads:
                thread.join()

            total_duration = time.perf_counter() - stream_start_time
            stage.logger.info(
                "stream_finished",
                items_in=total_items_in,
                items_out=total_items_out,
                errors=stage.metrics["errors"],
                duration=round(total_duration, 4),
            )
