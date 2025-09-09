from __future__ import annotations

import asyncio
import time
import multiprocessing as mp
from typing import TYPE_CHECKING, Any, AsyncIterator, AsyncIterable, List, Tuple
import dill as serializer
import queue

from .base import (
    BaseRunner,
)
from ..core.utils import ensure_iterable

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage

SENTINEL = "__LIJNDING_WORKER_SENTINEL__"

# This function runs in a separate process.
def _worker_process(
    in_queue: mp.Queue,
    out_queue: mp.Queue,
    stage_payload: bytes,
    context_proxies: tuple | None,
    worker_id: int,
):
    """Target function for each worker process."""
    from ..core.context import Context

    stage = serializer.loads(stage_payload)
    context = Context(_from_proxies=context_proxies)

    if stage.hooks.on_worker_init:
        context.worker_state = stage.hooks.on_worker_init(context) or {}

    while True:
        try:
            item = in_queue.get()
            if item == SENTINEL:
                break

            results = stage._invoke(context, item)
            output = list(ensure_iterable(results))
            out_queue.put(output)

        except Exception as e:
            out_queue.put(e)

    out_queue.put(SENTINEL)
    if stage.hooks.on_worker_exit:
        stage.hooks.on_worker_exit(context)


class ProcessingRunner(BaseRunner):
    """
    A runner that executes sync stages concurrently in a manually managed
    pool of processes.
    """

    async def _run_itemwise(
        self,
        stage: "Stage",
        context: "Context",
        iterable: AsyncIterable[Any],
        *,
        executor=None, # This is no longer used but kept for interface consistency
    ) -> AsyncIterator[Any]:
        """Processes items concurrently in a process pool."""
        stage.logger.info("stream_started", backend="processing", workers=stage.workers)
        stream_start_time = time.perf_counter()
        total_items_in = 0
        total_items_out = 0

        try:
            mp.set_start_method("spawn", force=True)
        except RuntimeError:
            pass

        loop = asyncio.get_running_loop()

        # We must use multiprocessing queues for inter-process communication
        in_queue = mp.Queue(maxsize=stage.buffer_size or stage.workers)
        out_queue = mp.Queue()

        context_proxies = (
            (context._data, context._lock) if getattr(context, "_mp_safe", False) else None
        )
        stage_payload = serializer.dumps(stage)

        processes = []
        for i in range(stage.workers):
            process = mp.Process(
                target=_worker_process,
                args=(in_queue, out_queue, stage_payload, context_proxies, i),
                daemon=True,
            )
            process.start()
            processes.append(process)

        async def feeder():
            """Feeds items from the async iterable to the input queue."""
            nonlocal total_items_in
            async for item in iterable:
                total_items_in += 1
                stage.metrics["items_in"] += 1
                await loop.run_in_executor(None, in_queue.put, item)

            for _ in range(stage.workers):
                await loop.run_in_executor(None, in_queue.put, SENTINEL)

        feeder_task = asyncio.create_task(feeder())

        finished_workers = 0
        try:
            while finished_workers < stage.workers:
                try:
                    # Run the blocking get() in a thread to not block the event loop
                    result = await loop.run_in_executor(None, out_queue.get)
                except queue.Empty:
                    await asyncio.sleep(0.001) # Should not happen with blocking get
                    continue

                if result == SENTINEL:
                    finished_workers += 1
                    continue

                if isinstance(result, Exception):
                    # TODO: Add retry logic here
                    raise result

                # result is a list of items from the worker
                for res in result:
                    total_items_out += 1
                    stage.metrics["items_out"] += 1
                    yield res
        finally:
            if not feeder_task.done():
                feeder_task.cancel()

            # Ensure all processes are cleaned up
            for p in processes:
                p.join(timeout=1.0)
                if p.is_alive():
                    p.terminate()

            total_duration = time.perf_counter() - stream_start_time
            stage.logger.info(
                "stream_finished",
                items_in=total_items_in,
                items_out=total_items_out,
                errors=stage.metrics["errors"],
                duration=round(total_duration, 4),
            )
