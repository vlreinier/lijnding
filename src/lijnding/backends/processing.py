from __future__ import annotations

import asyncio
import time
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Any, AsyncIterator, AsyncIterable, List, Tuple
import dill as serializer

from .base import (
    BaseRunner,
    _handle_route_to_pipeline,
    _handle_transform_and_retry,
)
from ..core.utils import ensure_iterable

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


# This function runs in a separate process.
def _worker_fn(
    stage_payload: bytes, context_proxies: tuple | None, item: Any
) -> Tuple[List[Any], dict, Exception | None]:
    """
    The target function for the process pool executor.
    It deserializes the stage, runs the item through it, and returns
    the results, metrics, and any error.
    """
    from ..core.context import Context

    stage = serializer.loads(stage_payload)
    context = Context(_from_proxies=context_proxies)

    # Minimal metrics for this single item
    metrics = {"items_in": 1, "items_out": 0, "errors": 0, "time_total": 0.0}

    try:
        item_start_time = time.perf_counter()
        results = stage._invoke(context, item)
        output = list(ensure_iterable(results))
        item_elapsed = time.perf_counter() - item_start_time

        metrics["items_out"] = len(output)
        metrics["time_total"] = item_elapsed
        return output, metrics, None
    except Exception as e:
        item_elapsed = time.perf_counter() - item_start_time
        metrics["errors"] = 1
        metrics["time_total"] = item_elapsed
        # Return the exception to be re-raised in the main process
        return [], metrics, e


class ProcessingRunner(BaseRunner):
    """
    A runner that executes sync stages concurrently in a process pool.
    """

    async def _run_itemwise(
        self, stage: "Stage", context: "Context", iterable: AsyncIterable[Any]
    ) -> AsyncIterator[Any]:
        """Processes items concurrently in a process pool."""
        stage.logger.info("stream_started", backend="processing", workers=stage.workers)
        stream_start_time = time.perf_counter()
        total_items_in = 0
        total_items_out = 0

        # Create a ProcessPoolExecutor
        executor = ProcessPoolExecutor(max_workers=stage.workers)
        loop = asyncio.get_running_loop()

        # Get context proxies if the context is mp-safe
        context_proxies = (
            (context._data, context._lock) if getattr(context, "_mp_safe", False) else None
        )
        if not context_proxies:
            stage.logger.warning(
                "Context is not multiprocessing-safe. Changes to context "
                "in a 'process' backend stage will not be propagated."
            )

        # Serialize the stage once to send to all worker processes
        stage_payload = serializer.dumps(stage)

        # Sentinel to signal the end of the stream
        END_OF_QUEUE = object()
        results_queue = asyncio.Queue()

        async def _process_item(item):
            """Submits one item to the process pool."""
            future = loop.run_in_executor(
                executor, _worker_fn, stage_payload, context_proxies, item
            )
            await results_queue.put(future)

        async def feeder():
            """Feeds items from the input iterable to the worker tasks."""
            tasks = [asyncio.create_task(_process_item(item)) async for item in iterable]
            await asyncio.gather(*tasks)
            await results_queue.put(END_OF_QUEUE)

        feeder_task = asyncio.create_task(feeder())

        try:
            while True:
                future_or_sentinel = await results_queue.get()
                if future_or_sentinel is END_OF_QUEUE:
                    break

                future = future_or_sentinel
                output, metrics, error = await future

                # Update metrics in the main process
                for key, value in metrics.items():
                    stage.metrics[key] += value

                total_items_in += metrics["items_in"]
                total_items_out += metrics["items_out"]

                if error:
                    # TODO: Add retry logic here
                    raise error

                for res in output:
                    yield res
        finally:
            if not feeder_task.done():
                feeder_task.cancel()
            executor.shutdown(wait=True)

            total_duration = time.perf_counter() - stream_start_time
            stage.logger.info(
                "stream_finished",
                items_in=total_items_in,
                items_out=total_items_out,
                errors=stage.metrics["errors"],
                duration=round(total_duration, 4),
            )
