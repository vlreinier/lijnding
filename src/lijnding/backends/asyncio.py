from __future__ import annotations

import asyncio
import inspect
import time
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


class AsyncioRunner(BaseRunner):
    """
    A runner that executes async stages concurrently using Python's asyncio.
    """

    async def _run_itemwise(
        self,
        stage: "Stage",
        context: "Context",
        iterable: AsyncIterable[Any],
        *,
        executor=None,
    ) -> AsyncIterator[Any]:
        """Processes items concurrently using asyncio, with structured logging."""
        stage.logger.info("stream_started", backend="asyncio", workers=stage.workers)
        stream_start_time = time.perf_counter()
        total_items_in = 0
        total_items_out = 0

        # Sentinel to signal the end of the stream
        END_OF_QUEUE = object()

        # Use a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(stage.workers)
        # Use a queue to gather results from concurrent tasks
        results_queue = asyncio.Queue(maxsize=stage.workers)

        if stage.hooks and stage.hooks.on_worker_init:
            # In asyncio, there's no persistent worker, but we can call this once.
            context.worker_state = stage.hooks.on_worker_init(context) or {}

        async def _process_item(item):
            """Worker coroutine to process a single item."""
            nonlocal total_items_in
            total_items_in += 1
            stage.metrics["items_in"] += 1
            item_start_time = time.perf_counter()
            attempts = 0

            async with semaphore:
                if stage.hooks and stage.hooks.before_stage:
                    stage.hooks.before_stage(stage, context, item)

                while True: # Retry loop
                    try:
                        if not stage.is_async:
                             raise TypeError(
                                f"Stage '{stage.name}' is not an async function, "
                                f"but it's using the 'async' backend. "
                                f"Use 'serial' or 'thread' for sync functions."
                            )

                        result_obj = stage._invoke(context, item)

                        count_out = 0
                        if inspect.isasyncgen(result_obj):
                            async for res in result_obj:
                                await results_queue.put(res)
                                count_out += 1
                        else: # Coroutine
                            results = await result_obj
                            for res in ensure_iterable(results):
                                await results_queue.put(res)
                                count_out += 1

                        stage.metrics["items_out"] += count_out
                        item_elapsed = time.perf_counter() - item_start_time
                        stage.logger.debug("item_processed", items_out=count_out, duration=round(item_elapsed, 4))
                        return # Success

                    except Exception as e:
                        context.on_stage_error(stage, e)
                        attempts += 1
                        stage.metrics["errors"] += 1
                        stage.logger.warning("item_error", error=str(e), attempts=attempts)

                        if stage.hooks and stage.hooks.on_error:
                            stage.hooks.on_error(stage, context, item, e, attempts)

                        policy = stage.error_policy
                        if policy.mode == "route_to_pipeline":
                            await _handle_route_to_pipeline(stage, context, item)
                            return
                        elif policy.mode == "route_to_pipeline_and_retry" and attempts <= policy.retries:
                            item = await _handle_transform_and_retry(stage, context, item)
                            if policy.backoff > 0: await asyncio.sleep(policy.backoff * attempts)
                            continue
                        elif policy.mode == "retry" and attempts <= policy.retries:
                            if policy.backoff > 0: await asyncio.sleep(policy.backoff * attempts)
                            continue
                        elif policy.mode == "skip":
                            return

                        # Fail by default and propagate the error to the main task
                        await results_queue.put(e)
                        return
                    finally:
                        elapsed = time.perf_counter() - item_start_time
                        stage.metrics["time_total"] += elapsed
                        if stage.hooks and stage.hooks.after_stage:
                            stage.hooks.after_stage(stage, context, item, None, elapsed)

        async def feeder():
            """Feeds items from the input iterable to the worker tasks."""
            tasks = []
            async for item in iterable:
                task = asyncio.create_task(_process_item(item))
                tasks.append(task)

            # Wait for all tasks to complete before signaling the end
            await asyncio.gather(*tasks)
            await results_queue.put(END_OF_QUEUE)

        # Start the feeder task in the background
        feeder_task = asyncio.create_task(feeder())

        try:
            # Yield results from the queue as they arrive
            while True:
                result = await results_queue.get()
                if result is END_OF_QUEUE:
                    break
                if isinstance(result, Exception):
                    # An error was caught and propagated
                    raise result

                total_items_out += 1
                yield result
        finally:
            # Cleanup: ensure feeder task is cancelled if the consumer stops early
            if not feeder_task.done():
                feeder_task.cancel()
                try:
                    await feeder_task
                except asyncio.CancelledError:
                    pass

            if stage.hooks and stage.hooks.on_worker_exit:
                stage.hooks.on_worker_exit(context)

            total_duration = time.perf_counter() - stream_start_time
            stage.logger.info(
                "stream_finished",
                items_in=total_items_in,
                items_out=total_items_out,
                errors=stage.metrics["errors"],
                duration=round(total_duration, 4),
            )
