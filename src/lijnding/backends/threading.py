from __future__ import annotations

import asyncio
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


class ThreadingRunner(BaseRunner):
    """
    A runner that executes sync stages concurrently in a thread pool.
    """

    async def _run_itemwise(
        self, stage: "Stage", context: "Context", iterable: AsyncIterable[Any]
    ) -> AsyncIterator[Any]:
        """Processes items concurrently in a thread pool, with structured logging."""
        stage.logger.info("stream_started", backend="threading", workers=stage.workers)
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
            # This hook is called once per pipeline run for this stage,
            # not per thread, which is a change in semantics but fits the
            # new async model better.
            context.worker_state = stage.hooks.on_worker_init(context) or {}

        async def _process_item(item):
            """Worker coroutine to process a single item in a thread."""
            nonlocal total_items_in
            total_items_in += 1
            stage.metrics["items_in"] += 1
            item_start_time = time.perf_counter()
            attempts = 0

            async with semaphore:
                if stage.hooks and stage.hooks.before_stage:
                    stage.hooks.before_stage(stage, context, item)

                while True:  # Retry loop
                    try:
                        if stage.is_async:
                            raise TypeError(
                                f"Stage '{stage.name}' is an async function, "
                                f"but it's using the 'thread' backend. "
                                f"Use 'async' for async functions."
                            )

                        # Run the sync function in a thread to avoid blocking the loop
                        results = await asyncio.to_thread(stage._invoke, context, item)
                        output_stream = ensure_iterable(results)

                        count_out = 0
                        for res in output_stream:
                            await results_queue.put(res)
                            count_out += 1

                        stage.metrics["items_out"] += count_out
                        item_elapsed = time.perf_counter() - item_start_time
                        stage.logger.debug("item_processed", items_out=count_out, duration=round(item_elapsed, 4))
                        return  # Success

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

                        # Fail by default and propagate the error
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

            await asyncio.gather(*tasks)
            await results_queue.put(END_OF_QUEUE)

        feeder_task = asyncio.create_task(feeder())

        try:
            while True:
                result = await results_queue.get()
                if result is END_OF_QUEUE:
                    break
                if isinstance(result, Exception):
                    raise result

                total_items_out += 1
                yield result
        finally:
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
