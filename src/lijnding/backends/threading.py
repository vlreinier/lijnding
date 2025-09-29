from __future__ import annotations

import queue
import threading
from typing import TYPE_CHECKING, Any, Iterable, Iterator

from ..core.utils import ensure_iterable
from .base import (
    BaseRunner,
    _handle_route_to_pipeline,
    _handle_transform_and_retry,
)

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


SENTINEL = object()


class ThreadingRunner(BaseRunner):
    """
    A runner that executes itemwise stages in a pool of threads.
    Aggregator stages are run serially.
    """

    def should_run_in_own_loop(self) -> bool:
        return True

    def _run_itemwise(
        self, stage: "Stage", context: "Context", iterable: Iterable[Any]
    ) -> Iterator[Any]:
        import time

        stage.logger.info("stream_started", backend="threading", workers=stage.workers)
        stream_start_time = time.perf_counter()

        workers = stage.workers
        buffer_size = stage.buffer_size or (workers * 2)
        q_in: queue.Queue[Any] = queue.Queue(maxsize=buffer_size)
        q_out: queue.Queue[Any] = queue.Queue()

        total_items_in = 0
        total_items_out = 0

        def feeder():
            nonlocal total_items_in
            for item in iterable:
                q_in.put(item)
                total_items_in += 1
            for _ in range(workers):
                q_in.put(SENTINEL)

        def worker(worker_id: int):
            import copy

            worker_context = copy.copy(context)
            worker_context.worker_state = {}

            logger = stage.logger.bind(worker_id=worker_id)
            logger.debug("worker_started")

            try:
                if stage.hooks.on_worker_init:
                    worker_context.worker_state = (
                        stage.hooks.on_worker_init(worker_context) or {}
                    )

                while True:
                    item = q_in.get()
                    if item is SENTINEL:
                        break

                    item_start_time = time.perf_counter()
                    stage.metrics["items_in"] += 1
                    attempts = 0

                    while True:  # Retry loop
                        try:
                            if stage.is_async:
                                import asyncio
                                import inspect

                                async def arun_stage():
                                    result_obj = stage._invoke(worker_context, item)
                                    count_out = 0
                                    if inspect.isasyncgen(result_obj):
                                        async for res in result_obj:
                                            stage.metrics["items_out"] += 1
                                            count_out += 1
                                            q_out.put(res)
                                    else:  # Coroutine
                                        results = await result_obj
                                        for res in ensure_iterable(results):
                                            stage.metrics["items_out"] += 1
                                            count_out += 1
                                            q_out.put(res)
                                    return count_out

                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                                try:
                                    count_out = loop.run_until_complete(
                                        arun_stage()
                                    )
                                finally:
                                    loop.close()
                            else:
                                results = stage._invoke(worker_context, item)
                                output_stream = ensure_iterable(results)
                                count_out = 0
                                for res in output_stream:
                                    stage.metrics["items_out"] += 1
                                    count_out += 1
                                    q_out.put(res)

                            item_elapsed = time.perf_counter() - item_start_time
                            logger.debug(
                                "item_processed",
                                items_out=count_out,
                                duration=round(item_elapsed, 4),
                            )
                            break  # Success, exit retry loop

                        except Exception as e:
                            attempts += 1
                            stage.metrics["errors"] += 1
                            logger.warning(
                                "item_error", error=str(e), attempts=attempts
                            )

                            policy = stage.error_policy
                            if policy.mode == "route_to_pipeline":
                                _handle_route_to_pipeline(stage, worker_context, item)
                                break
                            elif (
                                policy.mode == "route_to_pipeline_and_retry"
                                and attempts <= policy.retries
                            ):
                                item = _handle_transform_and_retry(
                                    stage, worker_context, item
                                )
                                if policy.backoff > 0:
                                    time.sleep(policy.backoff * attempts)
                                continue
                            elif policy.mode == "retry" and attempts <= policy.retries:
                                if policy.backoff > 0:
                                    time.sleep(policy.backoff * attempts)
                                continue
                            elif policy.mode == "skip":
                                break

                            # If no policy handles it, raise to the main thread
                            q_out.put(e)
                            # Break the retry loop as we've escalated the error
                            break
                        finally:
                            item_elapsed = time.perf_counter() - item_start_time
                            stage.metrics["time_total"] += item_elapsed
            except Exception as e:
                logger.error("worker_error", error=str(e))
                q_out.put(e)
            finally:
                q_out.put(SENTINEL)
                logger.debug("worker_finished")
                if stage.hooks.on_worker_exit:
                    stage.hooks.on_worker_exit(worker_context)

        feeder_thread = threading.Thread(target=feeder, daemon=True)
        feeder_thread.start()

        threads = [
            threading.Thread(target=worker, args=(i,), daemon=True)
            for i in range(workers)
        ]
        for t in threads:
            t.start()

        finished_workers = 0
        try:
            while finished_workers < workers:
                result = q_out.get()
                if result is SENTINEL:
                    finished_workers += 1
                    continue
                if isinstance(result, Exception):
                    # Stop all threads and re-raise the exception
                    # This is a simplification; a more robust implementation might
                    # drain the queue or use other cancellation mechanisms.
                    raise result

                total_items_out += 1
                yield result
        finally:
            # Cleanup: ensure threads are joined
            for t in threads:
                t.join(timeout=0.1)

            total_duration = time.perf_counter() - stream_start_time
            stage.logger.info(
                "stream_finished",
                items_in=total_items_in,
                items_out=total_items_out,
                errors=stage.metrics["errors"],
                duration=round(total_duration, 4),
            )

    def _run_aggregator(
        self, stage: "Stage", context: "Context", iterable: Iterable[Any]
    ) -> Iterator[Any]:
        from .serial import SerialRunner

        return SerialRunner()._run_aggregator(stage, context, iterable)
