from __future__ import annotations

import time
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


import asyncio
from typing import AsyncIterable, AsyncIterator

class SerialRunner(BaseRunner):
    """
    A runner that executes sync stages sequentially, but in a non-blocking way
    with respect to the asyncio event loop.
    """

    async def _run_itemwise(
        self, stage: "Stage", context: "Context", iterable: AsyncIterable[Any]
    ) -> AsyncIterator[Any]:
        """
        Processes items one by one in a simple async loop.
        """
        stage.logger.info("stream_started")
        total_items_in = 0
        total_items_out = 0
        stream_start_time = time.perf_counter()

        if stage.hooks and stage.hooks.on_worker_init:
            context.worker_state = stage.hooks.on_worker_init(context) or {}

        try:
            async for item in iterable:
                total_items_in += 1
                stage.metrics["items_in"] += 1
                item_start_time = time.perf_counter()
                attempts = 0

                if stage.hooks and stage.hooks.before_stage:
                    stage.hooks.before_stage(stage, context, item)

                while True:
                    try:
                        # Run the sync function in a thread to avoid blocking the loop
                        results = await asyncio.to_thread(stage._invoke, context, item)
                        output_stream = ensure_iterable(results)

                        count_out = 0
                        for res in output_stream:
                            stage.metrics["items_out"] += 1
                            total_items_out += 1
                            count_out += 1
                            yield res

                        item_elapsed = time.perf_counter() - item_start_time
                        stage.logger.debug(
                            "item_processed",
                            item_in=total_items_in,
                            items_out=count_out,
                            duration=round(item_elapsed, 4),
                        )
                        break  # Success, exit retry loop

                    except Exception as e:
                        context.on_stage_error(stage, e)
                        attempts += 1
                        stage.metrics["errors"] += 1
                        item_elapsed = time.perf_counter() - item_start_time
                        stage.logger.warning(
                            "item_error",
                            item_in=total_items_in,
                            error=str(e),
                            attempts=attempts,
                            duration=round(item_elapsed, 4),
                        )

                        if stage.hooks and stage.hooks.on_error:
                            stage.hooks.on_error(stage, context, item, e, attempts)

                        policy = stage.error_policy
                        if policy.mode == "route_to_pipeline":
                            await _handle_route_to_pipeline(stage, context, item)
                            break
                        elif (
                            policy.mode == "route_to_pipeline_and_retry"
                            and attempts <= policy.retries
                        ):
                            item = await _handle_transform_and_retry(
                                stage, context, item
                            )
                            if policy.backoff > 0:
                                await asyncio.sleep(policy.backoff * attempts)
                            continue
                        elif policy.mode == "retry" and attempts <= policy.retries:
                            if policy.backoff > 0:
                                await asyncio.sleep(policy.backoff * attempts)
                            continue
                        elif policy.mode == "skip":
                            break

                        # Default is to fail
                        raise e

                    finally:
                        elapsed = time.perf_counter() - item_start_time
                        stage.metrics["time_total"] += elapsed
                        if stage.hooks and stage.hooks.after_stage:
                            stage.hooks.after_stage(stage, context, item, None, elapsed)
        finally:
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
