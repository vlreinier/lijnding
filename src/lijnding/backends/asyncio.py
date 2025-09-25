from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable, Iterator

from .base import (
    BaseRunner,
    _handle_route_to_pipeline_async,
    _handle_transform_and_retry_async,
)
from ..core.utils import ensure_iterable, AsyncToSyncIterator

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


class AsyncioRunner(BaseRunner):
    """
    A runner that executes stages using Python's asyncio for non-blocking I/O.
    """

    # This runner has a different signature for its entry point to support `async`.
    # The Pipeline will need to be aware of this.
    async def run_async(
        self, stage: "Stage", context: "Context", iterable: AsyncIterator[Any]
    ) -> AsyncIterator[Any]:
        """Asynchronously executes the stage with structured logging."""
        if stage.stage_type == "source":
            # Let the pipeline handle the 'source' case directly
            results = stage._invoke(context)
            output_stream = ensure_iterable(results)
            if hasattr(output_stream, "__aiter__"):
                async for res in output_stream:
                    yield res
            else:
                for res in output_stream:
                    yield res

        elif stage.stage_type == "aggregator":
            # This is a simplified aggregator for async. A more robust implementation
            # would use the base class's _run_aggregator and adapt it for async.
            stage.logger.info("aggregator_started", backend="asyncio")
            start_time = asyncio.get_running_loop().time()

            items = [item async for item in iterable]
            items_in = len(items)
            items_out = 0

            try:
                if stage.is_async:
                    results = await stage._invoke(context, items)
                else:
                    results = await asyncio.to_thread(stage._invoke, context, items)

                output_stream = ensure_iterable(results)
                for res in output_stream:
                    items_out += 1
                    yield res
            finally:
                duration = asyncio.get_running_loop().time() - start_time
                stage.logger.info(
                    "aggregator_finished",
                    items_in=items_in,
                    items_out=items_out,
                    duration=round(duration, 4),
                )
        elif stage.stage_type == "generator":
            # A generator stage takes the entire iterable at once. This is
            # fundamentally different from an itemwise stage.
            if stage.is_async:
                async for res in stage._invoke(context, iterable):
                    yield res
            else:
                # For sync generators in an async pipeline, we need to
                # run the generator in a thread to avoid blocking the loop.
                loop = asyncio.get_running_loop()
                sync_iterable = AsyncToSyncIterator(iterable, loop)

                def run_sync_gen_in_thread():
                    # This will run the generator to completion and buffer
                    # results, which is not ideal for memory but consistent
                    # with how other sync stages are handled in `run_async`.
                    return list(stage._invoke(context, sync_iterable))

                results = await loop.run_in_executor(
                    None, run_sync_gen_in_thread
                )
                for res in results:
                    yield res
        else:  # itemwise
            # Itemwise processing delegates to the async itemwise runner
            async for res in self._run_itemwise_async(stage, context, iterable):
                yield res

    async def _run_itemwise_async(
        self, stage: "Stage", context: "Context", iterable: AsyncIterator[Any]
    ) -> AsyncIterator[Any]:
        """Processes items concurrently using asyncio, with structured logging."""
        stage.logger.info("stream_started", backend="asyncio")
        stream_start_time = asyncio.get_running_loop().time()
        total_items_in = 0
        total_items_out = 0

        try:
            async for item in iterable:
                total_items_in += 1
                stage.metrics["items_in"] += 1
                item_start_time = asyncio.get_running_loop().time()
                attempts = 0

                while True:  # Retry loop
                    try:
                        count_out = 0
                        if stage.is_async:
                            result_obj = stage._invoke(context, item)
                            if inspect.isasyncgen(result_obj):
                                async for res in result_obj:
                                    yield res
                                    count_out += 1
                            else:  # Coroutine
                                results = await result_obj
                                for res in ensure_iterable(results):
                                    yield res
                                    count_out += 1
                        else:  # Sync function
                            results = await asyncio.to_thread(
                                stage._invoke, context, item
                            )
                            for res in ensure_iterable(results):
                                yield res
                                count_out += 1

                        stage.metrics["items_out"] += count_out
                        total_items_out += count_out
                        item_elapsed = (
                            asyncio.get_running_loop().time() - item_start_time
                        )
                        stage.logger.debug(
                            "item_processed",
                            items_out=count_out,
                            duration=round(item_elapsed, 4),
                        )
                        break  # Success

                    except Exception as e:
                        attempts += 1
                        stage.metrics["errors"] += 1
                        stage.logger.warning(
                            "item_error", error=str(e), attempts=attempts
                        )

                        policy = stage.error_policy
                        if policy.mode == "route_to_pipeline":
                            await _handle_route_to_pipeline_async(stage, context, item)
                            break
                        elif (
                            policy.mode == "route_to_pipeline_and_retry"
                            and attempts <= policy.retries
                        ):
                            item = await _handle_transform_and_retry_async(
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

                        # Fail by default
                        raise e
        finally:
            total_duration = asyncio.get_running_loop().time() - stream_start_time
            stage.logger.info(
                "stream_finished",
                items_in=total_items_in,
                items_out=total_items_out,
                errors=stage.metrics["errors"],
                duration=round(total_duration, 4),
            )

    def run(
        self, stage: "Stage", context: "Context", iterable: Iterable[Any]
    ) -> Iterator[Any]:
        """
        Provides a synchronous entry point for the async runner by running
        a new event loop. This is useful for running an async-powered pipeline
        from a sync context, but it's not the primary use case.

        .. warning::
            This method is NOT lazy and will block until the entire async
            iterator is consumed. It also cannot be called from a running
            event loop. Use `run_async` for true async behavior.
        """

        # We need an async generator to pass to `asyncio.run`.
        async def _async_gen_wrapper():
            # Convert the sync iterable to an async one
            async def _to_async(it):
                for i in it:
                    yield i

            async for res in self.run_async(stage, context, _to_async(iterable)):
                yield res

        # This is a bridge from the async world to the sync world.
        # It collects all results and then returns them.
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:  # pragma: no cover
            # This occurs if we're in a thread that's not the main thread
            # and no event loop has been set.
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if loop.is_running():
            # If there's already a loop, we can't just call asyncio.run().
            # This is a complex problem. For now, we'll raise an error.
            raise RuntimeError(
                "Cannot run the asyncio backend from a running event loop with the sync `run` method. "
                "You must use `run_async` instead."
            )

        # This will block until the entire async generator is consumed.
        # This implementation is NOT lazy. A truly lazy bridge is more complex.
        results = asyncio.run(_collect_async_gen(_async_gen_wrapper()))
        yield from results

    def _run_itemwise(
        self, stage: "Stage", context: "Context", iterable: Iterable[Any]
    ) -> Iterator[Any]:
        """Implementation for the abstract method, uses the sync bridge."""
        yield from self.run(stage, context, iterable)


async def _collect_async_gen(agen):
    return [item async for item in agen]
