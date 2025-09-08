from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, AsyncIterable, AsyncIterator

from ..core.utils import ensure_iterable

if TYPE_CHECKING:
    from ..core.context import Context
    from ..core.stage import Stage


class BaseRunner(ABC):
    """
    Abstract base class for all stage runners.

    A runner is responsible for executing the logic of a single Stage,
    handling the flow of data, and managing the execution environment
    (e.g., serial, threaded, or multiprocessing).
    """

    async def run(
        self, stage: "Stage", context: "Context", iterable: AsyncIterable[Any], index: int
    ) -> AsyncIterator[Any]:
        """
        Executes the stage. This is the main entry point for a runner.
        It delegates to the appropriate method based on the stage type.
        """
        context.on_stage_start(stage, index)

        if stage.stage_type == "source":
            # Source stages ignore the input iterable and generate their own data.
            results = stage._invoke(context)
            output_stream = ensure_iterable(results)
            if hasattr(output_stream, "__aiter__"):
                async for res in output_stream:
                    yield res
            else:
                for res in output_stream:
                    yield res

        elif stage.stage_type == "aggregator":
            async for res in self._run_aggregator(stage, context, iterable):
                yield res
        else:
            # Default to itemwise processing
            async for res in self._run_itemwise(stage, context, iterable):
                yield res

    @abstractmethod
    async def _run_itemwise(
        self, stage: "Stage", context: "Context", iterable: AsyncIterable[Any]
    ) -> AsyncIterator[Any]:
        """
        Processes an iterable item by item.
        Each runner must implement this method.
        """
        raise NotImplementedError

    async def _run_aggregator(
        self, stage: "Stage", context: "Context", iterable: AsyncIterable[Any]
    ) -> AsyncIterator[Any]:
        """
        Processes an entire iterable at once with structured logging.
        This implementation fully consumes the input stream, logs metrics,
        and then calls the aggregator function.
        """
        stage.logger.info("aggregator_started")
        start_time = time.perf_counter()
        items_in = 0
        items_out = 0

        if stage.hooks and stage.hooks.on_worker_init:
            context.worker_state = stage.hooks.on_worker_init(context) or {}

        try:
            # Materialize the async iterable to know the count and allow hooks.
            materialized_items = [item async for item in iterable]
            items_in = len(materialized_items)
            stage.metrics["items_in"] += items_in

            stage.logger.debug("aggregation_input_materialized", items_in=items_in)

            if stage.hooks and stage.hooks.on_stream_end:
                stage.hooks.on_stream_end(context)

            if stage.is_async:
                results = await stage._invoke(context, materialized_items)
            else:
                results = await asyncio.to_thread(stage._invoke, context, materialized_items)

            output_stream = ensure_iterable(results)

            for res in output_stream:
                stage.metrics["items_out"] += 1
                items_out += 1
                yield res

        except Exception as e:
            stage.metrics["errors"] += 1
            stage.logger.error("aggregator_error", error=str(e))
            if stage.hooks and stage.hooks.on_error:
                # For aggregators, the "item" is the whole list
                stage.hooks.on_error(stage, context, materialized_items, e, 0)
            raise e
        finally:
            if stage.hooks and stage.hooks.on_worker_exit:
                stage.hooks.on_worker_exit(context)

            duration = time.perf_counter() - start_time
            stage.metrics["time_total"] += duration
            stage.logger.info(
                "aggregator_finished",
                items_in=items_in,
                items_out=items_out,
                errors=stage.metrics["errors"],
                duration=round(duration, 4),
            )


async def _handle_route_to_pipeline(stage: "Stage", context: "Context", item: Any):
    """Helper function to route a failed item to a separate pipeline."""
    # Local imports to avoid circular dependencies
    from ..core.pipeline import Pipeline
    from ..core.stage import Stage

    if not stage.error_policy.route_to_pipeline:
        return

    pipeline = stage.error_policy.route_to_pipeline
    if isinstance(pipeline, Stage):
        pipeline = Pipeline([pipeline])

    # Run the pipeline and consume the async stream to ensure it executes.
    stream, _ = await pipeline.run([item])
    async for _ in stream:
        pass


async def _handle_transform_and_retry(
    stage: "Stage", context: "Context", item: Any
) -> Any:
    """
    Helper function to route a failed item to a transformer pipeline and
    return the transformed item.
    """
    from ..core.pipeline import Pipeline
    from ..core.stage import Stage

    if not stage.error_policy.route_to_pipeline:
        raise ValueError("Missing 'route_to_pipeline' for transform_and_retry mode.")

    pipeline = stage.error_policy.route_to_pipeline
    if isinstance(pipeline, Stage):
        pipeline = Pipeline([pipeline])

    results, _ = await pipeline.collect([item])
    if len(results) != 1:
        raise ValueError(
            f"Transformer pipeline for 'route_to_pipeline_and_retry' must "
            f"produce exactly one item, but produced {len(results)}."
        )
    return results[0]
