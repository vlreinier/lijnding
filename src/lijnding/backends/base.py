from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Iterable, Iterator

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

    def run(
        self, stage: "Stage", context: "Context", iterable: Iterable[Any]
    ) -> Iterator[Any]:
        """
        Executes the stage. This is the main entry point for a runner.
        It delegates to the appropriate method based on the stage type.
        """
        if stage.stage_type == "source":
            # Source stages ignore the input iterable and generate their own data.
            return ensure_iterable(stage._invoke(context))
        if stage.stage_type == "aggregator":
            return self._run_aggregator(stage, context, iterable)
        if stage.stage_type == "generator":
            return self._run_generator(stage, context, iterable)

        # Default to itemwise processing
        return self._run_itemwise(stage, context, iterable)

    def _run_generator(
        self, stage: "Stage", context: "Context", iterable: Iterable[Any]
    ) -> Iterator[Any]:
        """
        Processes an iterable with a generator stage.
        This default implementation simply calls the stage function with the
        iterable and yields from the result. It does not materialize the input.
        """
        return ensure_iterable(stage._invoke(context, iterable))

    @abstractmethod
    def _run_itemwise(
        self, stage: "Stage", context: "Context", iterable: Iterable[Any]
    ) -> Iterator[Any]:
        """
        Processes an iterable item by item.
        Each runner must implement this method.
        """
        raise NotImplementedError

    def should_run_in_own_loop(self) -> bool:
        """
        Determines if a runner, when used as a sync bridge in an async pipeline,
        should be run in a new event loop on a separate thread instead of the
        main thread's executor. This is for runners that might spawn their
        own event loops, which would conflict with the main one.
        """
        return False

    def _run_aggregator(
        self, stage: "Stage", context: "Context", iterable: Iterable[Any]
    ) -> Iterator[Any]:
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
            # Materialize the iterable to know the count and allow hooks.
            materialized_items = list(iterable)
            items_in = len(materialized_items)
            stage.metrics["items_in"] += items_in

            stage.logger.debug("aggregation_input_materialized", items_in=items_in)

            if stage.hooks and stage.hooks.on_stream_end:
                stage.hooks.on_stream_end(context)

            results = stage._invoke(context, materialized_items)
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


def _handle_route_to_pipeline(stage: "Stage", context: "Context", item: Any):
    """Helper function to route a failed item to a separate pipeline."""
    # Local imports to avoid circular dependencies
    from ..core.pipeline import Pipeline
    from ..core.stage import Stage

    if not stage.error_policy.route_to_pipeline:
        return

    pipeline = stage.error_policy.route_to_pipeline
    if isinstance(pipeline, Stage):
        pipeline = Pipeline([pipeline])

    # We can just call the pipeline's collect method directly.
    # This is simpler than manually getting a runner.
    pipeline.collect([item])


def _handle_transform_and_retry(stage: "Stage", context: "Context", item: Any) -> Any:
    """
    Helper function to route a failed item to a transformer pipeline and
    return the transformed item.
    """
    from ..core.pipeline import Pipeline
    from ..core.stage import Stage

    if not stage.error_policy.route_to_pipeline:
        # This should be caught by the ErrorPolicy's validation, but as a safeguard:
        raise ValueError("Missing 'route_to_pipeline' for transform_and_retry mode.")

    pipeline = stage.error_policy.route_to_pipeline
    if isinstance(pipeline, Stage):
        pipeline = Pipeline([pipeline])

    results, _ = pipeline.collect([item])
    if len(results) != 1:
        raise ValueError(
            f"Transformer pipeline for 'route_to_pipeline_and_retry' must "
            f"produce exactly one item, but produced {len(results)}."
        )
    return results[0]


async def _handle_route_to_pipeline_async(
    stage: "Stage", context: "Context", item: Any
):
    """Async helper to route a failed item to a separate pipeline."""
    from ..core.pipeline import Pipeline
    from ..core.stage import Stage

    if not stage.error_policy.route_to_pipeline:
        return

    pipeline = stage.error_policy.route_to_pipeline
    if isinstance(pipeline, Stage):
        pipeline = Pipeline([pipeline])

    # Run the pipeline and consume the async stream to ensure it executes.
    stream, _ = await pipeline.arun([item])
    async for _ in stream:
        pass


async def _handle_transform_and_retry_async(
    stage: "Stage", context: "Context", item: Any
) -> Any:
    """
    Async helper to route a failed item to a transformer pipeline and
    return the transformed item.
    """
    from ..core.pipeline import Pipeline
    from ..core.stage import Stage

    if not stage.error_policy.route_to_pipeline:
        raise ValueError("Missing 'route_to_pipeline' for transform_and_retry mode.")

    pipeline = stage.error_policy.route_to_pipeline
    if isinstance(pipeline, Stage):
        pipeline = Pipeline([pipeline])

    stream, _ = await pipeline.arun([item])
    results = [res async for res in stream]
    if len(results) != 1:
        raise ValueError(
            f"Transformer pipeline for 'route_to_pipeline_and_retry' must "
            f"produce exactly one item, but produced {len(results)}."
        )
    return results[0]
