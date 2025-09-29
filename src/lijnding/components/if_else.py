"""
This module provides the `if_else` component, which allows for conditional
routing of items to one of two different sub-pipelines.
"""

from __future__ import annotations

from typing import Any, Callable, Union, Iterable, AsyncIterator

from ..core.pipeline import Pipeline
from ..core.stage import Stage, stage
from ..core.context import Context


def if_else(
    condition: Callable[[Any], bool],
    if_true: Union[Stage, Pipeline],
    if_false: Union[Stage, Pipeline],
) -> Stage:
    """
    A factory function for creating an If-Else component.

    This component routes an item to one of two pipelines based on a
    condition function.

    Args:
        condition: A callable that takes an item and returns True to route
                   to `if_true`, or False to route to `if_false`.
        if_true: The Stage or Pipeline to execute if the condition is True.
        if_false: The Stage or Pipeline to execute if the condition is False.

    Returns:
        A Stage that encapsulates the conditional logic.
    """
    if_true_pipeline = Pipeline([if_true]) if isinstance(if_true, Stage) else if_true
    if_false_pipeline = (
        Pipeline([if_false]) if isinstance(if_false, Stage) else if_false
    )

    if not isinstance(if_true_pipeline, Pipeline) or not isinstance(
        if_false_pipeline, Pipeline
    ):
        raise TypeError("if_true and if_false must be Stage or Pipeline instances.")

    # If either of the branches is async, the entire component must be async.
    is_async = (
        "async" in if_true_pipeline._get_required_backend_names()
        or "async" in if_false_pipeline._get_required_backend_names()
    )

    if is_async:

        @stage(name="IfElse", stage_type="itemwise", backend="async")
        async def _if_else_func_async(
            context: Context, item: Any
        ) -> AsyncIterator[Any]:
            # Route the item to the appropriate pipeline based on the condition.
            if condition(item):
                stream, _ = await if_true_pipeline.arun(data=[item])
            else:
                stream, _ = await if_false_pipeline.arun(data=[item])

            # Yield the results from the chosen pipeline.
            async for res in stream:
                yield res

        return _if_else_func_async
    else:

        @stage(name="IfElse", stage_type="itemwise")
        def _if_else_func_sync(context: Context, item: Any) -> Iterable[Any]:
            # Route the item to the appropriate pipeline based on the condition.
            if condition(item):
                stream, _ = if_true_pipeline.run(data=[item])
            else:
                stream, _ = if_false_pipeline.run(data=[item])

            # Yield the results from the chosen pipeline.
            yield from stream

        return _if_else_func_sync
