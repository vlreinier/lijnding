"""
This module provides the `while_loop` component, which allows for creating
loops within a pipeline that execute as long as a condition is true.
"""

from __future__ import annotations

from typing import Any, Callable, Union, Iterable, AsyncIterator, List

from ..core.pipeline import Pipeline
from ..core.stage import Stage, stage
from ..core.context import Context


def while_loop(condition: Callable[[Any], bool], body: Union[Stage, Pipeline]) -> Stage:
    """
    A factory function for creating a While component.

    This component executes a 'body' pipeline repeatedly as long as a
    'condition' is true.

    The body pipeline must take a single item as input and produce a single
    item as output. This output is then fed back into the condition and the
    next iteration of the body.

    Args:
        condition: A callable that takes an item and returns True if the
                   loop should continue, False otherwise.
        body: The Stage or Pipeline to execute in each iteration.

    Returns:
        A Stage that encapsulates the while loop logic.
    """
    if isinstance(body, Stage):
        body_pipeline = Pipeline([body])
    elif isinstance(body, Pipeline):
        body_pipeline = body
    else:
        raise TypeError(f"Body must be a Stage or Pipeline, not {type(body)}")

    # The `while_loop` component will always be defined as an async stage.
    # This is because the synchronous version (`_while_func_sync`) can cause
    # deadlocks when executed by an async runner, as it uses the blocking
    # `collect()` method. The async version is more robust and can handle
    # both sync and async bodies correctly by using `run_async()`.

    @stage(name="While", stage_type="itemwise", backend="async")
    async def _while_func_async(context: Context, item: Any) -> AsyncIterator[Any]:
        current_item = item
        # The loop first checks the condition, then executes the body.
        while condition(current_item):
            stream, _ = await body_pipeline.run_async(data=[current_item])
            results: List[Any] = [res async for res in stream]

            # The body must produce a single output item to be used in the
            # next iteration's condition check.
            if len(results) != 1:
                raise ValueError(
                    f"The body of a while_loop must produce exactly one item, "
                    f"but it produced {len(results)} items."
                )
            current_item = results[0]
        # Yield the final item after the loop terminates.
        yield current_item

    return _while_func_async
