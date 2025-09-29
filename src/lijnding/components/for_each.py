"""
This module provides the `for_each` component, which is used to apply a
sub-pipeline to each element of a nested iterable within an item.
"""

from __future__ import annotations

from typing import Any, Callable, Union, Iterable, AsyncIterator, List

from ..core.pipeline import Pipeline
from ..core.stage import Stage, stage
from ..core.context import Context


def for_each(
    pipeline: Union[Stage, Pipeline],
    selector: Callable[[Any], Iterable[Any]],
) -> Stage:
    """
    A factory function for creating a ForEach component.

    This component takes an item, extracts an iterable from it using a
    `selector` function, and then runs a sub-pipeline for each element
    in that iterable.

    The results from all runs of the sub-pipeline are collected into a single
    list, which is then yielded as the output of the stage.

    Args:
        pipeline: The Stage or Pipeline to execute for each element.
        selector: A function that takes the input item and returns an
                  iterable of elements to be processed.

    Returns:
        A Stage that, for each input item, yields a single list containing
        all the results from the sub-pipeline runs.
    """
    sub_pipeline = Pipeline([pipeline]) if isinstance(pipeline, Stage) else pipeline

    if not isinstance(sub_pipeline, Pipeline):
        raise TypeError("The 'pipeline' argument must be a Stage or Pipeline instance.")

    is_async = "async" in sub_pipeline._get_required_backend_names()

    if is_async:

        @stage(name="ForEach", stage_type="itemwise")
        async def _for_each_func_async(
            context: Context, item: Any
        ) -> AsyncIterator[Any]:
            # Use the selector to extract the iterable of elements from the input item.
            elements = selector(item)
            all_results = []
            # For each element, run the sub-pipeline and collect the results.
            for element in elements:
                stream, _ = await sub_pipeline.arun(data=[element])
                results: List[Any] = [res async for res in stream]
                all_results.extend(results)
            # Yield a single list containing all the results.
            yield all_results

        return _for_each_func_async
    else:

        @stage(name="ForEach", stage_type="itemwise")
        def _for_each_func_sync(context: Context, item: Any) -> Iterable[Any]:
            # Use the selector to extract the iterable of elements from the input item.
            elements = selector(item)
            all_results = []
            # For each element, run the sub-pipeline and collect the results.
            for element in elements:
                results, _ = sub_pipeline.collect(data=[element])
                all_results.extend(results)
            # Yield a single list containing all the results.
            yield all_results

        return _for_each_func_sync
