"""
This module provides the `retry` component, which wraps a sub-pipeline
and retries its execution on failure.
"""

from __future__ import annotations
import time
import asyncio
from typing import Any, Union, Iterable, AsyncIterator, List

from ..core.pipeline import Pipeline
from ..core.stage import Stage, stage
from ..core.context import Context


def retry(
    pipeline: Union[Stage, Pipeline],
    retries: int = 3,
    backoff: float = 0.1,
) -> Stage:
    """
    A factory function for creating a Retry component.

    This component wraps a pipeline and retries its execution if it fails.

    Args:
        pipeline: The Stage or Pipeline to execute and retry.
        retries: The maximum number of times to retry on failure.
        backoff: The delay in seconds between retries.

    Returns:
        A Stage that encapsulates the retry logic.
    """
    sub_pipeline = Pipeline([pipeline]) if isinstance(pipeline, Stage) else pipeline

    if not isinstance(sub_pipeline, Pipeline):
        raise TypeError("The 'pipeline' argument must be a Stage or Pipeline instance.")

    is_async = "async" in sub_pipeline._get_required_backend_names()

    if is_async:

        @stage(name=f"Retry(retries={retries})", stage_type="itemwise")
        async def _retry_func_async(context: Context, item: Any) -> AsyncIterator[Any]:
            last_exception = None
            # The loop runs `retries + 1` times (e.g., 1 initial attempt + 3 retries).
            for attempt in range(retries + 1):
                try:
                    stream, _ = await sub_pipeline.arun(data=[item])
                    results: List[Any] = [res async for res in stream]
                    for res in results:
                        yield res
                    return  # On success, exit the function.
                except Exception as e:
                    last_exception = e
                    if attempt < retries:
                        context.logger.warning(
                            f"Attempt {attempt + 1} failed. Retrying in {backoff}s...",
                            exc_info=e,
                        )
                        await asyncio.sleep(backoff)
            # If all attempts fail, raise the last recorded exception.
            context.logger.error(f"All {retries + 1} attempts failed.")
            raise last_exception

        return _retry_func_async
    else:

        @stage(name=f"Retry(retries={retries})", stage_type="itemwise")
        def _retry_func_sync(context: Context, item: Any) -> Iterable[Any]:
            last_exception = None
            # The loop runs `retries + 1` times (e.g., 1 initial attempt + 3 retries).
            for attempt in range(retries + 1):
                try:
                    results, _ = sub_pipeline.collect(data=[item])
                    yield from results
                    return  # On success, exit the function.
                except Exception as e:
                    last_exception = e
                    if attempt < retries:
                        context.logger.warning(
                            f"Attempt {attempt + 1} failed. Retrying in {backoff}s...",
                            exc_info=e,
                        )
                        time.sleep(backoff)
            # If all attempts fail, raise the last recorded exception.
            context.logger.error(f"All {retries + 1} attempts failed.")
            raise last_exception

        return _retry_func_sync
