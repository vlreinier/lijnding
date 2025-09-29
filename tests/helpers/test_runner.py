from typing import Iterable, Any, Tuple, List
from lijnding.core import Pipeline, Context
import inspect

# A list of all backends to be tested
BACKENDS = ["serial", "thread", "process", "async"]


async def run_pipeline(
    pipeline: Pipeline, data: Iterable[Any]
) -> Tuple[List[Any], Context]:
    """
    Runs a pipeline and collects its results, automatically handling sync and async backends.
    """
    is_async_data = inspect.isasyncgen(data)
    if "async" in pipeline._get_required_backend_names() or is_async_data:
        return await pipeline.acollect(data)
    else:
        # This is a blocking call, but it's what's needed for sync backends.
        # The `pytest-asyncio` runner will handle running this in the event loop.
        return pipeline.collect(data)
