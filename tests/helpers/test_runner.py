from typing import Iterable, Any, Tuple, List
from lijnding.core import Pipeline, Context
import inspect

# A list of all backends to be tested
BACKENDS = ["serial", "thread", "process", "async"]


async def run_pipeline(
    pipeline: Pipeline, data: Iterable[Any]
) -> Tuple[List[Any], Context]:
    """
    Runs a pipeline and collects its results.
    Assumes it is being called from an async context (e.g., a test marked
    with @pytest.mark.asyncio).
    """
    stream, context = await pipeline.run(data)
    results = [item async for item in stream]
    return results, context
