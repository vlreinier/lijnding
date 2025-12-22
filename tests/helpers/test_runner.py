from typing import Iterable, Any, Tuple, List
import asyncio

from lijnding.core import Pipeline, PipelineContext


async def run_pipeline(
    pipeline: Pipeline, data: Iterable[Any]
) -> Tuple[List[Any], PipelineContext]:
    """
    Runs a pipeline and collects its results, automatically handling sync and async backends.
    """
    return await pipeline.run(data)
