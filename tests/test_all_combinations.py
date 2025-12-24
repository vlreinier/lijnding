import multiprocessing
import asyncio
import pytest
from lijnding.core.pipeline import Pipeline, PipelineConfig, Runner
from lijnding.core.context import PipelineContext

multiprocessing.freeze_support()

async def async_stage_func(data, ctx: "PipelineContext"):
    ctx.increment_counter()
    await asyncio.sleep(0.01)
    return data * 2

def sync_stage_func(data, ctx: "PipelineContext"):
    ctx.increment_counter()
    return data * 2

@pytest.mark.parametrize("runner1", [Runner.ASYNC, Runner.SYNC, Runner.PROCESS, Runner.THREAD])
@pytest.mark.parametrize("runner2", [Runner.ASYNC, Runner.SYNC, Runner.PROCESS, Runner.THREAD])
@pytest.mark.asyncio
async def test_all_combinations(runner1, runner2):
    config = PipelineConfig(
        fail_fast=True,
        buffer_size=10,
        sync_gen_buffer=2,
    )
    pipeline = Pipeline(config)
    pipeline.add_stage(async_stage_func if runner1 == Runner.ASYNC else sync_stage_func, runner1, workers=2)
    pipeline.add_stage(async_stage_func if runner2 == Runner.ASYNC else sync_stage_func, runner2, workers=2)
    items = [1, 2, 3]
    results, count = await pipeline.run(items)
    assert len(results) == 3
    assert count == 6
    assert set(results) == {4, 8, 12}
