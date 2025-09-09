import pytest
from lijnding.core import Pipeline, stage
from tests.helpers.test_runner import run_pipeline, BACKENDS
import asyncio

@stage
def add_one_sync(x):
    return x + 1

@stage
def times_two_sync(x):
    return x * 2

@stage(backend="async")
async def add_one_async(x):
    await asyncio.sleep(0.001)
    return x + 1

@stage(backend="async")
async def times_two_async(x):
    await asyncio.sleep(0.001)
    return x * 2


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_multi_stage_transformation(backend):
    """Tests a pipeline with multiple simple transformation stages."""
    if backend == 'async':
        add_one = add_one_async
        times_two = times_two_async
    else:
        add_one = stage(add_one_sync.func, backend=backend)
        times_two = stage(times_two_sync.func, backend=backend)

    pipeline = Pipeline([add_one, times_two])
    data = [1, 2, 3]
    results, _ = await run_pipeline(pipeline, data)

    # x=1 -> 1+1=2 -> 2*2=4
    # x=2 -> 2+1=3 -> 3*2=6
    # x=3 -> 3+1=4 -> 4*2=8
    assert sorted(results) == [4, 6, 8]
