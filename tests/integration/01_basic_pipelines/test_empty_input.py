import pytest
from lijnding.core import Pipeline, stage
from tests.helpers.test_runner import run_pipeline, BACKENDS
import asyncio

@stage
def multiply_by_two_sync(x):
    return x * 2

@stage(backend="async")
async def multiply_by_two_async(x):
    await asyncio.sleep(0.001)
    return x * 2


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_empty_input(backend):
    """Tests that the pipeline runs correctly with an empty list of input data."""
    if backend == 'async':
        s = multiply_by_two_async
    else:
        s = stage(multiply_by_two_sync.func, backend=backend)

    pipeline = Pipeline([s])
    results, _ = await run_pipeline(pipeline, [])
    assert results == []
