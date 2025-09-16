import pytest
import asyncio
from lijnding.core.pipeline import Pipeline
from lijnding.components.batch import batch
from lijnding.components.reduce import reduce_


def test_batch_component_sync():
    """Tests the synchronous batching functionality (no timeout)."""
    pipeline = Pipeline([batch(size=2)])
    results, _ = pipeline.collect([1, 2, 3, 4])
    assert results == [[1, 2], [3, 4]]

    pipeline_partial = Pipeline([batch(size=3)])
    results_partial, _ = pipeline_partial.collect([1, 2, 3, 4, 5])
    assert results_partial == [[1, 2, 3], [4, 5]]


@pytest.mark.asyncio
async def test_batch_component_async_timeout():
    """Tests the asynchronous batching with a timeout."""
    # Test that a batch is emitted when the timeout is reached
    pipeline = Pipeline([batch(size=5, timeout=0.1)])

    async def data_stream():
        yield 1
        yield 2
        await asyncio.sleep(0.2)
        yield 3

    stream, _ = await pipeline.run_async(data_stream())
    final_results = [item async for item in stream]

    assert final_results == [[1, 2], [3]]


@pytest.mark.asyncio
async def test_batch_full_before_timeout():
    """Tests that a full batch is emitted immediately, even with a timeout."""
    pipeline = Pipeline([batch(size=2, timeout=5)]) # Long timeout

    async def data_stream():
        yield 1
        yield 2
        yield 3

    stream, _ = await pipeline.run_async(data_stream())
    final_results = [item async for item in stream]

    assert final_results == [[1, 2], [3]]


def test_reduce_component():
    """Tests the reduce component, now as a generator."""
    pipeline = Pipeline([reduce_(lambda a, b: a + b, 100)])
    results, _ = pipeline.collect([1, 2, 3])
    assert results == [106]

    pipeline_no_init = Pipeline([reduce_(lambda a, b: a + b)])
    results_no_init, _ = pipeline_no_init.collect([1, 2, 3])
    assert results_no_init == [6]

    pipeline_empty, _ = pipeline_no_init.collect([])
    assert pipeline_empty == []
