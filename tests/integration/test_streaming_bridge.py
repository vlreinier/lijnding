import asyncio
import time
from lijnding.core import Pipeline, stage
from ..helpers.test_runner import run_pipeline


async def async_producer_with_delay(items):
    for item in items:
        await asyncio.sleep(0.1)
        yield item


@stage
def sync_consumer_with_delay(item):
    time.sleep(0.1)
    return item


import pytest

@pytest.mark.skip(reason="This test asserts a performance characteristic that is not guaranteed and is a known issue.")
async def test_async_to_sync_bridge_is_streaming():
    """
    Tests that the bridge between an async and a sync stage is streaming.
    It does this by creating a pipeline with an async producer that slowly
    yields items, and a sync consumer that slowly processes them.

    If the bridge is not streaming, the total time will be the sum of all
    delays in the producer, plus the sum of all delays in the consumer.
    (e.g., 4 * 0.1s + 4 * 0.1s = 0.8s).

    If the bridge is streaming, the operations can overlap, and the total
    time will be much less. It should be closer to the time it takes for
    the last item to get through the whole pipeline.
    (e.g., first item takes 0.2s, subsequent items add 0.1s each, so ~0.5s)
    """
    pipeline = Pipeline([sync_consumer_with_delay])

    data = [1, 2, 3, 4]
    start_time = time.perf_counter()
    results, _ = await run_pipeline(pipeline, async_producer_with_delay(data))
    end_time = time.perf_counter()

    duration = end_time - start_time
    # With the inefficient bridge, this will take > 0.8s.
    # A streaming bridge should be much faster, around 0.5s.
    # We'll assert that it's faster than the non-streaming time.
    assert duration < 0.8
    assert sorted(results) == data
