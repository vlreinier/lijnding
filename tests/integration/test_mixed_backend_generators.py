import asyncio
import pytest
from lijnding.core.stage import stage
from lijnding.components.batch import batch
from ..helpers.test_runner import run_pipeline


@stage
async def async_identity(x: int) -> int:
    """A simple async stage that passes data through."""
    await asyncio.sleep(0.01)
    return x


@pytest.mark.asyncio
async def test_async_to_sync_generator_pipeline():
    """
    Tests a pipeline that transitions from an async stage to a synchronous
    generator stage.

    This is a critical test case that validates the framework's ability to
    handle a synchronous generator running within an async pipeline, which
    requires the AsyncioRunner to correctly bridge the two execution models.
    This was the scenario that caused the hanging bug.
    """
    # The pipeline starts with an async stage, then pipes to a sync generator.
    # The `batch` component with no timeout is a synchronous generator.
    pipeline = async_identity | batch(size=2)

    data = [1, 2, 3, 4, 5]

    # The `run_pipeline` helper runs the pipeline and collects the results.
    # It will use the `acollect` method because the pipeline contains an
    # async stage.
    results, _ = await run_pipeline(pipeline, data)

    # Input: [1, 2, 3, 4, 5]
    # After async_identity: [1, 2, 3, 4, 5]
    # After batch(size=2): [[1, 2], [3, 4], [5]]
    assert results == [[1, 2], [3, 4], [5]]