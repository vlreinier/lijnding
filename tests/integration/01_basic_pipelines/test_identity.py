import pytest
from lijnding.core import Pipeline, stage
from tests.helpers.test_runner import run_pipeline, BACKENDS


@stage
def identity_sync(x):
    return x

@stage(backend="async")
async def identity_async(x):
    return x


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.asyncio
async def test_identity_pipeline(backend):
    """Tests that a simple single-stage pipeline returns the data it was given."""
    if backend == 'async':
        identity_stage = identity_async
    else:
        # Create a new stage with the desired backend
        identity_stage = stage(identity_sync.func, backend=backend)

    pipeline = Pipeline([identity_stage])
    data = [1, 5, 2, 4, 3]

    results, _ = await run_pipeline(pipeline, data)

    assert sorted(results) == sorted(data)
