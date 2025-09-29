import pytest
from lijnding.core import stage


@pytest.mark.parametrize("backend", ["serial", "thread", "async"])
@pytest.mark.asyncio
async def test_source_stage_no_args(backend):
    """Tests that a pipeline with a source stage can be run with no input args."""
    # The 'process' backend cannot serialize a generator function defined
    # inside another function, so it is excluded from this test.

    @stage(backend=backend)
    def my_generator():
        yield 1
        yield 2
        yield 3

    pipeline = my_generator | stage(lambda x: x * 10, backend=backend)

    # For a source stage, we should be able to call run/collect with no arguments.
    if backend == "async":
        # We must use acollect for async backends
        results, _ = await pipeline.acollect()
    else:
        results, _ = pipeline.collect()

    assert sorted(results) == [10, 20, 30]
