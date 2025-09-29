import pytest

from lijnding.core.pipeline import Pipeline
from lijnding.core.stage import stage
from lijnding.components.while_loop import while_loop


# --- Synchronous Tests ---


def test_simple_while_loop():
    """Tests a basic while loop that iterates a few times."""

    @stage
    def increment(x):
        return x + 1

    # Loop while x < 5
    pipeline = while_loop(lambda x: x < 5, increment)
    result, _ = pipeline.collect([0])

    assert result == [5]


def test_while_loop_does_not_run():
    """Tests that the while loop doesn't run if the condition is initially false."""

    @stage
    def should_not_run(x):
        # This stage should never be executed
        pytest.fail("The body of the while loop should not have been executed.")
        return x

    # Loop while x < 0 (which is false for the input 0)
    pipeline = while_loop(lambda x: x < 0, should_not_run)
    result, _ = pipeline.collect([0])

    assert result == [0]


def test_while_loop_with_pipeline_body():
    """Tests a while loop where the body is a multi-stage pipeline."""

    @stage
    def add_one(x):
        return x + 1

    @stage
    def add_two(x):
        return x + 2

    body_pipeline = Pipeline([add_one, add_two])  # Adds 3 in total

    # Loop while x < 10
    pipeline = while_loop(lambda x: x < 10, body_pipeline)
    result, _ = pipeline.collect([0])

    # 0 -> 3 -> 6 -> 9 -> 12
    assert result == [12]


def test_while_loop_raises_error_on_multiple_items():
    """Tests that the loop raises an error if the body produces more than one item."""

    @stage
    def duplicate(x):
        yield x
        yield x

    pipeline = while_loop(lambda x: x < 5, duplicate)

    with pytest.raises(ValueError, match="must produce exactly one item"):
        pipeline.collect([0])


# --- Asynchronous Tests ---


@pytest.mark.asyncio
async def test_async_while_loop():
    """Tests a basic while loop in an async pipeline."""

    @stage(backend="async")
    async def increment_async(x):
        return x + 1

    # Loop while x < 5
    pipeline = while_loop(lambda x: x < 5, increment_async)
    result, _ = await pipeline.acollect([0])

    assert result == [5]


@pytest.mark.asyncio
async def test_async_while_loop_with_sync_body():
    """Tests an async while loop with a synchronous body pipeline."""

    @stage
    def add_one(x):
        return x + 1

    @stage
    def add_two(x):
        return x + 2

    body_pipeline = Pipeline([add_one, add_two])

    # Loop while x < 10
    pipeline = while_loop(lambda x: x < 10, body_pipeline)
    result, _ = await pipeline.acollect([0])

    # 0 -> 3 -> 6 -> 9 -> 12
    assert result == [12]


@pytest.mark.asyncio
async def test_async_while_loop_raises_error():
    """Tests that the async loop raises an error if the body produces more than one item."""

    @stage(backend="async")
    async def duplicate_async(x):
        yield x
        yield x

    pipeline = while_loop(lambda x: x < 5, duplicate_async)

    with pytest.raises(ValueError, match="must produce exactly one item"):
        await pipeline.acollect([0])
