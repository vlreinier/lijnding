import pytest

from lijnding.core.stage import stage
from lijnding.components.do_while import do_while


# --- Synchronous Tests ---


def test_simple_do_while_loop():
    """Tests a basic do-while loop that iterates a few times."""

    @stage
    def increment(x):
        return x + 1

    # Loop while x < 5. Should run for 0, 1, 2, 3, 4
    pipeline = do_while(lambda x: x < 5, increment)
    result, _ = pipeline.collect([0])

    assert result == [5]


def test_do_while_loop_runs_once():
    """Tests that the do-while loop runs exactly once if the condition is false after the first run."""

    @stage
    def increment(x):
        return x + 1

    # The condition `x < 0` will be false after the first iteration (0 -> 1)
    pipeline = do_while(lambda x: x < 0, increment)
    result, _ = pipeline.collect([0])

    assert result == [1]


# --- Asynchronous Tests ---


@pytest.mark.asyncio
async def test_async_do_while_loop():
    """Tests a basic do-while loop in an async pipeline."""

    @stage
    async def increment_async(x):
        return x + 1

    # Loop while x < 5
    pipeline = do_while(lambda x: x < 5, increment_async)
    result, _ = await pipeline.acollect([0])

    assert result == [5]


@pytest.mark.asyncio
async def test_async_do_while_runs_once():
    """Tests that the async do-while loop runs once if the condition is false."""

    @stage
    async def increment_async(x):
        return x + 1

    # The condition `x < 0` will be false after the first iteration (0 -> 1)
    pipeline = do_while(lambda x: x < 0, increment_async)
    result, _ = await pipeline.acollect([0])

    assert result == [1]
