import pytest

from lijnding.core.pipeline import Pipeline
from lijnding.core.stage import stage
from lijnding.components.if_else import if_else


def is_even(n):
    return n % 2 == 0


@stage
def double(x):
    return x * 2


@stage
def increment(x):
    return x + 1


# --- Synchronous Tests ---


def test_if_else_true_branch():
    """Tests that the 'if_true' branch is taken when the condition is true."""
    # If even, double it. If odd, increment it.
    pipeline = if_else(is_even, if_true=double, if_false=increment)

    # Input is 4 (even), so it should be doubled to 8
    result, _ = pipeline.collect([4])
    assert result == [8]


def test_if_else_false_branch():
    """Tests that the 'if_false' branch is taken when the condition is false."""
    pipeline = if_else(is_even, if_true=double, if_false=increment)

    # Input is 5 (odd), so it should be incremented to 6
    result, _ = pipeline.collect([5])
    assert result == [6]


def test_if_else_with_multiple_items():
    """Tests the if_else component with a stream of multiple items."""
    pipeline = Pipeline([if_else(is_even, if_true=double, if_false=increment)])

    data = [1, 2, 3, 4, 5]
    # Expected: [ (1+1), (2*2), (3+1), (4*2), (5+1) ] = [2, 4, 4, 8, 6]
    expected = [2, 4, 4, 8, 6]

    result, _ = pipeline.collect(data)
    assert result == expected


# --- Asynchronous Tests ---


@stage(backend="async")
async def double_async(x):
    return x * 2


@stage(backend="async")
async def increment_async(x):
    return x + 1


@pytest.mark.asyncio
async def test_async_if_else_true_branch():
    """Tests the async 'if_true' branch."""
    pipeline = if_else(is_even, if_true=double_async, if_false=increment_async)

    result, _ = await pipeline.acollect([4])
    assert result == [8]


@pytest.mark.asyncio
async def test_async_if_else_false_branch():
    """Tests the async 'if_false' branch."""
    pipeline = if_else(is_even, if_true=double_async, if_false=increment_async)

    result, _ = await pipeline.acollect([5])
    assert result == [6]
