import pytest

from lijnding.core import stage
from lijnding.components.branch import branch


@stage
def to_upper(x: str) -> str:
    return x.upper()


@stage
def to_lower(x: str) -> str:
    return x.lower()


@stage
def exclaim(x: str) -> str:
    return f"{x}!"


def test_branch_concat():
    pipeline = stage(lambda x: x) | branch(to_upper, to_lower)
    results, _ = pipeline.collect(["Hello", "World"])
    assert results == ["HELLO", "hello", "WORLD", "world"]


def test_branch_zip():
    pipeline = stage(lambda x: x) | branch(to_upper, exclaim, merge="zip")
    results, _ = pipeline.collect(["a", "b"])
    assert results == [("A", "a!"), ("B", "b!")]


def test_branch_with_uneven_outputs():
    @stage
    def multi_yield(x):
        yield x
        yield x

    # The 'zip' strategy stops when the shortest branch is exhausted.
    # `exclaim` yields 1 item, `multi_yield` yields 2. The pipeline stops after 1.
    pipeline_zip = stage(lambda x: x) | branch(multi_yield, exclaim, merge="zip")
    results_zip, _ = pipeline_zip.collect(["a", "b"])
    assert results_zip == [("a", "a!"), ("b", "b!")]

    # The 'zip_longest' strategy continues until the longest branch is exhausted.
    # The second result from `multi_yield` is paired with the `fillvalue` (None).
    pipeline_zip_longest = stage(lambda x: x) | branch(
        multi_yield, exclaim, merge="zip_longest"
    )
    results_zip_longest, _ = pipeline_zip_longest.collect(["a"])
    assert results_zip_longest == [("a", "a!"), ("a", None)]


@stage(backend="async")
async def to_upper_async(x: str) -> str:
    return x.upper()


@stage(backend="async")
async def exclaim_async(x: str) -> str:
    return f"{x}!"


@pytest.mark.asyncio
async def test_branch_zip_async():
    pipeline = stage(lambda x: x) | branch(to_upper_async, exclaim_async, merge="zip")
    stream, _ = await pipeline.run(["a", "b"])
    results = [item async for item in stream]
    assert results == [("A", "a!"), ("B", "b!")]


@pytest.mark.asyncio
async def test_branch_with_uneven_outputs_async():
    @stage(backend="async")
    async def multi_yield_async(x):
        yield x
        yield x

    # Test with one sync and one async stage. The 'zip' strategy stops when
    # the shortest branch (`exclaim`, which yields 1 item) is exhausted.
    pipeline_zip = stage(lambda x: x) | branch(multi_yield_async, exclaim, merge="zip")
    stream_zip, _ = await pipeline_zip.run(["a", "b"])
    results_zip = [item async for item in stream_zip]
    assert results_zip == [("a", "a!"), ("b", "b!")]

    # The 'zip_longest' strategy continues until the longest branch is exhausted.
    pipeline_zip_longest = stage(lambda x: x) | branch(
        multi_yield_async, exclaim, merge="zip_longest"
    )
    stream_zip_longest, _ = await pipeline_zip_longest.run(["a"])
    results_zip_longest = [item async for item in stream_zip_longest]
    assert results_zip_longest == [("a", "a!"), ("a", None)]
