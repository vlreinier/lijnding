import pytest

from lijnding.core.pipeline import Pipeline
from lijnding.core.stage import stage
from lijnding.components.for_each import for_each


@stage
def double(x):
    return x * 2


@stage
def to_string(x):
    return str(x)


# --- Synchronous Tests ---


def test_for_each_simple_case():
    """Tests that for_each applies a pipeline to each element of a list."""

    # The input item will be a list of numbers, e.g., [1, 2, 3]
    # The selector will just return the item itself.
    # The pipeline will double each number.
    pipeline = for_each(pipeline=double, selector=lambda x: x)

    result, _ = pipeline.collect([[1, 2, 3, 4]])

    # The result should be a single item (a list) containing the results
    # of applying the pipeline to each element.
    assert result == [[2, 4, 6, 8]]


def test_for_each_with_dict():
    """Tests that for_each can iterate over values in a dictionary."""

    # The input item is a dict. The selector will extract the values.
    # The pipeline will convert each value to a string.
    data = {"a": 1, "b": 2, "c": 3}
    pipeline = for_each(pipeline=to_string, selector=lambda d: d.values())

    result, _ = pipeline.collect([data])

    assert result == [["1", "2", "3"]]


def test_for_each_with_pipeline_body():
    """Tests for_each with a multi-stage sub-pipeline."""

    sub_pipeline = Pipeline([double, to_string])
    pipeline = for_each(pipeline=sub_pipeline, selector=lambda x: x)

    result, _ = pipeline.collect([[1, 2, 3]])

    assert result == [["2", "4", "6"]]


# --- Asynchronous Tests ---


@stage(backend="async")
async def double_async(x):
    return x * 2


@pytest.mark.asyncio
async def test_async_for_each():
    """Tests an async for_each component."""

    pipeline = for_each(pipeline=double_async, selector=lambda x: x)

    result, _ = await pipeline.acollect([[1, 2, 3, 4]])

    assert result == [[2, 4, 6, 8]]
