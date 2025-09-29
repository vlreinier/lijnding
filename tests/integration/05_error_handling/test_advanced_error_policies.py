import pytest
from lijnding.core.pipeline import Pipeline
from lijnding.core.stage import stage
from lijnding.core.errors import ErrorPolicy

# --- Stages for testing ---


class Failer:
    def __init__(self, message="failed"):
        self.message = message

    def __call__(self, x):
        raise ValueError(self.message)


class StatefulFailer:
    def __init__(self, fail_on_items):
        self.fail_on_items = fail_on_items

    def __call__(self, x):
        if x in self.fail_on_items:
            raise ValueError(f"Intentional failure on item {x}")
        return x


def to_string(x):
    return str(x)


dead_letter_results = []


def collect_to_dead_letter(x):
    dead_letter_results.append(x)


# --- Synchronous Tests ---


def test_route_to_pipeline():
    """Tests the 'route_to_pipeline' error policy."""
    dead_letter_results.clear()

    # This stage will fail only for item 123
    failing_stage = stage(
        StatefulFailer(fail_on_items=[123]),
        error_policy=ErrorPolicy(
            mode="route_to_pipeline", route_to_pipeline=stage(collect_to_dead_letter)
        ),
    )

    pipeline = failing_stage | stage(to_string)

    result, _ = pipeline.collect([123, 456])

    assert dead_letter_results == [123]
    assert result == ["456"]


def test_transform_and_retry_succeeds():
    """Tests 'route_to_pipeline_and_retry' where the transform fixes the item."""

    def process_string(x):
        if not isinstance(x, str):
            raise TypeError("Input must be a string")
        return f"processed_{x}"

    transformer = Pipeline([stage(to_string)])

    failing_stage = stage(
        process_string,
        error_policy=ErrorPolicy(
            mode="route_to_pipeline_and_retry", route_to_pipeline=transformer, retries=1
        ),
    )

    result, _ = failing_stage.collect([123])

    assert result == ["processed_123"]


def test_transform_and_retry_fails():
    """Tests 'route_to_pipeline_and_retry' where the transform doesn't fix it."""

    # This stage will always fail, regardless of the transform.
    failing_stage = stage(
        Failer(message="always fails"),
        error_policy=ErrorPolicy(
            mode="route_to_pipeline_and_retry",
            route_to_pipeline=stage(to_string),
            retries=1,
        ),
    )

    with pytest.raises(ValueError, match="always fails"):
        failing_stage.collect([123])


# --- Asynchronous Tests ---


@pytest.mark.asyncio
async def test_async_route_to_pipeline():
    """Tests the async 'route_to_pipeline' error policy."""
    dead_letter_results.clear()

    failing_stage = stage(
        StatefulFailer(fail_on_items=[123]),
        backend="async",
        error_policy=ErrorPolicy(
            mode="route_to_pipeline",
            route_to_pipeline=stage(collect_to_dead_letter, backend="async"),
        ),
    )

    pipeline = failing_stage | stage(to_string, backend="async")

    result, _ = await pipeline.acollect([123, 456])

    assert dead_letter_results == [123]
    assert result == ["456"]
