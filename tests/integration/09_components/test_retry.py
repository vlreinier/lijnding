import pytest

from lijnding.core.stage import stage
from lijnding.components.retry import retry


class FailCounter:
    def __init__(self, fail_times):
        self.attempts = 0
        self.fail_times = fail_times

    def call(self, x):
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise ValueError(f"Intentional failure on attempt {self.attempts}")
        return x * 2


# --- Synchronous Tests ---


def test_retry_succeeds_on_first_try():
    """Tests that the pipeline succeeds without retrying if there's no failure."""
    fail_counter = FailCounter(fail_times=0)

    @stage
    def successful_stage(x):
        return fail_counter.call(x)

    pipeline = retry(successful_stage, retries=3)
    result, _ = pipeline.collect([10])

    assert result == [20]
    assert fail_counter.attempts == 1


def test_retry_succeeds_after_failures():
    """Tests that the pipeline succeeds after a few retries."""
    fail_counter = FailCounter(fail_times=2)  # Fails 2 times, succeeds on the 3rd

    @stage
    def failing_stage(x):
        return fail_counter.call(x)

    pipeline = retry(failing_stage, retries=3, backoff=0.01)
    result, _ = pipeline.collect([10])

    assert result == [20]
    assert fail_counter.attempts == 3


def test_retry_fails_after_all_attempts():
    """Tests that the pipeline fails when all retry attempts are exhausted."""
    fail_counter = FailCounter(
        fail_times=4
    )  # Fails all 4 attempts (1 initial + 3 retries)

    @stage
    def failing_stage(x):
        return fail_counter.call(x)

    pipeline = retry(failing_stage, retries=3, backoff=0.01)

    with pytest.raises(ValueError, match="Intentional failure on attempt 4"):
        pipeline.collect([10])

    assert fail_counter.attempts == 4


# --- Asynchronous Tests ---


@pytest.mark.asyncio
async def test_async_retry_succeeds_after_failures():
    """Tests that the async retry component succeeds after failures."""
    fail_counter = FailCounter(fail_times=2)

    @stage
    async def failing_stage_async(x):
        return fail_counter.call(x)

    pipeline = retry(failing_stage_async, retries=3, backoff=0.01)

    result, _ = await pipeline.acollect([10])

    assert result == [20]
    assert fail_counter.attempts == 3
