"""
Integration tests for advanced features of the LijnDing framework, such as
nested pipelines, branching, error handling, and different execution backends.
"""

import pytest
import time
from lijnding.core import stage, aggregator_stage, Pipeline
from lijnding.core.errors import ErrorPolicy
from lijnding.components.branch import branch

# --- Test Stages ---

def add(n):
    """A component factory that creates a stage to add a number."""
    @stage(name=f"add_{n}")
    def _add(x: int) -> int:
        return x + n
    return _add


def multiply(n):
    """A component factory that creates a stage to multiply by a number."""
    @stage(name=f"multiply_{n}")
    def _multiply(x: int) -> int:
        return x * n
    return _multiply

@stage
def to_string(x):
    return str(x)

@stage(name="failable_stage")
def failable_stage(x):
    if x == 2:
        raise ValueError("I don't like the number 2")
    return x

@stage
def slow_stage(x):
    time.sleep(0.1)
    return x

# --- Tests ---

def test_nested_pipeline():
    """Tests that a pipeline can be nested inside another."""
    inner_pipeline = add(1) | multiply(2)  # (x + 1) * 2

    # The inner pipeline is converted to a single stage
    outer_pipeline = multiply(10) | inner_pipeline.to_stage() | to_string

    data = [1, 2, 3]
    results, _ = outer_pipeline.collect(data)

    # 1 -> 10 -> (10 + 1) * 2 = 22 -> "22"
    # 2 -> 20 -> (20 + 1) * 2 = 42 -> "42"
    # 3 -> 30 -> (30 + 1) * 2 = 62 -> "62"
    assert results == ["22", "42", "62"]

def test_branching_zip():
    """Tests the branch component with the 'zip' merge strategy."""
    pipeline = branch(
        add(1),
        multiply(2),
        to_string,
        merge="zip"
    )
    data = [1, 10]
    results, _ = pipeline.collect(data)

    # 1 -> (2, 2, "1")
    # 10 -> (11, 20, "10")
    assert results == [(2, 2, "1"), (11, 20, "10")]

def test_branching_concat():
    """Tests the branch component with the 'concat' merge strategy."""
    pipeline = branch(
        add(1),
        multiply(2),
        merge="concat"
    )
    data = [1, 10]
    results, _ = pipeline.collect(data)

    # 1 -> (2, 2)
    # 10 -> (11, 20)
    # Concatenated: [2, 2, 11, 20]
    assert results == [2, 2, 11, 20]

def test_error_policy_skip():
    """Tests that the 'skip' error policy correctly skips failing items."""
    pipeline = failable_stage | add(1)
    pipeline.stages[0].error_policy = ErrorPolicy(mode="skip")

    data = [1, 2, 3]
    results, _ = pipeline.collect(data)

    # 1 -> 1 -> 2
    # 2 -> raises error, skipped
    # 3 -> 3 -> 4
    assert results == [2, 4]

@pytest.mark.parametrize("backend", ["thread", "process"])
def test_concurrent_backends(backend):
    """Tests that the thread and process backends execute correctly."""
    pipeline = slow_stage | add(1)
    pipeline.stages[0].backend = backend
    pipeline.stages[0].workers = 2

    data = [1, 2, 3, 4]
    start_time = time.time()
    results, _ = pipeline.collect(data)
    end_time = time.time()

    # The 4 tasks of 0.1s should run on 2 workers, taking ~0.2s
    # A serial pipeline would take > 0.4s. We give a very generous buffer
    # to account for process startup overhead.
    assert (end_time - start_time) < 1.0
    assert sorted(results) == [2, 3, 4, 5]

# --- Retry Test Helpers ---

RETRY_ATTEMPTS = {}

@stage(name="retry_stage")
def retry_stage(x):
    """A stage that fails the first two times it sees a specific item."""
    if x not in RETRY_ATTEMPTS:
        RETRY_ATTEMPTS[x] = 0
    RETRY_ATTEMPTS[x] += 1

    if RETRY_ATTEMPTS[x] < 3:
        raise ValueError(f"Attempt {RETRY_ATTEMPTS[x]} for {x} failed")
    return x

def test_error_policy_retry():
    """Tests that the 'retry' error policy correctly retries and succeeds."""
    # Reset state for this test
    global RETRY_ATTEMPTS
    RETRY_ATTEMPTS = {}

    pipeline = retry_stage | add(1)
    # The stage will be retried 2 times on failure
    pipeline.stages[0].error_policy = ErrorPolicy(mode="retry", retries=2, backoff=0.001)

    data = [1, 2]
    results, _ = pipeline.collect(data)

    # Each item fails twice and succeeds on the third attempt (1 initial + 2 retries)
    # 1 -> fail, retry -> fail, retry -> succeed -> 2
    # 2 -> fail, retry -> fail, retry -> succeed -> 3
    assert results == [2, 3]
    assert RETRY_ATTEMPTS[1] == 3
    assert RETRY_ATTEMPTS[2] == 3