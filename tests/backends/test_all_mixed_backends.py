"""
Tests for the new async-native backend architecture.
This file covers various combinations of mixed backends to ensure
the new design is robust and free of deadlocks.
"""

import pytest
import time
import asyncio

from lijnding import Pipeline, stage, from_iterable
from lijnding.core.context import Context

# --- Test Stages ---

@stage(backend="serial")
def add_one_sync(item: int) -> int:
    """A simple synchronous stage."""
    return item + 1

@stage(backend="async", workers=4)
async def add_ten_async(item: int) -> int:
    """A native async stage."""
    await asyncio.sleep(0.01)  # Simulate non-blocking I/O
    return item + 10

@stage(backend="thread", workers=4)
def add_hundred_sync_thread(item: int) -> int:
    """A sync stage for I/O-bound work, run in the thread backend."""
    time.sleep(0.01)
    return item + 100

@stage(backend="process", workers=2)
def add_thousand_sync_process(item: int) -> int:
    """A sync stage for CPU-bound work, run in the process backend."""
    # Simulate CPU work
    for i in range(1000):
        _ = i * i
    return item + 1000

@stage(backend="serial")
def context_inc_sync(context: Context, item: int) -> int:
    """Increments a counter in the context."""
    context.inc("counter")
    return item

@stage(backend="thread", workers=4)
def context_inc_thread(context: Context, item: int) -> int:
    """Increments a counter in the context from a thread."""
    context.inc("thread_counter")
    time.sleep(0.01)
    return item

# --- Test Cases ---

# A list of all basic stages to test permutations
STAGES = {
    "serial": add_one_sync,
    "async": add_ten_async,
    "thread": add_hundred_sync_thread,
    "process": add_thousand_sync_process,
}

# Define the permutations of backends to test
# This is not all 4! = 24 permutations, but a representative sample.
backend_combinations = [
    ("serial", "thread", "async", "process"),
    ("process", "thread", "async", "serial"),
    ("async", "serial", "thread", "process"),
    ("thread", "process", "serial", "async"),
    ("serial", "serial", "serial", "serial"),
    ("async", "async", "async", "async"),
    ("thread", "thread", "thread", "thread"),
]

@pytest.mark.parametrize("backends", backend_combinations)
def test_mixed_backend_data_flow(backends):
    """
    Tests the data flow and integrity across a chain of different backends.
    """
    input_data = list(range(3))

    # Build the pipeline dynamically based on the parameter
    pipeline = from_iterable(input_data)
    expected_offset = 0
    for backend_name in backends:
        stage_func = STAGES[backend_name]
        pipeline |= stage_func
        # Calculate the expected result offset for this stage
        if backend_name == "serial":
            expected_offset += 1
        elif backend_name == "async":
            expected_offset += 10
        elif backend_name == "thread":
            expected_offset += 100
        elif backend_name == "process":
            expected_offset += 1000

    # Run the pipeline and collect results
    results, _ = pipeline.collect()

    expected_results = [x + expected_offset for x in input_data]

    assert sorted(results) == expected_results

def test_context_propagation_across_threads():
    """
    Tests that context is correctly shared and updated across thread boundaries.
    """
    input_data = list(range(10))

    pipeline = (
        from_iterable(input_data)
        | context_inc_sync
        | context_inc_thread
        | context_inc_sync
    )

    # Create a context to pass to the pipeline
    context = Context()
    context.set("counter", 0)
    context.set("thread_counter", 0)

    _, final_context = pipeline.collect(context=context)

    # Each of the 10 items goes through 2 sync stages and 1 thread stage
    assert final_context.get("counter") == 20
    assert final_context.get("thread_counter") == 10


@pytest.mark.slow  # Mark this test as slow as it involves process startup
def test_context_propagation_across_processes():
    """
    Tests that a multiprocessing-safe context is correctly shared and
    updated across process boundaries.
    """
    # This import is here because the processing runner might set the start method
    from lijnding.backends.processing import ProcessingRunner

    @stage(backend="process", workers=2)
    def context_inc_process(context: Context, item: int) -> int:
        context.inc("process_counter")
        return item

    input_data = list(range(4))

    pipeline = (
        from_iterable(input_data)
        | context_inc_sync
        | context_inc_process
        | context_inc_thread
    )

    # The pipeline will automatically create a multiprocessing-safe context
    # because it sees the "process" backend.

    results, context = pipeline.collect()

    # 4 items go through the sync stage
    assert context.get("counter") == 4
    # 4 items go through the process stage
    assert context.get("process_counter") == 4
    # 4 items go through the thread stage
    assert context.get("thread_counter") == 4
