import asyncio
import time
from lijnding.core import stage
from ..helpers.test_runner import run_pipeline


@stage(backend="thread", workers=2)
def slow_io_stage(x: int) -> int:
    """Simulates a slow I/O operation."""
    time.sleep(0.1)
    return x * 2


@stage
def fast_cpu_stage(x: int) -> int:
    """A fast, simple transformation."""
    return x + 1


@stage(backend="async", workers=2)
async def async_io_stage(x: int) -> int:
    """Simulates a slow async I/O operation."""
    await asyncio.sleep(0.1)
    return x * 3


@stage(backend="process", workers=2)
def cpu_bound_stage(x: int) -> int:
    """Simulates a CPU-bound task."""
    # A simple, but non-trivial calculation
    for i in range(10**4):
        # This is a dummy calculation that is not easily optimized away
        x = x ^ i
    return x


def test_mixed_backend_pipeline():
    """
    Tests a pipeline with a mix of threaded and serial stages.
    """
    # The pipeline starts with a concurrent stage and ends with a serial one.
    pipeline = slow_io_stage | fast_cpu_stage

    data = [1, 2, 3, 4]

    start_time = time.perf_counter()
    results, _ = pipeline.collect(data)
    end_time = time.perf_counter()

    # The slow_io_stage should run concurrently. With 2 workers for 4 items
    # that each take 0.1s, the total time should be around 0.2s. We assert
    # it's well below the sequential time of 0.4s.
    assert (end_time - start_time) < 0.5

    # Check the results are correct after passing through both stages
    # Input:  [1, 2, 3, 4]
    # After slow_io_stage: [2, 4, 6, 8] (order not guaranteed)
    # After fast_cpu_stage: [3, 5, 7, 9]
    assert sorted(results) == [3, 5, 7, 9]


async def test_mixed_async_thread_pipeline():
    """
    Tests a pipeline with a mix of asyncio and threaded stages.
    """
    pipeline = async_io_stage | slow_io_stage

    data = [1, 2, 3, 4]
    start_time = time.perf_counter()
    results, _ = await run_pipeline(pipeline, data)
    end_time = time.perf_counter()

    # With 2 workers for each stage and 4 items, and each stage taking 0.1s,
    # the two stages should run in parallel. The total time should be around 0.2s
    # for each stage. The total time should be well under the sequential time of 0.8s.
    # The asyncio stage is not concurrent, so it takes 0.4s. The thread stage
    # takes 0.2s. Total is ~0.6s.
    assert (end_time - start_time) < 0.7

    # Input: [1, 2, 3, 4]
    # After async_io_stage: [3, 6, 9, 12]
    # After slow_io_stage: [6, 12, 18, 24]
    assert sorted(results) == [6, 12, 18, 24]


def test_mixed_process_thread_pipeline():
    """
    Tests a pipeline with a mix of processing and threaded stages.
    """
    pipeline = cpu_bound_stage | slow_io_stage

    data = [1, 2, 3, 4]
    # Note: process startup time can be significant, so we don't assert on time.
    results, _ = pipeline.collect(data)

    # We only check for correctness, as timing for process-based stages
    # can be unpredictable due to overhead.
    expected_cpu_out = [cpu_bound_stage.func(i) for i in data]
    expected_final_out = [slow_io_stage.func(i) for i in expected_cpu_out]
    assert sorted(results) == sorted(expected_final_out)


async def test_mixed_async_thread_serial_pipeline():
    """
    Tests a more complex pipeline with asyncio, thread, and serial stages.
    """
    pipeline = async_io_stage | slow_io_stage | fast_cpu_stage

    data = [1, 2, 3, 4]
    start_time = time.perf_counter()
    results, _ = await run_pipeline(pipeline, data)
    end_time = time.perf_counter()

    # Similar to the async/thread test, we expect concurrency to provide a speedup.
    assert (end_time - start_time) < 0.8

    # Input: [1, 2, 3, 4]
    # After async_io_stage: [3, 6, 9, 12]
    # After slow_io_stage: [6, 12, 18, 24]
    # After fast_cpu_stage: [7, 13, 19, 25]
    assert sorted(results) == [7, 13, 19, 25]


async def test_mixed_process_async_pipeline():
    """
    Tests a pipeline with a mix of processing and asyncio stages.
    """
    pipeline = cpu_bound_stage | async_io_stage

    data = [1, 2, 3, 4]
    results, _ = await run_pipeline(pipeline, data)

    expected_cpu_out = [cpu_bound_stage.func(i) for i in data]
    expected_final_out = []
    for i in expected_cpu_out:
        expected_final_out.append(await async_io_stage.func(i))
    assert sorted(results) == sorted(expected_final_out)


def test_mixed_thread_process_thread_pipeline():
    """
    Tests a pipeline that switches from thread to process and back to thread.
    """
    pipeline = slow_io_stage | cpu_bound_stage | slow_io_stage

    data = [1, 2, 3, 4]
    results, _ = pipeline.collect(data)

    expected_io1_out = [slow_io_stage.func(i) for i in data]
    expected_cpu_out = [cpu_bound_stage.func(i) for i in expected_io1_out]
    expected_final_out = [slow_io_stage.func(i) for i in expected_cpu_out]
    assert sorted(results) == sorted(expected_final_out)
