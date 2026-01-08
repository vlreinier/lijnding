import pytest
import asyncio
from lijnding import Pipeline, stage
from lijnding.core.context import PipelineContext

# --- Test Stages ---

@stage
def add_one_sync(item: int, context: PipelineContext) -> int:
    context.inc("add_one_sync")
    return item + 1

@stage(backend="async", workers=4)
async def add_ten_async(item: int, context: PipelineContext) -> int:
    context.inc("add_ten_async")
    await asyncio.sleep(0.01)
    return item + 10

@stage(backend="thread", workers=4)
def add_hundred_sync_thread(item: int, context: PipelineContext) -> int:
    context.inc("add_hundred_sync_thread")
    return item + 100

@stage(backend="process", workers=2)
def add_thousand_sync_process(item: int, context: PipelineContext) -> int:
    context.inc("add_thousand_sync_process")
    return item + 1000

# --- Test Cases ---

STAGES = {
    "serial": add_one_sync,
    "async": add_ten_async,
    "thread": add_hundred_sync_thread,
    "process": add_thousand_sync_process,
}

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
async def test_mixed_backend_data_flow(backends):
    input_data = list(range(3))

    pipeline = Pipeline()
    for backend_name in backends:
        pipeline |= STAGES[backend_name]

    results, context = await pipeline.run(input_data)

    expected_offset = 0
    for backend_name in backends:
        if backend_name == "serial":
            expected_offset += 1
        elif backend_name == "async":
            expected_offset += 10
        elif backend_name == "thread":
            expected_offset += 100
        elif backend_name == "process":
            expected_offset += 1000

    expected_results = [x + expected_offset for x in input_data]
    assert sorted(results) == expected_results
