import multiprocessing
import asyncio
import time
import pytest
from lijnding.core.pipeline import Pipeline
from lijnding.core.context import PipelineContext
from lijnding.core.config import PipelineConfig
from lijnding.core.runner import Runner

multiprocessing.freeze_support()

def heavy_sync_gen(data, ctx: "PipelineContext"):
    """Yields values slowly. Without streaming, this would block next stage."""
    ctx.increment_counter()
    for i in range(3):
        time.sleep(0.01)
        yield f"{data}_stream_{i}"

def risky_task(data):
    """Error Prone Task (Tests Fail-Fast)"""
    if "ERROR" in data:
        raise ValueError("Simulated User Error")
    return data

def formatting_task(text, suffix="Done"):
    """Multi-Arg Processing (Tests Payload/Kwargs)"""
    return f"{text} [{suffix}]"

@pytest.mark.asyncio
async def test_mixed_backends():
    config = PipelineConfig(
        fail_fast=True,
        buffer_size=10,
        sync_gen_buffer=2,
    )
    pipeline = Pipeline(config)
    pipeline.add_stage(heavy_sync_gen, Runner.SYNC, workers=2)
    pipeline.add_stage(risky_task, Runner.PROCESS, workers=2)
    pipeline.add_stage(formatting_task, Runner.THREAD, workers=2)
    items = ["Job1", "Job2", "Job3"]
    results, count = await pipeline.run(items)
    assert len(results) == 9
    assert count == 3
