from __future__ import annotations
import asyncio
import inspect
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from ..connectors.base import BaseConnector
    from .stage import Stage
    from .context import PipelineContext


async def _worker_loop(
    stage: "Stage",
    in_q: "BaseConnector",
    out_q: Optional["BaseConnector"],
    context: "PipelineContext",
    active_tracker: "multiprocessing.Value",
    next_stage_workers: int,
) -> None:
    """
    The core async-native worker loop. It can run any stage type by wrapping
    sync functions in `asyncio.to_thread`.
    """
    func = stage.func
    is_async = inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)

    while True:
        item = await in_q.get()
        if item is None:  # Sentinel value
            with active_tracker.get_lock():
                active_tracker.value -= 1
                remaining = active_tracker.value

            if remaining == 0 and out_q:
                for _ in range(next_stage_workers):
                    await out_q.put(None)
            break

        try:
            if is_async:
                result = await func(item, context)
            else:
                result = await asyncio.to_thread(func, item, context)

            if out_q:
                await out_q.put(result)
        except Exception as e:
            # A real implementation would have more robust error handling
            print(f"Error in stage {stage.name}: {e}")


def _entry_point(
    stage: "Stage",
    in_q: "BaseConnector",
    out_q: Optional["BaseConnector"],
    context: "PipelineContext",
    active_tracker: "multiprocessing.Value",
    next_stage_workers: int,
) -> None:
    """
    Entry point for running the async worker loop in a separate thread or process.
    """
    asyncio.run(_worker_loop(stage, in_q, out_q, context, active_tracker, next_stage_workers))
