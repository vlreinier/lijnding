from __future__ import annotations

import asyncio
import threading
import multiprocessing
import queue
import time
import logging
import inspect
from logging.handlers import QueueListener
from typing import Any, List, Dict, Optional, Tuple, Callable

from .config import PipelineConfig
from .connectors import (
    BaseConnector,
    AsyncMemoryConnector,
    ThreadSafeConnector,
    InterProcessConnector,
)
from .context import PipelineContext
from .runner import Runner
from .stage import FunctionInvoker
from dataclasses import dataclass, field


@dataclass
class HistoryRecord:
    stage: str
    runner: str
    timestamp: float
    worker_id: str


@dataclass
class Payload:
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    history: List[HistoryRecord] = field(default_factory=list)

    @staticmethod
    def pack(result: Any, history: List[HistoryRecord]) -> "Payload":
        if isinstance(result, Payload):
            result.history = history + result.history
            return result
        elif isinstance(result, tuple):
            return Payload(args=result, history=history)
        else:
            return Payload(args=(result,), history=history)


async def _bridge_sync_generator(func, args, kwargs, max_buffer=5):
    q = queue.Queue(maxsize=max_buffer)
    sentinel = object()

    def producer():
        try:
            for item in func(*args, **kwargs):
                q.put(item)
            q.put(sentinel)
        except Exception as e:
            q.put(e)

    t = threading.Thread(target=producer, daemon=True)
    t.start()

    while True:
        item = await asyncio.to_thread(q.get)
        if item is sentinel:
            break
        if isinstance(item, Exception):
            raise item
        yield item


async def _invoke_user_func(
    invoker: FunctionInvoker,
    payload: Payload,
    ctx: PipelineContext,
    worker_id: str,
    config: PipelineConfig,
):
    call_kwargs = invoker.prepare_kwargs(payload.kwargs, ctx, worker_id)
    try:
        if invoker.is_async:
            result = invoker.func(*payload.args, **call_kwargs)
            if inspect.isawaitable(result):
                result = await result
        else:
            if invoker.is_sync_gen:
                result = _bridge_sync_generator(
                    invoker.func, payload.args, call_kwargs, config.sync_gen_buffer
                )
            else:
                result = await asyncio.to_thread(
                    invoker.func, *payload.args, **call_kwargs
                )
    except Exception as e:
        raise e

    if inspect.isasyncgen(result):
        async for item in result:
            yield item
    elif invoker.is_sync_gen:
        async for item in result:
            yield item
    elif inspect.isgenerator(result):
        for item in result:
            yield item
    else:
        yield result


async def _worker_loop(
    stage_name: str,
    invoker: FunctionInvoker,
    runner_type: Runner,
    in_q: BaseConnector,
    out_q: Optional[BaseConnector],
    ctx: PipelineContext,
    active_tracker: Any,
    next_stage_workers: int,
    config: PipelineConfig,
):
    logger = ctx.get_logger(stage_name)

    if runner_type == Runner.PROCESS:
        worker_id = f"{stage_name}-PID{multiprocessing.current_process().pid}"
    elif runner_type == Runner.THREAD:
        worker_id = f"{stage_name}-{threading.current_thread().name}"
    else:
        worker_id = f"{stage_name}-{asyncio.current_task().get_name()}"

    while True:
        if config.fail_fast and ctx.is_shutting_down:
            break
        try:
            item = await in_q.get()
            if item is None:
                with active_tracker.get_lock():
                    active_tracker.value -= 1
                    remaining = active_tracker.value
                if remaining == 0 and out_q is not None:
                    for _ in range(next_stage_workers):
                        await out_q.put(None)
                break
            try:
                async for res in _invoke_user_func(
                    invoker, item, ctx, worker_id, config
                ):
                    new_hist = item.history.copy()
                    new_hist.append(
                        HistoryRecord(
                            stage_name, runner_type.value, time.time(), worker_id
                        )
                    )
                    output_payload = Payload.pack(res, new_hist)
                    if out_q:
                        await out_q.put(output_payload)
            except Exception as e:
                logger.error(f"Error in {worker_id}: {e}")
                if config.fail_fast:
                    logger.critical(
                        f"Pipeline halting due to error in {stage_name}"
                    )
                    ctx.signal_failure()
                    break
        except Exception as critical:
            logger.critical(f"System Error in {stage_name}: {critical}")
            ctx.signal_failure()
            break


def _entry_point(args):
    try:
        asyncio.run(_worker_loop(*args))
    except KeyboardInterrupt:
        pass


class Pipeline:
    def __init__(self, config: PipelineConfig = None):
        self.config = config or PipelineConfig()
        self.stages: List[Dict] = []
        self.manager = multiprocessing.Manager()
        self.log_queue = self.manager.Queue()
        self.context = PipelineContext(self.manager, self.log_queue)
        console = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S"
        )
        console.setFormatter(fmt)
        self.log_listener = QueueListener(self.log_queue, console)
        self.log_listener.start()

    def add_stage(self, func, runner: Runner = Runner.ASYNC, workers=1):
        name = f"S{len(self.stages) + 1}_{func.__name__}"
        invoker = FunctionInvoker(func, name)
        self.stages.append(
            {"invoker": invoker, "runner": runner, "workers": workers, "name": name}
        )
        return self

    def _factory_connector(self, r1: Runner, r2: Runner) -> BaseConnector:
        size = self.config.buffer_size
        if r1 == Runner.PROCESS or r2 == Runner.PROCESS:
            return InterProcessConnector(size)
        if r1 == Runner.THREAD or r2 == Runner.THREAD:
            return ThreadSafeConnector(size)
        if r1 == Runner.SYNC or r2 == Runner.SYNC:
            return ThreadSafeConnector(size)
        return AsyncMemoryConnector(size)

    async def run(self, input_data: list):
        if not self.stages:
            return []
        connectors = []
        connectors.append(
            self._factory_connector(Runner.ASYNC, self.stages[0]["runner"])
        )
        for i in range(len(self.stages) - 1):
            connectors.append(
                self._factory_connector(
                    self.stages[i]["runner"], self.stages[i + 1]["runner"]
                )
            )
        connectors.append(
            self._factory_connector(self.stages[-1]["runner"], Runner.ASYNC)
        )
        handles = []
        active_trackers = []
        for i, stage in enumerate(self.stages):
            in_q = connectors[i]
            out_q = connectors[i + 1]
            workers_count = stage["workers"]
            active_tracker = multiprocessing.Value("i", workers_count)
            active_trackers.append(active_tracker)
            next_workers = (
                self.stages[i + 1]["workers"] if i < len(self.stages) - 1 else 1
            )
            args = (
                stage["name"],
                stage["invoker"],
                stage["runner"],
                in_q,
                out_q,
                self.context,
                active_tracker,
                next_workers,
                self.config,
            )
            for j in range(workers_count):
                if stage["runner"] == Runner.PROCESS:
                    p = multiprocessing.Process(target=_entry_point, args=(args,))
                    p.start()
                    handles.append(("proc", p))
                elif stage["runner"] == Runner.THREAD:
                    t = threading.Thread(target=_entry_point, args=(args,))
                    t.start()
                    handles.append(("thread", t))
                else:
                    t = asyncio.create_task(
                        _worker_loop(*args), name=f"Task-{stage['name']}-{j}"
                    )
                    handles.append(("task", t))
        input_conn = connectors[0]
        for x in input_data:
            if isinstance(x, tuple):
                await input_conn.put(Payload(args=x))
            else:
                await input_conn.put(Payload(args=(x,)))
        for _ in range(self.stages[0]["workers"]):
            await input_conn.put(None)
        results = []
        final_q = connectors[-1]
        try:
            while True:
                if self.config.fail_fast and self.context.is_shutting_down:
                    break
                try:
                    item = await asyncio.wait_for(final_q.get(), timeout=0.2)
                except asyncio.TimeoutError:
                    continue
                if item is None:
                    break
                if len(item.args) == 1 and not item.kwargs:
                    results.append(item.args[0])
                else:
                    results.append(item.args)
        except Exception as e:
            print(f"Collector Exception: {e}")
        finally:
            for connector in connectors:
                if isinstance(connector, InterProcessConnector):
                    connector.q.close()
                    connector.q.join_thread()
            for type_, h in handles:
                if type_ == "proc":
                    h.join(timeout=5)
                elif type_ == "thread":
                    h.join()
                elif type_ == "task":
                    if not h.done():
                        h.cancel()
                        try:
                            await h
                        except asyncio.CancelledError:
                            pass
            self.log_listener.stop()
            self.manager.shutdown()
        return results, self.context.get_counter()
