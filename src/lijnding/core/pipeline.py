from __future__ import annotations
import asyncio
import threading
import multiprocessing
from typing import (
    Any,
    Iterable,
    List,
    Optional,
    Tuple,
    AsyncIterator,
)

from .stage import Stage
from .worker import _worker_loop, _entry_point
from .context import PipelineContext
from ..connectors.base import BaseConnector
from ..connectors.async_connector import AsyncConnector
from ..connectors.thread_connector import ThreadConnector
from ..connectors.process_connector import ProcessConnector


class Pipeline:
    def __init__(self, stages: Optional[List[Stage]] = None):
        self.stages: List[Stage] = stages or []
        self.manager = multiprocessing.Manager()
        self.context = PipelineContext(self.manager)

    def __or__(self, other: Any) -> "Pipeline":
        if not isinstance(other, Stage):
            raise TypeError(f"Unsupported type for pipeline composition: {type(other)}")
        return Pipeline(self.stages + [other])

    def _get_connector(
        self,
        upstream_backend: Optional[str],
        downstream_backend: Optional[str],
    ) -> BaseConnector:
        """Factory for creating the correct connector between stages."""
        if "process" in (upstream_backend, downstream_backend):
            return ProcessConnector()
        if "thread" in (upstream_backend, downstream_backend):
            return ThreadConnector()
        return AsyncConnector()

    async def run(
        self, data: Optional[Iterable[Any]] = None
    ) -> Tuple[List[Any], "PipelineContext"]:
        """
        The primary async-native execution method for the pipeline.
        """
        if not self.stages:
            return [], self.context

        connectors: List[BaseConnector] = []
        # Input connector
        connectors.append(
            self._get_connector(None, getattr(self.stages[0], "backend", "serial"))
        )
        for i in range(len(self.stages) - 1):
            connectors.append(
                self._get_connector(
                    getattr(self.stages[i], "backend", "serial"),
                    getattr(self.stages[i + 1], "backend", "serial"),
                )
            )
        # Output connector
        connectors.append(
            self._get_connector(getattr(self.stages[-1], "backend", "serial"), None)
        )

        handles = []
        active_trackers = []
        for i, stage in enumerate(self.stages):
            in_q = connectors[i]
            out_q = connectors[i + 1]
            backend = getattr(stage, "backend", "serial")
            workers_count = stage.workers

            active_tracker = multiprocessing.Value('i', workers_count)
            active_trackers.append(active_tracker)

            next_stage_workers = self.stages[i + 1].workers if i < len(self.stages) - 1 else 0

            for _ in range(workers_count):
                args = (stage, in_q, out_q, self.context, active_tracker, next_stage_workers)
                if backend == "process":
                    p = multiprocessing.Process(
                        target=_entry_point, args=args
                    )
                    p.start()
                    handles.append(("process", p))
                elif backend == "thread":
                    t = threading.Thread(
                        target=_entry_point, args=args
                    )
                    t.start()
                    handles.append(("thread", t))
                else: # "serial" and "async"
                    task = asyncio.create_task(_worker_loop(*args))
                    handles.append(("task", task))

        # Feed the pipeline
        input_q = connectors[0]
        for item in data or []:
            await input_q.put(item)

        # Send sentinels to the first stage
        for _ in range(self.stages[0].workers):
            await input_q.put(None)

        # Collect results
        output_q = connectors[-1]
        results = []

        # The final stage's active tracker is our signal for when to stop
        last_tracker = active_trackers[-1]
        while True:
            try:
                item = await asyncio.wait_for(output_q.get(), timeout=0.1)
                if item is None: continue
                results.append(item)
            except asyncio.TimeoutError:
                # Break only if the last stage has finished AND its output queue is empty
                if last_tracker.value == 0 and output_q.q.empty():
                    break
                continue

        # Await all tasks and join all threads/processes
        for handle_type, handle in handles:
            if handle_type == "task":
                await handle
            elif handle_type == "thread":
                handle.join()
            elif handle_type == "process":
                handle.join()

        self.manager.shutdown()
        return results, self.context
