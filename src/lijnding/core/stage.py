from __future__ import annotations
import inspect
from typing import (
    Any,
    Callable,
    Dict,
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from .context import PipelineContext


class FunctionInvoker:
    def __init__(self, func: Callable, stage_name: str):
        self.func = func
        self.stage_name = stage_name
        self.sig = inspect.signature(func)
        self.pass_ctx = 'ctx' in self.sig.parameters
        self.pass_worker = 'worker_name' in self.sig.parameters
        self.is_async = inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)
        self.is_sync_gen = inspect.isgeneratorfunction(func)

    def prepare_kwargs(self, base_kwargs: Dict, ctx: "PipelineContext", worker_id: str):
        call_kwargs = base_kwargs.copy()
        if self.pass_ctx:
            call_kwargs['ctx'] = ctx
        if self.pass_worker:
            call_kwargs['worker_name'] = worker_id
        return call_kwargs
