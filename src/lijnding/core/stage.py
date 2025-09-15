"""
This module defines the `Stage` class and the `@stage` decorator.

A `Stage` is the fundamental building block of a `Pipeline`. It wraps a Python
function and adds metadata and configuration for how that function should be
executed within the pipeline.
"""

from __future__ import annotations

import inspect
from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    Type,
    Union,
    List,
    Tuple,
    AsyncIterator,
    AsyncIterable,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from .pipeline import Pipeline

from ..typing.inference import infer_types
from .context import Context
from .errors import ErrorPolicy
from .hooks import Hooks
from .log import get_logger
from typeguard import typechecked


class Stage:
    """A single, executable step in a data pipeline.

    A Stage wraps a user-provided function and holds configuration details
    about its execution, such as its name, backend, and error handling policy.
    Users typically do not create `Stage` objects directly, but rather by
    using the `@stage` decorator.

    Attributes:
        func: The callable object that this stage executes.
        name: The name of the stage, used for logging and metrics.
        stage_type: The type of stage ('itemwise', 'aggregator', 'source').
        backend: The name of the execution backend to use.
        workers: The number of parallel workers for concurrent backends.
        error_policy: The policy for handling errors that occur in the stage.
        is_async: A boolean indicating if the stage's function is async.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        name: Optional[str] = None,
        stage_type: str = "itemwise",
        backend: str = "serial",
        workers: int = 1,
        buffer_size: Optional[int] = None,
        input_type: Optional[Type[Any]] = None,
        output_type: Optional[Type[Any]] = None,
        error_policy: Optional[ErrorPolicy] = None,
        hooks: Optional[Hooks] = None,
        branch_pipelines: Optional[List["Pipeline"]] = None,
        wrapped_pipeline: Optional["Pipeline"] = None,
    ):
        """Initializes a Stage object.

        Args:
            func: The function to be wrapped by the stage.
            name: A custom name for the stage.
            stage_type: The type of stage ('itemwise', 'aggregator', or 'source').
            backend: The execution backend ('serial', 'thread', 'process', 'async').
            workers: The number of parallel workers.
            buffer_size: The size of the input buffer for concurrent backends.
            input_type: The expected type of input data.
            output_type: The expected type of output data.
            error_policy: The error handling policy.
            hooks: A collection of hooks for monitoring.
            branch_pipelines: Used internally by the `branch` component.
            wrapped_pipeline: Used internally when a `Pipeline` is nested.
        """

        self.func = func
        self.name = name or getattr(func, "__name__", "Stage")
        self.logger = get_logger(f"lijnding.stage.{self.name}")
        self.stage_type = stage_type
        self.backend = backend
        self.workers = workers
        self.buffer_size = buffer_size
        self.error_policy = error_policy or ErrorPolicy()
        self.hooks = hooks or Hooks()
        self.is_async = inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(
            func
        )
        self.branch_pipelines = branch_pipelines
        self.wrapped_pipeline = wrapped_pipeline

        inferred_in, inferred_out, num_args = infer_types(func)
        self.input_type = input_type or inferred_in
        self.output_type = output_type or inferred_out
        self._inject_context = "context" in inspect.signature(func).parameters

        if num_args == 0:
            self.stage_type = "source"

        self.metrics: dict[str, Any] = {
            "items_in": 0,
            "items_out": 0,
            "errors": 0,
            "time_total": 0.0,
        }

    def __repr__(self) -> str:
        return f"Stage(name='{self.name}', type='{self.stage_type}')"

    def __or__(self, other: Union["Stage", "Pipeline"]) -> "Pipeline":
        """Composes this stage with another using the `|` operator."""
        from .pipeline import Pipeline

        return Pipeline([self]) | other

    def __rshift__(self, other: Union["Stage", "Pipeline"]) -> "Pipeline":
        """Provides an alternative `>>` operator for composition."""
        from .pipeline import Pipeline

        return Pipeline([self]) | other

    def visualize(self) -> str:
        """Generates a DOT graph representation of the stage.

        This is a convenience method that treats the stage as a single-stage
        pipeline for visualization purposes.

        Returns:
            A string containing the stage's structure in DOT format.
        """
        from .pipeline import Pipeline

        pipeline = Pipeline([self], name=self.name)
        return pipeline.visualize()

    def collect(
        self, data: Optional[Iterable[Any]] = None, config_path: Optional[str] = None
    ) -> Tuple[List[Any], Context]:
        """Executes the stage as a single-stage pipeline and collects all
        results into a list.

        Args:
            data: An iterable of data to process.
            config_path: Path to a YAML configuration file.

        Returns:
            A tuple containing the list of results and the execution context.
        """
        from .pipeline import Pipeline

        pipeline = Pipeline([self])
        return pipeline.collect(data, config_path=config_path)

    def _invoke(self, context: Context, *args: Any, **kwargs: Any) -> Any:
        """Invokes the wrapped function, injecting context if required."""
        if self._inject_context:
            original_logger = context.logger
            context.logger = self.logger
            try:
                if self.stage_type == "source":
                    return self.func(context)
                return self.func(context, *args, **kwargs)
            finally:
                context.logger = original_logger
        else:
            if self.stage_type == "source":
                return self.func()
            return self.func(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """Provides a more helpful error message for mis-used methods."""
        from .pipeline import Pipeline

        if hasattr(Pipeline, name):
            if name == "run":
                raise AttributeError(
                    f"'Stage' object has no attribute 'run'. The pipeline is now async-native. "
                    f"Please use 'Pipeline([stage]).collect(...)' for a blocking call, or "
                    f"'await Pipeline([stage]).run(...)' in an async context."
                )
            if name == "collect":
                 raise AttributeError(f"'Stage' object has no attribute '{name}'")

            message = (
                f"'Stage' object has no attribute '{name}'. "
                f"Did you mean to wrap it in a Pipeline first? "
                f"e.g., Pipeline([{self.name}]).{name}(...)"
            )
            raise AttributeError(message)

        raise AttributeError(f"'Stage' object has no attribute '{name}'")


def stage(
    _func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    stage_type: str = "itemwise",
    backend: str = "serial",
    workers: int = 1,
    buffer_size: Optional[int] = None,
    input_type: Optional[Type[Any]] = None,
    output_type: Optional[Type[Any]] = None,
    error_policy: Optional[ErrorPolicy] = None,
    hooks: Optional[Hooks] = None,
    branch_pipelines: Optional[List["Pipeline"]] = None,
    wrapped_pipeline: Optional["Pipeline"] = None,
) -> Union[Stage, Callable[[Callable[..., Any]], Stage]]:
    """A decorator to create a pipeline Stage from a function.

    This is the primary way to define the building blocks of a pipeline. It can
    be used with or without arguments.

    Example:
        .. code-block:: python

            @stage
            def my_simple_stage(item):
                return item * 2

            @stage(backend="thread", workers=4)
            def my_threaded_stage(item):
                # I/O-bound work
                return requests.get(item).json()

    Args:
        name: A custom name for the stage. If not provided, the function's
            name is used.
        stage_type: The type of stage. Can be 'itemwise', 'aggregator', or
            'source'. Defaults to 'itemwise'.
        backend: The execution backend to use for this stage.
            Defaults to 'serial'.
        workers: The number of parallel workers for concurrent backends
            ('thread', 'process'). Defaults to 1.
        buffer_size: The maximum number of items to buffer for concurrent
            backends. Defaults to a sensible value based on worker count.
        input_type: The expected input type for this stage.
        output_type: The expected output type for this stage.
        error_policy: The error handling policy for this stage.
        hooks: A collection of hooks for monitoring and tracing.
        branch_pipelines: For internal use by the `branch` component.
        wrapped_pipeline: For internal use by `Pipeline.to_stage`.

    Returns:
        A `Stage` object if used as `@stage`, or a decorator that returns a
        `Stage` object if used as `@stage(...)`.
    """

    def wrapper(func: Callable[..., Any]) -> Stage:
        return Stage(
            typechecked(func),
            name=name,
            stage_type=stage_type,
            backend=backend,
            workers=workers,
            buffer_size=buffer_size,
            input_type=input_type,
            output_type=output_type,
            error_policy=error_policy,
            hooks=hooks,
            branch_pipelines=branch_pipelines,
            wrapped_pipeline=wrapped_pipeline,
        )

    if _func is not None:
        # Used as `@stage`
        return wrapper(_func)
    # Used as `@stage(...)`
    return wrapper


def aggregator_stage(
    _func: Optional[Callable[..., Any]] = None,
    *,
    name: Optional[str] = None,
    backend: str = "serial",
    workers: int = 1,
    buffer_size: Optional[int] = None,
    input_type: Optional[Type[Any]] = None,
    output_type: Optional[Type[Any]] = None,
    error_policy: Optional[ErrorPolicy] = None,
    hooks: Optional[Hooks] = None,
    branch_pipelines: Optional[List["Pipeline"]] = None,
    wrapped_pipeline: Optional["Pipeline"] = None,
) -> Union[Stage, Callable[[Callable[..., Any]], Stage]]:
    """A decorator to create an aggregator stage.

    This is a convenience decorator equivalent to `@stage(stage_type="aggregator")`.
    Aggregator stages receive the entire input stream as a single iterable
    argument.

    Example:
        .. code-block:: python

            @aggregator_stage
            def calculate_sum(numbers: Iterable[int]) -> int:
                return sum(numbers)

    Args:
        All arguments from the `@stage` decorator are accepted here.

    Returns:
        A `Stage` object or a decorator, same as `@stage`.
    """
    return stage(
        _func,
        name=name,
        stage_type="aggregator",
        backend=backend,
        workers=workers,
        buffer_size=buffer_size,
        input_type=input_type,
        output_type=output_type,
        error_policy=error_policy,
        hooks=hooks,
        branch_pipelines=branch_pipelines,
        wrapped_pipeline=wrapped_pipeline,
    )
