"""
This module defines the core Pipeline class, which is the primary entry point for
building and executing data processing workflows.

A Pipeline is a sequence of Stages that are composed together. Data flows
through the pipeline, being processed by each stage in turn. Pipelines can be
run synchronously or asynchronously, and can be nested within other pipelines.
"""

from __future__ import annotations
from typing import (
    Any,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
    AsyncIterator,
    AsyncIterable,
)
import time
import asyncio

from ..backends.runner_registry import get_runner
from .context import Context
from .stage import Stage, stage
from .log import get_logger
from .utils import AsyncToSyncIterator
from ..config import Config, load_config


class Pipeline:
    """A sequence of stages that process data.

    The Pipeline class is the main orchestrator for running a series of stages.
    It manages the flow of data, context, and execution backends.

    Attributes:
        stages: A list of Stage objects that make up the pipeline.
        name: The name of the pipeline, used for logging.
        logger: A logger instance for the pipeline.
    """

    def __init__(
        self, stages: Optional[List[Stage]] = None, *, name: Optional[str] = None
    ):
        """Initializes a new Pipeline.

        Args:
            stages: A list of initial stages for the pipeline.
            name: An optional name for the pipeline. If not provided, a default
                name will be used.
        """
        self.stages: List[Stage] = stages or []
        self.name = name or "Pipeline"
        self.logger = get_logger(f"lijnding.pipeline.{self.name}")

    def add(self, other: Any) -> "Pipeline":
        """Adds a component (Stage or Pipeline) to this pipeline.

        This method allows for a fluent, chainable interface for building pipelines.

        Args:
            other: The component to add. Can be a `Stage` or another `Pipeline`.

        Returns:
            The pipeline instance, allowing for method chaining.

        Raises:
            TypeError: If the object being added is not a `Stage` or `Pipeline`,
                or if you try to connect two 'source' stages.
        """
        other_stage: Stage
        if isinstance(other, Stage):
            other_stage = other
        elif isinstance(other, Pipeline):
            other_stage = other.to_stage()
        else:
            raise TypeError(f"Unsupported type for pipeline composition: {type(other)}")

        if self.stages:
            last_stage = self.stages[-1]
            if last_stage.stage_type == "source" and other_stage.stage_type == "source":
                raise TypeError("Cannot pipe from one 'source' stage to another.")

        self.stages.append(other_stage)
        return self

    def __or__(self, other: Any) -> "Pipeline":
        """Composes this pipeline with another component using the `|` operator.

        This is the primary way to build pipelines.

        Args:
            other: The component to add. Can be a `Stage` or another `Pipeline`.

        Returns:
            A new `Pipeline` instance representing the composition.

        Raises:
            TypeError: If the object being added is not a `Stage` or `Pipeline`.
        """
        other_stage: Stage
        if isinstance(other, Stage):
            other_stage = other
        elif isinstance(other, Pipeline):
            # If the other item is a full pipeline, convert it to a single stage
            other_stage = other.to_stage()
        else:
            raise TypeError(f"Unsupported type for pipeline composition: {type(other)}")

        if self.stages:
            last_stage = self.stages[-1]
            if last_stage.stage_type == "source" and other_stage.stage_type == "source":
                raise TypeError("Cannot pipe from one 'source' stage to another.")

        if not self.stages:
            return Pipeline([other_stage])
        return Pipeline(self.stages + [other_stage])

    def __rshift__(self, other: Union[Stage, "Pipeline"]) -> "Pipeline":
        """Provides an alternative `>>` operator for pipeline composition."""
        return self.__or__(other)

    def _get_required_backend_names(self) -> set[str]:
        return set(getattr(s, "backend", "serial") for s in self.stages)

    def _build_context(self, config: Optional[Config] = None) -> Context:
        needs_mp = "process" in self._get_required_backend_names()
        return Context(mp_safe=needs_mp, config=config, pipeline_name=self.name)

    def run(
        self,
        data: Optional[Iterable[Any]] = None,
        *,
        collect: bool = False,
        config_path: Optional[str] = None,
    ) -> Tuple[Union[List[Any], Iterable[Any]], Context]:
        """Runs the pipeline synchronously.

        This method executes the pipeline stages in order, passing the output of
        one stage as the input to the next.

        Args:
            data: An iterable of input data to be fed into the pipeline. If the
                first stage is a 'source' stage, this can be `None`.
            collect: If `True`, the output of the pipeline will be collected into
                a list and returned. If `False` (default), a generator will be
                returned, allowing for streaming processing.
            config_path: The path to a YAML configuration file to be loaded into
                the pipeline's context.

        Returns:
            A tuple containing the pipeline's output (either a list or an
            iterable) and the final `Context` object.
        """
        self.logger.info("Pipeline run started.")
        start_time = time.time()
        config = load_config(config_path)
        context = self._build_context(config)

        if data is None:
            if not self.stages or self.stages[0].stage_type != "source":
                raise TypeError(
                    "Pipeline.run() requires a data argument unless the first stage is a source stage."
                )
            data = []

        stream: Iterable[Any] = data

        try:
            for stage_obj in self.stages:
                runner = get_runner(getattr(stage_obj, "backend", "serial"))
                stream = runner.run(stage_obj, context, stream)

            if collect:
                if hasattr(stream, "__aiter__"):

                    async def _collect_async(async_stream):
                        return [item async for item in async_stream]

                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)

                    return loop.run_until_complete(_collect_async(stream)), context
                else:
                    return list(stream), context
            return stream, context
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            self.logger.info(f"Pipeline run finished in {total_time:.4f} seconds.")

    def collect(
        self, data: Optional[Iterable[Any]] = None, config_path: Optional[str] = None
    ) -> Tuple[List[Any], Context]:
        """A convenience method that runs the pipeline and collects all results into a list.

        Args:
            data: An iterable of input data.
            config_path: The path to a YAML configuration file.

        Returns:
            A tuple containing a list of the pipeline's output and the final
            `Context` object.
        """
        stream, context = self.run(data, collect=True, config_path=config_path)
        return stream, context  # type: ignore

    async def run_async(
        self,
        data: Optional[Union[Iterable[Any], AsyncIterable[Any]]] = None,
        config_path: Optional[str] = None,
    ) -> Tuple[AsyncIterator[Any], Context]:
        """Asynchronously executes the pipeline.

        This method is required for pipelines that use the 'async' backend or
        process `AsyncIterable` data.

        Args:
            data: An iterable or async iterable of input data.
            config_path: The path to a YAML configuration file.

        Returns:
            A tuple containing an async iterator for the pipeline's output and
            the final `Context` object.
        """
        self.logger.info("Async pipeline run started.")
        start_time = time.time()
        config = load_config(config_path)
        context = self._build_context(config)

        if data is None:
            if not self.stages or self.stages[0].stage_type != "source":
                raise TypeError(
                    "Pipeline.run_async() requires a data argument unless the first stage is a source stage."
                )
            data = []

        async def _to_async(it):
            for i in it:
                yield i

        stream: AsyncIterable[Any]
        if not hasattr(data, "__aiter__"):
            stream = _to_async(data)
        else:
            stream = data  # type: ignore

        try:
            for stage_obj in self.stages:
                runner = get_runner(getattr(stage_obj, "backend", "serial"))
                if hasattr(runner, "run_async"):
                    stream = runner.run_async(stage_obj, context, stream)
                else:
                    loop = asyncio.get_running_loop()
                    sync_iterable = AsyncToSyncIterator(stream, loop)

                    def run_sync_stage_in_thread():
                        if runner.should_run_in_own_loop():
                            new_loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(new_loop)
                            try:
                                return list(
                                    runner.run(stage_obj, context, sync_iterable)
                                )
                            finally:
                                new_loop.close()
                        else:
                            return list(runner.run(stage_obj, context, sync_iterable))

                    sync_results = await loop.run_in_executor(
                        None, run_sync_stage_in_thread
                    )
                    stream = _to_async(sync_results)
            return stream, context
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            self.logger.info(
                f"Async pipeline run finished in {total_time:.4f} seconds."
            )

    @property
    def metrics(self) -> dict[str, Any]:
        """A dictionary containing metrics for each stage in the pipeline."""
        stage_metrics = {stage.name: stage.metrics for stage in self.stages}
        return {"stages": stage_metrics}

    def to_stage(self) -> Stage:
        """Converts the entire pipeline into a single, reusable Stage.

        This is useful for nesting pipelines within other pipelines. The resulting
        stage will be async if any stage in the pipeline is async.

        Returns:
            A `Stage` object that encapsulates the entire pipeline's logic.
        """
        pipeline_name = self.name or "nested_pipeline"
        is_async_pipeline = "async" in self._get_required_backend_names()

        if is_async_pipeline:

            @stage(
                name=pipeline_name,
                stage_type="itemwise",
                backend="async",
                wrapped_pipeline=self,
            )
            async def _pipeline_as_stage_func_async(
                context: Context, item: Any
            ) -> AsyncIterator[Any]:
                stream, _ = await self.run_async(data=[item])
                async for inner_item in stream:
                    yield inner_item

            return _pipeline_as_stage_func_async
        else:

            @stage(
                name=pipeline_name,
                stage_type="itemwise",
                wrapped_pipeline=self,
            )
            def _pipeline_as_stage_func_sync(
                context: Context, item: Any
            ) -> Iterable[Any]:
                inner_results, _ = self.run(data=[item], collect=True)
                yield from inner_results

            return _pipeline_as_stage_func_sync

    def visualize(self) -> str:
        """Generates a DOT graph representation of the pipeline.

        The output can be rendered using Graphviz.

        Example:
            ```bash
            # Requires graphviz to be installed (e.g., `brew install graphviz`)
            python my_pipeline_script.py > pipeline.dot
            dot -Tpng -o pipeline.png pipeline.dot
            ```

        Returns:
            A string containing the pipeline's structure in DOT format.
        """

        def get_node_id(obj: Any) -> str:
            return f'"{id(obj)}"'

        def escape_label(label: str) -> str:
            return label.replace('"', '\\"').replace("{", "\\{").replace("}", "\\}")

        def format_label(name: str, stage_obj: Optional[Stage] = None) -> str:
            name = escape_label(name)
            if not stage_obj:
                return f'"{name}"'

            label_parts = [name]
            if stage_obj.backend != "serial":
                label_parts.append(f"backend: {stage_obj.backend}")
            if stage_obj.workers > 1:
                label_parts.append(f"workers: {stage_obj.workers}")

            return '"' + "\\n".join(label_parts) + '"'

        def _traverse(
            pipeline: "Pipeline", graph_name: str, parent_node: Optional[str] = None
        ) -> Tuple[List[str], Optional[str], Optional[str]]:
            dot: List[str] = []
            prefix = "  " * (graph_name.count("cluster") + 1)
            first_node = None
            last_node = parent_node

            if not pipeline.stages:
                return [], None, None

            dot.append(f'{prefix}subgraph "{graph_name}" {{')
            dot.append(f'{prefix}  label = "{escape_label(pipeline.name)}";')
            dot.append(f'{prefix}  style = "rounded";')

            for i, stage_obj in enumerate(pipeline.stages):
                node_id = get_node_id(stage_obj)
                if not first_node:
                    first_node = node_id

                if stage_obj.branch_pipelines:
                    branch_cluster_name = f"cluster_branch_{id(stage_obj)}"
                    dot.append(f'{prefix}  subgraph "{branch_cluster_name}" {{')
                    dot.append(f'{prefix}    label = "{escape_label(stage_obj.name)}";')
                    dot.append(f'{prefix}    style = "dashed";')

                    branch_entry_id = f'"branch_entry_{id(stage_obj)}"'
                    branch_exit_id = f'"branch_exit_{id(stage_obj)}"'
                    dot.append(
                        f'{prefix}    {branch_entry_id} [label="start", shape=circle, style=filled, fillcolor=grey];'
                    )
                    dot.append(
                        f'{prefix}    {branch_exit_id} [label="end", shape=circle, style=filled, fillcolor=grey];'
                    )

                    for j, branch_pipeline in enumerate(stage_obj.branch_pipelines):
                        branch_graph_name = f"cluster_branch_{id(stage_obj)}_sub_{j}"
                        branch_dot, branch_first, branch_last = _traverse(
                            branch_pipeline, branch_graph_name
                        )
                        dot.extend(branch_dot)
                        if branch_first:
                            dot.append(
                                f"{prefix}    {branch_entry_id} -> {branch_first};"
                            )
                        if branch_last:
                            dot.append(
                                f"{prefix}    {branch_last} -> {branch_exit_id};"
                            )

                    dot.append(f"{prefix}  }}")

                    if last_node:
                        dot.append(f"{prefix}  {last_node} -> {branch_entry_id};")
                    last_node = branch_exit_id

                elif stage_obj.wrapped_pipeline:
                    nested_pipeline = stage_obj.wrapped_pipeline
                    nested_cluster_name = f"cluster_nested_{id(stage_obj)}"
                    nested_dot, nested_first, nested_last = _traverse(
                        nested_pipeline, nested_cluster_name
                    )
                    dot.extend(nested_dot)
                    if last_node and nested_first:
                        dot.append(f"{prefix}  {last_node} -> {nested_first};")
                    if nested_last:
                        last_node = nested_last

                else:
                    label = format_label(stage_obj.name, stage_obj)
                    dot.append(f"{prefix}  {node_id} [label={label}, shape=box];")
                    if last_node:
                        dot.append(f"{prefix}  {last_node} -> {node_id};")
                    last_node = node_id

            dot.append(f"{prefix}}}")
            return dot, first_node, last_node

        dot_lines = ["digraph G {", "  rankdir=TB;"]
        traversed_statements, _, _ = _traverse(self, "cluster_main", None)
        dot_lines.extend(traversed_statements)
        dot_lines.append("}")

        return "\n".join(dot_lines)

    def __repr__(self) -> str:
        stage_names = " | ".join(s.name for s in self.stages)
        return f"Pipeline(name='{self.name}', stages=[{stage_names}])"
