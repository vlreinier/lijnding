# Core Concepts

Lijnding is built around a few core concepts that work together to create powerful and flexible data pipelines.

- **Stage**: The fundamental building block of a pipeline. A stage is a simple Python function decorated with `@stage`. It performs a single, specific operation on data flowing through the pipeline.

- **Pipeline**: A sequence of stages connected by the `|` (pipe) operator. Data flows from one stage to the next. A `Pipeline` can also be treated as a single `Stage`, allowing you to nest pipelines.

- **Component**: A pre-built, configurable stage, usually created by a factory function (e.g., `branch()`, `batch()`). Components handle common tasks like branching, batching, and reducing.

- **Backend**: The execution engine for a stage. LijnDing supports different backends (`serial`, `thread`) for different needs. This is configured via the `@stage` decorator (e.g., `@stage(backend="thread")`).

## Key Features

- **Simple & Composable API**: Use the `@stage` decorator and the `|` operator to build elegant, readable pipelines.
- **Multiple Backends**: Run your pipelines serially, in a thread pool for I/O-bound tasks, or in a process pool for CPU-bound tasks.
- **Integrated Logging**: The framework emits detailed logs for pipeline and stage events, configurable through Python's standard `logging` module. Access the logger from your stages via `context.logger`.
- **Rich Component Library**: Includes pre-built components for filtering, file I/O, HTTP requests, branching, and more.
- **Checkpointing**: Use the `save_progress` and `read_from_file` components to easily checkpoint and resume long-running pipelines.
- **Extensible**: Add your own execution backends with the `register_backend` function.
- **Error Handling**: Configure how your pipeline behaves on errors with policies like `fail`, `skip`, or `retry`.
- **Nestable Pipelines**: Encapsulate and reuse complex workflows by using a pipeline as a stage within another pipeline.
- **Modular and Extensible**: The framework is split into a core package and optional components that can be installed separately.
- **Web-Based GUI**: An optional, standalone web interface for real-time monitoring of pipeline runs.

---

## Execution: Sync vs. Async

Lijnding provides two primary methods for executing a pipeline:

- **`.collect()` (Synchronous)**: This is the simplest way to run a pipeline. It executes all stages and returns the results as a list. This method is ideal for pipelines that only contain synchronous stages (like `serial`, `thread`, or `process` backends).

  *Important*: If you attempt to use `.collect()` on a pipeline that contains an `async` stage from within an already running `asyncio` event loop, the framework will raise a `RuntimeError`. The improved error message will now guide you to the correct asynchronous method.

- **`await .run_async()` (Asynchronous)**: This method is required when your pipeline includes any stage that uses the `async` backend. It returns an `async_iterator` that you can use to process results as they become available. This is the correct way to execute pipelines in an asynchronous application.

## Context Management

The `Context` object is a powerful feature that allows you to share state, manage resources, and access logging across the stages of your pipeline.

- **Internal Management**: The pipeline creates and manages the `Context` object for you. You do not need to instantiate it yourself or pass it to the execution methods.
- **Accessing the Context**: The final `Context` object, containing all state and metrics from the run, is returned by both `.collect()` and `.run_async()`.
- **Stricter API**: To prevent incorrect usage, the `.collect()` method will now raise a `TypeError` if you attempt to pass a `context` argument to it. The framework is designed to handle the context's lifecycle internally.
