# Core Concepts

Lijnding is built around a few core concepts that work together to create powerful and flexible data pipelines.

- **Stage**: The fundamental building block of a pipeline. A stage is a simple Python function decorated with `@stage`. It performs a single, specific operation on data flowing through the pipeline.

- **Pipeline**: A sequence of stages connected by the `|` (pipe) operator. Data flows from one stage to the next. A `Pipeline` can also be treated as a single `Stage`, allowing you to nest pipelines.

- **Component**: A pre-built, configurable stage, usually created by a factory function (e.g., `branch()`, `batch()`). Components handle common tasks like branching, batching, and reducing.

- **Backend**: The execution engine for a stage. The framework automatically infers the backend from your function's signature: `async` for `async def` functions and `serial` for regular `def` functions. You only need to specify the `backend` for concurrency:
  - `@stage(backend="thread")` for I/O-bound tasks.
  - `@stage(backend="process")` for CPU-bound tasks.

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

Lijnding provides four primary methods for executing a pipeline, following a naming convention that clearly separates synchronous and asynchronous operations:

- **`.run()` (Synchronous Iterator)**: Executes a synchronous pipeline and returns an iterator over the results. This is useful for streaming results from long-running synchronous workflows without consuming all the memory at once.

- **`.collect()` (Synchronous List)**: This is the simplest way to run a synchronous pipeline. It executes all stages and returns the results as a single list.

- **`await .arun()` (Asynchronous Iterator)**: This is the asynchronous equivalent of `.run()`. It is required when your pipeline includes any stage that uses the `async` backend and returns an `async_iterator` that you can use to process results as they become available.

- **`await .acollect()` (Asynchronous List)**: This is the asynchronous equivalent of `.collect()`. It runs the entire asynchronous pipeline and returns the results as a single list.

  *Important*: If you attempt to use a synchronous method like `.collect()` on a pipeline that contains an `async` stage from within an already running `asyncio` event loop, the framework will raise a `RuntimeError`. The improved error message will now guide you to use `arun()` or `acollect()`.

## Context Management

The `Context` object is a powerful feature that allows you to share state, manage resources, and access logging across the stages of your pipeline.

- **Internal Management**: The pipeline creates and manages the `Context` object for you. You do not need to instantiate it yourself or pass it to the execution methods.
- **Accessing the Context**: The final `Context` object, containing all state and metrics from the run, is returned by all execution methods (`.run()`, `.collect()`, `.arun()`, and `.acollect()`).
- **Stricter API**: To prevent incorrect usage, the `.collect()` method will now raise a `TypeError` if you attempt to pass a `context` argument to it. The framework is designed to handle the context's lifecycle internally.
