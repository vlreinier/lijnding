import pytest
import asyncio

from lijnding.core import Pipeline, stage
from lijnding.core.stage import Stage


@stage
def add_one_func(x):
    return x + 1


@stage
def multiply_by_two_func(x):
    return x * 2


@pytest.mark.parametrize(
    "backend_outer, backend_inner",
    [
        ("serial", "thread"),
        ("thread", "serial"),
        ("process", "thread"),
        ("thread", "process"),
        ("serial", "process"),
        ("process", "serial"),
        ("async", "thread"),
        ("thread", "async"),
    ],
)
def test_nested_mixed_backend_pipeline(backend_outer, backend_inner):
    """
    Tests a pipeline with a nested pipeline that uses a different backend.
    """
    # Create the stages for the inner pipeline with the correct backend
    add_one_inner = Stage(add_one_func.func, backend=backend_inner)
    multiply_by_two_inner = Stage(multiply_by_two_func.func, backend=backend_inner)

    # Create the inner pipeline
    inner_pipeline = Pipeline(
        [
            add_one_inner,
            multiply_by_two_inner,
        ]
    )

    # Create the stages for the outer pipeline with the correct backend
    add_one_outer = Stage(add_one_func.func, backend=backend_outer)
    multiply_by_two_outer = Stage(multiply_by_two_func.func, backend=backend_outer)

    # Create the outer pipeline
    outer_pipeline = Pipeline(
        [
            add_one_outer,
            inner_pipeline.to_stage(),
            multiply_by_two_outer,
        ]
    )

    # The backend of the stage created from the inner pipeline needs to be set
    # to the outer backend, so that the outer runner is used to run the inner pipeline.
    outer_pipeline.stages[1].backend = backend_outer

    input_data = list(range(10))

    if backend_outer == "async" or backend_inner == "async":
        output_data, _ = asyncio.run(outer_pipeline.acollect(input_data))
    else:
        output_data, _ = outer_pipeline.collect(input_data)

    expected_data = [(((x + 1) + 1) * 2) * 2 for x in input_data]

    assert sorted(output_data) == sorted(expected_data)
