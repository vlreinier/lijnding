"""
This module provides the `reduce_` component, which is used to apply a
reduction function across all items in a stream to produce a single output.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Generator
import functools

from ..core.stage import Stage, generator_stage


def reduce_(
    func: Callable[[Any, Any], Any], initializer: Optional[Any] = None
) -> Stage:
    """
    Creates a stage that reduces an entire input stream to a single value.

    This is a generator stage that consumes all items from the input stream
    and applies a reduction function, yielding a single result. It is a wrapper
    around Python's `functools.reduce`.

    Args:
        func: The two-argument reduction function.
        initializer: The optional starting value for the reduction.

    Returns:
        A Stage configured to perform the reduction.
    """

    @generator_stage(name="reduce")
    def _reduce_func(iterable: Iterable[Any]) -> Generator[Any, None, None]:
        iterator = iter(iterable)

        if initializer is None:
            try:
                # If no initializer is provided, the first item of the iterable
                # is used as the starting value.
                initial_value = next(iterator)
            except StopIteration:
                # If the iterable is empty and there's no initializer, the
                # reduction is empty and yields nothing.
                return
        else:
            initial_value = initializer

        # Perform the reduction on the rest of the iterator.
        result = functools.reduce(func, iterator, initial_value)
        yield result

    return _reduce_func
