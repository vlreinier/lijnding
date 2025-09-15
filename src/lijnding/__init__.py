"""Lijnding: A Composable Pipeline Framework for Python."""

__version__ = "0.1.0"

from .core.pipeline import Pipeline
from .core.stage import stage, aggregator_stage
from .components.io import from_iterable

# Make the public API explicit
__all__ = ["Pipeline", "stage", "aggregator_stage", "from_iterable"]