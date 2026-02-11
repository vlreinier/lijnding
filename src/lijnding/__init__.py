"""Lijnding: A Composable Pipeline Framework for Python."""

__version__ = "0.1.0"

from .core.pipeline import Pipeline

# Make the public API explicit
__all__ = ["Pipeline"]
