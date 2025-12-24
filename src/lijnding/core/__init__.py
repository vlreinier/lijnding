# lijnding.core
# This package contains the core classes of the LijnDing framework,
# such as Pipeline, Stage, and Context.

from .pipeline import Pipeline
from .context import PipelineContext
from .errors import ErrorPolicy
from .hooks import Hooks

__all__ = [
    "Pipeline",
    "PipelineContext",
    "ErrorPolicy",
    "Hooks",
]
