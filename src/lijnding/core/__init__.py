# lijnding.core
# This package contains the core classes of the LijnDing framework,
# such as Pipeline, Stage, and Context.

from .pipeline import Pipeline
from .stage import stage, Stage, aggregator_stage
from .errors import ErrorPolicy
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import Context
from .hooks import Hooks

__all__ = [
    "Pipeline",
    "stage",
    "aggregator_stage",
    "Stage",
    "Context",
    "ErrorPolicy",
    "Hooks",
]
