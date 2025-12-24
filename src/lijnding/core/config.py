from dataclasses import dataclass

@dataclass
class PipelineConfig:
    """Explicit execution policy configuration."""
    fail_fast: bool = True
    buffer_size: int = 64
    sync_gen_buffer: int = 5
