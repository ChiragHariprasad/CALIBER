"""CALIBRE Phase 1 evidence-processing agents."""

from .runner import AgentManager, PipelineOutput, run_payload
from .config import AgentConfig

__version__ = "0.1.0"
__all__ = ["AgentConfig", "AgentManager", "PipelineOutput", "run_payload"]
