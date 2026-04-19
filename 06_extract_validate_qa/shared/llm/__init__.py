# shared/llm/__init__.py
from .local_llm import LocalLLM
from .prompt_templates import PromptTemplates

__all__ = ["LocalLLM", "PromptTemplates"]
