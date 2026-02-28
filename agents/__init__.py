"""
Learn Accounting Agent Package
智能账务审核系统核心包
"""

from .base_agent import BaseAgent
from .accounting_agent import AccountingAgent
from .config import AgentConfig

__version__ = "1.0.0"
__author__ = "Learn Accounting Agent Team"

__all__ = [
    "BaseAgent",
    "AccountingAgent", 
    "AgentConfig"
]
