"""
智能体基类模块
定义智能体的通用能力和接口规范
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import logging


class BaseAgent(ABC):
    """
    智能体基类：定义通用能力
    提供技能注册、任务调度、状态管理等基础功能
    """
    
    def __init__(self, name: str = "BaseAgent", skills: Dict[str, Any] = None):
        """
        初始化智能体
        
        Args:
            name: 智能体名称
            skills: 技能注册表，格式为 {skill_name: skill_function}
        """
        self.name = name
        self.skills = skills or {}
        self.logger = logging.getLogger(f"{__name__}.{name}")
        self.created_at = datetime.now()
        self.last_used = None
        self.usage_count = 0
        
    def register_skill(self, skill_name: str, skill_func: Callable) -> None:
        """
        注册技能
        
        Args:
            skill_name: 技能名称
            skill_func: 技能函数，必须接受data参数并返回结果
            
        Raises:
            ValueError: 当技能名称已存在时抛出
        """
        if skill_name in self.skills:
            raise ValueError(f"技能 '{skill_name}' 已存在，请先注销或使用其他名称")
            
        self.skills[skill_name] = skill_func
        self.logger.info(f"技能 '{skill_name}' 注册成功")
        
    def unregister_skill(self, skill_name: str) -> None:
        """
        注销技能
        
        Args:
            skill_name: 技能名称
            
        Raises:
            KeyError: 当技能名称不存在时抛出
        """
        if skill_name not in self.skills:
            raise KeyError(f"技能 '{skill_name}' 不存在")
            
        del self.skills[skill_name]
        self.logger.info(f"技能 '{skill_name}' 注销成功")
        
    def list_skills(self) -> List[str]:
        """
        获取已注册技能列表
        
        Returns:
            技能名称列表
        """
        return list(self.skills.keys())
        
    def has_skill(self, skill_name: str) -> bool:
        """
        检查是否包含指定技能
        
        Args:
            skill_name: 技能名称
            
        Returns:
            是否包含该技能
        """
        return skill_name in self.skills
        
    @abstractmethod
    def run(self, task: str, data: Any, **kwargs) -> Any:
        """
        执行核心任务（子类必须实现）
        
        Args:
            task: 任务类型
            data: 输入数据
            **kwargs: 额外参数
            
        Returns:
            任务执行结果
            
        Raises:
            ValueError: 当任务类型不支持时抛出
            NotImplementedError: 当方法未实现时抛出
        """
        pass
        
    def get_skill_info(self) -> Dict[str, Dict[str, Any]]:
        """
        获取技能详细信息
        
        Returns:
            技能信息字典，包含名称、描述、参数等信息
        """
        skill_info = {}
        for name, func in self.skills.items():
            skill_info[name] = {
                "name": name,
                "doc": func.__doc__ or "无描述",
                "module": func.__module__,
                "callable": callable(func)
            }
        return skill_info
        
    def get_agent_info(self) -> Dict[str, Any]:
        """
        获取智能体基本信息
        
        Returns:
            智能体信息字典
        """
        return {
            "name": self.name,
            "type": self.__class__.__name__,
            "created_at": self.created_at.isoformat(),
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "usage_count": self.usage_count,
            "skills_count": len(self.skills),
            "skills": self.list_skills()
        }
        
    def _update_usage(self) -> None:
        """更新使用统计信息"""
        self.last_used = datetime.now()
        self.usage_count += 1
        
    def validate_task(self, task: str) -> bool:
        """
        验证任务是否支持
        
        Args:
            task: 任务类型
            
        Returns:
            是否支持该任务
        """
        return task in self.skills
        
    def get_supported_tasks(self) -> List[str]:
        """
        获取支持的任务列表
        
        Returns:
            支持的任务名称列表
        """
        return self.list_skills()


class SkillError(Exception):
    """技能相关异常"""
    pass


class TaskError(Exception):
    """任务执行相关异常"""
    pass
