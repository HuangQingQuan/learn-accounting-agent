"""
账务审核智能体主类
继承BaseAgent，实现账务审核场景的专用智能体
"""

from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime

from .base_agent import BaseAgent, TaskError, SkillError
from .config import AgentConfig


class AccountingAgent(BaseAgent):
    """
    账务审核智能体：继承基类，适配账务场景
    提供账务数据解析、规则审核、异常检测、报告生成等核心功能
    """
    
    def __init__(self, config: Optional[AgentConfig] = None):
        """
        初始化账务审核智能体
        
        Args:
            config: 智能体配置对象
        """
        super().__init__(name="AccountingAgent")
        self.config = config or AgentConfig()
        self.audit_history: List[Dict[str, Any]] = []
        
        # 注册核心任务类型
        self._register_core_tasks()
        
    def _register_core_tasks(self) -> None:
        """注册核心任务类型映射"""
        self.core_tasks = {
            "single_check": "单条账务审核",
            "batch_check": "批量账务审核", 
            "anomaly_detect": "异常检测分析",
            "generate_report": "生成审核报告",
            "rule_config": "规则配置管理",
            "data_parse": "数据解析处理"
        }
        
    def run(self, task: str, data: Any, **kwargs) -> Any:
        """
        执行账务审核任务
        
        Args:
            task: 任务类型，支持:
                - "single_check": 单条账务审核
                - "batch_check": 批量账务审核
                - "anomaly_detect": 异常检测分析
                - "generate_report": 生成审核报告
                - "rule_config": 规则配置管理
                - "data_parse": 数据解析处理
            data: 输入数据
            **kwargs: 额外参数
            
        Returns:
            任务执行结果
            
        Raises:
            TaskError: 当任务类型不支持时抛出
            SkillError: 当技能执行失败时抛出
        """
        # 更新使用统计
        self._update_usage()
        
        # 验证任务类型
        if not self.validate_task(task):
            available_tasks = ", ".join(self.get_supported_tasks())
            raise TaskError(f"不支持的任务类型：{task}。支持的任务类型：{available_tasks}")
            
        # 记录任务开始
        task_start = datetime.now()
        self.logger.info(f"开始执行任务：{task}")
        
        try:
            # 执行技能
            skill_func = self.skills[task]
            result = skill_func(data, **kwargs)
            
            # 记录任务完成
            task_end = datetime.now()
            duration = (task_end - task_start).total_seconds()
            
            # 记录审计历史
            audit_record = {
                "task": task,
                "start_time": task_start.isoformat(),
                "end_time": task_end.isoformat(),
                "duration": duration,
                "status": "success",
                "data_size": len(data) if hasattr(data, '__len__') else 1,
                "result_summary": self._summarize_result(result)
            }
            self.audit_history.append(audit_record)
            
            self.logger.info(f"任务 {task} 执行成功，耗时 {duration:.2f} 秒")
            return result
            
        except Exception as e:
            # 记录任务失败
            task_end = datetime.now()
            duration = (task_end - task_start).total_seconds()
            
            audit_record = {
                "task": task,
                "start_time": task_start.isoformat(),
                "end_time": task_end.isoformat(),
                "duration": duration,
                "status": "failed",
                "error": str(e),
                "data_size": len(data) if hasattr(data, '__len__') else 1
            }
            self.audit_history.append(audit_record)
            
            self.logger.error(f"任务 {task} 执行失败：{str(e)}")
            raise SkillError(f"技能执行失败：{str(e)}") from e
            
    def _summarize_result(self, result: Any) -> Dict[str, Any]:
        """
        汇总结果信息
        
        Args:
            result: 任务执行结果
            
        Returns:
            结果汇总信息
        """
        summary = {"type": type(result).__name__}
        
        if isinstance(result, pd.DataFrame):
            summary.update({
                "rows": len(result),
                "columns": len(result.columns),
                "shape": result.shape
            })
        elif isinstance(result, dict):
            summary.update({
                "keys": list(result.keys()),
                "size": len(result)
            })
        elif isinstance(result, list):
            summary.update({
                "length": len(result),
                "item_type": type(result[0]).__name__ if result else "empty"
            })
            
        return summary
        
    def get_audit_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        获取审计历史记录
        
        Args:
            limit: 返回记录数量限制，None表示返回全部
            
        Returns:
            审计历史记录列表
        """
        if limit is None:
            return self.audit_history.copy()
        return self.audit_history[-limit:]
        
    def get_task_statistics(self) -> Dict[str, Any]:
        """
        获取任务执行统计信息
        
        Returns:
            统计信息字典
        """
        if not self.audit_history:
            return {"total_tasks": 0}
            
        total_tasks = len(self.audit_history)
        successful_tasks = sum(1 for record in self.audit_history if record["status"] == "success")
        failed_tasks = total_tasks - successful_tasks
        
        # 按任务类型统计
        task_stats = {}
        for record in self.audit_history:
            task = record["task"]
            if task not in task_stats:
                task_stats[task] = {"total": 0, "success": 0, "failed": 0}
            task_stats[task]["total"] += 1
            if record["status"] == "success":
                task_stats[task]["success"] += 1
            else:
                task_stats[task]["failed"] += 1
                
        # 计算平均执行时间
        successful_records = [r for r in self.audit_history if r["status"] == "success"]
        avg_duration = sum(r["duration"] for r in successful_records) / len(successful_records) if successful_records else 0
        
        return {
            "total_tasks": total_tasks,
            "successful_tasks": successful_tasks,
            "failed_tasks": failed_tasks,
            "success_rate": successful_tasks / total_tasks if total_tasks > 0 else 0,
            "average_duration": avg_duration,
            "task_breakdown": task_stats,
            "last_task": self.audit_history[-1] if self.audit_history else None
        }
        
    def clear_audit_history(self) -> None:
        """清空审计历史记录"""
        self.audit_history.clear()
        self.logger.info("审计历史记录已清空")
        
    def get_core_tasks_info(self) -> Dict[str, str]:
        """
        获取核心任务信息
        
        Returns:
            任务信息字典 {task_name: description}
        """
        return self.core_tasks.copy()
        
    def validate_task_data(self, task: str, data: Any) -> bool:
        """
        验证任务数据格式
        
        Args:
            task: 任务类型
            data: 输入数据
            
        Returns:
            数据格式是否有效
        """
        if task == "data_parse":
            # 数据解析任务需要文件路径字符串
            return isinstance(data, str)
        elif task in ["single_check", "batch_check", "anomaly_detect"]:
            # 审核任务需要DataFrame或字典列表
            return isinstance(data, (pd.DataFrame, list, dict))
        elif task == "generate_report":
            # 报告生成需要审核结果数据
            return isinstance(data, (dict, list))
        elif task == "rule_config":
            # 规则配置需要配置字典
            return isinstance(data, dict)
        else:
            return True  # 默认通过验证
