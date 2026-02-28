"""
批量处理模块
优化批量账务审核功能，支持分块、并行处理和进度监控
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Callable, Iterator, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import threading
import time
import queue
from pathlib import Path
import pickle
import json

from .accounting_agent import AccountingAgent
from .config import AgentConfig
from ..skills.impl.data_parse import parse_account_data
from ..skills.impl.rule_check import rule_check_skill
from ..skills.impl.anomaly_detect import anomaly_detect_skill
from ..skills.impl.llm_explain import llm_explain_skill
from .utils.db import AuditRecord, save_audit_record, create_task_execution, get_task_execution_manager

logger = logging.getLogger(__name__)


@dataclass
class BatchConfig:
    """批量处理配置"""
    batch_size: int = 1000                    # 批次大小
    max_workers: int = 4                      # 最大工作线程数
    use_multiprocessing: bool = False          # 是否使用多进程
    enable_progress_tracking: bool = True     # 是否启用进度跟踪
    enable_checkpoint: bool = True            # 是否启用检查点
    checkpoint_interval: int = 10             # 检查点间隔（批次）
    output_dir: str = "batch_results"         # 输出目录
    save_intermediate_results: bool = True    # 是否保存中间结果
    memory_limit_mb: int = 1024               # 内存限制（MB）
    timeout_per_batch: int = 300              # 每批次超时时间（秒）


@dataclass
class BatchProgress:
    """批量处理进度"""
    task_id: str
    total_records: int
    processed_records: int = 0
    failed_records: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    current_batch: int = 0
    total_batches: int = 0
    status: str = "running"  # running, completed, failed, cancelled
    error_message: Optional[str] = None
    processing_rate: float = 0.0  # 记录/秒
    
    def update_progress(self, processed: int, failed: int = 0):
        """更新进度"""
        self.processed_records += processed
        self.failed_records += failed
        self.processing_rate = self.processed_records / max(1, (datetime.now() - self.start_time).total_seconds())
        
    def get_progress_percentage(self) -> float:
        """获取进度百分比"""
        return (self.processed_records / self.total_records) * 100 if self.total_records > 0 else 0
        
    def get_eta(self) -> Optional[timedelta]:
        """获取预计完成时间"""
        if self.processing_rate <= 0:
            return None
        remaining_records = self.total_records - self.processed_records
        remaining_seconds = remaining_records / self.processing_rate
        return timedelta(seconds=remaining_seconds)


class BatchProcessor:
    """批量处理器"""
    
    def __init__(self, agent: AccountingAgent, config: Optional[BatchConfig] = None):
        """
        初始化批量处理器
        
        Args:
            agent: 智能体实例
            config: 批量处理配置
        """
        self.agent = agent
        self.config = config or BatchConfig()
        self.progress: Optional[BatchProgress] = None
        self.checkpoint_dir = Path(self.config.output_dir) / "checkpoints"
        self.results_dir = Path(self.config.output_dir) / "results"
        
        # 创建目录
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # 进度回调函数
        self.progress_callbacks: List[Callable[[BatchProgress], None]] = []
        
    def add_progress_callback(self, callback: Callable[[BatchProgress], None]):
        """添加进度回调函数"""
        self.progress_callbacks.append(callback)
        
    def _notify_progress(self):
        """通知进度更新"""
        if self.progress and self.progress_callbacks:
            for callback in self.progress_callbacks:
                try:
                    callback(self.progress)
                except Exception as e:
                    logger.error(f"进度回调执行失败: {e}")
                    
    def _create_batches(self, data: pd.DataFrame) -> Iterator[Tuple[int, pd.DataFrame]]:
        """创建数据批次"""
        total_batches = (len(data) + self.config.batch_size - 1) // self.config.batch_size
        
        for i in range(total_batches):
            start_idx = i * self.config.batch_size
            end_idx = min((i + 1) * self.config.batch_size, len(data))
            batch_data = data.iloc[start_idx:end_idx].copy()
            yield i, batch_data
            
    def _process_batch(self, batch_id: int, batch_data: pd.DataFrame, 
                      tasks: List[str]) -> Dict[str, Any]:
        """处理单个批次"""
        batch_start_time = datetime.now()
        
        try:
            results = {
                "batch_id": batch_id,
                "start_time": batch_start_time,
                "records_count": len(batch_data),
                "task_results": {}
            }
            
            # 执行各项任务
            for task in tasks:
                task_start = datetime.now()
                
                try:
                    if task == "data_parse":
                        # 数据解析任务（通常在批量处理前已完成）
                        result = {"status": "skipped", "message": "数据已预解析"}
                    elif task == "rule_check":
                        result = self.agent.run("rule_check", batch_data)
                    elif task == "anomaly_detect":
                        result = self.agent.run("anomaly_detect", batch_data)
                    elif task == "llm_explain":
                        # LLM解释通常只对有问题记录执行
                        if hasattr(result, 'anomalies') and result.anomalies:
                            anomaly_records = [a for a in result.anomalies if a.score > 0.7]
                            if anomaly_records:
                                explain_data = {
                                    "anomaly_result": anomaly_records[0].__dict__,
                                    "context_data": batch_data
                                }
                                result = self.agent.run("llm_explain", explain_data, explanation_type="anomaly_analysis")
                            else:
                                result = {"status": "skipped", "message": "无需解释"}
                        else:
                            result = {"status": "skipped", "message": "无异常需要解释"}
                    else:
                        result = {"status": "error", "message": f"未知任务: {task}"}
                        
                    task_end = datetime.now()
                    
                    results["task_results"][task] = {
                        "status": "success",
                        "result": result,
                        "processing_time": (task_end - task_start).total_seconds()
                    }
                    
                except Exception as e:
                    task_end = datetime.now()
                    logger.error(f"批次 {batch_id} 任务 {task} 执行失败: {e}")
                    
                    results["task_results"][task] = {
                        "status": "error",
                        "error": str(e),
                        "processing_time": (task_end - task_start).total_seconds()
                    }
                    
            batch_end = datetime.now()
            results["end_time"] = batch_end
            results["total_processing_time"] = (batch_end - batch_start).total_seconds()
            
            return results
            
        except Exception as e:
            logger.error(f"批次 {batch_id} 处理失败: {e}")
            return {
                "batch_id": batch_id,
                "status": "error",
                "error": str(e),
                "start_time": batch_start_time,
                "end_time": datetime.now()
            }
            
    def _save_checkpoint(self, batch_id: int, batch_result: Dict[str, Any]):
        """保存检查点"""
        if not self.config.enable_checkpoint:
            return
            
        try:
            checkpoint_file = self.checkpoint_dir / f"batch_{batch_id:06d}.pkl"
            with open(checkpoint_file, 'wb') as f:
                pickle.dump(batch_result, f)
                
            logger.debug(f"检查点已保存: {checkpoint_file}")
            
        except Exception as e:
            logger.error(f"保存检查点失败: {e}")
            
    def _load_checkpoint(self, batch_id: int) -> Optional[Dict[str, Any]]:
        """加载检查点"""
        if not self.config.enable_checkpoint:
            return None
            
        try:
            checkpoint_file = self.checkpoint_dir / f"batch_{batch_id:06d}.pkl"
            if checkpoint_file.exists():
                with open(checkpoint_file, 'rb') as f:
                    result = pickle.load(f)
                logger.debug(f"检查点已加载: {checkpoint_file}")
                return result
        except Exception as e:
            logger.error(f"加载检查点失败: {e}")
            
        return None
        
    def _save_batch_result(self, batch_id: int, batch_result: Dict[str, Any]):
        """保存批次结果"""
        if not self.config.save_intermediate_results:
            return
            
        try:
            result_file = self.results_dir / f"batch_{batch_id:06d}_result.json"
            
            # 转换不可序列化的对象
            serializable_result = self._make_serializable(batch_result)
            
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(serializable_result, f, ensure_ascii=False, indent=2, default=str)
                
            logger.debug(f"批次结果已保存: {result_file}")
            
        except Exception as e:
            logger.error(f"保存批次结果失败: {e}")
            
    def _make_serializable(self, obj: Any) -> Any:
        """转换对象为可序列化格式"""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return {k: self._make_serializable(v) for k, v in obj.__dict__.items() if not k.startswith('_')}
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        else:
            return obj
            
    def process_batch_sequential(self, data: pd.DataFrame, tasks: List[str]) -> Dict[str, Any]:
        """顺序批量处理"""
        logger.info(f"开始顺序批量处理，总记录数: {len(data)}")
        
        # 初始化进度
        self.progress = BatchProgress(
            task_id=f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            total_records=len(data),
            total_batches=(len(data) + self.config.batch_size - 1) // self.config.batch_size
        )
        
        # 创建任务执行记录
        create_task_execution(self.progress.task_id, "batch_processing", {
            "total_records": len(data),
            "batch_size": self.config.batch_size,
            "tasks": tasks
        })
        
        all_results = []
        failed_batches = []
        
        try:
            for batch_id, batch_data in self._create_batches(data):
                self.progress.current_batch = batch_id + 1
                self._notify_progress()
                
                # 检查是否已有检查点
                batch_result = self._load_checkpoint(batch_id)
                if batch_result:
                    logger.info(f"从检查点加载批次 {batch_id}")
                else:
                    # 处理批次
                    batch_result = self._process_batch(batch_id, batch_data, tasks)
                    
                    # 保存检查点
                    if batch_id % self.config.checkpoint_interval == 0:
                        self._save_checkpoint(batch_id, batch_result)
                
                # 保存结果
                self._save_batch_result(batch_id, batch_result)
                all_results.append(batch_result)
                
                # 更新进度
                if batch_result.get("status") == "error":
                    failed_count = batch_result.get("records_count", 0)
                    self.progress.update_progress(0, failed_count)
                    failed_batches.append(batch_id)
                else:
                    self.progress.update_progress(batch_result.get("records_count", 0))
                    
                # 检查内存使用
                if self._check_memory_limit():
                    logger.warning("内存使用接近限制，建议减少批次大小")
                    
        except Exception as e:
            self.progress.status = "failed"
            self.progress.error_message = str(e)
            logger.error(f"批量处理失败: {e}")
            raise
            
        finally:
            self.progress.end_time = datetime.now()
            self.progress.status = "completed" if not failed_batches else "completed_with_errors"
            self._notify_progress()
            
            # 更新任务执行记录
            task_manager = get_task_execution_manager()
            task_manager.update_task_execution(
                self.progress.task_id,
                status=self.progress.status,
                end_time=self.progress.end_time,
                total_records=len(data),
                processed_records=self.progress.processed_records,
                failed_records=self.progress.failed_records
            )
            
        return {
            "task_id": self.progress.task_id,
            "total_records": len(data),
            "processed_records": self.progress.processed_records,
            "failed_records": self.progress.failed_records,
            "failed_batches": failed_batches,
            "total_batches": self.progress.total_batches,
            "processing_time": (self.progress.end_time - self.progress.start_time).total_seconds(),
            "results": all_results
        }
        
    def process_batch_parallel(self, data: pd.DataFrame, tasks: List[str]) -> Dict[str, Any]:
        """并行批量处理"""
        logger.info(f"开始并行批量处理，总记录数: {len(data)}, 工作线程数: {self.config.max_workers}")
        
        # 初始化进度
        self.progress = BatchProgress(
            task_id=f"parallel_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            total_records=len(data),
            total_batches=(len(data) + self.config.batch_size - 1) // self.config.batch_size
        )
        
        # 创建任务执行记录
        create_task_execution(self.progress.task_id, "parallel_batch_processing", {
            "total_records": len(data),
            "batch_size": self.config.batch_size,
            "max_workers": self.config.max_workers,
            "tasks": tasks
        })
        
        # 准备批次数据
        batches = list(self._create_batches(data))
        
        # 结果收集
        all_results = []
        failed_batches = []
        progress_lock = threading.Lock()
        
        def process_batch_worker(batch_info):
            """工作线程函数"""
            batch_id, batch_data = batch_info
            
            try:
                # 检查检查点
                batch_result = self._load_checkpoint(batch_id)
                if batch_result:
                    return batch_id, batch_result
                    
                # 处理批次
                batch_result = self._process_batch(batch_id, batch_data, tasks)
                
                # 保存检查点和结果
                if batch_id % self.config.checkpoint_interval == 0:
                    self._save_checkpoint(batch_id, batch_result)
                self._save_batch_result(batch_id, batch_result)
                
                return batch_id, batch_result
                
            except Exception as e:
                logger.error(f"并行处理批次 {batch_id} 失败: {e}")
                error_result = {
                    "batch_id": batch_id,
                    "status": "error",
                    "error": str(e),
                    "records_count": len(batch_data) if batch_data is not None else 0
                }
                return batch_id, error_result
                
        # 选择执行器类型
        executor_class = ProcessPoolExecutor if self.config.use_multiprocessing else ThreadPoolExecutor
        
        with executor_class(max_workers=self.config.max_workers) as executor:
            # 提交任务
            future_to_batch = {
                executor.submit(process_batch_worker, batch_info): batch_info[0]
                for batch_info in batches
            }
            
            # 收集结果
            for future in as_completed(future_to_batch, timeout=self.config.timeout_per_batch):
                batch_id = future_to_batch[future]
                
                try:
                    result_batch_id, batch_result = future.result()
                    
                    all_results.append(batch_result)
                    
                    # 更新进度
                    with progress_lock:
                        if batch_result.get("status") == "error":
                            failed_count = batch_result.get("records_count", 0)
                            self.progress.update_progress(0, failed_count)
                            failed_batches.append(batch_id)
                        else:
                            self.progress.update_progress(batch_result.get("records_count", 0))
                            
                        self.progress.current_batch = len(all_results)
                        self._notify_progress()
                        
                except Exception as e:
                    logger.error(f"获取批次 {batch_id} 结果失败: {e}")
                    failed_batches.append(batch_id)
                    
        # 完成处理
        self.progress.end_time = datetime.now()
        self.progress.status = "completed" if not failed_batches else "completed_with_errors"
        self._notify_progress()
        
        # 更新任务执行记录
        task_manager = get_task_execution_manager()
        task_manager.update_task_execution(
            self.progress.task_id,
            status=self.progress.status,
            end_time=self.progress.end_time,
            total_records=len(data),
            processed_records=self.progress.processed_records,
            failed_records=self.progress.failed_records
        )
        
        return {
            "task_id": self.progress.task_id,
            "total_records": len(data),
            "processed_records": self.progress.processed_records,
            "failed_records": self.progress.failed_records,
            "failed_batches": failed_batches,
            "total_batches": self.progress.total_batches,
            "processing_time": (self.progress.end_time - self.progress.start_time).total_seconds(),
            "results": all_results
        }
        
    def _check_memory_limit(self) -> bool:
        """检查内存使用限制"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            return memory_mb > self.config.memory_limit_mb
            
        except ImportError:
            # 如果没有psutil，使用简单的检查
            return False
            
    def cancel_processing(self):
        """取消批量处理"""
        if self.progress:
            self.progress.status = "cancelled"
            self.progress.end_time = datetime.now()
            self._notify_progress()
            logger.info("批量处理已取消")
            
    def get_progress(self) -> Optional[BatchProgress]:
        """获取当前进度"""
        return self.progress
        
    def cleanup_checkpoints(self):
        """清理检查点文件"""
        try:
            for checkpoint_file in self.checkpoint_dir.glob("*.pkl"):
                checkpoint_file.unlink()
            logger.info("检查点文件已清理")
        except Exception as e:
            logger.error(f"清理检查点文件失败: {e}")


class BatchResultAggregator:
    """批量结果聚合器"""
    
    @staticmethod
    def aggregate_results(batch_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """聚合批量处理结果"""
        if not batch_results:
            return {}
            
        total_records = sum(r.get("records_count", 0) for r in batch_results)
        total_processing_time = sum(r.get("total_processing_time", 0) for r in batch_results)
        
        # 聚合任务结果
        task_aggregates = {}
        
        for batch_result in batch_results:
            if "task_results" not in batch_result:
                continue
                
            for task_name, task_result in batch_result["task_results"].items():
                if task_name not in task_aggregates:
                    task_aggregates[task_name] = {
                        "total_batches": 0,
                        "successful_batches": 0,
                        "failed_batches": 0,
                        "total_processing_time": 0
                    }
                    
                task_aggregates[task_name]["total_batches"] += 1
                task_aggregates[task_name]["total_processing_time"] += task_result.get("processing_time", 0)
                
                if task_result.get("status") == "success":
                    task_aggregates[task_name]["successful_batches"] += 1
                else:
                    task_aggregates[task_name]["failed_batches"] += 1
                    
        return {
            "summary": {
                "total_records": total_records,
                "total_batches": len(batch_results),
                "total_processing_time": total_processing_time,
                "average_processing_time": total_processing_time / len(batch_results),
                "records_per_second": total_records / max(1, total_processing_time)
            },
            "task_summary": task_aggregates,
            "detailed_results": batch_results
        }
        
    @staticmethod
    def generate_report(aggregate_result: Dict[str, Any], output_path: str):
        """生成批量处理报告"""
        report = {
            "generated_at": datetime.now().isoformat(),
            "processing_summary": aggregate_result.get("summary", {}),
            "task_performance": aggregate_result.get("task_summary", {}),
            "recommendations": []
        }
        
        # 生成建议
        summary = aggregate_result.get("summary", {})
        if summary.get("records_per_second", 0) < 10:
            report["recommendations"].append("处理速度较慢，建议增加批次大小或使用并行处理")
            
        task_summary = aggregate_result.get("task_summary", {})
        for task_name, task_stats in task_summary.items():
            failure_rate = task_stats["failed_batches"] / task_stats["total_batches"]
            if failure_rate > 0.1:
                report["recommendations"].append(f"任务 {task_name} 失败率较高 ({failure_rate:.1%})，建议检查配置和数据质量")
                
        # 保存报告
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            
        logger.info(f"批量处理报告已生成: {output_path}")


# 便捷函数
def create_batch_processor(agent: AccountingAgent, **config_kwargs) -> BatchProcessor:
    """创建批量处理器的便捷函数"""
    config = BatchConfig(**config_kwargs)
    return BatchProcessor(agent, config)


def process_large_dataset(file_path: str, tasks: List[str], **config_kwargs) -> Dict[str, Any]:
    """处理大数据集的便捷函数"""
    # 解析数据
    data = parse_account_data(file_path)
    
    # 创建智能体
    agent = AccountingAgent()
    
    # 注册技能
    if "rule_check" in tasks:
        agent.register_skill("rule_check", rule_check_skill)
    if "anomaly_detect" in tasks:
        agent.register_skill("anomaly_detect", anomaly_detect_skill)
    if "llm_explain" in tasks:
        agent.register_skill("llm_explain", llm_explain_skill)
        
    # 创建批量处理器
    processor = create_batch_processor(agent, **config_kwargs)
    
    # 执行批量处理
    if config_kwargs.get("use_multiprocessing", False):
        result = processor.process_batch_parallel(data, tasks)
    else:
        result = processor.process_batch_sequential(data, tasks)
        
    # 聚合结果
    aggregated = BatchResultAggregator.aggregate_results(result["results"])
    
    # 生成报告
    report_path = Path(config_kwargs.get("output_dir", "batch_results")) / f"report_{result['task_id']}.json"
    BatchResultAggregator.generate_report(aggregated, str(report_path))
    
    return {
        "processing_result": result,
        "aggregated_result": aggregated,
        "report_path": str(report_path)
    }
