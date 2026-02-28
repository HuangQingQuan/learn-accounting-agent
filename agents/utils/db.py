"""
数据库工具模块
提供审核记录的持久化存储和管理功能
"""

import sqlite3
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import json
from pathlib import Path
import threading

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool

logger = logging.getLogger(__name__)

Base = declarative_base()


@dataclass
class AuditRecord:
    """审核记录数据类"""
    id: Optional[int] = None
    record_id: str = ""
    audit_date: datetime = None
    task_type: str = ""
    data_source: str = ""
    passed: bool = False
    risk_level: str = "low"
    rule_results: Optional[List[Dict[str, Any]]] = None
    anomaly_results: Optional[List[Dict[str, Any]]] = None
    explanation: Optional[str] = None
    suggestions: Optional[List[str]] = None
    processing_time: float = 0.0
    created_at: datetime = None
    
    def __post_init__(self):
        if self.audit_date is None:
            self.audit_date = datetime.now()
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.rule_results is None:
            self.rule_results = []
        if self.anomaly_results is None:
            self.anomaly_results = []
        if self.suggestions is None:
            self.suggestions = []


class AuditRecordTable(Base):
    """审核记录数据库表"""
    __tablename__ = 'audit_records'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    record_id = Column(String(100), nullable=False, index=True)
    audit_date = Column(DateTime, nullable=False, index=True)
    task_type = Column(String(50), nullable=False)
    data_source = Column(String(200))
    passed = Column(Boolean, nullable=False, default=False)
    risk_level = Column(String(20), nullable=False, default='low')
    rule_results = Column(JSON)
    anomaly_results = Column(JSON)
    explanation = Column(Text)
    suggestions = Column(JSON)
    processing_time = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class TaskExecutionTable(Base):
    """任务执行记录表"""
    __tablename__ = 'task_executions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(100), nullable=False, unique=True)
    task_type = Column(String(50), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime)
    status = Column(String(20), nullable=False, default='running')
    total_records = Column(Integer, default=0)
    processed_records = Column(Integer, default=0)
    failed_records = Column(Integer, default=0)
    error_message = Column(Text)
    config = Column(JSON)
    created_at = Column(DateTime, default=datetime.now)


class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, database_url: str = "sqlite:///accounting_agent.db"):
        """
        初始化数据库管理器
        
        Args:
            database_url: 数据库连接URL
        """
        self.database_url = database_url
        self.engine = None
        self.SessionLocal = None
        self._lock = threading.Lock()
        
    def initialize(self):
        """初始化数据库连接和表结构"""
        with self._lock:
            try:
                # 创建数据库引擎
                if self.database_url.startswith("sqlite"):
                    # SQLite特殊配置
                    self.engine = create_engine(
                        self.database_url,
                        poolclass=StaticPool,
                        connect_args={
                            "check_same_thread": False,
                            "timeout": 20
                        },
                        echo=False
                    )
                else:
                    self.engine = create_engine(self.database_url, echo=False)
                
                # 创建会话工厂
                self.SessionLocal = sessionmaker(bind=self.engine)
                
                # 创建表结构
                Base.metadata.create_all(bind=self.engine)
                
                logger.info(f"数据库初始化成功: {self.database_url}")
                
            except Exception as e:
                logger.error(f"数据库初始化失败: {e}")
                raise
                
    def get_session(self) -> Session:
        """获取数据库会话"""
        if self.SessionLocal is None:
            self.initialize()
        return self.SessionLocal()
        
    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            logger.info("数据库连接已关闭")


class AuditRecordManager:
    """审核记录管理器"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化审核记录管理器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        
    def save_audit_record(self, record: AuditRecord) -> int:
        """
        保存审核记录
        
        Args:
            record: 审核记录对象
            
        Returns:
            记录ID
        """
        session = self.db_manager.get_session()
        try:
            # 转换为数据库模型
            db_record = AuditRecordTable(
                record_id=record.record_id,
                audit_date=record.audit_date,
                task_type=record.task_type,
                data_source=record.data_source,
                passed=record.passed,
                risk_level=record.risk_level,
                rule_results=record.rule_results,
                anomaly_results=record.anomaly_results,
                explanation=record.explanation,
                suggestions=record.suggestions,
                processing_time=record.processing_time
            )
            
            session.add(db_record)
            session.commit()
            session.refresh(db_record)
            
            record_id = db_record.id
            logger.info(f"审核记录已保存，ID: {record_id}")
            
            return record_id
            
        except Exception as e:
            session.rollback()
            logger.error(f"保存审核记录失败: {e}")
            raise
        finally:
            session.close()
            
    def save_batch_audit_records(self, records: List[AuditRecord]) -> List[int]:
        """
        批量保存审核记录
        
        Args:
            records: 审核记录列表
            
        Returns:
            记录ID列表
        """
        session = self.db_manager.get_session()
        record_ids = []
        
        try:
            db_records = []
            for record in records:
                db_record = AuditRecordTable(
                    record_id=record.record_id,
                    audit_date=record.audit_date,
                    task_type=record.task_type,
                    data_source=record.data_source,
                    passed=record.passed,
                    risk_level=record.risk_level,
                    rule_results=record.rule_results,
                    anomaly_results=record.anomaly_results,
                    explanation=record.explanation,
                    suggestions=record.suggestions,
                    processing_time=record.processing_time
                )
                db_records.append(db_record)
            
            session.add_all(db_records)
            session.commit()
            
            for db_record in db_records:
                session.refresh(db_record)
                record_ids.append(db_record.id)
                
            logger.info(f"批量保存审核记录成功，数量: {len(record_ids)}")
            
            return record_ids
            
        except Exception as e:
            session.rollback()
            logger.error(f"批量保存审核记录失败: {e}")
            raise
        finally:
            session.close()
            
    def get_audit_record(self, record_id: int) -> Optional[AuditRecord]:
        """
        获取单条审核记录
        
        Args:
            record_id: 记录ID
            
        Returns:
            审核记录对象或None
        """
        session = self.db_manager.get_session()
        try:
            db_record = session.query(AuditRecordTable).filter(
                AuditRecordTable.id == record_id
            ).first()
            
            if db_record:
                return self._convert_to_audit_record(db_record)
            return None
            
        except Exception as e:
            logger.error(f"获取审核记录失败: {e}")
            return None
        finally:
            session.close()
            
    def get_audit_records_by_filter(self, 
                                   start_date: Optional[datetime] = None,
                                   end_date: Optional[datetime] = None,
                                   task_type: Optional[str] = None,
                                   risk_level: Optional[str] = None,
                                   passed: Optional[bool] = None,
                                   limit: int = 1000) -> List[AuditRecord]:
        """
        根据条件查询审核记录
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            task_type: 任务类型
            risk_level: 风险等级
            passed: 是否通过
            limit: 返回记录数限制
            
        Returns:
            审核记录列表
        """
        session = self.db_manager.get_session()
        try:
            query = session.query(AuditRecordTable)
            
            # 应用过滤条件
            if start_date:
                query = query.filter(AuditRecordTable.audit_date >= start_date)
            if end_date:
                query = query.filter(AuditRecordTable.audit_date <= end_date)
            if task_type:
                query = query.filter(AuditRecordTable.task_type == task_type)
            if risk_level:
                query = query.filter(AuditRecordTable.risk_level == risk_level)
            if passed is not None:
                query = query.filter(AuditRecordTable.passed == passed)
                
            # 排序和限制
            query = query.order_by(AuditRecordTable.audit_date.desc()).limit(limit)
            
            db_records = query.all()
            
            return [self._convert_to_audit_record(record) for record in db_records]
            
        except Exception as e:
            logger.error(f"查询审核记录失败: {e}")
            return []
        finally:
            session.close()
            
    def get_audit_statistics(self, 
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        获取审核统计信息
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            统计信息字典
        """
        session = self.db_manager.get_session()
        try:
            query = session.query(AuditRecordTable)
            
            if start_date:
                query = query.filter(AuditRecordTable.audit_date >= start_date)
            if end_date:
                query = query.filter(AuditRecordTable.audit_date <= end_date)
                
            # 基础统计
            total_records = query.count()
            passed_records = query.filter(AuditRecordTable.passed == True).count()
            failed_records = total_records - passed_records
            
            # 风险等级统计
            risk_stats = {}
            for risk_level in ['low', 'medium', 'high', 'critical']:
                count = query.filter(AuditRecordTable.risk_level == risk_level).count()
                risk_stats[risk_level] = count
                
            # 任务类型统计
            task_stats = {}
            task_types = session.query(AuditRecordTable.task_type).distinct().all()
            for (task_type,) in task_types:
                count = query.filter(AuditRecordTable.task_type == task_type).count()
                task_stats[task_type] = count
                
            # 时间趋势统计
            daily_stats = {}
            if start_date and end_date:
                # 按日期分组统计
                from sqlalchemy import func, extract
                daily_query = session.query(
                    func.date(AuditRecordTable.audit_date).label('date'),
                    func.count(AuditRecordTable.id).label('count'),
                    func.sum(func.case([(AuditRecordTable.passed == True, 1)], else_=0)).label('passed')
                ).filter(
                    AuditRecordTable.audit_date >= start_date,
                    AuditRecordTable.audit_date <= end_date
                ).group_by(
                    func.date(AuditRecordTable.audit_date)
                ).all()
                
                for date, count, passed in daily_query:
                    daily_stats[date.strftime('%Y-%m-%d')] = {
                        'total': count,
                        'passed': passed or 0,
                        'failed': count - (passed or 0)
                    }
                    
            return {
                "total_records": total_records,
                "passed_records": passed_records,
                "failed_records": failed_records,
                "pass_rate": passed_records / total_records if total_records > 0 else 0,
                "risk_distribution": risk_stats,
                "task_distribution": task_stats,
                "daily_trends": daily_stats,
                "period": {
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None
                }
            }
            
        except Exception as e:
            logger.error(f"获取审核统计失败: {e}")
            return {}
        finally:
            session.close()
            
    def delete_audit_record(self, record_id: int) -> bool:
        """
        删除审核记录
        
        Args:
            record_id: 记录ID
            
        Returns:
            是否删除成功
        """
        session = self.db_manager.get_session()
        try:
            db_record = session.query(AuditRecordTable).filter(
                AuditRecordTable.id == record_id
            ).first()
            
            if db_record:
                session.delete(db_record)
                session.commit()
                logger.info(f"审核记录已删除，ID: {record_id}")
                return True
            return False
            
        except Exception as e:
            session.rollback()
            logger.error(f"删除审核记录失败: {e}")
            return False
        finally:
            session.close()
            
    def _convert_to_audit_record(self, db_record: AuditRecordTable) -> AuditRecord:
        """将数据库记录转换为审核记录对象"""
        return AuditRecord(
            id=db_record.id,
            record_id=db_record.record_id,
            audit_date=db_record.audit_date,
            task_type=db_record.task_type,
            data_source=db_record.data_source,
            passed=db_record.passed,
            risk_level=db_record.risk_level,
            rule_results=db_record.rule_results,
            anomaly_results=db_record.anomaly_results,
            explanation=db_record.explanation,
            suggestions=db_record.suggestions,
            processing_time=db_record.processing_time,
            created_at=db_record.created_at
        )


class TaskExecutionManager:
    """任务执行管理器"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化任务执行管理器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        
    def create_task_execution(self, task_id: str, task_type: str, 
                             config: Optional[Dict[str, Any]] = None) -> bool:
        """
        创建任务执行记录
        
        Args:
            task_id: 任务ID
            task_type: 任务类型
            config: 任务配置
            
        Returns:
            是否创建成功
        """
        session = self.db_manager.get_session()
        try:
            task_execution = TaskExecutionTable(
                task_id=task_id,
                task_type=task_type,
                start_time=datetime.now(),
                status='running',
                config=config
            )
            
            session.add(task_execution)
            session.commit()
            
            logger.info(f"任务执行记录已创建: {task_id}")
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"创建任务执行记录失败: {e}")
            return False
        finally:
            session.close()
            
    def update_task_execution(self, task_id: str, 
                             status: Optional[str] = None,
                             end_time: Optional[datetime] = None,
                             total_records: Optional[int] = None,
                             processed_records: Optional[int] = None,
                             failed_records: Optional[int] = None,
                             error_message: Optional[str] = None) -> bool:
        """
        更新任务执行记录
        
        Args:
            task_id: 任务ID
            status: 任务状态
            end_time: 结束时间
            total_records: 总记录数
            processed_records: 已处理记录数
            failed_records: 失败记录数
            error_message: 错误信息
            
        Returns:
            是否更新成功
        """
        session = self.db_manager.get_session()
        try:
            task_execution = session.query(TaskExecutionTable).filter(
                TaskExecutionTable.task_id == task_id
            ).first()
            
            if task_execution:
                if status is not None:
                    task_execution.status = status
                if end_time is not None:
                    task_execution.end_time = end_time
                if total_records is not None:
                    task_execution.total_records = total_records
                if processed_records is not None:
                    task_execution.processed_records = processed_records
                if failed_records is not None:
                    task_execution.failed_records = failed_records
                if error_message is not None:
                    task_execution.error_message = error_message
                    
                session.commit()
                logger.info(f"任务执行记录已更新: {task_id}")
                return True
            return False
            
        except Exception as e:
            session.rollback()
            logger.error(f"更新任务执行记录失败: {e}")
            return False
        finally:
            session.close()
            
    def get_task_execution(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务执行记录
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务执行记录或None
        """
        session = self.db_manager.get_session()
        try:
            task_execution = session.query(TaskExecutionTable).filter(
                TaskExecutionTable.task_id == task_id
            ).first()
            
            if task_execution:
                return {
                    "id": task_execution.id,
                    "task_id": task_execution.task_id,
                    "task_type": task_execution.task_type,
                    "start_time": task_execution.start_time.isoformat(),
                    "end_time": task_execution.end_time.isoformat() if task_execution.end_time else None,
                    "status": task_execution.status,
                    "total_records": task_execution.total_records,
                    "processed_records": task_execution.processed_records,
                    "failed_records": task_execution.failed_records,
                    "error_message": task_execution.error_message,
                    "config": task_execution.config,
                    "created_at": task_execution.created_at.isoformat()
                }
            return None
            
        except Exception as e:
            logger.error(f"获取任务执行记录失败: {e}")
            return None
        finally:
            session.close()


# 全局数据库管理器实例
_db_manager = None
_audit_manager = None
_task_manager = None


def get_database_manager(database_url: str = None) -> DatabaseManager:
    """获取数据库管理器实例"""
    global _db_manager
    if _db_manager is None:
        url = database_url or "sqlite:///accounting_agent.db"
        _db_manager = DatabaseManager(url)
        _db_manager.initialize()
    return _db_manager


def get_audit_record_manager(database_url: str = None) -> AuditRecordManager:
    """获取审核记录管理器实例"""
    global _audit_manager
    if _audit_manager is None:
        db_manager = get_database_manager(database_url)
        _audit_manager = AuditRecordManager(db_manager)
    return _audit_manager


def get_task_execution_manager(database_url: str = None) -> TaskExecutionManager:
    """获取任务执行管理器实例"""
    global _task_manager
    if _task_manager is None:
        db_manager = get_database_manager(database_url)
        _task_manager = TaskExecutionManager(db_manager)
    return _task_manager


# 便捷函数
def save_audit_record(record: AuditRecord, database_url: str = None) -> int:
    """保存审核记录的便捷函数"""
    manager = get_audit_record_manager(database_url)
    return manager.save_audit_record(record)


def get_audit_statistics(start_date: datetime = None, 
                       end_date: datetime = None,
                       database_url: str = None) -> Dict[str, Any]:
    """获取审核统计的便捷函数"""
    manager = get_audit_record_manager(database_url)
    return manager.get_audit_statistics(start_date, end_date)


def create_task_execution(task_id: str, task_type: str, 
                        config: Dict[str, Any] = None,
                        database_url: str = None) -> bool:
    """创建任务执行记录的便捷函数"""
    manager = get_task_execution_manager(database_url)
    return manager.create_task_execution(task_id, task_type, config)
