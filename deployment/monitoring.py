"""
监控和日志收集模块
提供系统性能监控、健康检查和日志管理功能
"""

import psutil
import logging
import time
import json
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import threading
from pathlib import Path
import requests
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import schedule

logger = logging.getLogger(__name__)


@dataclass
class SystemMetrics:
    """系统指标数据类"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    disk_usage_percent: float
    disk_used_gb: float
    network_io: Dict[str, int]
    process_count: int
    load_average: List[float]
    uptime_seconds: float


@dataclass
class ApplicationMetrics:
    """应用指标数据类"""
    timestamp: datetime
    active_connections: int
    total_requests: int
    successful_requests: int
    failed_requests: int
    average_response_time: float
    active_tasks: int
    completed_tasks: int
    database_connections: int
    cache_hit_rate: float


class MetricsCollector:
    """指标收集器"""
    
    def __init__(self, collection_interval: int = 30):
        self.collection_interval = collection_interval
        self.is_running = False
        self.metrics_history: List[SystemMetrics] = []
        self.app_metrics_history: List[ApplicationMetrics] = []
        
        # Prometheus指标
        self.cpu_gauge = Gauge('system_cpu_percent', 'CPU使用率')
        self.memory_gauge = Gauge('system_memory_percent', '内存使用率')
        self.disk_gauge = Gauge('system_disk_percent', '磁盘使用率')
        self.requests_counter = Counter('http_requests_total', 'HTTP请求总数', ['method', 'endpoint', 'status'])
        self.response_time_histogram = Histogram('http_request_duration_seconds', 'HTTP请求响应时间')
        self.active_tasks_gauge = Gauge('active_tasks', '活跃任务数')
        
    def collect_system_metrics(self) -> SystemMetrics:
        """收集系统指标"""
        try:
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 内存使用情况
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_used_mb = memory.used / 1024 / 1024
            
            # 磁盘使用情况
            disk = psutil.disk_usage('/')
            disk_usage_percent = disk.percent
            disk_used_gb = disk.used / 1024 / 1024 / 1024
            
            # 网络IO
            network_io = psutil.net_io_counters()._asdict()
            
            # 进程数
            process_count = len(psutil.pids())
            
            # 系统负载
            load_average = list(psutil.getloadavg()) if hasattr(psutil, 'getloadavg') else [0, 0, 0]
            
            # 系统运行时间
            uptime_seconds = time.time() - psutil.boot_time()
            
            metrics = SystemMetrics(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                memory_used_mb=memory_used_mb,
                disk_usage_percent=disk_usage_percent,
                disk_used_gb=disk_used_gb,
                network_io=network_io,
                process_count=process_count,
                load_average=load_average,
                uptime_seconds=uptime_seconds
            )
            
            # 更新Prometheus指标
            self.cpu_gauge.set(cpu_percent)
            self.memory_gauge.set(memory_percent)
            self.disk_gauge.set(disk_usage_percent)
            
            return metrics
            
        except Exception as e:
            logger.error(f"收集系统指标失败: {e}")
            return None
            
    def collect_application_metrics(self) -> ApplicationMetrics:
        """收集应用指标"""
        try:
            # 这里应该从应用中获取实际指标
            # 示例数据，实际实现需要与应用集成
            metrics = ApplicationMetrics(
                timestamp=datetime.now(),
                active_connections=150,
                total_requests=10000,
                successful_requests=9500,
                failed_requests=500,
                average_response_time=0.25,
                active_tasks=5,
                completed_tasks=995,
                database_connections=10,
                cache_hit_rate=0.85
            )
            
            # 更新Prometheus指标
            self.active_tasks_gauge.set(metrics.active_tasks)
            
            return metrics
            
        except Exception as e:
            logger.error(f"收集应用指标失败: {e}")
            return None
            
    def start_collection(self):
        """开始指标收集"""
        self.is_running = True
        
        def collect_loop():
            while self.is_running:
                try:
                    # 收集系统指标
                    system_metrics = self.collect_system_metrics()
                    if system_metrics:
                        self.metrics_history.append(system_metrics)
                        # 保留最近1000条记录
                        if len(self.metrics_history) > 1000:
                            self.metrics_history = self.metrics_history[-1000:]
                    
                    # 收集应用指标
                    app_metrics = self.collect_application_metrics()
                    if app_metrics:
                        self.app_metrics_history.append(app_metrics)
                        if len(self.app_metrics_history) > 1000:
                            self.app_metrics_history = self.app_metrics_history[-1000:]
                    
                    time.sleep(self.collection_interval)
                    
                except Exception as e:
                    logger.error(f"指标收集循环错误: {e}")
                    time.sleep(5)
        
        collection_thread = threading.Thread(target=collect_loop, daemon=True)
        collection_thread.start()
        logger.info("指标收集已启动")
        
    def stop_collection(self):
        """停止指标收集"""
        self.is_running = False
        logger.info("指标收集已停止")
        
    def get_current_metrics(self) -> Dict[str, Any]:
        """获取当前指标"""
        current_system = self.metrics_history[-1] if self.metrics_history else None
        current_app = self.app_metrics_history[-1] if self.app_metrics_history else None
        
        return {
            "system": asdict(current_system) if current_system else None,
            "application": asdict(current_app) if current_app else None,
            "timestamp": datetime.now().isoformat()
        }
        
    def get_metrics_history(self, hours: int = 1) -> Dict[str, List[Dict]]:
        """获取历史指标"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        system_history = [
            asdict(m) for m in self.metrics_history 
            if m.timestamp >= cutoff_time
        ]
        
        app_history = [
            asdict(m) for m in self.app_metrics_history 
            if m.timestamp >= cutoff_time
        ]
        
        return {
            "system": system_history,
            "application": app_history
        }


class HealthChecker:
    """健康检查器"""
    
    def __init__(self):
        self.checks = {}
        
    def register_check(self, name: str, check_func: callable, timeout: int = 30):
        """注册健康检查"""
        self.checks[name] = {
            "func": check_func,
            "timeout": timeout,
            "last_result": None,
            "last_check": None
        }
        
    def run_check(self, name: str) -> Dict[str, Any]:
        """运行单个健康检查"""
        if name not in self.checks:
            return {
                "name": name,
                "status": "unknown",
                "message": "检查不存在",
                "timestamp": datetime.now().isoformat()
            }
            
        check_info = self.checks[name]
        
        try:
            start_time = time.time()
            result = check_info["func"]()
            duration = time.time() - start_time
            
            check_result = {
                "name": name,
                "status": "healthy" if result else "unhealthy",
                "message": "检查通过" if result else "检查失败",
                "duration": duration,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            check_result = {
                "name": name,
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }
            
        # 更新检查结果
        check_info["last_result"] = check_result
        check_info["last_check"] = datetime.now()
        
        return check_result
        
    def run_all_checks(self) -> Dict[str, Any]:
        """运行所有健康检查"""
        results = {}
        overall_status = "healthy"
        
        for name in self.checks:
            result = self.run_check(name)
            results[name] = result
            
            # 确定整体状态
            if result["status"] in ["unhealthy", "error"]:
                overall_status = "unhealthy"
            elif result["status"] == "warning":
                if overall_status == "healthy":
                    overall_status = "warning"
                    
        return {
            "overall_status": overall_status,
            "checks": results,
            "timestamp": datetime.now().isoformat()
        }
        
    def get_check_status(self, name: str) -> Optional[Dict[str, Any]]:
        """获取检查状态"""
        if name not in self.checks:
            return None
            
        check_info = self.checks[name]
        return {
            "name": name,
            "last_result": check_info["last_result"],
            "last_check": check_info["last_check"].isoformat() if check_info["last_check"] else None
        }


class AlertManager:
    """告警管理器"""
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url
        self.alert_rules = []
        self.alert_history = []
        
    def add_alert_rule(self, name: str, condition: callable, 
                      severity: str = "warning", message: str = ""):
        """添加告警规则"""
        self.alert_rules.append({
            "name": name,
            "condition": condition,
            "severity": severity,
            "message": message,
            "enabled": True,
            "last_triggered": None
        })
        
    def check_alerts(self, metrics: Dict[str, Any]):
        """检查告警条件"""
        triggered_alerts = []
        
        for rule in self.alert_rules:
            if not rule["enabled"]:
                continue
                
            try:
                if rule["condition"](metrics):
                    alert = {
                        "name": rule["name"],
                        "severity": rule["severity"],
                        "message": rule["message"],
                        "timestamp": datetime.now().isoformat(),
                        "metrics": metrics
                    }
                    
                    triggered_alerts.append(alert)
                    self.alert_history.append(alert)
                    rule["last_triggered"] = datetime.now()
                    
                    # 发送告警通知
                    self.send_alert(alert)
                    
            except Exception as e:
                logger.error(f"告警规则 {rule['name']} 检查失败: {e}")
                
        return triggered_alerts
        
    def send_alert(self, alert: Dict[str, Any]):
        """发送告警通知"""
        if not self.webhook_url:
            logger.warning(f"告警通知URL未配置: {alert['name']}")
            return
            
        try:
            payload = {
                "alert": alert,
                "service": "accounting-agent",
                "timestamp": datetime.now().isoformat()
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"告警通知发送成功: {alert['name']}")
            else:
                logger.error(f"告警通知发送失败: {response.status_code}")
                
        except Exception as e:
            logger.error(f"发送告警通知异常: {e}")
            
    def get_alert_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """获取告警历史"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            alert for alert in self.alert_history 
            if datetime.fromisoformat(alert["timestamp"]) >= cutoff_time
        ]


class MonitoringService:
    """监控服务主类"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._get_default_config()
        
        # 初始化组件
        self.metrics_collector = MetricsCollector(
            collection_interval=self.config.get("metrics_interval", 30)
        )
        self.health_checker = HealthChecker()
        self.alert_manager = AlertManager(
            webhook_url=self.config.get("webhook_url")
        )
        
        # 注册默认健康检查
        self._register_default_health_checks()
        
        # 注册默认告警规则
        self._register_default_alert_rules()
        
        # 启动监控
        self.is_running = False
        
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "metrics_interval": 30,
            "health_check_interval": 60,
            "alert_check_interval": 60,
            "webhook_url": None,
            "prometheus_port": 8001
        }
        
    def _register_default_health_checks(self):
        """注册默认健康检查"""
        def check_database():
            try:
                from agents.utils.db import get_audit_record_manager
                manager = get_audit_record_manager()
                stats = manager.get_audit_statistics()
                return True
            except Exception:
                return False
                
        def check_redis():
            try:
                import redis
                r = redis.Redis(host='redis', port=6379, db=0)
                r.ping()
                return True
            except Exception:
                return False
                
        def check_disk_space():
            disk = psutil.disk_usage('/')
            return disk.percent < 90
            
        def check_memory():
            memory = psutil.virtual_memory()
            return memory.percent < 90
            
        self.health_checker.register_check("database", check_database)
        self.health_checker.register_check("redis", check_redis)
        self.health_checker.register_check("disk_space", check_disk_space)
        self.health_checker.register_check("memory", check_memory)
        
    def _register_default_alert_rules(self):
        """注册默认告警规则"""
        def cpu_high(metrics):
            system = metrics.get("system", {})
            return system.get("cpu_percent", 0) > 80
            
        def memory_high(metrics):
            system = metrics.get("system", {})
            return system.get("memory_percent", 0) > 85
            
        def disk_high(metrics):
            system = metrics.get("system", {})
            return system.get("disk_usage_percent", 0) > 90
            
        def response_time_high(metrics):
            app = metrics.get("application", {})
            return app.get("average_response_time", 0) > 1.0
            
        def error_rate_high(metrics):
            app = metrics.get("application", {})
            total = app.get("total_requests", 1)
            failed = app.get("failed_requests", 0)
            return (failed / total) > 0.05
            
        self.alert_manager.add_alert_rule(
            "cpu_high", cpu_high, "warning", "CPU使用率过高"
        )
        self.alert_manager.add_alert_rule(
            "memory_high", memory_high, "warning", "内存使用率过高"
        )
        self.alert_manager.add_alert_rule(
            "disk_high", disk_high, "critical", "磁盘使用率过高"
        )
        self.alert_manager.add_alert_rule(
            "response_time_high", response_time_high, "warning", "响应时间过长"
        )
        self.alert_manager.add_alert_rule(
            "error_rate_high", error_rate_high, "critical", "错误率过高"
        )
        
    def start(self):
        """启动监控服务"""
        if self.is_running:
            logger.warning("监控服务已在运行")
            return
            
        self.is_running = True
        
        # 启动指标收集
        self.metrics_collector.start_collection()
        
        # 启动Prometheus HTTP服务器
        try:
            start_http_server(self.config.get("prometheus_port", 8001))
            logger.info(f"Prometheus指标服务器已启动，端口: {self.config.get('prometheus_port', 8001)}")
        except Exception as e:
            logger.error(f"启动Prometheus服务器失败: {e}")
        
        # 启动健康检查和告警检查
        def monitoring_loop():
            while self.is_running:
                try:
                    # 获取当前指标
                    current_metrics = self.metrics_collector.get_current_metrics()
                    
                    # 检查告警
                    self.alert_manager.check_alerts(current_metrics)
                    
                    # 等待下次检查
                    time.sleep(self.config.get("alert_check_interval", 60))
                    
                except Exception as e:
                    logger.error(f"监控循环错误: {e}")
                    time.sleep(10)
                    
        monitoring_thread = threading.Thread(target=monitoring_loop, daemon=True)
        monitoring_thread.start()
        
        logger.info("监控服务已启动")
        
    def stop(self):
        """停止监控服务"""
        self.is_running = False
        self.metrics_collector.stop_collection()
        logger.info("监控服务已停止")
        
    def get_status(self) -> Dict[str, Any]:
        """获取监控状态"""
        health_status = self.health_checker.run_all_checks()
        current_metrics = self.metrics_collector.get_current_metrics()
        recent_alerts = self.alert_manager.get_alert_history(24)
        
        return {
            "monitoring_active": self.is_running,
            "health_status": health_status,
            "current_metrics": current_metrics,
            "recent_alerts": recent_alerts,
            "timestamp": datetime.now().isoformat()
        }


# 便捷函数
def create_monitoring_service(config: Optional[Dict[str, Any]] = None) -> MonitoringService:
    """创建监控服务的便捷函数"""
    return MonitoringService(config)


def setup_default_monitoring():
    """设置默认监控"""
    # 从环境变量读取配置
    config = {
        "metrics_interval": int(os.getenv("METRICS_INTERVAL", "30")),
        "webhook_url": os.getenv("ALERT_WEBHOOK_URL"),
        "prometheus_port": int(os.getenv("PROMETHEUS_PORT", "8001"))
    }
    
    service = create_monitoring_service(config)
    service.start()
    
    return service


if __name__ == "__main__":
    import os
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 启动监控服务
    monitoring_service = setup_default_monitoring()
    
    try:
        # 保持服务运行
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到停止信号，正在关闭监控服务...")
        monitoring_service.stop()
