"""
智能体配置模块
管理智能体的配置参数和环境变量
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class AgentConfig:
    """
    智能体配置类
    管理所有智能体相关的配置参数
    """
    
    # 基础配置
    name: str = "AccountingAgent"
    version: str = "1.0.0"
    debug: bool = False
    
    # 日志配置
    log_level: str = "INFO"
    log_file: Optional[str] = None
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # 数据库配置
    database_url: str = "sqlite:///accounting_agent.db"
    database_echo: bool = False
    
    # LLM配置
    llm_provider: str = "openai"  # openai, azure, local
    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-3.5-turbo"
    openai_temperature: float = 0.1
    openai_max_tokens: int = 2000
    
    # 文件路径配置
    data_dir: str = "data"
    reports_dir: str = "reports"
    temp_dir: str = "temp"
    rules_file: str = "config/rules.json"
    
    # 审核配置
    max_batch_size: int = 1000
    default_rules: Dict[str, Any] = field(default_factory=lambda: {
        "max_amount": 100000,
        "forbidden_subjects": ["违规科目1", "违规科目2"],
        "duplicate_check": True,
        "date_range_check": True,
        "account_balance_check": True
    })
    
    # 异常检测配置
    anomaly_threshold: float = 2.0  # 标准差倍数
    outlier_method: str = "zscore"  # zscore, iqr, isolation_forest
    min_samples_for_anomaly: int = 10
    
    # 报告配置
    report_template_dir: str = "templates"
    default_report_format: str = "html"  # html, pdf, docx
    include_charts: bool = True
    
    # API配置
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 1
    api_reload: bool = False
    
    def __post_init__(self):
        """初始化后处理"""
        # 从环境变量加载配置
        self._load_from_env()
        
        # 创建必要的目录
        self._ensure_directories()
        
    def _load_from_env(self):
        """从环境变量加载配置"""
        # LLM配置
        if os.getenv("OPENAI_API_KEY"):
            self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if os.getenv("OPENAI_MODEL"):
            self.openai_model = os.getenv("OPENAI_MODEL")
            
        # 数据库配置
        if os.getenv("DATABASE_URL"):
            self.database_url = os.getenv("DATABASE_URL")
            
        # API配置
        if os.getenv("API_HOST"):
            self.api_host = os.getenv("API_HOST")
        if os.getenv("API_PORT"):
            self.api_port = int(os.getenv("API_PORT"))
            
        # 调试模式
        if os.getenv("DEBUG"):
            self.debug = os.getenv("DEBUG").lower() in ("true", "1", "yes")
            
    def _ensure_directories(self):
        """确保必要的目录存在"""
        directories = [
            self.data_dir,
            self.reports_dir,
            self.temp_dir,
            os.path.dirname(self.rules_file),
            self.report_template_dir
        ]
        
        for directory in directories:
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                
    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置"""
        return {
            "url": self.database_url,
            "echo": self.database_echo
        }
        
    def get_llm_config(self) -> Dict[str, Any]:
        """获取LLM配置"""
        config = {
            "provider": self.llm_provider,
            "model": self.openai_model,
            "temperature": self.openai_temperature,
            "max_tokens": self.openai_max_tokens
        }
        
        if self.llm_provider == "openai" and self.openai_api_key:
            config["api_key"] = self.openai_api_key
            
        return config
        
    def get_api_config(self) -> Dict[str, Any]:
        """获取API配置"""
        return {
            "host": self.api_host,
            "port": self.api_port,
            "workers": self.api_workers,
            "reload": self.api_reload
        }
        
    def get_rules_file_path(self) -> Path:
        """获取规则文件路径"""
        return Path(self.rules_file)
        
    def update_config(self, **kwargs):
        """更新配置参数"""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise ValueError(f"未知的配置参数: {key}")
                
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "name": self.name,
            "version": self.version,
            "debug": self.debug,
            "log_level": self.log_level,
            "database_url": self.database_url,
            "llm_provider": self.llm_provider,
            "openai_model": self.openai_model,
            "data_dir": self.data_dir,
            "reports_dir": self.reports_dir,
            "max_batch_size": self.max_batch_size,
            "anomaly_threshold": self.anomaly_threshold,
            "default_report_format": self.default_report_format,
            "api_host": self.api_host,
            "api_port": self.api_port
        }
        
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "AgentConfig":
        """从字典创建配置对象"""
        return cls(**config_dict)
        
    @classmethod
    def from_file(cls, config_file: str) -> "AgentConfig":
        """从配置文件加载"""
        import json
        with open(config_file, 'r', encoding='utf-8') as f:
            config_dict = json.load(f)
        return cls.from_dict(config_dict)
        
    def save_to_file(self, config_file: str):
        """保存配置到文件"""
        import json
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)


# 全局配置实例
default_config = AgentConfig()


def get_config() -> AgentConfig:
    """获取全局配置实例"""
    return default_config


def set_config(config: AgentConfig):
    """设置全局配置实例"""
    global default_config
    default_config = config
