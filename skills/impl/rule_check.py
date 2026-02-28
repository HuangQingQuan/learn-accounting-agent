"""
规则引擎技能实现
提供可配置的账务审核规则，支持多种合规检查和自定义规则
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import re

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """风险等级枚举"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class RuleResult:
    """规则检查结果"""
    rule_name: str
    passed: bool
    risk_level: RiskLevel
    message: str
    details: Optional[Dict[str, Any]] = None


@dataclass
class AuditResult:
    """审核结果"""
    record_id: Union[int, str]
    passed: bool
    risk_level: RiskLevel
    rule_results: List[RuleResult]
    summary: str
    suggestions: List[str]


class RuleType(Enum):
    """规则类型枚举"""
    AMOUNT_THRESHOLD = "amount_threshold"
    ACCOUNT_VALIDATION = "account_validation"
    BALANCE_CHECK = "balance_check"
    DUPLICATE_CHECK = "duplicate_check"
    DATE_VALIDATION = "date_validation"
    CUSTOM_RULE = "custom_rule"


class AccountRuleEngine:
    """
    账务审核规则引擎
    提供可配置的规则管理和执行功能
    """
    
    def __init__(self, rules: Optional[Dict[str, Any]] = None):
        """
        初始化规则引擎
        
        Args:
            rules: 规则配置字典
        """
        self.rules = rules or self._get_default_rules()
        self.custom_rules: Dict[str, Callable] = {}
        self.rule_statistics: Dict[str, Dict[str, Any]] = {}
        
    def _get_default_rules(self) -> Dict[str, Any]:
        """获取默认规则配置"""
        return {
            # 金额阈值规则
            "amount_threshold": {
                "enabled": True,
                "max_single_amount": 100000,
                "max_daily_amount": 500000,
                "risk_level": "medium",
                "message": "金额超过阈值"
            },
            
            # 科目验证规则
            "account_validation": {
                "enabled": True,
                "forbidden_accounts": ["违规科目1", "违规科目2"],
                "required_accounts": [],
                "account_patterns": {
                    "allowed": [r"^\d{4}$"],  # 4位数字科目编码
                    "forbidden": [r"^[A-Za-z]+$"]  # 纯字母科目
                },
                "risk_level": "high",
                "message": "科目验证失败"
            },
            
            # 借贷平衡检查
            "balance_check": {
                "enabled": True,
                "tolerance": 0.01,  # 允许的误差
                "risk_level": "high",
                "message": "借贷不平衡"
            },
            
            # 重复交易检查
            "duplicate_check": {
                "enabled": True,
                "check_fields": ["日期", "科目", "借方金额", "贷方金额", "摘要"],
                "time_window_days": 7,
                "risk_level": "medium",
                "message": "发现重复交易"
            },
            
            # 日期验证规则
            "date_validation": {
                "enabled": True,
                "min_date": "2020-01-01",
                "max_date": None,  # None表示不限制
                "future_date_check": True,
                "risk_level": "low",
                "message": "日期验证失败"
            }
        }
        
    def register_custom_rule(self, rule_name: str, rule_func: Callable, 
                           risk_level: RiskLevel = RiskLevel.MEDIUM,
                           message: str = "自定义规则检查失败"):
        """
        注册自定义规则
        
        Args:
            rule_name: 规则名称
            rule_func: 规则函数，签名应为 (record: pd.Series, **kwargs) -> RuleResult
            risk_level: 风险等级
            message: 规则失败时的默认消息
        """
        self.custom_rules[rule_name] = {
            "function": rule_func,
            "risk_level": risk_level,
            "message": message
        }
        logger.info(f"自定义规则 '{rule_name}' 注册成功")
        
    def unregister_custom_rule(self, rule_name: str):
        """注销自定义规则"""
        if rule_name in self.custom_rules:
            del self.custom_rules[rule_name]
            logger.info(f"自定义规则 '{rule_name}' 注销成功")
        else:
            logger.warning(f"自定义规则 '{rule_name}' 不存在")
            
    def check_amount_threshold(self, record: pd.Series, **kwargs) -> RuleResult:
        """检查金额阈值"""
        rule_config = self.rules["amount_threshold"]
        
        if not rule_config["enabled"]:
            return RuleResult("amount_threshold", True, RiskLevel.LOW, "规则已禁用")
            
        max_amount = rule_config["max_single_amount"]
        debit_amount = record.get("借方金额", 0)
        credit_amount = record.get("贷方金额", 0)
        max_record_amount = max(debit_amount, credit_amount)
        
        if max_record_amount > max_amount:
            return RuleResult(
                "amount_threshold",
                False,
                RiskLevel[rule_config["risk_level"].upper()],
                f"单笔金额 {max_record_amount:.2f} 超过阈值 {max_amount:.2f}",
                {"amount": max_record_amount, "threshold": max_amount}
            )
            
        return RuleResult("amount_threshold", True, RiskLevel.LOW, "金额检查通过")
        
    def check_account_validation(self, record: pd.Series, **kwargs) -> RuleResult:
        """检查科目有效性"""
        rule_config = self.rules["account_validation"]
        
        if not rule_config["enabled"]:
            return RuleResult("account_validation", True, RiskLevel.LOW, "规则已禁用")
            
        account = record.get("科目", "")
        
        # 检查禁用科目
        forbidden_accounts = rule_config["forbidden_accounts"]
        if account in forbidden_accounts:
            return RuleResult(
                "account_validation",
                False,
                RiskLevel[rule_config["risk_level"].upper()],
                f"科目 '{account}' 在禁用列表中",
                {"account": account, "forbidden_accounts": forbidden_accounts}
            )
            
        # 检查科目模式
        patterns = rule_config.get("account_patterns", {})
        
        # 检查允许的模式
        if "allowed" in patterns:
            allowed_patterns = patterns["allowed"]
            if not any(re.match(pattern, str(account)) for pattern in allowed_patterns):
                return RuleResult(
                    "account_validation",
                    False,
                    RiskLevel[rule_config["risk_level"].upper()],
                    f"科目 '{account}' 不符合允许的模式",
                    {"account": account, "allowed_patterns": allowed_patterns}
                )
                
        # 检查禁止的模式
        if "forbidden" in patterns:
            forbidden_patterns = patterns["forbidden"]
            if any(re.match(pattern, str(account)) for pattern in forbidden_patterns):
                return RuleResult(
                    "account_validation",
                    False,
                    RiskLevel[rule_config["risk_level"].upper()],
                    f"科目 '{account}' 符合禁止的模式",
                    {"account": account, "forbidden_patterns": forbidden_patterns}
                )
                
        return RuleResult("account_validation", True, RiskLevel.LOW, "科目验证通过")
        
    def check_balance(self, record: pd.Series, **kwargs) -> RuleResult:
        """检查借贷平衡"""
        rule_config = self.rules["balance_check"]
        
        if not rule_config["enabled"]:
            return RuleResult("balance_check", True, RiskLevel.LOW, "规则已禁用")
            
        debit_amount = float(record.get("借方金额", 0))
        credit_amount = float(record.get("贷方金额", 0))
        tolerance = rule_config["tolerance"]
        
        # 对于单条记录，检查是否有且只有一个方向有金额
        if abs(debit_amount) > tolerance and abs(credit_amount) > tolerance:
            return RuleResult(
                "balance_check",
                False,
                RiskLevel[rule_config["risk_level"].upper()],
                f"借贷双方都有金额：借方 {debit_amount:.2f}，贷方 {credit_amount:.2f}",
                {"debit": debit_amount, "credit": credit_amount}
            )
            
        # 检查金额是否为负数
        if debit_amount < -tolerance or credit_amount < -tolerance:
            return RuleResult(
                "balance_check",
                False,
                RiskLevel[rule_config["risk_level"].upper()],
                f"发现负金额：借方 {debit_amount:.2f}，贷方 {credit_amount:.2f}",
                {"debit": debit_amount, "credit": credit_amount}
            )
            
        return RuleResult("balance_check", True, RiskLevel.LOW, "借贷平衡检查通过")
        
    def check_duplicate(self, record: pd.Series, all_records: pd.DataFrame = None, **kwargs) -> RuleResult:
        """检查重复交易"""
        rule_config = self.rules["duplicate_check"]
        
        if not rule_config["enabled"] or all_records is None:
            return RuleResult("duplicate_check", True, RiskLevel.LOW, "规则已禁用或无数据")
            
        check_fields = rule_config["check_fields"]
        time_window_days = rule_config["time_window_days"]
        
        # 构建检查条件
        conditions = []
        for field in check_fields:
            if field in record and field in all_records.columns:
                conditions.append(all_records[field] == record[field])
            else:
                conditions.append(pd.Series([True] * len(all_records)))
                
        # 组合条件
        mask = pd.Series([True] * len(all_records))
        for condition in conditions:
            mask = mask & condition
            
        # 检查时间窗口
        if "日期" in record and "日期" in all_records.columns:
            record_date = pd.to_datetime(record["日期"])
            time_window = timedelta(days=time_window_days)
            date_mask = (all_records["日期"] >= record_date - time_window) & \
                       (all_records["日期"] <= record_date + time_window)
            mask = mask & date_mask
            
        # 排除当前记录
        if hasattr(record, 'name') and record.name is not None:
            mask = mask & (all_records.index != record.name)
            
        duplicate_count = mask.sum()
        
        if duplicate_count > 0:
            return RuleResult(
                "duplicate_check",
                False,
                RiskLevel[rule_config["risk_level"].upper()],
                f"发现 {duplicate_count} 条重复交易",
                {"duplicate_count": duplicate_count, "time_window": time_window_days}
            )
            
        return RuleResult("duplicate_check", True, RiskLevel.LOW, "重复检查通过")
        
    def check_date_validation(self, record: pd.Series, **kwargs) -> RuleResult:
        """检查日期有效性"""
        rule_config = self.rules["date_validation"]
        
        if not rule_config["enabled"]:
            return RuleResult("date_validation", True, RiskLevel.LOW, "规则已禁用")
            
        date_value = record.get("日期")
        if pd.isna(date_value):
            return RuleResult(
                "date_validation",
                False,
                RiskLevel[rule_config["risk_level"].upper()],
                "日期为空",
                {"date": date_value}
            )
            
        try:
            record_date = pd.to_datetime(date_value)
        except:
            return RuleResult(
                "date_validation",
                False,
                RiskLevel[rule_config["risk_level"].upper()],
                f"日期格式无效: {date_value}",
                {"date": date_value}
            )
            
        # 检查最小日期
        min_date = rule_config.get("min_date")
        if min_date:
            min_date = pd.to_datetime(min_date)
            if record_date < min_date:
                return RuleResult(
                    "date_validation",
                    False,
                    RiskLevel[rule_config["risk_level"].upper()],
                    f"日期 {record_date.date()} 早于最小日期 {min_date.date()}",
                    {"date": record_date, "min_date": min_date}
                )
                
        # 检查最大日期
        max_date = rule_config.get("max_date")
        if max_date:
            max_date = pd.to_datetime(max_date)
            if record_date > max_date:
                return RuleResult(
                    "date_validation",
                    False,
                    RiskLevel[rule_config["risk_level"].upper()],
                    f"日期 {record_date.date()} 晚于最大日期 {max_date.date()}",
                    {"date": record_date, "max_date": max_date}
                )
                
        # 检查未来日期
        if rule_config.get("future_date_check", True):
            today = pd.to_datetime(datetime.now().date())
            if record_date > today:
                return RuleResult(
                    "date_validation",
                    False,
                    RiskLevel[rule_config["risk_level"].upper()],
                    f"日期 {record_date.date()} 是未来日期",
                    {"date": record_date, "today": today}
                )
                
        return RuleResult("date_validation", True, RiskLevel.LOW, "日期验证通过")
        
    def check_single_record(self, record: pd.Series, all_records: pd.DataFrame = None) -> AuditResult:
        """
        检查单条记录
        
        Args:
            record: 单条记录
            all_records: 所有记录（用于重复检查等）
            
        Returns:
            审核结果
        """
        rule_results = []
        failed_rules = []
        suggestions = []
        
        # 执行内置规则
        rule_methods = {
            "amount_threshold": self.check_amount_threshold,
            "account_validation": self.check_account_validation,
            "balance_check": self.check_balance,
            "duplicate_check": self.check_duplicate,
            "date_validation": self.check_date_validation
        }
        
        for rule_name, rule_method in rule_methods.items():
            try:
                if rule_name == "duplicate_check":
                    result = rule_method(record, all_records=all_records)
                else:
                    result = rule_method(record)
                    
                rule_results.append(result)
                
                if not result.passed:
                    failed_rules.append(result)
                    suggestions.append(f"请检查{result.message}")
                    
                # 更新统计信息
                if rule_name not in self.rule_statistics:
                    self.rule_statistics[rule_name] = {"total": 0, "failed": 0}
                self.rule_statistics[rule_name]["total"] += 1
                if not result.passed:
                    self.rule_statistics[rule_name]["failed"] += 1
                    
            except Exception as e:
                logger.error(f"规则 {rule_name} 执行失败: {e}")
                error_result = RuleResult(
                    rule_name,
                    False,
                    RiskLevel.HIGH,
                    f"规则执行异常: {str(e)}"
                )
                rule_results.append(error_result)
                failed_rules.append(error_result)
                
        # 执行自定义规则
        for rule_name, rule_config in self.custom_rules.items():
            try:
                result = rule_config["function"](record)
                
                # 如果自定义规则没有返回RuleResult，包装一下
                if not isinstance(result, RuleResult):
                    if isinstance(result, bool):
                        result = RuleResult(
                            rule_name,
                            result,
                            rule_config["risk_level"],
                            rule_config["message"] if not result else "检查通过"
                        )
                    else:
                        result = RuleResult(
                            rule_name,
                            False,
                            rule_config["risk_level"],
                            str(result)
                        )
                        
                rule_results.append(result)
                
                if not result.passed:
                    failed_rules.append(result)
                    suggestions.append(f"请检查{result.message}")
                    
            except Exception as e:
                logger.error(f"自定义规则 {rule_name} 执行失败: {e}")
                error_result = RuleResult(
                    rule_name,
                    False,
                    RiskLevel.HIGH,
                    f"自定义规则执行异常: {str(e)}"
                )
                rule_results.append(error_result)
                failed_rules.append(error_result)
                
        # 确定整体结果和风险等级
        passed = len(failed_rules) == 0
        risk_level = RiskLevel.LOW
        
        if failed_rules:
            # 取最高的风险等级
            risk_levels = [rule.risk_level for rule in failed_rules]
            risk_order = [RiskLevel.CRITICAL, RiskLevel.HIGH, RiskLevel.MEDIUM, RiskLevel.LOW]
            for level in risk_order:
                if level in risk_levels:
                    risk_level = level
                    break
                    
        # 生成摘要
        if passed:
            summary = "所有规则检查通过"
        else:
            failed_rule_names = [rule.rule_name for rule in failed_rules]
            summary = f"以下规则检查失败: {', '.join(failed_rule_names)}"
            
        # 获取记录ID
        record_id = record.get("凭证号", record.name if hasattr(record, 'name') else len(record))
        
        return AuditResult(
            record_id=record_id,
            passed=passed,
            risk_level=risk_level,
            rule_results=rule_results,
            summary=summary,
            suggestions=suggestions
        )
        
    def check_batch_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        批量检查记录
        
        Args:
            df: 账务数据DataFrame
            
        Returns:
            添加审核结果的DataFrame
        """
        results = []
        
        for idx, record in df.iterrows():
            audit_result = self.check_single_record(record, all_records=df)
            results.append(audit_result)
            
        # 将结果添加到原DataFrame
        df["审核结果"] = results
        df["审核通过"] = [result.passed for result in results]
        df["风险等级"] = [result.risk_level.value for result in results]
        df["审核摘要"] = [result.summary for result in results]
        
        return df
        
    def get_rule_statistics(self) -> Dict[str, Any]:
        """获取规则执行统计"""
        stats = {}
        for rule_name, rule_stats in self.rule_statistics.items():
            total = rule_stats["total"]
            failed = rule_stats["failed"]
            stats[rule_name] = {
                "total": total,
                "failed": failed,
                "success_rate": (total - failed) / total if total > 0 else 0
            }
        return stats
        
    def update_rule_config(self, rule_name: str, config: Dict[str, Any]):
        """更新规则配置"""
        if rule_name in self.rules:
            self.rules[rule_name].update(config)
            logger.info(f"规则 '{rule_name}' 配置已更新")
        else:
            logger.warning(f"规则 '{rule_name}' 不存在")
            
    def get_rule_config(self, rule_name: str = None) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        """获取规则配置"""
        if rule_name:
            return self.rules.get(rule_name, {})
        return self.rules.copy()


# 技能函数接口
def rule_check_skill(data: pd.DataFrame, rules: Dict[str, Any] = None) -> pd.DataFrame:
    """
    规则检查技能接口
    
    Args:
        data: 账务数据DataFrame
        rules: 自定义规则配置
        
    Returns:
        添加审核结果的DataFrame
    """
    engine = AccountRuleEngine(rules)
    return engine.check_batch_records(data)
