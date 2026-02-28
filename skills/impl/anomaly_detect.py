"""
异常检测技能实现
基于统计学和机器学习算法识别账务数据中的异常模式
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
import warnings

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)


class AnomalyType(Enum):
    """异常类型枚举"""
    STATISTICAL_OUTLIER = "statistical_outlier"    # 统计离群值
    TEMPORAL_ANOMALY = "temporal_anomaly"          # 时间异常
    AMOUNT_ANOMALY = "amount_anomaly"              # 金额异常
    FREQUENCY_ANOMALY = "frequency_anomaly"        # 频率异常
    PATTERN_ANOMALY = "pattern_anomaly"            # 模式异常
    CLUSTER_ANOMALY = "cluster_anomaly"            # 聚类异常


class AnomalySeverity(Enum):
    """异常严重程度"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AnomalyResult:
    """异常检测结果"""
    record_id: Union[int, str]
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    score: float                    # 异常分数 (0-1)
    description: str                # 异常描述
    details: Dict[str, Any]         # 详细信息
    confidence: float               # 置信度 (0-1)
    suggested_action: str           # 建议处理动作


@dataclass
class AnomalyReport:
    """异常检测报告"""
    total_records: int
    anomaly_count: int
    anomaly_rate: float
    anomalies: List[AnomalyResult]
    summary: Dict[str, Any]
    detection_time: datetime


class StatisticalDetector:
    """统计异常检测器"""
    
    def __init__(self, method: str = "zscore", threshold: float = 2.0):
        """
        初始化统计检测器
        
        Args:
            method: 检测方法 ("zscore", "iqr", "modified_zscore")
            threshold: 异常阈值
        """
        self.method = method
        self.threshold = threshold
        
    def detect_outliers(self, data: pd.Series) -> np.ndarray:
        """
        检测离群值
        
        Args:
            data: 数据序列
            
        Returns:
            异常标记数组 (True表示异常)
        """
        if len(data) < 3:
            return np.zeros(len(data), dtype=bool)
            
        data_clean = data.dropna()
        if len(data_clean) < 3:
            return np.zeros(len(data), dtype=bool)
            
        if self.method == "zscore":
            return self._zscore_detection(data_clean)
        elif self.method == "iqr":
            return self._iqr_detection(data_clean)
        elif self.method == "modified_zscore":
            return self._modified_zscore_detection(data_clean)
        else:
            raise ValueError(f"不支持的检测方法: {self.method}")
            
    def _zscore_detection(self, data: pd.Series) -> np.ndarray:
        """Z-score检测"""
        z_scores = np.abs((data - data.mean()) / data.std())
        return z_scores > self.threshold
        
    def _iqr_detection(self, data: pd.Series) -> np.ndarray:
        """四分位距检测"""
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return (data < lower_bound) | (data > upper_bound)
        
    def _modified_zscore_detection(self, data: pd.Series) -> np.ndarray:
        """修正Z-score检测（基于中位数）"""
        median = np.median(data)
        mad = np.median(np.abs(data - median))
        modified_z_scores = 0.6745 * (data - median) / mad
        return np.abs(modified_z_scores) > self.threshold


class TemporalDetector:
    """时间异常检测器"""
    
    def __init__(self, window_size: int = 7):
        """
        初始化时间检测器
        
        Args:
            window_size: 时间窗口大小（天）
        """
        self.window_size = window_size
        
    def detect_temporal_anomalies(self, df: pd.DataFrame, date_col: str = "日期") -> List[AnomalyResult]:
        """
        检测时间异常
        
        Args:
            df: 账务数据
            date_col: 日期列名
            
        Returns:
            异常结果列表
        """
        anomalies = []
        
        if date_col not in df.columns:
            return anomalies
            
        # 确保日期格式正确
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col)
        
        # 检测周末/节假日交易
        weekend_anomalies = self._detect_weekend_transactions(df, date_col)
        anomalies.extend(weekend_anomalies)
        
        # 检测异常时间段交易
        time_anomalies = self._detect_abnormal_time_patterns(df, date_col)
        anomalies.extend(time_anomalies)
        
        # 检测交易频率异常
        frequency_anomalies = self._detect_frequency_anomalies(df, date_col)
        anomalies.extend(frequency_anomalies)
        
        return anomalies
        
    def _detect_weekend_transactions(self, df: pd.DataFrame, date_col: str) -> List[AnomalyResult]:
        """检测周末交易"""
        anomalies = []
        
        for idx, row in df.iterrows():
            transaction_date = row[date_col]
            if transaction_date.dayofweek >= 5:  # 周六、周日
                severity = AnomalySeverity.MEDIUM
                if transaction_date.dayofweek == 6:  # 周日
                    severity = AnomalySeverity.HIGH
                    
                anomaly = AnomalyResult(
                    record_id=idx,
                    anomaly_type=AnomalyType.TEMPORAL_ANOMALY,
                    severity=severity,
                    score=0.7,
                    description=f"周末交易: {transaction_date.strftime('%Y-%m-%d %A')}",
                    details={"date": transaction_date, "weekday": transaction_date.strftime('%A')},
                    confidence=0.9,
                    suggested_action="核实周末交易的必要性"
                )
                anomalies.append(anomaly)
                
        return anomalies
        
    def _detect_abnormal_time_patterns(self, df: pd.DataFrame, date_col: str) -> List[AnomalyResult]:
        """检测异常时间模式"""
        anomalies = []
        
        # 按日期分组统计交易次数
        daily_counts = df.groupby(date_col).size()
        
        # 使用统计方法检测异常日期
        detector = StatisticalDetector(method="iqr", threshold=1.5)
        outlier_mask = detector.detect_outliers(daily_counts)
        
        for date, is_outlier in daily_counts.items():
            if is_outlier:
                count = daily_counts[date]
                anomaly = AnomalyResult(
                    record_id=f"date_{date.strftime('%Y%m%d')}",
                    anomaly_type=AnomalyType.FREQUENCY_ANOMALY,
                    severity=AnomalySeverity.MEDIUM,
                    score=0.6,
                    description=f"异常交易频率: {date.strftime('%Y-%m-%d')} 有 {count} 笔交易",
                    details={"date": date, "transaction_count": count},
                    confidence=0.8,
                    suggested_action="检查该日期的批量交易"
                )
                anomalies.append(anomaly)
                
        return anomalies
        
    def _detect_frequency_anomalies(self, df: pd.DataFrame, date_col: str) -> List[AnomalyResult]:
        """检测频率异常"""
        anomalies = []
        
        # 检测同一账户在短时间内的多次交易
        if "科目" in df.columns:
            for account in df["科目"].unique():
                account_data = df[df["科目"] == account].copy()
                account_data = account_data.sort_values(date_col)
                
                # 检测1小时内的多次交易
                for i in range(1, len(account_data)):
                    time_diff = account_data.iloc[i][date_col] - account_data.iloc[i-1][date_col]
                    if time_diff.total_seconds() < 3600:  # 1小时内
                        anomaly = AnomalyResult(
                            record_id=account_data.iloc[i].name,
                            anomaly_type=AnomalyType.FREQUENCY_ANOMALY,
                            severity=AnomalySeverity.LOW,
                            score=0.5,
                            description=f"高频交易: 科目 '{account}' 在短时间内有多次交易",
                            details={"account": account, "time_diff": str(time_diff)},
                            confidence=0.7,
                            suggested_action="核实高频交易的合理性"
                        )
                        anomalies.append(anomaly)
                        
        return anomalies


class AmountDetector:
    """金额异常检测器"""
    
    def __init__(self, threshold: float = 2.0):
        """
        初始化金额检测器
        
        Args:
            threshold: 异常阈值
        """
        self.threshold = threshold
        
    def detect_amount_anomalies(self, df: pd.DataFrame) -> List[AnomalyResult]:
        """
        检测金额异常
        
        Args:
            df: 账务数据
            
        Returns:
            异常结果列表
        """
        anomalies = []
        
        # 检测借方金额异常
        if "借方金额" in df.columns:
            debit_anomalies = self._detect_amount_outliers(df, "借方金额")
            anomalies.extend(debit_anomalies)
            
        # 检测贷方金额异常
        if "贷方金额" in df.columns:
            credit_anomalies = self._detect_amount_outliers(df, "贷方金额")
            anomalies.extend(credit_anomalies)
            
        # 检测整数金额异常（可能的整数化）
        integer_anomalies = self._detect_integer_amounts(df)
        anomalies.extend(integer_anomalies)
        
        # 检测金额模式异常
        pattern_anomalies = self._detect_amount_patterns(df)
        anomalies.extend(pattern_anomalies)
        
        return anomalies
        
    def _detect_amount_outliers(self, df: pd.DataFrame, amount_col: str) -> List[AnomalyResult]:
        """检测金额离群值"""
        anomalies = []
        
        amounts = df[amount_col]
        amounts_positive = amounts[amounts > 0]
        
        if len(amounts_positive) < 3:
            return anomalies
            
        detector = StatisticalDetector(method="iqr", threshold=self.threshold)
        outlier_mask = detector.detect_outliers(amounts_positive)
        
        for idx, is_outlier in amounts_positive.items():
            if is_outlier:
                amount = amounts_positive[idx]
                severity = AnomalySeverity.HIGH if amount > amounts_positive.quantile(0.95) else AnomalySeverity.MEDIUM
                
                anomaly = AnomalyResult(
                    record_id=idx,
                    anomaly_type=AnomalyType.AMOUNT_ANOMALY,
                    severity=severity,
                    score=0.8,
                    description=f"金额异常: {amount_col} {amount:.2f} 显著偏离正常范围",
                    details={"amount": amount, "field": amount_col, "percentile": (amounts_positive < amount).mean()},
                    confidence=0.85,
                    suggested_action="核实大额交易的真实性和合规性"
                )
                anomalies.append(anomaly)
                
        return anomalies
        
    def _detect_integer_amounts(self, df: pd.DataFrame) -> List[AnomalyResult]:
        """检测整数金额异常"""
        anomalies = []
        
        amount_cols = ["借方金额", "贷方金额"]
        for col in amount_cols:
            if col in df.columns:
                amounts = df[col][df[col] > 0]
                integer_amounts = amounts[amounts == amounts.astype(int)]
                
                if len(integer_amounts) > 0:
                    integer_ratio = len(integer_amounts) / len(amounts)
                    if integer_ratio > 0.8:  # 80%以上是整数
                        for idx, amount in integer_amounts.items():
                            if amount > 10000:  # 大额整数
                                anomaly = AnomalyResult(
                                    record_id=idx,
                                    anomaly_type=AnomalyType.PATTERN_ANOMALY,
                                    severity=AnomalySeverity.LOW,
                                    score=0.4,
                                    description=f"大额整数金额: {col} {amount:.0f}",
                                    details={"amount": amount, "field": col, "integer_ratio": integer_ratio},
                                    confidence=0.6,
                                    suggested_action="检查整数金额是否经过合理计算"
                                )
                                anomalies.append(anomaly)
                                
        return anomalies
        
    def _detect_amount_patterns(self, df: pd.DataFrame) -> List[AnomalyResult]:
        """检测金额模式异常"""
        anomalies = []
        
        # 检测重复金额
        amount_cols = ["借方金额", "贷方金额"]
        for col in amount_cols:
            if col in df.columns:
                amounts = df[col][df[col] > 0]
                amount_counts = amounts.value_counts()
                
                # 检测高频重复金额
                for amount, count in amount_counts.items():
                    if count > 5 and amount > 1000:  # 重复5次以上且金额较大
                        indices = amounts[amounts == amount].index
                        for idx in indices:
                            anomaly = AnomalyResult(
                                record_id=idx,
                                anomaly_type=AnomalyType.PATTERN_ANOMALY,
                                severity=AnomalySeverity.LOW,
                                score=0.3,
                                description=f"重复金额模式: {col} {amount:.2f} 出现 {count} 次",
                                details={"amount": amount, "field": col, "frequency": count},
                                confidence=0.7,
                                suggested_action="检查重复金额的交易是否合理"
                            )
                            anomalies.append(anomaly)
                            
        return anomalies


class MLDetector:
    """机器学习异常检测器"""
    
    def __init__(self, method: str = "isolation_forest", contamination: float = 0.1):
        """
        初始化ML检测器
        
        Args:
            method: 检测方法 ("isolation_forest", "dbscan")
            contamination: 异常比例估计
        """
        self.method = method
        self.contamination = contamination
        self.scaler = StandardScaler()
        self.model = None
        
    def fit_predict(self, df: pd.DataFrame) -> np.ndarray:
        """
        训练并预测异常
        
        Args:
            df: 账务数据
            
        Returns:
            异常标记数组 (-1表示异常，1表示正常)
        """
        # 特征工程
        features = self._extract_features(df)
        
        if features is None or len(features) < 2:
            return np.ones(len(df))  # 数据不足，全部标记为正常
            
        # 标准化
        features_scaled = self.scaler.fit_transform(features)
        
        # 训练模型
        if self.method == "isolation_forest":
            self.model = IsolationForest(contamination=self.contamination, random_state=42)
        elif self.method == "dbscan":
            self.model = DBSCAN(eps=0.5, min_samples=5)
        else:
            raise ValueError(f"不支持的ML方法: {self.method}")
            
        # 预测
        predictions = self.model.fit_predict(features_scaled)
        return predictions
        
    def _extract_features(self, df: pd.DataFrame) -> Optional[np.ndarray]:
        """提取特征"""
        features_list = []
        
        # 金额特征
        if "借方金额" in df.columns:
            features_list.append(df["借方金额"].fillna(0))
        if "贷方金额" in df.columns:
            features_list.append(df["贷方金额"].fillna(0))
            
        # 时间特征
        if "日期" in df.columns:
            dates = pd.to_datetime(df["日期"])
            features_list.append(dates.dt.dayofweek)
            features_list.append(dates.dt.day)
            features_list.append(dates.dt.month)
            
        # 科目特征（编码）
        if "科目" in df.columns:
            account_codes = pd.factorize(df["科目"])[0]
            features_list.append(account_codes)
            
        if not features_list:
            return None
            
        return np.column_stack(features_list)


class AnomalyDetector:
    """异常检测主类"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化异常检测器
        
        Args:
            config: 检测配置
        """
        self.config = config or self._get_default_config()
        
        # 初始化各个检测器
        self.statistical_detector = StatisticalDetector(
            method=self.config.get("statistical_method", "zscore"),
            threshold=self.config.get("statistical_threshold", 2.0)
        )
        
        self.temporal_detector = TemporalDetector(
            window_size=self.config.get("temporal_window", 7)
        )
        
        self.amount_detector = AmountDetector(
            threshold=self.config.get("amount_threshold", 2.0)
        )
        
        self.ml_detector = MLDetector(
            method=self.config.get("ml_method", "isolation_forest"),
            contamination=self.config.get("ml_contamination", 0.1)
        )
        
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "statistical_method": "zscore",
            "statistical_threshold": 2.0,
            "temporal_window": 7,
            "amount_threshold": 2.0,
            "ml_method": "isolation_forest",
            "ml_contamination": 0.1,
            "enable_statistical": True,
            "enable_temporal": True,
            "enable_amount": True,
            "enable_ml": True
        }
        
    def detect_anomalies(self, df: pd.DataFrame) -> AnomalyReport:
        """
        执行异常检测
        
        Args:
            df: 账务数据
            
        Returns:
            异常检测报告
        """
        start_time = datetime.now()
        all_anomalies = []
        
        # 统计异常检测
        if self.config.get("enable_statistical", True):
            statistical_anomalies = self._detect_statistical_anomalies(df)
            all_anomalies.extend(statistical_anomalies)
            
        # 时间异常检测
        if self.config.get("enable_temporal", True):
            temporal_anomalies = self.temporal_detector.detect_temporal_anomalies(df)
            all_anomalies.extend(temporal_anomalies)
            
        # 金额异常检测
        if self.config.get("enable_amount", True):
            amount_anomalies = self.amount_detector.detect_amount_anomalies(df)
            all_anomalies.extend(amount_anomalies)
            
        # 机器学习异常检测
        if self.config.get("enable_ml", True):
            ml_anomalies = self._detect_ml_anomalies(df)
            all_anomalies.extend(ml_anomalies)
            
        # 去重和汇总
        unique_anomalies = self._deduplicate_anomalies(all_anomalies)
        
        # 生成报告
        report = AnomalyReport(
            total_records=len(df),
            anomaly_count=len(unique_anomalies),
            anomaly_rate=len(unique_anomalies) / len(df) if len(df) > 0 else 0,
            anomalies=unique_anomalies,
            summary=self._generate_summary(unique_anomalies),
            detection_time=datetime.now()
        )
        
        return report
        
    def _detect_statistical_anomalies(self, df: pd.DataFrame) -> List[AnomalyResult]:
        """检测统计异常"""
        anomalies = []
        
        # 对数值列进行统计异常检测
        numeric_cols = ["借方金额", "贷方金额"]
        for col in numeric_cols:
            if col in df.columns:
                data = df[col][df[col] > 0]  # 只检测正数
                if len(data) >= 3:
                    outlier_mask = self.statistical_detector.detect_outliers(data)
                    
                    for idx, is_outlier in data.items():
                        if is_outlier:
                            anomaly = AnomalyResult(
                                record_id=idx,
                                anomaly_type=AnomalyType.STATISTICAL_OUTLIER,
                                severity=AnomalySeverity.MEDIUM,
                                score=0.7,
                                description=f"统计离群值: {col} {data[idx]:.2f}",
                                details={"field": col, "value": data[idx], "method": self.statistical_detector.method},
                                confidence=0.8,
                                suggested_action="检查该数值是否合理"
                            )
                            anomalies.append(anomaly)
                            
        return anomalies
        
    def _detect_ml_anomalies(self, df: pd.DataFrame) -> List[AnomalyResult]:
        """检测机器学习异常"""
        anomalies = []
        
        try:
            predictions = self.ml_detector.fit_predict(df)
            
            for idx, prediction in enumerate(predictions):
                if prediction == -1:  # 异常
                    anomaly = AnomalyResult(
                        record_id=idx,
                        anomaly_type=AnomalyType.CLUSTER_ANOMALY,
                        severity=AnomalySeverity.MEDIUM,
                        score=0.6,
                        description=f"机器学习检测异常: {self.ml_detector.method}",
                        details={"method": self.ml_detector.method, "prediction": prediction},
                        confidence=0.7,
                        suggested_action="综合检查该记录的多个特征"
                    )
                    anomalies.append(anomaly)
                    
        except Exception as e:
            logger.warning(f"机器学习异常检测失败: {e}")
            
        return anomalies
        
    def _deduplicate_anomalies(self, anomalies: List[AnomalyResult]) -> List[AnomalyResult]:
        """去除重复异常"""
        unique_anomalies = []
        seen = set()
        
        for anomaly in anomalies:
            # 创建唯一标识
            identifier = (anomaly.record_id, anomaly.anomaly_type)
            
            if identifier not in seen:
                seen.add(identifier)
                unique_anomalies.append(anomaly)
            else:
                # 如果已存在，更新严重程度（取最高）
                for existing in unique_anomalies:
                    if existing.record_id == anomaly.record_id and existing.anomaly_type == anomaly.anomaly_type:
                        if anomaly.severity.value > existing.severity.value:
                            existing.severity = anomaly.severity
                        break
                        
        return unique_anomalies
        
    def _generate_summary(self, anomalies: List[AnomalyResult]) -> Dict[str, Any]:
        """生成异常摘要"""
        summary = {
            "by_type": {},
            "by_severity": {},
            "top_anomalies": []
        }
        
        # 按类型统计
        for anomaly in anomalies:
            anomaly_type = anomaly.anomaly_type.value
            summary["by_type"][anomaly_type] = summary["by_type"].get(anomaly_type, 0) + 1
            
        # 按严重程度统计
        for anomaly in anomalies:
            severity = anomaly.severity.value
            summary["by_severity"][severity] = summary["by_severity"].get(severity, 0) + 1
            
        # 获取最高分的异常
        sorted_anomalies = sorted(anomalies, key=lambda x: x.score, reverse=True)
        summary["top_anomalies"] = [
            {
                "record_id": a.record_id,
                "type": a.anomaly_type.value,
                "severity": a.severity.value,
                "score": a.score,
                "description": a.description
            }
            for a in sorted_anomalies[:10]
        ]
        
        return summary


# 技能函数接口
def anomaly_detect_skill(data: pd.DataFrame, config: Dict[str, Any] = None) -> AnomalyReport:
    """
    异常检测技能接口
    
    Args:
        data: 账务数据DataFrame
        config: 检测配置
        
    Returns:
        异常检测报告
    """
    detector = AnomalyDetector(config)
    return detector.detect_anomalies(data)
