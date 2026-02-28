"""
LLM解释技能实现
使用大语言模型为账务审核结果生成智能解释和建议
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import json
import re

# LangChain imports for LLM integration
try:
    from langchain.llms import OpenAI
    from langchain.chat_models import ChatOpenAI
    from langchain.prompts import PromptTemplate, ChatPromptTemplate
    from langchain.chains import LLMChain
    from langchain.schema import HumanMessage, SystemMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    logging.warning("LangChain not available. LLM functionality will be limited.")

logger = logging.getLogger(__name__)


class ExplanationType(Enum):
    """解释类型枚举"""
    RULE_VIOLATION = "rule_violation"        # 规则违规解释
    ANOMALY_ANALYSIS = "anomaly_analysis"    # 异常分析解释
    RISK_ASSESSMENT = "risk_assessment"      # 风险评估解释
    COMPLIANCE_GUIDANCE = "compliance_guidance" # 合规指导解释
    REMEDIATION_SUGGESTION = "remediation_suggestion" # 整改建议解释


class LLMProvider(Enum):
    """LLM提供商枚举"""
    OPENAI = "openai"
    AZURE = "azure"
    LOCAL = "local"
    MOCK = "mock"  # 用于测试


@dataclass
class ExplanationResult:
    """解释结果"""
    explanation_type: ExplanationType
    content: str                    # 解释内容
    confidence: float               # 置信度 (0-1)
    suggestions: List[str]          # 建议列表
    risk_level: str                 # 风险等级
    references: List[str]           # 参考法规或条款
    processing_time: float          # 处理时间（秒）
    token_usage: Optional[Dict[str, int]] = None  # Token使用情况


@dataclass
class ExplanationRequest:
    """解释请求"""
    request_type: ExplanationType
    context: Dict[str, Any]        # 上下文信息
    data: Union[pd.Series, Dict]   # 相关数据
    priority: str = "normal"       # 优先级
    max_tokens: int = 1000         # 最大Token数


class PromptManager:
    """提示词管理器"""
    
    def __init__(self):
        self.templates = self._initialize_templates()
        
    def _initialize_templates(self) -> Dict[str, Any]:
        """初始化提示词模板"""
        return {
            "rule_violation": {
                "system": """你是一名专业的财务审计专家，具有丰富的会计准则和法规知识。
你的任务是为账务审核中的规则违规提供专业、准确的解释和建议。

请遵循以下原则：
1. 基于会计准则和相关法规进行解释
2. 提供具体的违规原因和影响
3. 给出切实可行的整改建议
4. 语言要专业、准确、易懂
5. 引用相关的会计准则或法规条款""",
                
                "human": """请分析以下账务规则违规情况：

违规信息：
- 规则名称：{rule_name}
- 违规描述：{violation_description}
- 涉及金额：{amount}
- 交易日期：{date}
- 科目：{account}
- 摘要：{description}

请提供：
1. 违规原因分析
2. 潜在风险影响
3. 整改建议
4. 预防措施"""
            },
            
            "anomaly_analysis": {
                "system": """你是一名专业的财务数据分析专家，擅长识别和分析财务数据中的异常模式。
你的任务是为检测到的异常提供深入的分析和解释。

请遵循以下原则：
1. 基于数据特征进行科学分析
2. 考虑业务背景和合理性
3. 识别可能的根本原因
4. 评估异常的严重程度
5. 提供针对性的调查建议""",
                
                "human": """请分析以下财务数据异常：

异常信息：
- 异常类型：{anomaly_type}
- 异常描述：{anomaly_description}
- 异常分数：{anomaly_score}
- 涉及数据：{data_context}
- 检测方法：{detection_method}

请提供：
1. 异常特征分析
2. 可能的业务原因
3. 风险等级评估
4. 调查和验证建议"""
            },
            
            "risk_assessment": {
                "system": """你是一名专业的财务风险管理专家，具有丰富的风险评估经验。
你的任务是为财务风险提供全面的评估和分析。

请遵循以下原则：
1. 采用系统性的风险评估方法
2. 考虑财务、合规、操作等多维度风险
3. 评估风险的可能性和影响程度
4. 提供风险缓释措施
5. 建议监控和预警机制""",
                
                "human": """请评估以下财务风险：

风险信息：
- 风险类型：{risk_type}
- 风险描述：{risk_description}
- 涉及金额：{amount}
- 风险等级：{risk_level}
- 相关数据：{context_data}

请提供：
1. 风险影响分析
2. 风险等级评估
3. 可能的损失范围
4. 风险缓释措施
5. 长期监控建议"""
            },
            
            "compliance_guidance": {
                "system": """你是一名专业的财务合规专家，精通会计准则、税法和相关法规。
你的任务是为合规问题提供专业的指导和解释。

请遵循以下原则：
1. 基于最新的会计准则和法规
2. 提供准确的合规要求
3. 解释违规的法律后果
4. 给出合规整改方案
5. 建议长效合规机制""",
                
                "human": """请提供以下合规问题的指导：

合规信息：
- 合规问题：{compliance_issue}
- 涉及法规：{regulation}
- 违规情节：{violation_details}
- 影响范围：{impact_scope}
- 历史情况：{history_context}

请提供：
1. 相关法规要求
2. 违规性质认定
3. 法律后果分析
4. 整改方案建议
5. 预防措施"""
            }
        }
    
    def get_prompt(self, explanation_type: str, context: Dict[str, Any]) -> tuple:
        """获取提示词"""
        if explanation_type not in self.templates:
            raise ValueError(f"不支持的解释类型: {explanation_type}")
            
        template = self.templates[explanation_type]
        system_prompt = template["system"]
        human_prompt = template["human"].format(**context)
        
        return system_prompt, human_prompt


class MockLLM:
    """模拟LLM类，用于测试"""
    
    def __init__(self, **kwargs):
        self.model_name = kwargs.get("model_name", "mock-model")
        
    def __call__(self, prompt: str) -> str:
        """模拟LLM响应"""
        return f"这是模拟LLM的响应。输入提示词长度: {len(prompt)} 字符。"


class LLMExplainer:
    """LLM解释器主类"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化LLM解释器
        
        Args:
            config: 配置参数
        """
        self.config = config or self._get_default_config()
        self.prompt_manager = PromptManager()
        self.llm = self._initialize_llm()
        self.usage_statistics = {
            "total_requests": 0,
            "total_tokens": 0,
            "total_processing_time": 0
        }
        
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "provider": "openai",
            "model": "gpt-3.5-turbo",
            "api_key": None,
            "temperature": 0.1,
            "max_tokens": 1000,
            "timeout": 30,
            "enable_cache": True,
            "cache_ttl": 3600,  # 缓存1小时
            "fallback_to_mock": True
        }
        
    def _initialize_llm(self):
        """初始化LLM"""
        if not LANGCHAIN_AVAILABLE:
            logger.warning("LangChain不可用，使用模拟LLM")
            return MockLLM()
            
        provider = self.config.get("provider", "openai")
        
        try:
            if provider == "openai":
                api_key = self.config.get("api_key")
                if not api_key:
                    logger.warning("未配置OpenAI API密钥，使用模拟LLM")
                    return MockLLM()
                    
                return ChatOpenAI(
                    model=self.config.get("model", "gpt-3.5-turbo"),
                    temperature=self.config.get("temperature", 0.1),
                    max_tokens=self.config.get("max_tokens", 1000),
                    openai_api_key=api_key
                )
            else:
                logger.warning(f"不支持的LLM提供商: {provider}，使用模拟LLM")
                return MockLLM()
                
        except Exception as e:
            logger.error(f"LLM初始化失败: {e}")
            if self.config.get("fallback_to_mock", True):
                return MockLLM()
            else:
                raise
                
    def explain_rule_violation(self, rule_result: Dict[str, Any], 
                             record: pd.Series) -> ExplanationResult:
        """
        解释规则违规
        
        Args:
            rule_result: 规则检查结果
            record: 相关记录
            
        Returns:
            解释结果
        """
        start_time = datetime.now()
        
        context = {
            "rule_name": rule_result.get("rule_name", "未知规则"),
            "violation_description": rule_result.get("message", "违规描述不明确"),
            "amount": record.get("借方金额", 0) or record.get("贷方金额", 0),
            "date": record.get("日期", "未知日期"),
            "account": record.get("科目", "未知科目"),
            "description": record.get("摘要", "无摘要")
        }
        
        system_prompt, human_prompt = self.prompt_manager.get_prompt("rule_violation", context)
        
        try:
            if isinstance(self.llm, MockLLM):
                # 使用模拟响应
                content = self._generate_mock_rule_violation_explanation(context)
                token_usage = {"prompt_tokens": 100, "completion_tokens": 200, "total_tokens": 300}
            else:
                # 使用真实LLM
                messages = [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=human_prompt)
                ]
                response = self.llm(messages)
                content = response.content
                token_usage = getattr(response, 'usage', None)
                
            # 解析响应
            suggestions = self._extract_suggestions(content)
            references = self._extract_references(content)
            confidence = self._calculate_confidence(content, context)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # 更新统计信息
            self._update_statistics(token_usage, processing_time)
            
            return ExplanationResult(
                explanation_type=ExplanationType.RULE_VIOLATION,
                content=content,
                confidence=confidence,
                suggestions=suggestions,
                risk_level=rule_result.get("risk_level", "medium"),
                references=references,
                processing_time=processing_time,
                token_usage=token_usage
            )
            
        except Exception as e:
            logger.error(f"规则违规解释失败: {e}")
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return ExplanationResult(
                explanation_type=ExplanationType.RULE_VIOLATION,
                content=f"解释生成失败: {str(e)}",
                confidence=0.0,
                suggestions=["请检查系统配置或联系技术支持"],
                risk_level="unknown",
                references=[],
                processing_time=processing_time
            )
            
    def explain_anomaly(self, anomaly_result: Dict[str, Any], 
                       context_data: pd.DataFrame) -> ExplanationResult:
        """
        解释异常检测结果
        
        Args:
            anomaly_result: 异常检测结果
            context_data: 上下文数据
            
        Returns:
            解释结果
        """
        start_time = datetime.now()
        
        context = {
            "anomaly_type": anomaly_result.get("anomaly_type", "未知类型"),
            "anomaly_description": anomaly_result.get("description", "异常描述不明确"),
            "anomaly_score": anomaly_result.get("score", 0),
            "data_context": self._summarize_context_data(context_data),
            "detection_method": anomaly_result.get("details", {}).get("method", "未知方法")
        }
        
        system_prompt, human_prompt = self.prompt_manager.get_prompt("anomaly_analysis", context)
        
        try:
            if isinstance(self.llm, MockLLM):
                content = self._generate_mock_anomaly_explanation(context)
                token_usage = {"prompt_tokens": 120, "completion_tokens": 250, "total_tokens": 370}
            else:
                messages = [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=human_prompt)
                ]
                response = self.llm(messages)
                content = response.content
                token_usage = getattr(response, 'usage', None)
                
            suggestions = self._extract_suggestions(content)
            references = self._extract_references(content)
            confidence = self._calculate_confidence(content, context)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self._update_statistics(token_usage, processing_time)
            
            return ExplanationResult(
                explanation_type=ExplanationType.ANOMALY_ANALYSIS,
                content=content,
                confidence=confidence,
                suggestions=suggestions,
                risk_level=anomaly_result.get("severity", "medium"),
                references=references,
                processing_time=processing_time,
                token_usage=token_usage
            )
            
        except Exception as e:
            logger.error(f"异常解释失败: {e}")
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return ExplanationResult(
                explanation_type=ExplanationType.ANOMALY_ANALYSIS,
                content=f"异常解释生成失败: {str(e)}",
                confidence=0.0,
                suggestions=["请检查异常检测配置或联系技术支持"],
                risk_level="unknown",
                references=[],
                processing_time=processing_time
            )
            
    def generate_risk_assessment(self, audit_results: List[Dict[str, Any]], 
                               overall_context: Dict[str, Any]) -> ExplanationResult:
        """
        生成风险评估报告
        
        Args:
            audit_results: 审核结果列表
            overall_context: 整体上下文
            
        Returns:
            解释结果
        """
        start_time = datetime.now()
        
        # 汇总风险信息
        risk_summary = self._summarize_risks(audit_results)
        
        context = {
            "risk_type": "综合风险评估",
            "risk_description": f"基于{len(audit_results)}项审核结果的综合风险评估",
            "amount": sum(r.get("amount", 0) for r in audit_results),
            "risk_level": risk_summary["overall_risk"],
            "context_data": json.dumps(overall_context, ensure_ascii=False, indent=2)
        }
        
        system_prompt, human_prompt = self.prompt_manager.get_prompt("risk_assessment", context)
        
        try:
            if isinstance(self.llm, MockLLM):
                content = self._generate_mock_risk_assessment(context, risk_summary)
                token_usage = {"prompt_tokens": 150, "completion_tokens": 300, "total_tokens": 450}
            else:
                messages = [
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=human_prompt)
                ]
                response = self.llm(messages)
                content = response.content
                token_usage = getattr(response, 'usage', None)
                
            suggestions = self._extract_suggestions(content)
            references = self._extract_references(content)
            confidence = self._calculate_confidence(content, context)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self._update_statistics(token_usage, processing_time)
            
            return ExplanationResult(
                explanation_type=ExplanationType.RISK_ASSESSMENT,
                content=content,
                confidence=confidence,
                suggestions=suggestions,
                risk_level=risk_summary["overall_risk"],
                references=references,
                processing_time=processing_time,
                token_usage=token_usage
            )
            
        except Exception as e:
            logger.error(f"风险评估生成失败: {e}")
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return ExplanationResult(
                explanation_type=ExplanationType.RISK_ASSESSMENT,
                content=f"风险评估生成失败: {str(e)}",
                confidence=0.0,
                suggestions=["请检查审核数据或联系技术支持"],
                risk_level="unknown",
                references=[],
                processing_time=processing_time
            )
            
    def _generate_mock_rule_violation_explanation(self, context: Dict[str, Any]) -> str:
        """生成模拟规则违规解释"""
        rule_name = context.get("rule_name", "未知规则")
        amount = context.get("amount", 0)
        
        explanations = {
            "amount_threshold": f"""
根据会计准则和内控要求，单笔交易金额{amount:.2f}元超过了设定的阈值限制。

**违规原因分析：**
1. 大额交易缺乏适当的审批流程
2. 可能存在资金挪用或违规支付风险
3. 违反了公司财务管理制度第X章第X条

**潜在风险影响：**
1. 资金安全风险：可能导致资金损失
2. 合规风险：违反相关财务法规
3. 内控风险：反映内控体系存在缺陷

**整改建议：**
1. 立即核实交易的真实性和合规性
2. 补充相应的审批文件和证明材料
3. 加强大额交易的审批控制

**预防措施：**
1. 完善财务审批流程
2. 建立大额交易预警机制
3. 定期进行内控检查和评估
""",
            
            "account_validation": f"""
科目使用{context.get('account', '未知科目')}存在合规性问题。

**违规原因分析：**
1. 科目使用不符合会计准则规定
2. 可能存在故意错列科目的情况
3. 反映财务人员专业能力不足

**潜在风险影响：**
1. 财务报告失真风险
2. 税务合规风险
3. 审计风险

**整改建议：**
1. 重新进行账务处理
2. 加强财务人员培训
3. 建立科目使用审核机制
"""
        }
        
        return explanations.get(rule_name, f"检测到规则违规：{context.get('violation_description', '描述不明确')}。建议立即核实并采取相应整改措施。")
        
    def _generate_mock_anomaly_explanation(self, context: Dict[str, Any]) -> str:
        """生成模拟异常解释"""
        anomaly_type = context.get("anomaly_type", "未知类型")
        score = context.get("anomaly_score", 0)
        
        return f"""
**异常类型：** {anomaly_type}
**异常分数：** {score:.2f}

**异常特征分析：**
该异常在统计上显著偏离正常模式，异常分数为{score:.2f}，表明该交易具有较强的异常特征。

**可能的业务原因：**
1. 特殊业务场景导致的数据异常
2. 数据录入错误或系统故障
3. 人为操作失误或故意行为
4. 外部环境变化导致的正常波动

**风险等级评估：**
基于异常分数和业务背景，建议进行进一步调查以确定风险等级。

**调查和验证建议：**
1. 核实交易的真实性和合理性
2. 检查相关支持文件和审批记录
3. 与相关业务部门确认交易背景
4. 分析历史同期数据进行对比
5. 必要时进行现场核查
"""
        
    def _generate_mock_risk_assessment(self, context: Dict[str, Any], 
                                    risk_summary: Dict[str, Any]) -> str:
        """生成模拟风险评估"""
        overall_risk = risk_summary.get("overall_risk", "medium")
        
        return f"""
**综合风险评估报告**

**风险概况：**
基于对审核结果的全面分析，当前整体风险等级为：{overall_risk}

**风险影响分析：**
1. **财务风险：** 可能导致直接经济损失
2. **合规风险：** 违反相关法规要求
3. **声誉风险：** 影响企业形象和信誉
4. **操作风险：** 反映内控流程缺陷

**风险等级评估：**
{overall_risk}风险等级需要管理层关注并采取相应措施。

**风险缓释措施：**
1. 立即整改已发现的问题
2. 完善内控制度和流程
3. 加强员工培训和监督
4. 建立风险预警机制

**长期监控建议：**
1. 定期进行风险评估
2. 建立关键风险指标监控
3. 完善风险报告机制
4. 持续改进风险管理体系
"""
        
    def _summarize_context_data(self, df: pd.DataFrame) -> str:
        """汇总上下文数据"""
        if df.empty:
            return "无上下文数据"
            
        summary = f"""
数据概况：
- 记录数量：{len(df)}
- 时间范围：{df.get('日期', ['未知'])[0] if '日期' in df.columns else '未知'} 到 {df.get('日期', ['未知'])[-1] if '日期' in df.columns else '未知'}
- 涉及科目：{df.get('科目', pd.Series(['未知'])).nunique() if '科目' in df.columns else 0} 个
- 总金额：{df.get('借方金额', pd.Series([0])).sum() if '借方金额' in df.columns else 0:.2f}
"""
        return summary
        
    def _summarize_risks(self, audit_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """汇总风险信息"""
        if not audit_results:
            return {"overall_risk": "low", "risk_counts": {}}
            
        # 统计风险等级
        risk_counts = {}
        total_amount = 0
        
        for result in audit_results:
            risk_level = result.get("risk_level", "low")
            risk_counts[risk_level] = risk_counts.get(risk_level, 0) + 1
            total_amount += result.get("amount", 0)
            
        # 确定整体风险等级
        if risk_counts.get("critical", 0) > 0:
            overall_risk = "critical"
        elif risk_counts.get("high", 0) > 0:
            overall_risk = "high"
        elif risk_counts.get("medium", 0) > 2:
            overall_risk = "medium"
        else:
            overall_risk = "low"
            
        return {
            "overall_risk": overall_risk,
            "risk_counts": risk_counts,
            "total_amount": total_amount,
            "total_issues": len(audit_results)
        }
        
    def _extract_suggestions(self, content: str) -> List[str]:
        """从内容中提取建议"""
        suggestions = []
        
        # 查找建议相关的段落
        patterns = [
            r'建议[：:](.*?)(?=\n|$)',
            r'整改建议[：:](.*?)(?=\n|$)',
            r'预防措施[：:](.*?)(?=\n|$)',
            r'措施[：:](.*?)(?=\n|$)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            for match in matches:
                # 清理和分割建议
                cleaned = re.sub(r'\*\*|##|###', '', match.strip())
                if cleaned and len(cleaned) > 5:
                    # 按数字或项目符号分割
                    items = re.split(r'\d+\.|[-*•]', cleaned)
                    for item in items:
                        item = item.strip()
                        if item and len(item) > 3:
                            suggestions.append(item)
                            
        return suggestions[:10]  # 最多返回10个建议
        
    def _extract_references(self, content: str) -> List[str]:
        """从内容中提取参考信息"""
        references = []
        
        # 查找法规、准则等参考
        patterns = [
            r'《([^》]+)》',
            r'([^第]*第\d+条)',
            r'(会计准则[^，。]*)',
            r'(税法[^，。]*)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            references.extend(matches)
            
        return list(set(references))  # 去重
        
    def _calculate_confidence(self, content: str, context: Dict[str, Any]) -> float:
        """计算置信度"""
        # 基于内容长度和结构计算置信度
        if len(content) < 50:
            return 0.3
        elif len(content) < 200:
            return 0.6
        else:
            return 0.8
            
    def _update_statistics(self, token_usage: Optional[Dict[str, int]], 
                          processing_time: float):
        """更新统计信息"""
        self.usage_statistics["total_requests"] += 1
        self.usage_statistics["total_processing_time"] += processing_time
        
        if token_usage:
            self.usage_statistics["total_tokens"] += token_usage.get("total_tokens", 0)
            
    def get_usage_statistics(self) -> Dict[str, Any]:
        """获取使用统计"""
        stats = self.usage_statistics.copy()
        if stats["total_requests"] > 0:
            stats["average_processing_time"] = stats["total_processing_time"] / stats["total_requests"]
            stats["average_tokens_per_request"] = stats["total_tokens"] / stats["total_requests"]
        else:
            stats["average_processing_time"] = 0
            stats["average_tokens_per_request"] = 0
            
        return stats


# 技能函数接口
def llm_explain_skill(data: Union[Dict[str, Any], List[Dict[str, Any]]], 
                     explanation_type: str = "rule_violation",
                     config: Dict[str, Any] = None) -> Union[ExplanationResult, List[ExplanationResult]]:
    """
    LLM解释技能接口
    
    Args:
        data: 输入数据
        explanation_type: 解释类型
        config: 配置参数
        
    Returns:
        解释结果
    """
    explainer = LLMExplainer(config)
    
    if explanation_type == "rule_violation":
        if isinstance(data, dict) and "rule_result" in data and "record" in data:
            return explainer.explain_rule_violation(data["rule_result"], data["record"])
    elif explanation_type == "anomaly_analysis":
        if isinstance(data, dict) and "anomaly_result" in data and "context_data" in data:
            return explainer.explain_anomaly(data["anomaly_result"], data["context_data"])
    elif explanation_type == "risk_assessment":
        if isinstance(data, dict) and "audit_results" in data:
            return explainer.generate_risk_assessment(data["audit_results"], data.get("overall_context", {}))
            
    raise ValueError(f"不支持的解释类型或数据格式: {explanation_type}")
