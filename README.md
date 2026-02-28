# Learn Accounting Agent

一个教学型的账务审核智能体项目，通过结构化的12步骤学习路径，帮助开发者掌握智能体在会计领域的应用。

## 项目概述

Learn Accounting Agent 是一个专门为会计数据审核设计的智能体系统，采用渐进式学习方法，让开发者能够：

- 🎯 **循序渐进**: 通过12个结构化步骤逐步掌握智能体开发
- 📚 **理论实践结合**: 每个步骤包含详细文档和可运行代码
- 🔧 **模块化设计**: 技能插件化架构，易于扩展和维护
- 🏢 **业务导向**: 聚焦会计审核实际场景，解决真实业务问题

## 核心功能

- 📊 **多格式数据解析**: 支持Excel、CSV、JSON等格式的账务数据
- 🔍 **智能规则审核**: 基于可配置规则的自动化合规检查
- 🚨 **异常检测**: 使用算法识别可疑交易和异常模式
- 🤖 **LLM智能解释**: 生成自然语言的违规说明和处理建议
- 📋 **自动报告生成**: 生成标准化的审核报告和分析结果
- 🌐 **REST API**: 提供HTTP接口供外部系统集成

## 项目结构

```
learn-accounting-agent/
├── docs/                # 分步骤教学文档 (s1-s12)
│   ├── s1-项目初始化与环境搭建.md
│   ├── s2-账务审核核心场景拆解.md
│   ├── s3-智能体基础框架搭建.md
│   └── ...
├── agents/              # 智能体核心代码
│   ├── __init__.py
│   ├── base_agent.py    # 智能体基类
│   ├── accounting_agent.py  # 账务审核智能体
│   ├── config.py        # 配置管理
│   └── utils/           # 工具函数
├── skills/              # 技能说明+实现
│   ├── data_parse_skill.md
│   ├── rule_check_skill.md
│   ├── anomaly_detect_skill.md
│   ├── llm_explain_skill.md
│   └── impl/            # 技能代码实现
│       ├── data_parse.py
│       ├── rule_check.py
│       ├── anomaly_detect.py
│       └── llm_explain.py
├── examples/            # 示例数据+演示代码
│   ├── sample_account_data.xlsx
│   └── demo.py
├── requirements.txt     # 依赖包列表
└── README.md           # 项目说明
```

## 学习路径

### 🚀 Phase 1: 基础搭建 (S1-S3)
- **S1**: 项目初始化与环境搭建
- **S2**: 账务审核核心场景拆解  
- **S3**: 智能体基础框架搭建

### 🔧 Phase 2: 核心技能 (S4-S6)
- **S4**: 账务数据解析技能开发
- **S5**: 规则引擎（审核规则）实现
- **S6**: 异常账务识别逻辑

### 🚀 Phase 3: 高级功能 (S7-S9)
- **S7**: LLM集成（智能问答/解释）
- **S8**: 数据持久化（审核记录存储）
- **S9**: 批量账务审核功能

### 🏭 Phase 4: 生产就绪 (S10-S12)
- **S10**: 审核报告自动生成
- **S11**: 接口封装与API暴露
- **S12**: 部署与监控

## 快速开始

### 1. 环境准备

```bash
# 克隆项目
git clone https://github.com/your-username/learn-accounting-agent.git
cd learn-accounting-agent

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate     # Windows

# 安装依赖
pip install -r requirements.txt
```

### 2. 基础使用

```python
from agents.accounting_agent import AccountingAgent
from skills.impl.data_parse import parse_account_data
from skills.impl.rule_check import rule_check_skill

# 初始化智能体
agent = AccountingAgent()

# 注册技能
agent.register_skill("data_parse", parse_account_data)
agent.register_skill("rule_check", rule_check_skill)

# 执行审核任务
data = agent.run("data_parse", "examples/sample_account_data.xlsx")
result = agent.run("rule_check", data)

print(f"审核完成，共处理 {len(result)} 条记录")
```

### 3. 配置设置

创建 `.env` 文件：

```bash
# OpenAI配置
OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-3.5-turbo

# 数据库配置
DATABASE_URL=sqlite:///accounting_agent.db

# API配置
API_HOST=0.0.0.0
API_PORT=8000
```

## 技术栈

- **Python 3.9+**: 核心开发语言
- **pandas**: 数据处理和分析
- **FastAPI**: Web框架和API服务
- **SQLAlchemy**: 数据库ORM
- **LangChain**: LLM集成框架
- **Jinja2**: 模板引擎
- **OpenAI**: 大语言模型服务

## 开发指南

### 添加新技能

1. 在 `skills/impl/` 目录创建技能实现文件
2. 在 `skills/` 目录创建对应的技能文档
3. 在智能体中注册技能：

```python
from skills.impl.your_skill import your_skill_function

agent.register_skill("your_skill", your_skill_function)
```

### 扩展规则

在 `skills/impl/rule_check.py` 中添加新的规则：

```python
def custom_rule(record: pd.Series) -> Dict[str, Any]:
    # 自定义规则逻辑
    return {"pass": True, "anomalies": []}
```

### 自定义配置

```python
from agents.config import AgentConfig

config = AgentConfig(
    max_batch_size=2000,
    anomaly_threshold=3.0,
    openai_model="gpt-4"
)

agent = AccountingAgent(config)
```

## 示例场景

### 单条账务审核

```python
single_record = {
    "日期": "2024-01-01",
    "科目": "银行存款",
    "借方金额": 10000.0,
    "贷方金额": 0,
    "摘要": "销售收入"
}

result = agent.run("single_check", single_record)
```

### 批量审核

```python
# 批量审核Excel文件
data = agent.run("data_parse", "account_data.xlsx")
audit_result = agent.run("batch_check", data)

# 生成报告
report = agent.run("generate_report", audit_result)
```

## 测试

```bash
# 运行所有测试
pytest

# 运行特定测试
pytest tests/test_agent.py

# 生成覆盖率报告
pytest --cov=agents tests/
```

## 贡献指南

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 联系方式

- 项目主页: https://github.com/your-username/learn-accounting-agent
- 问题反馈: https://github.com/your-username/learn-accounting-agent/issues
- 邮箱: your-email@example.com

## 致谢

- 感谢所有贡献者的支持
- 参考了优秀的开源项目和企业实践
- 特别感谢会计和AI领域的专家指导

---

⭐ 如果这个项目对你有帮助，请给我们一个星标！
