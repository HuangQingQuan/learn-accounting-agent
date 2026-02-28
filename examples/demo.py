"""
Learn Accounting Agent 演示程序
展示智能体的基本使用方法和核心功能
"""

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from agents.accounting_agent import AccountingAgent
from agents.config import AgentConfig


def create_sample_data():
    """创建示例账务数据"""
    import pandas as pd
    
    data = [
        {
            "日期": "2024-01-01",
            "凭证号": "记-001",
            "科目编码": "1001",
            "科目名称": "库存现金",
            "借方金额": 10000.00,
            "贷方金额": 0.00,
            "摘要": "销售收入",
            "制单人": "张三"
        },
        {
            "日期": "2024-01-01", 
            "凭证号": "记-001",
            "科目编码": "6001",
            "科目名称": "主营业务收入",
            "借方金额": 0.00,
            "贷方金额": 10000.00,
            "摘要": "销售收入",
            "制单人": "张三"
        },
        {
            "日期": "2024-01-02",
            "凭证号": "记-002", 
            "科目编码": "1002",
            "科目名称": "银行存款",
            "借方金额": 50000.00,
            "贷方金额": 0.00,
            "摘要": "股东投资",
            "制单人": "李四"
        },
        {
            "日期": "2024-01-02",
            "凭证号": "记-002",
            "科目编码": "4001", 
            "科目名称": "实收资本",
            "借方金额": 0.00,
            "贷方金额": 50000.00,
            "摘要": "股东投资",
            "制单人": "李四"
        },
        {
            "日期": "2024-01-03",
            "凭证号": "记-003",
            "科目编码": "1001",
            "科目名称": "库存现金", 
            "借方金额": 150000.00,
            "贷方金额": 0.00,
            "摘要": "大额现金收入",
            "制单人": "王五"
        },
        {
            "日期": "2024-01-03",
            "凭证号": "记-003",
            "科目编码": "6001",
            "科目名称": "主营业务收入",
            "借方金额": 0.00,
            "贷方金额": 150000.00,
            "摘要": "大额现金收入", 
            "制单人": "王五"
        }
    ]
    
    return pd.DataFrame(data)


def demo_basic_usage():
    """演示基础使用方法"""
    print("=" * 60)
    print("🚀 Learn Accounting Agent 基础使用演示")
    print("=" * 60)
    
    # 创建智能体实例
    config = AgentConfig(debug=True)
    agent = AccountingAgent(config)
    
    print(f"✅ 智能体创建成功: {agent.name}")
    print(f"📊 智能体信息: {agent.get_agent_info()}")
    
    # 创建示例数据
    print("\n📋 创建示例账务数据...")
    sample_data = create_sample_data()
    print(f"📈 数据规模: {len(sample_data)} 条记录")
    print("\n数据预览:")
    print(sample_data[["日期", "科目名称", "借方金额", "贷方金额", "摘要"]].to_string())
    
    return agent, sample_data


def demo_skill_management(agent):
    """演示技能管理功能"""
    print("\n" + "=" * 60)
    print("🔧 技能管理演示")
    print("=" * 60)
    
    # 查看当前技能
    print("📋 当前已注册技能:")
    skills = agent.list_skills()
    for skill in skills:
        print(f"  - {skill}")
    
    # 注册示例技能
    def demo_skill(data, **kwargs):
        """示例技能：简单统计"""
        if isinstance(data, list):
            return {"记录数": len(data), "数据类型": "list"}
        elif hasattr(data, '__len__'):
            return {"记录数": len(data), "数据类型": type(data).__name__}
        else:
            return {"记录数": 1, "数据类型": type(data).__name__}
    
    print("\n➕ 注册示例技能...")
    agent.register_skill("demo_stats", demo_skill)
    
    # 使用技能
    print("\n🎯 执行示例技能:")
    result = agent.run("demo_stats", [1, 2, 3, 4, 5])
    print(f"结果: {result}")
    
    # 查看技能信息
    print("\n📊 技能详细信息:")
    skill_info = agent.get_skill_info()
    for name, info in skill_info.items():
        print(f"  {name}: {info['doc']}")


def demo_task_statistics(agent):
    """演示任务统计功能"""
    print("\n" + "=" * 60)
    print("📈 任务统计演示")
    print("=" * 60)
    
    # 获取统计信息
    stats = agent.get_task_statistics()
    print("📊 任务执行统计:")
    print(f"  总任务数: {stats['total_tasks']}")
    print(f"  成功任务数: {stats['successful_tasks']}")
    print(f"  失败任务数: {stats['failed_tasks']}")
    print(f"  成功率: {stats['success_rate']:.2%}")
    print(f"  平均执行时间: {stats['average_duration']:.2f}秒")
    
    # 获取审计历史
    print("\n📜 最近执行历史:")
    history = agent.get_audit_history(3)
    for record in history:
        print(f"  {record['task']} - {record['status']} - {record['duration']:.2f}s")


def demo_error_handling(agent):
    """演示错误处理"""
    print("\n" + "=" * 60)
    print("⚠️  错误处理演示")
    print("=" * 60)
    
    try:
        # 尝试执行不存在的任务
        agent.run("nonexistent_task", "data")
    except Exception as e:
        print(f"❌ 捕获到预期错误: {type(e).__name__}: {e}")
    
    try:
        # 尝试注册重复技能
        def duplicate_skill(data):
            return data
        agent.register_skill("demo_stats", duplicate_skill)
    except Exception as e:
        print(f"❌ 捕获到预期错误: {type(e).__name__}: {e}")


def demo_configuration():
    """演示配置管理"""
    print("\n" + "=" * 60)
    print("⚙️  配置管理演示")
    print("=" * 60)
    
    # 创建自定义配置
    custom_config = AgentConfig(
        name="CustomAccountingAgent",
        debug=True,
        max_batch_size=500,
        openai_model="gpt-4"
    )
    
    print("📋 自定义配置:")
    config_dict = custom_config.to_dict()
    for key, value in config_dict.items():
        print(f"  {key}: {value}")
    
    # 使用自定义配置创建智能体
    custom_agent = AccountingAgent(custom_config)
    print(f"\n✅ 自定义智能体创建成功: {custom_agent.config.name}")


def main():
    """主演示函数"""
    print("🎯 Learn Accounting Agent 演示程序")
    print("📚 这是一个教学型的账务审核智能体项目演示")
    
    try:
        # 基础使用演示
        agent, sample_data = demo_basic_usage()
        
        # 技能管理演示
        demo_skill_management(agent)
        
        # 任务统计演示
        demo_task_statistics(agent)
        
        # 错误处理演示
        demo_error_handling(agent)
        
        # 配置管理演示
        demo_configuration()
        
        print("\n" + "=" * 60)
        print("🎉 演示完成！")
        print("=" * 60)
        print("💡 提示:")
        print("  - 查看 docs/ 目录了解详细的学习步骤")
        print("  - 查看 skills/ 目录了解技能实现")
        print("  - 查看 agents/ 目录了解智能体架构")
        print("  - 运行 pytest 进行单元测试")
        
    except Exception as e:
        print(f"\n❌ 演示过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
