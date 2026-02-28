"""
创建示例账务数据
用于测试和演示智能体的各项功能
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

def create_sample_account_data():
    """创建示例账务数据"""
    
    # 基础数据
    accounts = {
        "1001": "库存现金",
        "1002": "银行存款", 
        "1121": "应收账款",
        "1401": "材料采购",
        "1601": "固定资产",
        "2001": "短期借款",
        "2202": "应付账款",
        "4001": "实收资本",
        "5001": "生产成本",
        "6001": "主营业务收入",
        "6401": "主营业务成本",
        "6601": "销售费用",
        "6602": "管理费用",
        "6603": "财务费用"
    }
    
    # 生成日期范围（最近3个月）
    start_date = datetime.now() - timedelta(days=90)
    dates = [start_date + timedelta(days=i) for i in range(90)]
    
    # 生成交易数据
    transactions = []
    voucher_id = 1
    
    for date in dates:
        # 每天生成2-8笔交易
        num_transactions = random.randint(2, 8)
        
        for i in range(num_transactions):
            # 随机选择交易类型
            transaction_type = random.choice(['normal', 'large_amount', 'weekend_special', 'duplicate'])
            
            if transaction_type == 'normal':
                # 正常交易
                debit_account = random.choice(list(accounts.keys()))
                credit_account = random.choice([k for k in accounts.keys() if k != debit_account])
                
                # 正常金额范围
                amount = round(random.uniform(100, 50000), 2)
                
            elif transaction_type == 'large_amount':
                # 大额交易（可能触发异常）
                debit_account = random.choice(["1001", "1002", "1601"])
                credit_account = random.choice(["4001", "2001"])
                
                # 大额金额
                amount = round(random.uniform(80000, 200000), 2)
                
            elif transaction_type == 'weekend_special' and date.weekday() >= 5:
                # 周末特殊交易
                debit_account = "1001"  # 现金
                credit_account = "6001"  # 收入
                amount = round(random.uniform(1000, 30000), 2)
                
            else:
                # 普通交易
                debit_account = random.choice(list(accounts.keys()))
                credit_account = random.choice([k for k in accounts.keys() if k != debit_account])
                amount = round(random.uniform(100, 50000), 2)
            
            # 创建交易记录
            transaction = {
                "日期": date.strftime("%Y-%m-%d"),
                "凭证号": f"记-{voucher_id:04d}",
                "科目编码": debit_account,
                "科目名称": accounts[debit_account],
                "借方金额": amount,
                "贷方金额": 0,
                "摘要": generate_description(debit_account, credit_account, amount),
                "制单人": random.choice(["张三", "李四", "王五", "赵六"])
            }
            transactions.append(transaction)
            
            # 对应的贷方记录
            transaction = {
                "日期": date.strftime("%Y-%m-%d"),
                "凭证号": f"记-{voucher_id:04d}",
                "科目编码": credit_account,
                "科目名称": accounts[credit_account],
                "借方金额": 0,
                "贷方金额": amount,
                "摘要": generate_description(debit_account, credit_account, amount),
                "制单人": random.choice(["张三", "李四", "王五", "赵六"])
            }
            transactions.append(transaction)
            
            voucher_id += 1
    
    return pd.DataFrame(transactions)

def generate_description(debit_account, credit_account, amount):
    """生成交易摘要"""
    descriptions = {
        "1001": ["现金收入", "现金支出", "现金提取", "现金存入"],
        "1002": ["银行转账", "银行存款", "银行取款", "电汇"],
        "1121": ["应收账款", "销售回款", "客户付款"],
        "1401": ["材料采购", "原材料购买", "采购付款"],
        "1601": ["固定资产购置", "设备购买", "资产投资"],
        "2001": ["短期借款", "银行贷款", "借款"],
        "2202": ["应付账款", "采购付款", "供应商付款"],
        "4001": ["实收资本", "股东投资", "资本注入"],
        "5001": ["生产成本", "制造费用", "生产支出"],
        "6001": ["销售收入", "营业外收入", "服务收入"],
        "6401": ["销售成本", "产品成本", "营业成本"],
        "6601": ["销售费用", "营销费用", "广告费"],
        "6602": ["管理费用", "办公费用", "人工成本"],
        "6603": ["财务费用", "利息支出", "手续费"]
    }
    
    debit_desc = random.choice(descriptions.get(debit_account, ["其他支出"]))
    credit_desc = random.choice(descriptions.get(credit_account, ["其他收入"]))
    
    return f"{debit_desc}-{credit_desc}"

def create_anomaly_data():
    """创建包含异常的数据"""
    
    # 先创建正常数据
    df = create_sample_account_data()
    
    # 添加一些异常记录
    anomalies = []
    
    # 异常1: 大额现金交易
    anomalies.append({
        "日期": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"),
        "凭证号": f"记-{len(df)+len(anomalies)+1:04d}",
        "科目编码": "1001",
        "科目名称": "库存现金",
        "借方金额": 150000.00,
        "贷方金额": 0,
        "摘要": "大额现金提取",
        "制单人": "张三"
    })
    
    anomalies.append({
        "日期": (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d"),
        "凭证号": f"记-{len(df)+len(anomalies)+1:04d}",
        "科目编码": "6001",
        "科目名称": "主营业务收入",
        "借方金额": 0,
        "贷方金额": 150000.00,
        "摘要": "大额现金收入",
        "制单人": "张三"
    })
    
    # 异常2: 周末交易
    weekend_date = datetime.now() - timedelta(days=3)  # 假设是周末
    while weekend_date.weekday() < 5:  # 确保是周末
        weekend_date -= timedelta(days=1)
        
    anomalies.append({
        "日期": weekend_date.strftime("%Y-%m-%d"),
        "凭证号": f"记-{len(df)+len(anomalies)+1:04d}",
        "科目编码": "1002",
        "科目名称": "银行存款",
        "借方金额": 25000.00,
        "贷方金额": 0,
        "摘要": "周末银行转账",
        "制单人": "李四"
    })
    
    anomalies.append({
        "日期": weekend_date.strftime("%Y-%m-%d"),
        "凭证号": f"记-{len(df)+len(anomalies)+1:04d}",
        "科目编码": "4001",
        "科目名称": "实收资本",
        "借方金额": 0,
        "贷方金额": 25000.00,
        "摘要": "周末投资款",
        "制单人": "李四"
    })
    
    # 异常3: 重复交易
    repeat_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    for i in range(3):
        anomalies.append({
            "日期": repeat_date,
            "凭证号": f"记-{len(df)+len(anomalies)+1:04d}",
            "科目编码": "1121",
            "科目名称": "应收账款",
            "借方金额": 10000.00,
            "贷方金额": 0,
            "摘要": "客户回款",
            "制单人": "王五"
        })
        
        anomalies.append({
            "日期": repeat_date,
            "凭证号": f"记-{len(df)+len(anomalies)+1:04d}",
            "科目编码": "1002",
            "科目名称": "银行存款",
            "借方金额": 0,
            "贷方金额": 10000.00,
            "摘要": "客户回款",
            "制单人": "王五"
        })
    
    # 异常4: 整数金额
    anomalies.append({
        "日期": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        "凭证号": f"记-{len(df)+len(anomalies)+1:04d}",
        "科目编码": "1601",
        "科目名称": "固定资产",
        "借方金额": 100000.00,
        "贷方金额": 0,
        "摘要": "设备采购整数金额",
        "制单人": "赵六"
    })
    
    anomalies.append({
        "日期": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        "凭证号": f"记-{len(df)+len(anomalies)+1:04d}",
        "科目编码": "2202",
        "科目名称": "应付账款",
        "借方金额": 0,
        "贷方金额": 100000.00,
        "摘要": "设备采购整数金额",
        "制单人": "赵六"
    })
    
    # 合并数据
    anomaly_df = pd.DataFrame(anomalies)
    combined_df = pd.concat([df, anomaly_df], ignore_index=True)
    
    return combined_df

def save_sample_data():
    """保存示例数据到文件"""
    
    # 创建examples目录
    examples_dir = Path(__file__).parent
    examples_dir.mkdir(exist_ok=True)
    
    # 生成并保存正常数据
    print("生成正常示例数据...")
    normal_data = create_sample_account_data()
    normal_file = examples_dir / "sample_account_data.xlsx"
    normal_data.to_excel(normal_file, index=False, engine='openpyxl')
    print(f"正常数据已保存到: {normal_file}")
    print(f"数据形状: {normal_data.shape}")
    print(f"数据预览:")
    print(normal_data.head())
    
    # 生成并保存包含异常的数据
    print("\n生成包含异常的示例数据...")
    anomaly_data = create_anomaly_data()
    anomaly_file = examples_dir / "sample_account_data_with_anomalies.xlsx"
    anomaly_data.to_excel(anomaly_file, index=False, engine='openpyxl')
    print(f"异常数据已保存到: {anomaly_file}")
    print(f"数据形状: {anomaly_data.shape}")
    print(f"数据预览:")
    print(anomaly_data.tail())
    
    # 保存CSV格式
    csv_file = examples_dir / "sample_account_data.csv"
    normal_data.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"\nCSV数据已保存到: {csv_file}")
    
    # 保存JSON格式
    json_file = examples_dir / "sample_account_data.json"
    normal_data.to_json(json_file, orient='records', date_format='iso', force_ascii=False, indent=2)
    print(f"JSON数据已保存到: {json_file}")
    
    # 生成数据统计报告
    print("\n数据统计报告:")
    print(f"总记录数: {len(normal_data)}")
    print(f"日期范围: {normal_data['日期'].min()} 到 {normal_data['日期'].max()}")
    print(f"涉及科目数: {normal_data['科目名称'].nunique()}")
    print(f"制单人数量: {normal_data['制单人'].nunique()}")
    
    # 金额统计
    debit_total = normal_data['借方金额'].sum()
    credit_total = normal_data['贷方金额'].sum()
    print(f"借方总金额: {debit_total:,.2f}")
    print(f"贷方总金额: {credit_total:,.2f}")
    print(f"借贷平衡: {'是' if abs(debit_total - credit_total) < 0.01 else '否'}")
    
    return normal_file, anomaly_file

def create_test_scenarios():
    """创建特定测试场景的数据"""
    
    scenarios = {}
    
    # 场景1: 大额交易测试
    large_amount_data = pd.DataFrame([
        {
            "日期": "2024-01-01",
            "凭证号": "记-0001",
            "科目编码": "1002",
            "科目名称": "银行存款",
            "借方金额": 150000.00,
            "贷方金额": 0,
            "摘要": "大额银行存款",
            "制单人": "张三"
        },
        {
            "日期": "2024-01-01",
            "凭证号": "记-0001",
            "科目编码": "4001",
            "科目名称": "实收资本",
            "借方金额": 0,
            "贷方金额": 150000.00,
            "摘要": "大额银行存款",
            "制单人": "张三"
        }
    ])
    scenarios['large_amount'] = large_amount_data
    
    # 场景2: 周末交易测试
    weekend_data = pd.DataFrame([
        {
            "日期": "2024-01-06",  # 周六
            "凭证号": "记-0002",
            "科目编码": "1001",
            "科目名称": "库存现金",
            "借方金额": 5000.00,
            "贷方金额": 0,
            "摘要": "周末现金交易",
            "制单人": "李四"
        }
    ])
    scenarios['weekend'] = weekend_data
    
    # 场景3: 重复交易测试
    duplicate_data = pd.DataFrame([
        {
            "日期": "2024-01-02",
            "凭证号": "记-0003",
            "科目编码": "1121",
            "科目名称": "应收账款",
            "借方金额": 8000.00,
            "贷方金额": 0,
            "摘要": "重复交易测试",
            "制单人": "王五"
        },
        {
            "日期": "2024-01-02",
            "凭证号": "记-0004",
            "科目编码": "1121",
            "科目名称": "应收账款",
            "借方金额": 8000.00,
            "贷方金额": 0,
            "摘要": "重复交易测试",
            "制单人": "王五"
        }
    ])
    scenarios['duplicate'] = duplicate_data
    
    # 保存测试场景
    examples_dir = Path(__file__).parent
    for name, data in scenarios.items():
        file_path = examples_dir / f"test_scenario_{name}.xlsx"
        data.to_excel(file_path, index=False, engine='openpyxl')
        print(f"测试场景 '{name}' 已保存到: {file_path}")
    
    return scenarios

if __name__ == "__main__":
    print("开始创建示例数据...")
    
    # 保存主要示例数据
    normal_file, anomaly_file = save_sample_data()
    
    # 创建测试场景
    print("\n创建测试场景数据...")
    scenarios = create_test_scenarios()
    
    print("\n✅ 所有示例数据创建完成！")
    print("\n📁 生成的文件:")
    print(f"  - 正常数据: {normal_file}")
    print(f"  - 异常数据: {anomaly_file}")
    print(f"  - 测试场景: {len(scenarios)} 个")
    
    print("\n🎯 使用建议:")
    print("1. 使用 normal_data 测试基础功能")
    print("2. 使用 anomaly_data 测试异常检测")
    print("3. 使用 test_scenario_* 测试特定场景")
    print("4. 运行 examples/demo.py 查看完整演示")
