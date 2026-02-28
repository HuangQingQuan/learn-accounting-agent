"""
数据解析技能实现
支持多种格式的账务数据解析，包括Excel、CSV、JSON等格式
"""

import pandas as pd
import json
import logging
from typing import Union, List, Dict, Any, Optional
from pathlib import Path
import chardet
from datetime import datetime

logger = logging.getLogger(__name__)


class DataParser:
    """
    账务数据解析器
    支持多种数据格式的解析和标准化处理
    """
    
    # 支持的文件格式
    SUPPORTED_FORMATS = {
        '.xlsx': 'excel',
        '.xls': 'excel', 
        '.csv': 'csv',
        '.json': 'json'
    }
    
    # 必需字段映射
    REQUIRED_FIELDS = {
        '日期': ['日期', 'date', '交易日期', 'voucher_date', '凭证日期'],
        '科目': ['科目', '科目名称', 'account', 'account_name', '科目编码'],
        '借方金额': ['借方金额', 'debit', 'debit_amount', '借方'],
        '贷方金额': ['贷方金额', 'credit', 'credit_amount', '贷方'],
        '摘要': ['摘要', 'description', 'desc', '备注', 'remark']
    }
    
    # 可选字段
    OPTIONAL_FIELDS = {
        '凭证号': ['凭证号', 'voucher_no', '凭证编号'],
        '制单人': ['制单人', 'creator', '录入人', '操作员'],
        '审核人': ['审核人', 'auditor', '复核人'],
        '附件数': ['附件数', 'attachments', '附件张数']
    }
    
    def __init__(self, encoding: str = 'utf-8'):
        """
        初始化数据解析器
        
        Args:
            encoding: 默认文件编码
        """
        self.encoding = encoding
        self.field_mapping = {}
        
    def detect_encoding(self, file_path: str) -> str:
        """
        检测文件编码
        
        Args:
            file_path: 文件路径
            
        Returns:
            检测到的编码格式
        """
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # 读取前10KB检测编码
                result = chardet.detect(raw_data)
                return result['encoding'] or self.encoding
        except Exception as e:
            logger.warning(f"编码检测失败，使用默认编码: {e}")
            return self.encoding
            
    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        映射列名到标准字段名
        
        Args:
            df: 原始数据框
            
        Returns:
            标准化列名的数据框
        """
        # 创建反向映射
        reverse_mapping = {}
        for standard_name, variants in {**self.REQUIRED_FIELDS, **self.OPTIONAL_FIELDS}.items():
            for variant in variants:
                reverse_mapping[variant.lower()] = standard_name
                
        # 重命名列
        new_columns = {}
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower in reverse_mapping:
                new_columns[col] = reverse_mapping[col_lower]
                self.field_mapping[reverse_mapping[col_lower]] = col
                
        df = df.rename(columns=new_columns)
        logger.info(f"列名映射完成: {self.field_mapping}")
        
        return df
        
    def validate_required_fields(self, df: pd.DataFrame) -> bool:
        """
        验证必需字段是否存在
        
        Args:
            df: 数据框
            
        Returns:
            是否包含所有必需字段
            
        Raises:
            ValueError: 缺少必需字段时抛出
        """
        missing_fields = []
        for field in self.REQUIRED_FIELDS.keys():
            if field not in df.columns:
                missing_fields.append(field)
                
        if missing_fields:
            raise ValueError(f"缺少必需字段: {missing_fields}")
            
        return True
        
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗和标准化数据
        
        Args:
            df: 原始数据框
            
        Returns:
            清洗后的数据框
        """
        # 创建副本避免修改原数据
        df = df.copy()
        
        # 处理日期字段
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
            
        # 处理金额字段
        for amount_field in ['借方金额', '贷方金额']:
            if amount_field in df.columns:
                # 移除非数字字符并转换为浮点数
                df[amount_field] = df[amount_field].astype(str).str.replace(r'[^\d.-]', '', regex=True)
                df[amount_field] = pd.to_numeric(df[amount_field], errors='coerce')
                # 填充NaN为0
                df[amount_field] = df[amount_field].fillna(0)
                
        # 处理文本字段
        text_fields = ['科目', '摘要', '制单人', '审核人']
        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).str.strip()
                df[field] = df[field].replace('nan', '')
                
        # 移除完全为空的行
        df = df.dropna(how='all')
        
        logger.info(f"数据清洗完成，剩余 {len(df)} 条记录")
        
        return df
        
    def parse_excel(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        解析Excel文件
        
        Args:
            file_path: Excel文件路径
            **kwargs: pandas.read_excel的额外参数
            
        Returns:
            解析后的数据框
        """
        try:
            # 尝试读取Excel文件
            df = pd.read_excel(file_path, **kwargs)
            logger.info(f"成功读取Excel文件: {file_path}, 共 {len(df)} 行")
            
            # 标准化处理
            df = self.map_columns(df)
            self.validate_required_fields(df)
            df = self.clean_data(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Excel文件解析失败: {e}")
            raise ValueError(f"Excel文件解析失败: {str(e)}")
            
    def parse_csv(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        解析CSV文件
        
        Args:
            file_path: CSV文件路径
            **kwargs: pandas.read_csv的额外参数
            
        Returns:
            解析后的数据框
        """
        try:
            # 检测编码
            encoding = kwargs.get('encoding') or self.detect_encoding(file_path)
            
            # 设置默认参数
            default_kwargs = {
                'encoding': encoding,
                'low_memory': False
            }
            default_kwargs.update(kwargs)
            
            # 读取CSV文件
            df = pd.read_csv(file_path, **default_kwargs)
            logger.info(f"成功读取CSV文件: {file_path}, 共 {len(df)} 行")
            
            # 标准化处理
            df = self.map_columns(df)
            self.validate_required_fields(df)
            df = self.clean_data(df)
            
            return df
            
        except Exception as e:
            logger.error(f"CSV文件解析失败: {e}")
            raise ValueError(f"CSV文件解析失败: {str(e)}")
            
    def parse_json(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        解析JSON文件
        
        Args:
            file_path: JSON文件路径
            **kwargs: 额外参数
            
        Returns:
            解析后的数据框
        """
        try:
            with open(file_path, 'r', encoding=self.encoding) as f:
                data = json.load(f)
                
            # 处理不同的JSON结构
            if isinstance(data, list):
                # 直接是记录列表
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if 'data' in data:
                    # 包含data字段
                    df = pd.DataFrame(data['data'])
                elif 'records' in data:
                    # 包含records字段
                    df = pd.DataFrame(data['records'])
                else:
                    # 尝试将字典转换为单行数据
                    df = pd.DataFrame([data])
            else:
                raise ValueError("不支持的JSON结构")
                
            logger.info(f"成功读取JSON文件: {file_path}, 共 {len(df)} 行")
            
            # 标准化处理
            df = self.map_columns(df)
            self.validate_required_fields(df)
            df = self.clean_data(df)
            
            return df
            
        except Exception as e:
            logger.error(f"JSON文件解析失败: {e}")
            raise ValueError(f"JSON文件解析失败: {str(e)}")
            
    def parse_file(self, file_path: str, **kwargs) -> pd.DataFrame:
        """
        自动识别文件格式并解析
        
        Args:
            file_path: 文件路径
            **kwargs: 解析参数
            
        Returns:
            解析后的数据框
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
            
        # 获取文件扩展名
        suffix = file_path.suffix.lower()
        
        if suffix not in self.SUPPORTED_FORMATS:
            raise ValueError(f"不支持的文件格式: {suffix}")
            
        # 根据格式选择解析方法
        format_type = self.SUPPORTED_FORMATS[suffix]
        
        if format_type == 'excel':
            return self.parse_excel(str(file_path), **kwargs)
        elif format_type == 'csv':
            return self.parse_csv(str(file_path), **kwargs)
        elif format_type == 'json':
            return self.parse_json(str(file_path), **kwargs)
        else:
            raise ValueError(f"未实现的格式解析: {format_type}")
            
    def get_parse_info(self) -> Dict[str, Any]:
        """
        获取解析信息
        
        Returns:
            解析信息字典
        """
        return {
            "supported_formats": list(self.SUPPORTED_FORMATS.keys()),
            "required_fields": list(self.REQUIRED_FIELDS.keys()),
            "optional_fields": list(self.OPTIONAL_FIELDS.keys()),
            "field_mapping": self.field_mapping
        }


# 技能函数接口
def parse_account_data(file_path: str, **kwargs) -> pd.DataFrame:
    """
    解析账务数据文件（技能接口）
    
    Args:
        file_path: 文件路径
        **kwargs: 解析参数
        
    Returns:
        标准化的账务数据DataFrame
    """
    parser = DataParser()
    return parser.parse_file(file_path, **kwargs)


def validate_data_format(data: Union[pd.DataFrame, str, Path]) -> Dict[str, Any]:
    """
    验证数据格式
    
    Args:
        data: 数据框或文件路径
        
    Returns:
        验证结果
    """
    result = {
        "valid": False,
        "errors": [],
        "warnings": [],
        "info": {}
    }
    
    try:
        if isinstance(data, (str, Path)):
            # 验证文件
            file_path = Path(data)
            if not file_path.exists():
                result["errors"].append("文件不存在")
                return result
                
            suffix = file_path.suffix.lower()
            if suffix not in DataParser.SUPPORTED_FORMATS:
                result["errors"].append(f"不支持的文件格式: {suffix}")
                return result
                
            # 尝试解析
            parser = DataParser()
            df = parser.parse_file(str(file_path))
            
        elif isinstance(data, pd.DataFrame):
            # 验证数据框
            df = data.copy()
            
        else:
            result["errors"].append("不支持的数据类型")
            return result
            
        # 验证必需字段
        parser = DataParser()
        try:
            df = parser.map_columns(df)
            parser.validate_required_fields(df)
        except ValueError as e:
            result["errors"].append(str(e))
            
        # 数据质量检查
        if len(df) == 0:
            result["warnings"].append("数据为空")
            
        # 检查日期格式
        if '日期' in df.columns:
            invalid_dates = df['日期'].isna().sum()
            if invalid_dates > 0:
                result["warnings"].append(f"发现 {invalid_dates} 条无效日期")
                
        # 检查金额格式
        for amount_field in ['借方金额', '贷方金额']:
            if amount_field in df.columns:
                negative_amounts = (df[amount_field] < 0).sum()
                if negative_amounts > 0:
                    result["warnings"].append(f"{amount_field} 字段发现 {negative_amounts} 个负值")
                    
        # 设置结果
        if not result["errors"]:
            result["valid"] = True
            
        result["info"] = {
            "rows": len(df),
            "columns": len(df.columns),
            "fields": list(df.columns),
            "parse_info": parser.get_parse_info()
        }
        
    except Exception as e:
        result["errors"].append(f"验证过程中发生错误: {str(e)}")
        
    return result
