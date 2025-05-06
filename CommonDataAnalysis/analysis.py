import os
import csv
import pandas as pd
import json
from typing import Any, Dict, List, Optional, Union
from mcp.server.fastmcp import FastMCP

# 初始化 FastMCP server
mcp = FastMCP("data-analysis")

# 数据目录路径
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data")

# 缓存已加载的数据
_data_cache = {}

def get_available_datasets() -> List[str]:
    """获取可用的数据集列表"""
    if not os.path.exists(DATA_DIR):
        return []
    
    return [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]

def load_dataset(filename: str) -> pd.DataFrame:
    """加载指定的数据集"""
    if filename in _data_cache:
        return _data_cache[filename]
    
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"数据集 {filename} 不存在")
    
    df = pd.read_csv(filepath)
    _data_cache[filename] = df
    return df

def get_dataset_info(filename: str) -> Dict[str, Any]:
    """获取数据集的基本信息"""
    df = load_dataset(filename)
    
    return {
        "filename": filename,
        "rows": len(df),
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "sample": df.head(5).to_dict(orient="records")
    }

@mcp.resource(uri="D:\mcpserver2\CommonDataAnalysis\Data")
def datasets() -> Dict[str, Any]:
    """获取所有可用数据集的列表和基本信息"""
    available_datasets = get_available_datasets()
    result = {
        "available_datasets": available_datasets,
        "datasets_info": {}
    }
    
    for dataset in available_datasets:
        try:
            result["datasets_info"][dataset] = get_dataset_info(dataset)
        except Exception as e:
            result["datasets_info"][dataset] = {"error": str(e)}
    
    return result

# @mcp.resource(uri="D:\mcpserver2\CommonDataAnalysis\Data")
# def dataset_content(filename: str) -> Dict[str, Any]:
#     """获取指定数据集的完整内容"""
#     try:
#         df = load_dataset(filename)
#         return {
#             "filename": filename,
#             "data": df.to_dict(orient="records")
#         }
#     except Exception as e:
#         return {"error": str(e)}

@mcp.tool()
def query_rows(filename: str, filters: Optional[Dict[str, Any]] = None, limit: int = 10) -> str:
    """查询数据集中的特定行
    
    Args:
        filename: 数据集文件名（例如 sample_data.csv）
        filters: 过滤条件，格式为 {"column": value} 或 {"column": {"operator": "value"}}
                支持的操作符: eq, ne, gt, lt, ge, le, contains
        limit: 返回的最大行数
    """
    try:
        df = load_dataset(filename)
        
        if filters:
            for column, condition in filters.items():
                if column not in df.columns:
                    return f"错误: 列 '{column}' 不存在"
                
                if isinstance(condition, dict):
                    for op, value in condition.items():
                        if op == "eq":
                            df = df[df[column] == value]
                        elif op == "ne":
                            df = df[df[column] != value]
                        elif op == "gt":
                            df = df[df[column] > value]
                        elif op == "lt":
                            df = df[df[column] < value]
                        elif op == "ge":
                            df = df[df[column] >= value]
                        elif op == "le":
                            df = df[df[column] <= value]
                        elif op == "contains":
                            df = df[df[column].astype(str).str.contains(str(value))]
                        else:
                            return f"错误: 不支持的操作符 '{op}'"
                else:
                    df = df[df[column] == condition]
        
        result = df.head(limit).to_dict(orient="records")
        return json.dumps(result, indent=2, ensure_ascii=False)
    
    except Exception as e:
        return f"错误: {str(e)}"

@mcp.tool()
def query_columns(filename: str, columns: List[str], filters: Optional[Dict[str, Any]] = None, limit: int = 10) -> str:
    """查询数据集中的特定列
    
    Args:
        filename: 数据集文件名（例如 sample_data.csv）
        columns: 要查询的列名列表
        filters: 过滤条件，格式为 {"column": value} 或 {"column": {"operator": "value"}}
        limit: 返回的最大行数
    """
    try:
        df = load_dataset(filename)
        
        # 验证列名
        invalid_columns = [col for col in columns if col not in df.columns]
        if invalid_columns:
            return f"错误: 列 {invalid_columns} 不存在"
        
        # 应用过滤器
        if filters:
            for column, condition in filters.items():
                if column not in df.columns:
                    return f"错误: 列 '{column}' 不存在"
                
                if isinstance(condition, dict):
                    for op, value in condition.items():
                        if op == "eq":
                            df = df[df[column] == value]
                        elif op == "ne":
                            df = df[df[column] != value]
                        elif op == "gt":
                            df = df[df[column] > value]
                        elif op == "lt":
                            df = df[df[column] < value]
                        elif op == "ge":
                            df = df[df[column] >= value]
                        elif op == "le":
                            df = df[df[column] <= value]
                        elif op == "contains":
                            df = df[df[column].astype(str).str.contains(str(value))]
                        else:
                            return f"错误: 不支持的操作符 '{op}'"
                else:
                    df = df[df[column] == condition]
        
        # 选择指定的列
        df = df[columns]
        
        result = df.head(limit).to_dict(orient="records")
        return json.dumps(result, indent=2, ensure_ascii=False)
    
    except Exception as e:
        return f"错误: {str(e)}"

@mcp.tool()
def get_summary_statistics(filename: str, columns: Optional[List[str]] = None) -> str:
    """获取数据集的统计摘要
    
    Args:
        filename: 数据集文件名（例如 sample_data.csv）
        columns: 要分析的列名列表，如果为空则分析所有数值列
    """
    try:
        df = load_dataset(filename)
        
        if columns:
            invalid_columns = [col for col in columns if col not in df.columns]
            if invalid_columns:
                return f"错误: 列 {invalid_columns} 不存在"
            
            # 只保留指定的列
            df = df[columns]
        
        # 只对数值列进行统计
        numeric_df = df.select_dtypes(include=['number'])
        
        if numeric_df.empty:
            return "没有可以统计的数值列"
        
        # 计算统计摘要
        stats = numeric_df.describe().to_dict()
        
        # 添加额外的统计信息
        for col in numeric_df.columns:
            stats[col]['median'] = numeric_df[col].median()
            stats[col]['mode'] = numeric_df[col].mode()[0] if not numeric_df[col].mode().empty else None
            stats[col]['missing'] = numeric_df[col].isna().sum()
            stats[col]['unique'] = numeric_df[col].nunique()
        
        return json.dumps(stats, indent=2, ensure_ascii=False)
    
    except Exception as e:
        return f"错误: {str(e)}"

@mcp.tool()
def group_by_analysis(filename: str, group_by_column: str, agg_columns: Dict[str, List[str]]) -> str:
    """按指定列分组并聚合数据
    
    Args:
        filename: 数据集文件名（例如 sample_data.csv）
        group_by_column: 用于分组的列名
        agg_columns: 聚合配置，格式为 {"column": ["agg_func1", "agg_func2"]}
                    支持的聚合函数: count, sum, mean, median, min, max, std, var
    """
    try:
        df = load_dataset(filename)
        
        if group_by_column not in df.columns:
            return f"错误: 分组列 '{group_by_column}' 不存在"
        
        # 验证聚合列和函数
        agg_dict = {}
        for col, funcs in agg_columns.items():
            if col not in df.columns:
                return f"错误: 聚合列 '{col}' 不存在"
            
            valid_funcs = []
            for func in funcs:
                if func not in ['count', 'sum', 'mean', 'median', 'min', 'max', 'std', 'var']:
                    return f"错误: 不支持的聚合函数 '{func}'"
                valid_funcs.append(func)
            
            if valid_funcs:
                agg_dict[col] = valid_funcs
        
        if not agg_dict:
            return "错误: 没有有效的聚合配置"
        
        # 执行分组聚合
        result = df.groupby(group_by_column).agg(agg_dict).reset_index()
        
        # 将多级列名转换为单级
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
        
        return result.to_string(index=False)
    
    except Exception as e:
        return f"错误: {str(e)}"

@mcp.tool()
def correlation_analysis(filename: str, columns: Optional[List[str]] = None) -> str:
    """计算数值列之间的相关性
    
    Args:
        filename: 数据集文件名（例如 sample_data.csv）
        columns: 要分析的列名列表，如果为空则分析所有数值列
    """
    try:
        df = load_dataset(filename)
        
        if columns:
            invalid_columns = [col for col in columns if col not in df.columns]
            if invalid_columns:
                return f"错误: 列 {invalid_columns} 不存在"
            
            # 只保留指定的列
            df = df[columns]
        
        # 只对数值列进行相关性分析
        numeric_df = df.select_dtypes(include=['number'])
        
        if numeric_df.empty:
            return "没有可以分析的数值列"
        
        if len(numeric_df.columns) < 2:
            return "至少需要两个数值列才能进行相关性分析"
        
        # 计算相关性矩阵
        corr_matrix = numeric_df.corr()
        
        return corr_matrix.to_string()
    
    except Exception as e:
        return f"错误: {str(e)}"

@mcp.prompt()
def data_exploration_prompt() -> str:
    """数据探索提示"""
    return """
# 数据探索提示

以下是一些常见的数据探索任务和相应的工具使用示例：

## 1. 查看可用的数据集

首先，您可以使用 `datasets` 资源来查看所有可用的数据集：

```
<access_mcp_resource>
<server_name>data-analysis</server_name>
<uri>datasets</uri>
</access_mcp_resource>
```

## 2. 查询特定行

您可以使用 `query_rows` 工具来查询满足特定条件的行：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>query_rows</tool_name>
<arguments>
{
  "filename": "sample_data.csv",
  "filters": {
    "age": {"gt": 30}
  },
  "limit": 5
}
</arguments>
</use_mcp_tool>
```

## 3. 查询特定列

您可以使用 `query_columns` 工具来查询特定列的数据：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>query_columns</tool_name>
<arguments>
{
  "filename": "sample_data.csv",
  "columns": ["name", "age", "salary"],
  "filters": {
    "city": "New York"
  }
}
</arguments>
</use_mcp_tool>
```

## 4. 获取统计摘要

您可以使用 `get_summary_statistics` 工具来获取数据的统计摘要：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>get_summary_statistics</tool_name>
<arguments>
{
  "filename": "sample_data.csv",
  "columns": ["age", "salary"]
}
</arguments>
</use_mcp_tool>
```

## 5. 分组分析

您可以使用 `group_by_analysis` 工具来按特定列分组并聚合数据：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "sales_data.csv",
  "group_by_column": "category",
  "agg_columns": {
    "quantity": ["sum", "mean"],
    "total": ["sum", "mean", "max"]
  }
}
</arguments>
</use_mcp_tool>
```

## 6. 相关性分析

您可以使用 `correlation_analysis` 工具来计算数值列之间的相关性：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>correlation_analysis</tool_name>
<arguments>
{
  "filename": "sample_data.csv",
  "columns": ["age", "salary"]
}
</arguments>
</use_mcp_tool>
```

## 数据分析建议

1. **先探索，后分析**：首先了解数据的结构、类型和基本统计特征，然后再进行深入分析。
2. **关注缺失值和异常值**：这些可能会影响分析结果的准确性。
3. **可视化数据**：虽然当前工具不直接支持可视化，但您可以使用统计结果来描述数据分布和关系。
4. **结合业务背景**：数据分析应该与业务问题相结合，关注对业务有意义的指标和关系。
5. **迭代分析**：根据初步分析结果，调整分析方向和方法，逐步深入。
"""

@mcp.prompt()
def data_cleaning_prompt() -> str:
    """数据清洗提示"""
    return """
# 数据清洗提示

数据清洗是数据分析的重要前提。以下是一些常见的数据清洗任务和相应的工具使用示例：

## 1. 检查缺失值

首先，您可以使用 `get_summary_statistics` 工具来检查数值列的缺失值：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>get_summary_statistics</tool_name>
<arguments>
{
  "filename": "sample_data.csv"
}
</arguments>
</use_mcp_tool>
```

统计结果中的 "missing" 字段显示了每列的缺失值数量。

## 2. 检查异常值

您可以使用 `query_rows` 工具来查找可能的异常值：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>query_rows</tool_name>
<arguments>
{
  "filename": "sample_data.csv",
  "filters": {
    "age": {"gt": 100}  // 查找年龄异常大的记录
  }
}
</arguments>
</use_mcp_tool>
```

## 3. 检查数据一致性

您可以使用 `group_by_analysis` 工具来检查数据的一致性：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "sales_data.csv",
  "group_by_column": "product",
  "agg_columns": {
    "price": ["min", "max"]  // 检查同一产品的价格是否一致
  }
}
</arguments>
</use_mcp_tool>
```

## 数据清洗建议

1. **处理缺失值**：根据数据特点和分析目的，可以选择删除含有缺失值的行、用平均值/中位数/众数填充缺失值，或者使用更复杂的插补方法。
2. **处理异常值**：识别并处理异常值，可以选择删除、替换或单独分析。
3. **标准化数据格式**：确保日期、货币等特殊类型的数据格式一致。
4. **去除重复数据**：识别并处理重复记录。
5. **数据转换**：根据需要进行数据类型转换、标准化或归一化处理。
6. **创建派生变量**：根据原始数据创建对分析有用的新变量。
7. **记录清洗过程**：详细记录数据清洗的每一步，确保过程可追溯和可重复。
"""

@mcp.prompt()
def data_analysis_prompt() -> str:
    """数据分析提示"""
    return """
# 数据分析提示

以下是一些常见的数据分析任务和相应的工具使用示例：

## 1. 描述性统计分析

您可以使用 `get_summary_statistics` 工具来获取数据的描述性统计信息：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>get_summary_statistics</tool_name>
<arguments>
{
  "filename": "sample_data.csv",
  "columns": ["age", "salary"]
}
</arguments>
</use_mcp_tool>
```

## 2. 分组比较分析

您可以使用 `group_by_analysis` 工具来比较不同组的数据特征：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "sample_data.csv",
  "group_by_column": "city",
  "agg_columns": {
    "salary": ["mean", "median", "std", "count"],
    "age": ["mean", "min", "max"]
  }
}
</arguments>
</use_mcp_tool>
```

## 3. 相关性分析

您可以使用 `correlation_analysis` 工具来分析变量之间的关系：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>correlation_analysis</tool_name>
<arguments>
{
  "filename": "sample_data.csv"
}
</arguments>
</use_mcp_tool>
```

## 4. 时间序列分析

对于包含日期/时间的数据，您可以按时间分组进行分析：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "sales_data.csv",
  "group_by_column": "date",
  "agg_columns": {
    "total": ["sum"]
  }
}
</arguments>
</use_mcp_tool>
```

## 5. 分类数据分析

对于分类数据，您可以分析各类别的分布：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "sales_data.csv",
  "group_by_column": "category",
  "agg_columns": {
    "product": ["count"],
    "total": ["sum", "mean"]
  }
}
</arguments>
</use_mcp_tool>
```

## 数据分析建议

1. **明确分析目标**：在开始分析前，明确您想要回答的问题或验证的假设。
2. **从简单到复杂**：先进行简单的描述性统计，再进行更复杂的分析。
3. **关注关键指标**：根据业务背景，确定最重要的指标进行重点分析。
4. **比较分析**：通过分组、分类等方式进行比较分析，发现差异和模式。
5. **寻找关联**：分析变量之间的相关性，但记住"相关不等于因果"。
6. **考虑上下文**：将分析结果放在业务和数据收集的上下文中解释。
7. **提出见解和建议**：基于分析结果，提出有价值的见解和可行的建议。
"""

if __name__ == "__main__":
    # 初始化并运行 server
    mcp.run(transport='stdio')
