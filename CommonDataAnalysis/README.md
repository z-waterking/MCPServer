# CommonDataAnalysis MCP 服务器

这是一个用于 CSV 数据分析的 MCP 服务器，提供了一系列工具和资源，用于读取、查询和分析 CSV 格式的数据。

## 功能特点

- 自动读取 `Data` 目录下的所有 CSV 文件
- 将数据作为资源暴露，方便访问
- 提供查询特定行/列的工具
- 支持常见的数据分析任务，如统计摘要、分组分析、相关性分析等
- 包含数据探索、数据清洗和数据分析的提示

## 安装依赖

```bash
pip install -e .
```

或者使用 uv（如果已安装）：

```bash
uv pip install -e .
```

## 运行服务器

```bash
python -m commondataanalysis
```

## 可用资源

### 1. datasets

获取所有可用数据集的列表和基本信息。

```
<access_mcp_resource>
<server_name>data-analysis</server_name>
<uri>datasets</uri>
</access_mcp_resource>
```

### 2. dataset_content

获取指定数据集的完整内容。

```
<access_mcp_resource>
<server_name>data-analysis</server_name>
<uri>dataset_content?filename=sample_data.csv</uri>
</access_mcp_resource>
```

## 可用工具

### 1. query_rows

查询数据集中的特定行。

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

### 2. query_columns

查询数据集中的特定列。

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

### 3. get_summary_statistics

获取数据集的统计摘要。

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

### 4. group_by_analysis

按指定列分组并聚合数据。

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

### 5. correlation_analysis

计算数值列之间的相关性。

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

## 提示

服务器提供了三种提示，帮助用户进行数据分析：

1. **数据探索提示**：如何探索和了解数据集
2. **数据清洗提示**：如何检查和处理数据质量问题
3. **数据分析提示**：如何进行各种类型的数据分析

可以通过以下方式访问这些提示：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>data_exploration_prompt</tool_name>
<arguments>
{}
</arguments>
</use_mcp_tool>
```

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>data_cleaning_prompt</tool_name>
<arguments>
{}
</arguments>
</use_mcp_tool>
```

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>data_analysis_prompt</tool_name>
<arguments>
{}
</arguments>
</use_mcp_tool>
```

## 数据格式

服务器支持标准的 CSV 格式数据。将 CSV 文件放在 `Data` 目录下，服务器会自动识别并加载。
