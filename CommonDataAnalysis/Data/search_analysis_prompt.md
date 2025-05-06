# 搜索数据分析提示

本文档提供了针对搜索指标数据（search_metrics.csv）的分析提示，帮助您利用MCP服务器进行深入分析。

## 数据概述

该数据集包含不同国家、不同APP（Chrome, Safari, Sapphire和EdgeMobile）上的Rewards用户和非Rewards用户进行在线搜索时，使用的Query所属的类目的统计值，包括：

- srpv (Search Results Page Views): 搜索结果页面浏览量
- RPM (Revenue Per Mille): 每千次展示收入
- clicks: 点击次数
- ctr (Click-Through Rate): 点击率

## 分析任务1：总体概括数据

### 1.1 获取数据集基本信息

```
<access_mcp_resource>
<server_name>data-analysis</server_name>
<uri>datasets</uri>
</access_mcp_resource>
```

### 1.2 获取数据统计摘要

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>get_summary_statistics</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "columns": ["srpv", "rpm", "clicks", "ctr"]
}
</arguments>
</use_mcp_tool>
```

### 1.3 按国家分组分析

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "country",
  "agg_columns": {
    "srpv": ["sum", "mean"],
    "rpm": ["mean", "max", "min"],
    "clicks": ["sum", "mean"],
    "ctr": ["mean"]
  }
}
</arguments>
</use_mcp_tool>
```

### 1.4 按APP分组分析

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "app",
  "agg_columns": {
    "srpv": ["sum", "mean"],
    "rpm": ["mean", "max", "min"],
    "clicks": ["sum", "mean"],
    "ctr": ["mean"]
  }
}
</arguments>
</use_mcp_tool>
```

### 1.5 按用户类型分组分析

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "user_type",
  "agg_columns": {
    "srpv": ["sum", "mean"],
    "rpm": ["mean", "max", "min"],
    "clicks": ["sum", "mean"],
    "ctr": ["mean"]
  }
}
</arguments>
</use_mcp_tool>
```

### 1.6 按查询类别分组分析

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "query_category",
  "agg_columns": {
    "srpv": ["sum", "mean"],
    "rpm": ["mean", "max", "min"],
    "clicks": ["sum", "mean"],
    "ctr": ["mean"]
  }
}
</arguments>
</use_mcp_tool>
```

## 分析任务2：国家和APP的对比分析

### 2.1 国家和APP的交叉分析

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "country",
  "agg_columns": {
    "srpv": ["sum"]
  }
}
</arguments>
</use_mcp_tool>
```

### 2.2 特定国家的APP对比

例如，分析美国的不同APP：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>query_rows</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "filters": {
    "country": "US"
  }
}
</arguments>
</use_mcp_tool>
```

然后按APP分组：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "app",
  "agg_columns": {
    "srpv": ["sum", "mean"],
    "rpm": ["mean"],
    "clicks": ["sum"],
    "ctr": ["mean"]
  }
}
</arguments>
</use_mcp_tool>
```

### 2.3 特定APP在不同国家的对比

例如，分析Chrome在不同国家的表现：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>query_rows</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "filters": {
    "app": "Chrome"
  }
}
</arguments>
</use_mcp_tool>
```

然后按国家分组：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "country",
  "agg_columns": {
    "srpv": ["sum", "mean"],
    "rpm": ["mean"],
    "clicks": ["sum"],
    "ctr": ["mean"]
  }
}
</arguments>
</use_mcp_tool>
```

## 分析任务3：Rewards用户和非Rewards用户的对比

### 3.1 用户类型在不同国家的对比

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "user_type",
  "agg_columns": {
    "rpm": ["mean"],
    "ctr": ["mean"]
  }
}
</arguments>
</use_mcp_tool>
```

### 3.2 用户类型在不同APP的对比

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>query_rows</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "filters": {
    "app": "Chrome"
  }
}
</arguments>
</use_mcp_tool>
```

然后按用户类型分组：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "user_type",
  "agg_columns": {
    "srpv": ["sum", "mean"],
    "rpm": ["mean"],
    "clicks": ["sum"],
    "ctr": ["mean"]
  }
}
</arguments>
</use_mcp_tool>
```

## 分析任务4：相关性分析

### 4.1 指标间的相关性

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>correlation_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "columns": ["srpv", "rpm", "clicks", "ctr"]
}
</arguments>
</use_mcp_tool>
```

## 分析任务5：多维度组合分析

### 5.1 特定国家特定APP的用户类型对比

例如，分析美国Chrome上的Rewards和非Rewards用户：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>query_rows</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "filters": {
    "country": "US",
    "app": "Chrome"
  }
}
</arguments>
</use_mcp_tool>
```

然后按用户类型分组：

```
<use_mcp_tool>
<server_name>data-analysis</server_name>
<tool_name>group_by_analysis</tool_name>
<arguments>
{
  "filename": "search_metrics.csv",
  "group_by_column": "user_type",
  "agg_columns": {
    "srpv": ["sum"],
    "rpm": ["mean"],
    "clicks": ["sum"],
    "ctr": ["mean"]
  }
}
</arguments>
</use_mcp_tool>
```

## 分析建议

1. **关注RPM差异**：Rewards用户和非Rewards用户之间的RPM差异是一个关键指标，反映了用户价值的差异。

2. **分析CTR模式**：不同APP和不同国家的CTR模式可能反映出用户行为的差异。

3. **查询类别偏好**：分析不同国家和不同APP用户对查询类别的偏好。

4. **流量分布**：通过srpv分析流量在不同国家、不同APP之间的分布情况。

5. **综合比较**：将多个维度结合起来进行分析，例如特定国家特定APP上的用户类型对比。
