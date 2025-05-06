# MCP 数据分析服务

这是一个基于MCP (Model, Controller, Processor)架构的数据分析服务，连接PostgreSQL数据库，提供数据查询和分析功能。该服务通过`mcp.tool`暴露各种工具函数，以便于在MCP环境中使用。

## 功能特点

- 连接PostgreSQL数据库（端口5432）
- 提供数据查询、分析和比较功能
- 通过MCP工具暴露数据查询和分析函数
- 支持多种数据分析方式（摘要统计、相关性分析、聚合分析）
- 支持两次查询结果的比较
- 支持CSV格式数据导出

## 文件结构

```
mcp_data_service/
├── app.py              # 主应用入口
├── db_manager.py       # 数据库连接和查询功能
├── tools.py            # MCP工具函数定义
├── utils.py            # 实用工具函数（数据处理、比较等）
├── config.py           # 配置文件
└── README.md           # 项目说明
```

## 安装依赖

在使用本服务前，请确保安装了以下依赖：

```bash
pip install fastapi uvicorn asyncpg pandas numpy scipy mcp
```

## 配置

服务配置可以通过环境变量设置，主要配置项包括：

- 数据库连接信息：
  - `DB_USER`: 数据库用户名（默认：postgres）
  - `DB_PASSWORD`: 数据库密码（默认：postgres）
  - `DB_HOST`: 数据库主机（默认：localhost）
  - `DB_PORT`: 数据库端口（默认：5432）
  - `DB_NAME`: 数据库名称（默认：postgres）

- 应用配置：
  - `APP_HOST`: 应用主机（默认：0.0.0.0）
  - `APP_PORT`: 应用端口（默认：8000）
  - `DEBUG`: 是否开启调试模式（默认：False）

- 日志配置：
  - `LOG_LEVEL`: 日志级别（默认：INFO）
  - `LOG_FILE`: 日志文件（默认：mcp_service.log）

- 安全配置：
  - `API_KEY_REQUIRED`: 是否需要API密钥（默认：False）
  - `API_KEY`: API密钥值

## 运行服务

启动服务：

```bash
python app.py
```

或者使用uvicorn：

```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

## API接口

### 1. 数据查询

```
POST /api/query
```

请求体：
```json
{
  "query": "SELECT * FROM users WHERE age > :min_age",
  "params": {
    "min_age": 18
  }
}
```

### 2. 比较两次查询结果

```
POST /api/compare
```

请求体：
```json
{
  "query1": "SELECT * FROM sales WHERE year = :year1",
  "query2": "SELECT * FROM sales WHERE year = :year2",
  "params1": {
    "year1": 2023
  },
  "params2": {
    "year2": 2024
  }
}
```

### 3. 数据分析

```
POST /api/analyze
```

请求体：
```json
{
  "query": "SELECT * FROM sales",
  "params": {},
  "analysis_type": "summary"
}
```

`analysis_type`可选值：
- `summary`: 基本摘要统计
- `correlation`: 相关性分析
- `aggregation`: 聚合分析

## MCP工具函数

本服务通过`mcp.tool`提供了以下工具函数：

1. `execute_sql_query`: 执行SQL查询并返回结果
2. `get_table_info`: 获取表的结构信息
3. `list_database_tables`: 列出数据库中的所有表
4. `filter_table_data`: 根据条件筛选表数据
5. `compare_queries`: 比较两个查询的结果
6. `analyze_query_result`: 分析查询结果
7. `get_query_as_csv`: 执行查询并返回CSV格式结果

## SQL查询示例

以下是一些常见的SQL查询示例：

### 基本查询
```sql
SELECT * FROM table_name LIMIT 10;
```

### 带条件的查询
```sql
SELECT * FROM table_name WHERE column_name > :value LIMIT 10;
```

### 聚合查询
```sql
SELECT category, COUNT(*) as count, AVG(value) as average 
FROM table_name 
GROUP BY category;
```

### 表连接查询
```sql
SELECT a.id, a.name, b.value 
FROM table_a a
JOIN table_b b ON a.id = b.a_id
WHERE b.value > :min_value;
```

### 子查询
```sql
SELECT * FROM table_name
WHERE value > (SELECT AVG(value) FROM table_name);
```

### 窗口函数
```sql
SELECT 
    id, 
    category, 
    value,
    RANK() OVER (PARTITION BY category ORDER BY value DESC) as rank
FROM table_name;
```

## 使用示例

### 1. 基本数据查询

```python
import requests

response = requests.post(
    "http://localhost:8000/api/query",
    json={
        "query": "SELECT * FROM employees WHERE department = :dept",
        "params": {"dept": "IT"}
    }
)
data = response.json()
print(data)
```

### 2. 分析数据

```python
import requests

response = requests.post(
    "http://localhost:8000/api/analyze",
    json={
        "query": "SELECT * FROM sales WHERE year = 2023",
        "analysis_type": "summary"
    }
)
analysis = response.json()
print(analysis)
```

### 3. 比较两年销售数据

```python
import requests

response = requests.post(
    "http://localhost:8000/api/compare",
    json={
        "query1": "SELECT * FROM sales WHERE year = 2023",
        "query2": "SELECT * FROM sales WHERE year = 2024"
    }
)
comparison = response.json()
print(comparison)
```

## 注意事项

1. 确保PostgreSQL服务已经启动，且配置的用户有足够的权限
2. 对于大型数据集，分析操作可能需要较长时间
3. 对于敏感操作，建议启用API密钥验证

## 许可证

MIT许可证