"""
配置文件
存储MCP服务的配置信息
"""
import os
from typing import Dict, Any

# 数据库配置
DATABASE_CONFIG = {
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "postgres"),
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "database": os.environ.get("DB_NAME", "postgres"),
}

# 应用配置
APP_CONFIG = {
    "host": os.environ.get("APP_HOST", "0.0.0.0"),
    "port": int(os.environ.get("APP_PORT", 8000)),
    "debug": os.environ.get("DEBUG", "False").lower() == "true",
}

# MCP工具配置
MCP_CONFIG = {
    "tool_prefix": os.environ.get("MCP_TOOL_PREFIX", "mcp_data_analysis"),
    "tool_description": "MCP数据分析服务，提供数据查询和分析功能"
}

# 日志配置
LOG_CONFIG = {
    "level": os.environ.get("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": os.environ.get("LOG_FILE", "mcp_service.log"),
}

# 安全配置
SECURITY_CONFIG = {
    "api_key_required": os.environ.get("API_KEY_REQUIRED", "False").lower() == "true",
    "api_key": os.environ.get("API_KEY", "your_api_key_here"),
}

# 常用SQL查询模板
SQL_TEMPLATES = {
    "basic_select": "SELECT * FROM {table} LIMIT {limit};",
    "filtered_select": "SELECT * FROM {table} WHERE {condition} LIMIT {limit};",
    "aggregation": "SELECT {group_by}, {agg_func}({agg_column}) FROM {table} GROUP BY {group_by};",
    "join": "SELECT a.*, b.* FROM {table_a} a JOIN {table_b} b ON a.{key_a} = b.{key_b};",
    "time_series": "SELECT time_column, value FROM {table} WHERE time_column BETWEEN :start_time AND :end_time ORDER BY time_column;"
}

# 数据分析配置
ANALYSIS_CONFIG = {
    "default_analysis_type": "summary",
    "correlation_threshold": 0.5,
    "outlier_detection_method": "iqr",
    "max_categories": 10,
    "max_rows_for_detailed_analysis": 10000
}