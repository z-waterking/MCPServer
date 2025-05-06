# db_manager.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

class DatabaseManager:
    def __init__(self):
        """初始化数据库连接配置"""
        self.DB_CONFIG = {
            "host": "localhost",
            "port": 5432,
            "database": os.environ.get("DB_NAME", "postgres"),
            "user": os.environ.get("DB_USER", "postgres"),
            "password": os.environ.get("DB_PASSWORD", "973366"),
        }
        self._connection = None
        self.connect()

    def __enter__(self):
        """支持上下文管理协议"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文时自动关闭连接"""
        self.close()

    def connect(self):
        """建立数据库连接"""
        try:
            self._connection = psycopg2.connect(
                host=self.DB_CONFIG["host"],
                port=self.DB_CONFIG["port"],
                database=self.DB_CONFIG["database"],
                user=self.DB_CONFIG["user"],
                password=self.DB_CONFIG["password"]
            )
            print("PostgreSQL Database connected successfully")
        except Exception as e:
            print("Failed to connect to PostgreSQL Database", e)
            raise

    def _ensure_connection(self):
        """确保连接有效性"""
        if self._connection is None or self._connection.closed:
            self.connect()

    def close(self):
        """关闭数据库连接"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            print("Database connection closed")

    def list_tables(self) -> List[str]:
        """获取所有表名单"""
        self._ensure_connection()
        try:
            with self._connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """)
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print("获取表列表失败:", e)
            return []

    def get_table_schema(self, table_name: str) -> List[Dict]:
        """获取表结构信息"""
        self._ensure_connection()
        try:
            with self._connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable, 
                        column_default
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """, (table_name,))
                return [{
                    "column_name": row[0],
                    "data_type": row[1],
                    "is_nullable": row[2],
                    "default_value": row[3]
                } for row in cursor.fetchall()]
        except Exception as e:
            print(f"获取表结构失败: {e}")
            return [{"error": str(e)}]

    def get_table_sample(self, table_name: str, limit: int = 10) -> List[Dict]:
        """获取表样本数据"""
        self._ensure_connection()
        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT %s;", (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"获取样本数据失败: {e}")
            return [{"error": str(e)}]

    def run_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """执行自定义查询"""
        self._ensure_connection()
        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or {})
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"执行查询失败: {e}")
            return [{"error": str(e)}]

    # 其他方法保持类似结构，使用self._connection进行数据库操作
    # 包括总结统计、相关性分析、分组分析等方法...
    # 示例其中几个核心方法的实现：

    def get_summary_stats(self, table_name: str, columns: List[str] = None) -> Dict:
        """获取统计摘要（示例方法）"""
        self._ensure_connection()
        try:
            numeric_cols = columns or self._get_numeric_columns(table_name)
            if not numeric_cols:
                return {"error": "未找到数值列"}
            
            results = {}
            for col in numeric_cols:
                with self._connection.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT 
                            AVG({col}), 
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col}),
                            STDDEV({col})
                        FROM {table_name};
                    """)
                    stats = cursor.fetchone()
                    results[col] = {
                        "mean": stats[0],
                        "median": stats[1],
                        "std_dev": stats[2]
                    }
            return results
        except Exception as e:
            print(f"统计摘要获取失败: {e}")
            return {"error": str(e)}

    def _get_numeric_columns(self, table_name: str) -> List[str]:
        """辅助方法：获取数值型列"""
        with self._connection.cursor() as cursor:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND data_type IN (
                    'integer', 'bigint', 'numeric', 
                    'real', 'double precision'
                );
            """, (table_name,))
            return [row[0] for row in cursor.fetchall()]

# 使用示例
if __name__ == "__main__":
    with DatabaseManager() as db:
        print("数据库表列表:", db.list_tables())
        print("users表结构:", db.get_table_schema("users"))
        print("orders表样本:", db.get_table_sample("orders", 5))
