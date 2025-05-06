"""
数据库管理模块
提供PostgreSQL数据库连接和查询功能
"""
import asyncio
from typing import Dict, List, Any, Optional, Union
import asyncpg
import pandas as pd
from io import StringIO

class DatabaseManager:
    """PostgreSQL数据库管理器"""
    
    def __init__(self, user: str, password: str, host: str, port: int, database: str):
        """初始化数据库连接参数"""
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.pool = None
    
    async def connect(self):
        """创建数据库连接池"""
        try:
            self.pool = await asyncpg.create_pool(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database
            )
            print("数据库连接池创建成功")
        except Exception as e:
            print(f"数据库连接失败: {str(e)}")
            raise
    
    async def disconnect(self):
        """关闭数据库连接池"""
        if self.pool:
            await self.pool.close()
            print("数据库连接池已关闭")
    
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """执行SQL查询并返回结果"""
        if not self.pool:
            raise Exception("数据库未连接")
        
        # 将params字典转换为位置参数
        query_params = []
        if params:
            # 替换查询中的命名参数为位置参数
            param_count = 1
            for key, value in params.items():
                placeholder = f":{key}"
                if placeholder in query:
                    query = query.replace(placeholder, f"${param_count}")
                    query_params.append(value)
                    param_count += 1
        
        async with self.pool.acquire() as conn:
            # 执行查询
            records = await conn.fetch(query, *query_params)
            
            # 将结果转换为字典列表
            result = [dict(record) for record in records]
            return result
    
    async def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """获取表结构信息"""
        if not self.pool:
            raise Exception("数据库未连接")
        
        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
        """
        
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query, table_name)
            return [dict(record) for record in records]
    
    async def list_tables(self) -> List[str]:
        """获取所有表名"""
        if not self.pool:
            raise Exception("数据库未连接")
        
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """
        
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query)
            return [record['table_name'] for record in records]
    
    async def execute_csv_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """执行查询并返回CSV格式结果"""
        result = await self.execute_query(query, params)
        if not result:
            return ""
        
        # 使用pandas将结果转换为CSV
        df = pd.DataFrame(result)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        return csv_buffer.getvalue()
    
    async def filter_data(self, table_name: str, conditions: Dict[str, Any]) -> List[Dict[str, Any]]:
        """根据条件筛选表数据"""
        if not self.pool:
            raise Exception("数据库未连接")
        
        # 构建WHERE子句
        where_clauses = []
        params = []
        param_count = 1
        
        for column, value in conditions.items():
            where_clauses.append(f"{column} = ${param_count}")
            params.append(value)
            param_count += 1
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "TRUE"
        query = f"SELECT * FROM {table_name} WHERE {where_clause}"
        
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query, *params)
            return [dict(record) for record in records]