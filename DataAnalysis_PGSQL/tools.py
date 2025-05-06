"""
MCP工具函数模块
定义并注册MCP工具函数，供MCP服务调用
"""
import mcp.tool
from typing import Dict, List, Any, Optional, Union
from fastapi import FastAPI
from db_manager import DatabaseManager
from utils import compare_query_results, analyze_data

def register_tools(app: FastAPI, db_manager: DatabaseManager):
    """注册MCP工具函数"""
    
    @mcp.tool
    async def execute_sql_query(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        执行SQL查询并返回结果
        
        Args:
            query: SQL查询语句
            params: 查询参数 (可选)
            
        Returns:
            查询结果列表
        
        示例:
            execute_sql_query("SELECT * FROM users WHERE age > :min_age", {"min_age": 18})
        """
        try:
            result = await db_manager.execute_query(query, params)
            return result
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool
    async def get_table_info(table_name: str) -> Dict[str, Any]:
        """
        获取表的结构信息
        
        Args:
            table_name: 表名
            
        Returns:
            表结构信息
        
        示例:
            get_table_info("users")
        """
        try:
            schema = await db_manager.get_table_schema(table_name)
            return {"table": table_name, "schema": schema}
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool
    async def list_database_tables() -> List[str]:
        """
        列出数据库中的所有表
        
        Returns:
            表名列表
        
        示例:
            list_database_tables()
        """
        try:
            tables = await db_manager.list_tables()
            return tables
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool
    async def filter_table_data(table_name: str, conditions: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        根据条件筛选表数据
        
        Args:
            table_name: 表名
            conditions: 筛选条件，格式为 {"列名": 值}
            
        Returns:
            满足条件的数据行
        
        示例:
            filter_table_data("users", {"department": "IT", "is_active": True})
        """
        try:
            result = await db_manager.filter_data(table_name, conditions)
            return result
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool
    async def compare_queries(query1: str, query2: str, 
                              params1: Optional[Dict[str, Any]] = None, 
                              params2: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        比较两个查询的结果
        
        Args:
            query1: 第一个SQL查询
            query2: 第二个SQL查询
            params1: 第一个查询的参数 (可选)
            params2: 第二个查询的参数 (可选)
            
        Returns:
            比较结果
        
        示例:
            compare_queries(
                "SELECT * FROM sales WHERE year = :year1", 
                "SELECT * FROM sales WHERE year = :year2",
                {"year1": 2023}, 
                {"year2": 2024}
            )
        """
        try:
            result1 = await db_manager.execute_query(query1, params1)
            result2 = await db_manager.execute_query(query2, params2)
            comparison = compare_query_results(result1, result2)
            return comparison
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool
    async def analyze_query_result(query: str, 
                                  params: Optional[Dict[str, Any]] = None,
                                  analysis_type: str = "summary") -> Dict[str, Any]:
        """
        分析查询结果
        
        Args:
            query: SQL查询语句
            params: 查询参数 (可选)
            analysis_type: 分析类型 (summary, correlation, aggregation)
            
        Returns:
            分析结果
        
        示例:
            analyze_query_result("SELECT * FROM sales", analysis_type="summary")
        """
        try:
            result = await db_manager.execute_query(query, params)
            analysis = analyze_data(result, analysis_type)
            return analysis
        except Exception as e:
            return {"error": str(e)}
    
    @mcp.tool
    async def get_query_as_csv(query: str, params: Optional[Dict[str, Any]] = None) -> str:
        """
        执行查询并返回CSV格式结果
        
        Args:
            query: SQL查询语句
            params: 查询参数 (可选)
            
        Returns:
            CSV格式的查询结果
        
        示例:
            get_query_as_csv("SELECT * FROM users WHERE department = :dept", {"dept": "IT"})
        """
        try:
            csv_data = await db_manager.execute_csv_query(query, params)
            return csv_data
        except Exception as e:
            return f"Error: {str(e)}"
    
    print("已注册MCP工具函数")