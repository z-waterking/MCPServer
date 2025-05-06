# tools.py
# encoding: utf-8
from typing import List, Dict, Optional
from db_manager import DatabaseManager

def register_tools(mcp, db):
    """注册MCP工具"""
    @mcp.tool("list_tables")
    def list_tables_tool() -> List[str]:
        """
        列出数据库中的所有表
        
        返回:
            数据库中所有表的列表
        """
        return db.list_tables()
    
    @mcp.tool("get_table_schema")
    def get_table_schema_tool(table_name: str) -> List[Dict[str, str]]:
        """
        获取指定表的结构信息
        
        参数:
            table_name: 表名
            
        返回:
            表结构信息，包含列名、数据类型等
        """
        return db.get_table_schema(table_name)
    
    @mcp.tool("get_table_sample")
    def get_table_sample_tool(table_name: str, limit: int = 10) -> List[Dict]:
        """
        获取指定表的样本数据
        
        参数:
            table_name: 表名
            limit: 返回的最大行数，默认10行
            
        返回:
            表的样本数据
        """
        return db.get_table_data(table_name, limit)
    
    @mcp.tool("run_query")
    def run_query_tool(query: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        执行自定义SQL查询
        
        参数:
            query: SQL查询语句
            params: 查询参数，用于参数化查询
            
        返回:
            查询结果
        """
        return db.run_custom_query(query, params)
    
    @mcp.tool("get_summary_statistics")
    def summary_stats_tool(table_name: str, columns: Optional[List[str]] = None) -> Dict:
        """
        获取表中数值列的统计摘要
        
        参数:
            table_name: 表名
            columns: 要分析的列名列表，如果为空则分析所有数值列
            
        返回:
            统计摘要，包括均值、中位数、标准差等
        """
        return db.get_summary_statistics(table_name, columns)
    
    @mcp.tool("analyze_correlations")
    def correlations_tool(table_name: str, columns: Optional[List[str]] = None) -> Dict:
        """
        分析表中数值列之间的相关性
        
        参数:
            table_name: 表名
            columns: 要分析的列名列表，如果为空则分析所有数值列
            
        返回:
            相关性矩阵
        """
        return db.analyze_correlations(table_name, columns)
    
    @mcp.tool("group_by_analysis")
    def group_by_tool(
        table_name: str, 
        group_column: str, 
        agg_columns: Dict[str, List[str]]
    ) -> List[Dict]:
        """
        按指定列分组并聚合数据
        
        参数:
            table_name: 表名
            group_column: 用于分组的列名
            agg_columns: 聚合配置，格式为 {"column": ["agg_func1", "agg_func2"]}
                        支持的聚合函数: count, sum, mean, median, min, max, std, var
            
        返回:
            分组聚合结果
        """
        return db.group_by_analysis(table_name, group_column, agg_columns)
    
    @mcp.tool("time_series_analysis")
    def time_series_tool(
        table_name: str, 
        time_column: str, 
        value_column: str,
        interval: str = "month"
    ) -> Dict:
        """
        时间序列分析
        
        参数:
            table_name: 表名
            time_column: 时间列名
            value_column: 值列名
            interval: 时间间隔 (day, week, month, quarter, year)
            
        返回:
            时间序列分析结果
        """
        return db.time_series_analysis(
            table_name, time_column, value_column, interval
        )
    
    @mcp.tool("detect_anomalies")
    def anomalies_tool(table_name: str, column: str, method: str = "zscore") -> List[Dict]:
        """
        检测数据异常值
        
        参数:
            table_name: 表名
            column: 列名
            method: 异常检测方法 (zscore, iqr)
            
        返回:
            异常值及其信息
        """
        return db.detect_anomalies(table_name, column, method)

def register_prompts(mcp):
    """注册常见数据分析任务的提示"""
    # 基本SQL查询指南
    mcp.prompt("basic_sql_guide", """
    以下是在PostgreSQL中执行基本SQL查询的指南:
    
    1. 查询表中所有数据:
       SELECT * FROM table_name LIMIT 10;
    
    2. 筛选数据:
       SELECT * FROM table_name WHERE condition;
       例如: SELECT * FROM sales WHERE amount > 1000;
    
    3. 聚合函数:
       SELECT column, COUNT(*), SUM(amount), AVG(price)
       FROM table_name
       GROUP BY column;
    
    4. 连接表:
       SELECT a.*, b.column_name
       FROM table_a a
       JOIN table_b b ON a.id = b.id;
    
    5. 排序数据:
       SELECT * FROM table_name ORDER BY column_name [ASC|DESC];
    
    您可以使用run_query工具执行这些SQL查询。
    """)

    # 数据分析任务建议
    mcp.prompt("data_analysis_tasks", """
    常见数据分析任务建议:
    
    1. 探索性数据分析
       - 使用get_table_schema了解数据结构
       - 使用get_table_sample获取样本数据
       - 使用get_summary_statistics获取统计摘要
    
    2. 相关性分析
       - 使用analyze_correlations查找列之间的关系
    
    3. 分组分析
       - 使用group_by_analysis按类别分析数据
    
    4. 时间序列分析
       - 使用time_series_analysis分析时间趋势
    
    5. 异常值检测
       - 使用detect_anomalies发现数据异常
    
    每个工具都有对应的参数说明，可以根据需要灵活使用。
    """)
