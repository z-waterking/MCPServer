import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from psycopg2 import pool
from contextlib import contextmanager
# PostgreSQL连接配置
# 全局数据库连接对象

class DatabaseManager:
    def __init__(self):
        """初始化数据库连接配置"""
        # self.DB_CONFIG = {
        #     "host": "localhost",
        #     "port": 5432,
        #     "database": os.environ.get("DB_NAME", "postgres"),
        #     "user": os.environ.get("DB_USER", "postgres"),
        #     "password": os.environ.get("DB_PASSWORD", "973366"),
        # }
        self._connection = None
        self._connection_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,  # 控制最大连接数
            host="localhost",
            port=5432,
            database=os.environ.get("DB_NAME", "postgres"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", "973366"),
        )
        # self.connect()

    @contextmanager
    def get_cursor(self, cursor_factory = None):
        """获取数据库游标的上下文管理器"""
        conn = self._connection_pool.getconn()
        try:
            if cursor_factory is not None:
                cursor = conn.cursor(cursor_factory)
            else:
                cursor = conn.cursor()
            yield cursor
        finally:
            cursor.close()
            if not conn.closed:
                self._connection_pool.putconn(conn)

    def list_tables(self) -> List[str]:
        """列出数据库中的所有表"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """)
                tables = [row[0] for row in cursor.fetchall()]
                return tables
        except Exception as e:
            print("Get table list failed:", e)
            return []

    def get_table_data(self, table_name: str, limit: int = 100) -> List[Dict]:
        """获取指定表的数据"""
        try:
            with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
                data = cursor.fetchall()
                return [dict(row) for row in data]
        except Exception as e:
            print("Get table data failed:", e)
            return []

    def run_custom_query(
        self, 
        query: str, 
        params: Optional[Dict] = None
    ) -> List[Dict]:
        """
        执行自定义SQL查询
        
        参数:
            connection: 数据库连接对象
            query: SQL查询语句
            params: 查询参数，用于参数化查询
            
        返回:
            查询结果列表
        """
        try:
            with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params or {})
                data = cursor.fetchall()
                return [dict(row) for row in data]
        except Exception as e:
            print(f"执行查询失败: {e}")
            return [{"error": str(e)}]

    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """
        获取指定表的结构信息
        
        参数:
            connection: 数据库连接对象
            table_name: 表名
            
        返回:
            表结构信息，包含列名、数据类型等
        """
        try:
            with self.get_cursor() as cursor:
                # 获取表结构信息
                cursor.execute("""
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable, 
                        column_default
                    FROM 
                        information_schema.columns
                    WHERE 
                        table_name = %s
                    ORDER BY 
                        ordinal_position;
                """, (table_name,))
                
                columns = []
                for row in cursor.fetchall():
                    columns.append({
                        "column_name": row[0],
                        "data_type": row[1],
                        "is_nullable": row[2],
                        "default_value": row[3]
                    })
                
                return columns
        except Exception as e:
            print(f"获取表结构失败: {e}")
            return [{"error": str(e)}]

    def get_numeric_columns(self, table_name: str) -> List[str]:
        """
        获取表中的数值类型列
        
        参数:
            connection: 数据库连接对象
            table_name: 表名
            
        返回:
            数值类型列名列表
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    AND data_type IN ('integer', 'bigint', 'smallint', 'decimal', 'numeric', 
                                    'real', 'double precision', 'float');
                """, (table_name,))
                
                numeric_columns = [row[0] for row in cursor.fetchall()]
                return numeric_columns
        except Exception as e:
            print(f"获取数值列失败: {e}")
            return []

    def get_date_columns(self, table_name: str) -> List[str]:
        """
        获取表中的日期类型列
        
        参数:
            connection: 数据库连接对象
            table_name: 表名
            
        返回:
            日期类型列名列表
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    AND data_type IN ('date', 'timestamp', 'timestamp without time zone', 
                                    'timestamp with time zone');
                """, (table_name,))
                
                date_columns = [row[0] for row in cursor.fetchall()]
                return date_columns
        except Exception as e:
            print(f"获取日期列失败: {e}")
            return []

    def get_summary_statistics(
        self, 
        table_name: str, 
        columns: Optional[List[str]] = None
    ) -> Dict:
        """
        获取表中数值列的统计摘要
        
        参数:
            connection: 数据库连接对象
            table_name: 表名
            columns: 要分析的列名列表，如果为空则分析所有数值列
            
        返回:
            统计摘要，包括均值、中位数、标准差等
        """
        try:
            # 如果未指定列，获取所有数值列
            if not columns:
                columns = self.get_numeric_columns(table_name)
            
            if not columns:
                return {"error": "未找到数值列"}
            
            result = {}
            
            for column in columns:
                with self.get_cursor() as cursor:
                    cursor.execute(f"""
                        SELECT
                            COUNT({column}) as count,
                            AVG({column}) as mean,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column}) as median,
                            MIN({column}) as min,
                            MAX({column}) as max,
                            STDDEV({column}) as std,
                            VAR_POP({column}) as variance
                        FROM {table_name}
                        WHERE {column} IS NOT NULL;
                    """)
                    
                    stats = cursor.fetchone()
                    if stats:
                        result[column] = {
                            "count": int(stats[0]),
                            "mean": float(stats[1]) if stats[1] is not None else None,
                            "median": float(stats[2]) if stats[2] is not None else None,
                            "min": float(stats[3]) if stats[3] is not None else None,
                            "max": float(stats[4]) if stats[4] is not None else None,
                            "std": float(stats[5]) if stats[5] is not None else None,
                            "variance": float(stats[6]) if stats[6] is not None else None
                        }
            return result
        except Exception as e:
            print(f"获取统计摘要失败: {e}")
            return {"error": str(e)}

    def analyze_correlations(
        self, 
        table_name: str, 
        columns: Optional[List[str]] = None
    ) -> Dict:
        """
        分析表中数值列之间的相关性
        
        参数:
            connection: 数据库连接对象
            table_name: 表名
            columns: 要分析的列名列表，如果为空则分析所有数值列
            
        返回:
            相关性矩阵
        """
        try:
            # 如果未指定列，获取所有数值列
            if not columns:
                columns = self.get_numeric_columns(table_name)
            
            if not columns or len(columns) < 2:
                return {"error": "至少需要两个数值列进行相关性分析"}
            
            # 构建数据框
            query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE "
            query += " AND ".join([f"{col} IS NOT NULL" for col in columns])
            
            with self.get_cursor() as cursor:
                cursor.execute(query)
                data = cursor.fetchall()
            
                # 计算相关性矩阵
                df = pd.DataFrame(data, columns=columns)
                corr_matrix = df.corr().round(4).to_dict()
                
                return corr_matrix
        except Exception as e:
            print(f"相关性分析失败: {e}")
            return {"error": str(e)}
        finally:
            self._connection_pool.putconn(self._connection)

    def group_by_analysis(
        self, 
        table_name: str, 
        group_column: str, 
        agg_columns: Dict[str, List[str]]
    ) -> List[Dict]:
        """
        按指定列分组并聚合数据
        
        参数:
            connection: 数据库连接对象
            table_name: 表名
            group_column: 用于分组的列名
            agg_columns: 聚合配置，格式为 {"column": ["agg_func1", "agg_func2"]}
                        支持的聚合函数: count, sum, mean, median, min, max, std, var
            
        返回:
            分组聚合结果
        """
        try:
            # 验证分组列
            with self.get_cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s AND column_name = %s;
                """, (table_name, group_column))
                
                if not cursor.fetchone():
                    return [{"error": f"分组列 '{group_column}' 不存在"}]
            
                # 构建聚合查询
                agg_exprs = []
                for col, funcs in agg_columns.items():
                    for func in funcs:
                        if func == "count":
                            agg_exprs.append(f"COUNT({col}) as {col}_{func}")
                        elif func == "sum":
                            agg_exprs.append(f"SUM({col}) as {col}_{func}")
                        elif func == "mean":
                            agg_exprs.append(f"AVG({col}) as {col}_{func}")
                        elif func == "median":
                            agg_exprs.append(f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {col}) as {col}_{func}")
                        elif func == "min":
                            agg_exprs.append(f"MIN({col}) as {col}_{func}")
                        elif func == "max":
                            agg_exprs.append(f"MAX({col}) as {col}_{func}")
                        elif func == "std":
                            agg_exprs.append(f"STDDEV({col}) as {col}_{func}")
                        elif func == "var":
                            agg_exprs.append(f"VAR_POP({col}) as {col}_{func}")
                        else:
                            return [{"error": f"不支持的聚合函数: '{func}'"}]
                
                query = f"""
                    SELECT {group_column}, {', '.join(agg_exprs)}
                    FROM {table_name}
                    GROUP BY {group_column}
                    ORDER BY {group_column};
                """
            
            with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
                # 执行查询
                cursor.execute(query)
                result = [dict(row) for row in cursor.fetchall()]
            
                return result
        except Exception as e:
            print(f"分组分析失败: {e}")
            return [{"error": str(e)}]

    def time_series_analysis(
        self, 
        table_name: str, 
        time_column: str, 
        value_column: str,
        interval: str = "month"
    ) -> Dict:
        """
        时间序列分析
        
        参数:
            connection: 数据库连接对象
            table_name: 表名
            time_column: 时间列名
            value_column: 值列名
            interval: 时间间隔 (day, week, month, quarter, year)
            
        返回:
            时间序列分析结果
        """
        try:
            # 验证时间列
            date_columns = self.get_date_columns(table_name)
            if time_column not in date_columns:
                return {"error": f"时间列 '{time_column}' 不是有效的日期类型列"}
            
            # 验证值列
            numeric_columns = self.get_numeric_columns(table_name)
            if value_column not in numeric_columns:
                return {"error": f"值列 '{value_column}' 不是有效的数值类型列"}
            
            # 确定时间分组格式
            time_format = {
                "day": f"DATE_TRUNC('day', {time_column})",
                "week": f"DATE_TRUNC('week', {time_column})",
                "month": f"DATE_TRUNC('month', {time_column})",
                "quarter": f"DATE_TRUNC('quarter', {time_column})",
                "year": f"DATE_TRUNC('year', {time_column})"
            }.get(interval.lower())
            
            if not time_format:
                return {"error": f"不支持的时间间隔: '{interval}'"}
            
            # 按时间间隔聚合数据
            query = f"""
                SELECT 
                    {time_format} as time_period,
                    COUNT(*) as count,
                    AVG({value_column}) as mean,
                    MIN({value_column}) as min,
                    MAX({value_column}) as max,
                    STDDEV({value_column}) as std
                FROM {table_name}
                WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL
                GROUP BY time_period
                ORDER BY time_period;
            """
            with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                time_series_data = [dict(row) for row in cursor.fetchall()]
                
                # 格式化日期
                for item in time_series_data:
                    if isinstance(item['time_period'], datetime):
                        item['time_period'] = item['time_period'].isoformat()
                
                # 计算趋势分析
                trends = {}
                if len(time_series_data) > 1:
                    values = [item['mean'] for item in time_series_data]
                    
                    # 计算整体趋势
                    first_value = values[0]
                    last_value = values[-1]
                    change = last_value - first_value
                    percent_change = (change / first_value * 100) if first_value != 0 else float('inf')
                    
                    trends = {
                        "overall_change": round(change, 2),
                        "percent_change": round(percent_change, 2),
                        "trend_direction": "up" if change > 0 else "down" if change < 0 else "stable"
                    }
                
                result = {
                    "time_series_data": time_series_data,
                    "trends": trends
                }
                
                return result
        except Exception as e:
            print(f"时间序列分析失败: {e}")
            return {"error": str(e)}

    def detect_anomalies(
        self, 
        table_name: str, 
        column: str, 
        method: str = "zscore"
    ) -> List[Dict]:
        """
        检测数据异常值
        
        参数:
            connection: 数据库连接对象
            table_name: 表名
            column: 列名
            method: 异常检测方法 (zscore, iqr)
            
        返回:
            异常值及其信息
        """
        try:
            # 验证列是否为数值类型
            numeric_columns = self.get_numeric_columns(table_name)
            if column not in numeric_columns:
                return [{"error": f"列 '{column}' 不是有效的数值类型列"}]
            
            # 获取所有行
            with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
            
                cursor.execute(f"SELECT * FROM {table_name} WHERE {column} IS NOT NULL;")
                all_data = [dict(row) for row in cursor.fetchall()]
                
                # 获取列值
                column_values = [float(row[column]) for row in all_data]
                
                anomalies = []
                
                if method.lower() == "zscore":
                    # Z-score方法
                    mean = np.mean(column_values)
                    std = np.std(column_values)
                    threshold = 3.0  # 标准阈值，通常为3
                    
                    for i, value in enumerate(column_values):
                        z_score = (value - mean) / std if std > 0 else 0
                        if abs(z_score) > threshold:
                            anomalies.append({
                                "row": all_data[i],
                                "value": value,
                                "z_score": round(z_score, 2),
                                "distance_from_mean": round(value - mean, 2)
                            })
                
                elif method.lower() == "iqr":
                    # IQR方法
                    q1 = np.percentile(column_values, 25)
                    q3 = np.percentile(column_values, 75)
                    iqr = q3 - q1
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    
                    for i, value in enumerate(column_values):
                        if value < lower_bound or value > upper_bound:
                            anomalies.append({
                                "row": all_data[i],
                                "value": value,
                                "bounds": {"lower": round(lower_bound, 2), "upper": round(upper_bound, 2)},
                                "distance_from_bound": round(min(abs(value - lower_bound), abs(value - upper_bound)), 2)
                            })
                else:
                    return [{"error": f"不支持的异常检测方法: '{method}'"}]
                
                return anomalies
        except Exception as e:
            print(f"异常值检测失败: {e}")
            return [{"error": str(e)}]