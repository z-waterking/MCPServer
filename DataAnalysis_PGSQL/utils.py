"""
实用工具函数
提供数据处理、比较和分析功能
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union

def compare_query_results(result1: List[Dict[str, Any]], 
                          result2: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    比较两个查询结果的差异
    
    Args:
        result1: 第一个查询结果
        result2: 第二个查询结果
        
    Returns:
        比较结果信息
    """
    # 转换为DataFrame进行比较
    df1 = pd.DataFrame(result1) if result1 else pd.DataFrame()
    df2 = pd.DataFrame(result2) if result2 else pd.DataFrame()
    
    # 基本统计信息
    comparison = {
        "result1_rows": len(df1),
        "result2_rows": len(df2),
        "row_count_diff": len(df1) - len(df2),
    }
    
    # 列比较
    if not df1.empty and not df2.empty:
        columns1 = set(df1.columns)
        columns2 = set(df2.columns)
        
        comparison.update({
            "common_columns": list(columns1.intersection(columns2)),
            "only_in_result1": list(columns1 - columns2),
            "only_in_result2": list(columns2 - columns1),
        })
        
        # 比较共有列的数值统计
        common_columns = columns1.intersection(columns2)
        stats_comparison = {}
        
        for col in common_columns:
            # 跳过非数值列
            if pd.api.types.is_numeric_dtype(df1[col]) and pd.api.types.is_numeric_dtype(df2[col]):
                stats_comparison[col] = {
                    "mean_diff": float(df1[col].mean() - df2[col].mean()),
                    "median_diff": float(df1[col].median() - df2[col].median()),
                    "max_diff": float(df1[col].max() - df2[col].max()),
                    "min_diff": float(df1[col].min() - df2[col].min()),
                    "std_diff": float(df1[col].std() - df2[col].std()) if len(df1) > 1 and len(df2) > 1 else None,
                }
                
        if stats_comparison:
            comparison["numerical_comparisons"] = stats_comparison
        
        # 唯一值比较（对于少量数据）
        value_comparisons = {}
        for col in common_columns:
            if len(df1) <= 1000 and len(df2) <= 1000:  # 限制在较小的数据集上进行
                unique1 = set(df1[col].astype(str).unique())
                unique2 = set(df2[col].astype(str).unique())
                
                value_comparisons[col] = {
                    "unique_values_result1": len(unique1),
                    "unique_values_result2": len(unique2),
                    "common_values": len(unique1.intersection(unique2)),
                    "only_in_result1": len(unique1 - unique2),
                    "only_in_result2": len(unique2 - unique1),
                }
        
        if value_comparisons:
            comparison["value_comparisons"] = value_comparisons
    
    return comparison

def analyze_data(data: List[Dict[str, Any]], analysis_type: str = "summary") -> Dict[str, Any]:
    """
    分析数据并返回结果
    
    Args:
        data: 要分析的数据
        analysis_type: 分析类型 (summary, correlation, aggregation)
        
    Returns:
        分析结果
    """
    # 转换为DataFrame进行分析
    df = pd.DataFrame(data) if data else pd.DataFrame()
    
    if df.empty:
        return {"error": "没有数据可供分析"}
    
    analysis_results = {"row_count": len(df), "column_count": len(df.columns)}
    
    # 基本摘要统计
    if analysis_type == "summary":
        # 检测数值列
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        categorical_columns = df.select_dtypes(exclude=['number']).columns.tolist()
        
        analysis_results["column_types"] = {
            "numeric": numeric_columns,
            "categorical": categorical_columns
        }
        
        # 数值列统计
        if numeric_columns:
            numeric_stats = {}
            for col in numeric_columns:
                try:
                    numeric_stats[col] = {
                        "mean": float(df[col].mean()),
                        "median": float(df[col].median()),
                        "std": float(df[col].std()) if len(df) > 1 else None,
                        "min": float(df[col].min()),
                        "max": float(df[col].max()),
                        "null_count": int(df[col].isna().sum()),
                        "null_percentage": float(df[col].isna().mean() * 100),
                    }
                except Exception as e:
                    numeric_stats[col] = {"error": str(e)}
                    
            analysis_results["numeric_stats"] = numeric_stats
        
        # 类别列统计
        if categorical_columns:
            categorical_stats = {}
            for col in categorical_columns:
                # 仅对字符串类型进行频率分析
                if df[col].dtype == 'object':
                    try:
                        value_counts = df[col].value_counts().head(10).to_dict()  # 取前10个最常见值
                        categorical_stats[col] = {
                            "unique_count": int(df[col].nunique()),
                            "null_count": int(df[col].isna().sum()),
                            "top_values": value_counts,
                            "null_percentage": float(df[col].isna().mean() * 100),
                        }
                    except Exception as e:
                        categorical_stats[col] = {"error": str(e)}
                        
            if categorical_stats:
                analysis_results["categorical_stats"] = categorical_stats
    
    # 相关性分析
    elif analysis_type == "correlation":
        # 仅对数值列计算相关系数
        numeric_df = df.select_dtypes(include=['number'])
        if not numeric_df.empty and len(numeric_df.columns) > 1:
            try:
                corr_matrix = numeric_df.corr().fillna(0).round(4)
                
                # 找出高相关性对
                high_correlations = []
                for i in range(len(corr_matrix.columns)):
                    for j in range(i+1, len(corr_matrix.columns)):
                        col1 = corr_matrix.columns[i]
                        col2 = corr_matrix.columns[j]
                        corr_value = corr_matrix.iloc[i, j]
                        
                        if abs(corr_value) > 0.5:  # 仅显示相关系数绝对值大于0.5的
                            high_correlations.append({
                                "col1": col1, 
                                "col2": col2, 
                                "correlation": float(corr_value)
                            })
                
                # 按相关系数绝对值降序排序
                high_correlations.sort(key=lambda x: abs(x["correlation"]), reverse=True)
                
                # 返回相关性信息
                analysis_results["correlation_matrix"] = corr_matrix.to_dict()
                analysis_results["high_correlations"] = high_correlations
            except Exception as e:
                analysis_results["correlation_error"] = str(e)
        else:
            analysis_results["correlation_error"] = "没有足够的数值列进行相关性分析"
    
    # 聚合分析
    elif analysis_type == "aggregation":
        analysis_results["aggregation"] = {}
        
        # 识别可能的分组列（优先考虑非数值类型的列）
        categorical_columns = df.select_dtypes(exclude=['number']).columns.tolist()
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        
        if categorical_columns and numeric_columns:
            # 对每个分类列进行聚合
            for cat_col in categorical_columns[:3]:  # 限制为前3个分类列
                try:
                    agg_results = {}
                    for num_col in numeric_columns[:5]:  # 限制为前5个数值列
                        # 计算每个分组的基本统计量
                        agg_df = df.groupby(cat_col)[num_col].agg(['count', 'mean', 'median', 'min', 'max'])
                        agg_df = agg_df.reset_index()
                        agg_df.columns = [cat_col, f"{num_col}_count", f"{num_col}_mean", 
                                        f"{num_col}_median", f"{num_col}_min", f"{num_col}_max"]
                        
                        # 转换为字典列表
                        agg_results[num_col] = agg_df.to_dict(orient='records')
                    
                    analysis_results["aggregation"][cat_col] = agg_results
                except Exception as e:
                    analysis_results["aggregation"][cat_col] = {"error": str(e)}
    
    return analysis_results

def detect_outliers(data: List[Dict[str, Any]], column: str, method: str = "iqr") -> List[int]:
    """
    检测数据中的异常值
    
    Args:
        data: 要分析的数据
        column: 要检测的列名
        method: 检测方法 (iqr, zscore)
        
    Returns:
        异常值的索引列表
    """
    df = pd.DataFrame(data)
    if column not in df.columns or not pd.api.types.is_numeric_dtype(df[column]):
        return []
    
    outlier_indices = []
    
    if method == "iqr":
        # IQR方法（四分位间距法）
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        # 查找异常值（小于Q1-1.5*IQR或大于Q3+1.5*IQR）
        outlier_indices = df.index[(df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR))].tolist()
    
    elif method == "zscore":
        # Z-score方法
        from scipy import stats
        z_scores = stats.zscore(df[column].dropna())
        abs_z_scores = np.abs(z_scores)
        outlier_indices = df.dropna(subset=[column]).index[abs_z_scores > 3].tolist()
    
    return outlier_indices

def generate_sql_examples() -> Dict[str, Dict[str, str]]:
    """
    生成常见SQL查询示例
    
    Returns:
        SQL查询示例字典
    """
    return {
        "basic_select": {
            "description": "基本查询示例",
            "query": "SELECT * FROM table_name LIMIT 10;"
        },
        "filtered_select": {
            "description": "带条件的查询",
            "query": "SELECT * FROM table_name WHERE column_name > :value LIMIT 10;"
        },
        "aggregation": {
            "description": "聚合查询",
            "query": "SELECT category, COUNT(*) as count, AVG(value) as average FROM table_name GROUP BY category;"
        },
        "join": {
            "description": "表连接查询",
            "query": """
                SELECT a.id, a.name, b.value 
                FROM table_a a
                JOIN table_b b ON a.id = b.a_id
                WHERE b.value > :min_value;
            """
        },
        "subquery": {
            "description": "子查询示例",
            "query": """
                SELECT * FROM table_name
                WHERE value > (SELECT AVG(value) FROM table_name);
            """
        },
        "window_function": {
            "description": "窗口函数",
            "query": """
                SELECT 
                    id, 
                    category, 
                    value,
                    RANK() OVER (PARTITION BY category ORDER BY value DESC) as rank
                FROM table_name;
            """
        }
    }