"""
MCP服务主应用入口
提供MCP服务，连接PostgreSQL数据库，提供数据查询和分析功能
"""
import mcp.tool
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Any, Optional, Union

from db_manager import DatabaseManager
from tools import register_tools
from utils import compare_query_results, analyze_data
from config import DATABASE_CONFIG

# 创建FastAPI应用
app = FastAPI(title="MCP数据分析服务")

# 创建数据库管理器实例
db_manager = DatabaseManager(
    user=DATABASE_CONFIG["user"],
    password=DATABASE_CONFIG["password"],
    host=DATABASE_CONFIG["host"],
    port=DATABASE_CONFIG["port"],
    database=DATABASE_CONFIG["database"]
)

# 请求模型定义
class QueryRequest(BaseModel):
    query: str
    params: Optional[Dict[str, Any]] = None

class CompareRequest(BaseModel):
    query1: str
    query2: str
    params1: Optional[Dict[str, Any]] = None
    params2: Optional[Dict[str, Any]] = None

class AnalysisRequest(BaseModel):
    query: str
    params: Optional[Dict[str, Any]] = None
    analysis_type: str = "summary"  # summary, correlation, aggregation

@app.on_event("startup")
async def startup():
    # 连接数据库
    await db_manager.connect()
    # 注册MCP工具
    register_tools(app, db_manager)

@app.on_event("shutdown")
async def shutdown():
    # 关闭数据库连接
    await db_manager.disconnect()

# 基本数据查询API
@app.post("/api/query")
async def execute_query(request: QueryRequest):
    try:
        result = await db_manager.execute_query(request.query, request.params)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# 比较两次查询结果API
@app.post("/api/compare")
async def compare_queries(request: CompareRequest):
    try:
        result1 = await db_manager.execute_query(request.query1, request.params1)
        result2 = await db_manager.execute_query(request.query2, request.params2)
        comparison = compare_query_results(result1, result2)
        return {"status": "success", "comparison": comparison}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# 数据分析API
@app.post("/api/analyze")
async def analyze_query_result(request: AnalysisRequest):
    try:
        result = await db_manager.execute_query(request.query, request.params)
        analysis = analyze_data(result, analysis_type=request.analysis_type)
        return {"status": "success", "analysis": analysis}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# 健康检查API
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# 在独立的异步运行环境中启动
def start_service():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    start_service()