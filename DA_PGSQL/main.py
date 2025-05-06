# -*- coding: utf-8 -*-
# main.py
from db_manager import DatabaseManager
from tools import register_tools, register_prompts
from mcp.server.fastmcp import FastMCP

def start_service():
    """启动MCP服务"""
    try:
        # 创建MCP实例
        mcp = FastMCP("data-analysis")
        
        db = DatabaseManager()
        db.connect()

        # 注册工具和提示
        register_tools(mcp, db)
        register_prompts(mcp)
        
        print("MCP Service is running...")
        # 启动服务
        mcp.run(transport='stdio')
    except Exception as e:
        print("Service startup failed:", e)
        raise

if __name__ == "__main__":
    start_service()
