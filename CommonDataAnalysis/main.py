from analysis import mcp

def main():
    print("启动数据分析 MCP 服务器...")
    mcp.run(transport='stdio')

if __name__ == "__main__":
    main()
