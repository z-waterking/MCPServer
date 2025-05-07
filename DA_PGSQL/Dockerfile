FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 复制requirements.txt（如果有）
COPY requirements.txt* ./

# 如果没有requirements.txt，创建一个包含所需依赖的文件
RUN if [ ! -f requirements.txt ]; then \
    echo "psycopg2-binary==2.9.6\npandas==2.0.0\nnumpy==1.24.3\nfastapi==0.95.1\nuvicorn==0.22.0" > requirements.txt; \
    fi

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 暴露服务端口
EXPOSE 8000

# 启动命令
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]