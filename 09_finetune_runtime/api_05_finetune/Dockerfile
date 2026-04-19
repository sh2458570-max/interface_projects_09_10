# ============ Dockerfile 内容开始 ============

# 第一阶段：构建阶段（可选，用于优化镜像大小）
# 如果你需要编译某些依赖，可以使用多阶段构建
# 这里我们直接使用运行时镜像

# 选择基础镜像
# 方案A：普通Python镜像（如果没有GPU）
#FROM python:3.10-slim

# 方案B：如果使用GPU，使用CUDA镜像（二选一，不要同时使用）
FROM nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04
# 注意：如果你不确定是否需要GPU，先用普通镜像测试

# 设置环境变量
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 设置工作目录
WORKDIR /app

# 安装系统依赖（根据你的需求调整）
# 如果你的训练需要编译某些包（如tokenizers），需要安装gcc等
RUN apt-get update && apt-get install -y \
    git \
    curl \
    wget \
    build-essential \
    # 清理缓存，减小镜像大小
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
# 使用清华镜像源加速下载（国内服务器推荐）
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 或者使用阿里云镜像源
# RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# 复制应用代码
# 注意：不要复制不必要的文件，可以使用 .dockerignore
COPY . .

# 创建必要的目录
RUN mkdir -p /app/models \
    && mkdir -p /app/logs

# 设置模型存储目录为数据卷（方便持久化存储）
VOLUME ["/app/models", "/app/logs"]

# 暴露端口（与Flask应用中的端口一致）
EXPOSE 5000

# 健康检查（可选）
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# 启动命令
# 方案A：直接运行app.py
CMD ["python", "app.py"]

# 方案B：使用gunicorn运行（生产环境推荐）
# 先安装gunicorn：pip install gunicorn
# CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "app:app"]

# 方案C：使用shell脚本启动（更灵活）
# COPY docker-entrypoint.sh .
# RUN chmod +x docker-entrypoint.sh
# ENTRYPOINT ["./docker-entrypoint.sh"]

# ============ Dockerfile 内容结束 ============