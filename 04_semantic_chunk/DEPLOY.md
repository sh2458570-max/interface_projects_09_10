# 04_semantic_chunk 部署说明

## 服务信息

- 目录：`04_semantic_chunk`
- 启动入口：`python app.py`
- 默认端口：`6104`
- 健康检查：`GET /health`
- 主接口：`POST /api/data/semantic_chunk`

## 依赖

- 必需：Python 虚拟环境、`requirements-all.txt`
- 必需：Embedding 模型、`pymilvus` 或 `milvus-lite`
- 推荐：MySQL、vLLM

## 启动

```bash
cd /srv/interface_projects
source .venv/bin/activate
source deploy/env.sh
cd 04_semantic_chunk
python app.py
```

## 测试

```bash
curl http://127.0.0.1:6104/health
cd /srv/interface_projects
bash test/run_smoke_tests.sh --suites health --interfaces 04
```

详细公共部署步骤见 [DEPLOYMENT.md](/home/hks/sh/interface_projects/DEPLOYMENT.md)。
