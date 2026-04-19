# 05_generate_qa 部署说明

## 服务信息

- 目录：`05_generate_qa`
- 启动入口：`python app.py`
- 默认端口：`6105`
- 健康检查：`GET /health`
- 主接口：`POST /api/knowledge/generate_qa`

## 依赖

- 必需：Python 虚拟环境、`requirements-all.txt`
- 必需：LLM 或 vLLM
- 推荐：MySQL

## 启动

```bash
cd /srv/interface_projects
source .venv/bin/activate
source deploy/env.sh
cd 05_generate_qa
python app.py
```

## 测试

```bash
curl http://127.0.0.1:6105/health
cd /srv/interface_projects
bash test/run_smoke_tests.sh --suites health --interfaces 05
```

详细公共部署步骤见 [DEPLOYMENT.md](/home/hks/sh/interface_projects/DEPLOYMENT.md)。
