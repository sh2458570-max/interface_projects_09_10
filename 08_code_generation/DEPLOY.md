# 08_code_generation 部署说明

## 服务信息

- 目录：`08_code_generation`
- 启动入口：`python app.py`
- 默认端口：`6108`
- 健康检查：`GET /health`
- 主接口：`POST /api/code_generation/generate`

## 依赖

- 必需：Python 虚拟环境、`requirements-all.txt`
- 外部数据库与模型：非必需

## 启动

```bash
cd /srv/interface_projects
source .venv/bin/activate
source deploy/env.sh
cd 08_code_generation
python app.py
```

## 测试

```bash
curl http://127.0.0.1:6108/health
curl -X POST http://127.0.0.1:6108/api/code_generation/generate -H "Content-Type: application/json" -d '{}'
cd /srv/interface_projects
bash test/run_smoke_tests.sh --suites health,contract,codegen --interfaces 08
```

详细公共部署步骤见 [DEPLOYMENT.md](/home/hks/sh/interface_projects/DEPLOYMENT.md)。
