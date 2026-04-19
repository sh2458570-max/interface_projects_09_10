# 03_clean 部署说明

## 服务信息

- 目录：`03_clean`
- 启动入口：`python app.py`
- 默认端口：`6103`
- 健康检查：`GET /health`
- 主接口：`POST /api/data/clean`

## 依赖

- 必需：Python 虚拟环境、`requirements-all.txt`
- 外部数据库：非强制

## 启动

```bash
cd /srv/interface_projects
source .venv/bin/activate
source deploy/env.sh
cd 03_clean
python app.py
```

## 测试

```bash
curl http://127.0.0.1:6103/health
cd /srv/interface_projects
bash test/run_smoke_tests.sh --suites health --interfaces 03
```

详细公共部署步骤见 [DEPLOYMENT.md](/home/hks/sh/interface_projects/DEPLOYMENT.md)。
