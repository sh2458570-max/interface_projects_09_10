# 02_upload_split 部署说明

## 服务信息

- 目录：`02_upload_split`
- 启动入口：`python app.py`
- 默认端口：`6102`
- 健康检查：`GET /health`
- 主接口：`POST /api/data/upload_split`

## 依赖

- 必需：Python 虚拟环境、`requirements-all.txt`
- 推荐：MySQL
- 可选：SQLite 回退

## 启动

```bash
cd /srv/interface_projects
source .venv/bin/activate
source deploy/env.sh
cd 02_upload_split
python app.py
```

## 测试

```bash
curl http://127.0.0.1:6102/health
cd /srv/interface_projects
bash test/run_smoke_tests.sh --suites health --interfaces 02
```

详细公共部署步骤见 [DEPLOYMENT.md](/home/hks/sh/interface_projects/DEPLOYMENT.md)。
