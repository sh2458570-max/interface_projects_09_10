# 09_finetune_runtime 部署说明

## 服务信息

- 目录：`09_finetune_runtime`
- 启动入口：`python app.py`
- 默认端口：`6109`
- 健康检查：`GET /health`
- 主接口：`POST /api/model/finetune/action`

## 依赖

- 必需：Python 虚拟环境、`requirements-all.txt`
- 强烈建议：GPU
- 必需：基础模型权重
- 推荐：MySQL

## 启动

```bash
cd /srv/interface_projects
source .venv/bin/activate
source deploy/env.sh
cd 09_finetune_runtime
python app.py
```

## 测试

```bash
curl http://127.0.0.1:6109/health
cd /srv/interface_projects
bash test/run_smoke_tests.sh --suites health --interfaces 09
```

详细公共部署步骤见 [DEPLOYMENT.md](/home/hks/sh/interface_projects/DEPLOYMENT.md)。
