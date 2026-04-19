# 07_protocol_generate_rules 部署说明

## 服务信息

- 目录：`07_protocol_generate_rules`
- 启动入口：`python app.py`
- 默认端口：`6107`
- 健康检查：`GET /health`
- 主接口：`POST /api/knowledge/protocol_generate_rules`
- 写回接口：`POST /api/knowledge/protocol_rules/manual_writeback`

## 依赖

- 必需：Python 虚拟环境、`requirements-all.txt`
- 必需：LLM 或 vLLM
- 推荐：Neo4j
- 推荐：MySQL
- 可选：本地 JSON 图谱回退

## 启动

```bash
cd /srv/interface_projects
source .venv/bin/activate
source deploy/env.sh
cd 07_protocol_generate_rules
python app.py
```

## 测试

```bash
curl http://127.0.0.1:6107/health
curl -X POST http://127.0.0.1:6107/api/knowledge/protocol_generate_rules -H "Content-Type: application/json" -d '{}'
cd /srv/interface_projects
bash test/run_smoke_tests.sh --suites health,contract --interfaces 07
```

详细公共部署步骤见 [DEPLOYMENT.md](/home/hks/sh/interface_projects/DEPLOYMENT.md)。
