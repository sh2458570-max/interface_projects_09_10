# 06 extract_validate_qa

主接口：`POST /api/knowledge/extract_validate_qa`

## 上下游

- 上游：`05_generate_qa`
- 下游：`07_protocol_generate_rules`、`09_finetune_runtime`

## 启动

```bash
python app.py
```

默认端口：`6106`

## 说明

- 本项目复制自 `api_03_extract_validate` 的代码副本，并在根入口只暴露 QA 抽取校验主接口。
- 由于原始实现与规则生成链路同仓，项目内同时携带一份代码生成器资产副本，避免依赖外部共享目录。
