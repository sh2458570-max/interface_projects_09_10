# 09 finetune_runtime

主接口：`POST /api/model/finetune/action`

## 上下游

- 上游：`05_generate_qa`、`06_extract_validate_qa`
- 下游：无

## 启动

```bash
python app.py
```

默认端口：`6109`

## 说明

- 本项目是模型微调运行时的自包含版本，提供启动、暂停、终止、流式监控和模型下载接口。
- 主路由为 `/api/model/finetune/action`，并兼容旧路径 `/api/finetune/job/*`。
- 规则级评估接口已拆分到独立目录 `10_rule_evaluate`，不再由本项目承载。
