# 接口独立项目总览

本目录按 `接口文档.docx` 与当前实现口径，将现有接口整理为独立项目副本。

## 设计原则

- 每个目录对应一个主接口项目
- 每个项目保留自己的代码副本和 `shared/`，不依赖仓库根目录共享层
- 原仓库 `api_*` 与 `shared/` 保留，作为研发主基座
- 本目录用于独立交付、独立启动和按上下游关系阅读

## 上下游关系

1. `01_validate_protocol_files`
2. `02_upload_split`
3. `03_clean`
4. `04_semantic_chunk`
5. `05_generate_qa`
6. `06_extract_validate_qa`
7. `07_protocol_generate_rules`
8. `08_code_generation`
9. `09_finetune_runtime`
10. `10_rule_evaluate`

主流程：

```text
validate_protocol_files
  -> upload_split
  -> clean
  -> semantic_chunk
  -> generate_qa
  -> extract_validate_qa
  -> protocol_generate_rules
  -> code_generation

protocol_generate_rules
  -> rule_evaluation
```

## 说明

- `09_finetune_runtime` 为模型微调运行时项目，主接口为 `POST /api/model/finetune/action`。
- `10_rule_evaluate` 为独立规则级评估项目，主接口为 `POST /api/knowledge/rule_evaluate`。
- `06_extract_validate_qa`、`07_protocol_generate_rules` 由于源实现同属 `api_03_extract_validate`，各自携带一份完整代码副本，但项目根入口只暴露对应主接口。

## 部署与测试

- 总部署文档：[DEPLOYMENT.md](/home/hks/sh/interface_projects/DEPLOYMENT.md)
- 统一测试目录：[test](/home/hks/sh/interface_projects/test)
- 部署辅助脚本目录：[deploy](/home/hks/sh/interface_projects/deploy)

每个接口目录下均已补充独立的 `DEPLOY.md`。
