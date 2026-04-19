# 07 protocol_generate_rules

主接口：

- `POST /api/knowledge/protocol_generate_rules`
- `POST /api/knowledge/protocol_rules/manual_writeback`

## 上下游

- 上游：`04_semantic_chunk`、`06_extract_validate_qa`
- 下游：`08_code_generation`

## 启动

```bash
python app.py
```

默认端口：`6107`

## 说明

- 本项目复制自 `api_03_extract_validate` 的代码副本，并在根入口只暴露规则生成主接口。
- 项目内包含 `code_generate/project_generator` 副本，用于兼容旧文档的规则文件落盘与后续代码生成衔接。
- `protocol_rules/manual_writeback` 用于前端人工审核后，将确认通过的规则直接写回知识图谱；服务端会统一写为 `approved/manual_review`。
