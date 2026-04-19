# 04 semantic_chunk

主接口：`POST /api/data/semantic_chunk`

## 上下游

- 上游：`03_clean`
- 下游：`05_generate_qa`、`07_protocol_generate_rules`

## 启动

```bash
python app.py
```

默认端口：`6104`

## 说明

- 本项目复制自 `api_06_semantic_chunk`。
- 主接口响应中包含 `doc_index`，与接口文档口径一致。
