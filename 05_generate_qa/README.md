# 05 generate_qa

主接口：`POST /api/knowledge/generate_qa`

## 上下游

- 上游：`04_semantic_chunk`
- 下游：`06_extract_validate_qa`、`09_finetune_runtime`

## 启动

```bash
python app.py
```

默认端口：`6105`

## 说明

- 本项目复制自 `api_04_generate_qa`。
- 项目自带一份 `shared/` 副本，不依赖仓库根目录共享层。
- 标准输入应来自上游接口沉淀后的 `block/chunk` 结果，例如 `source_block_ids`、`target_source_ids`、`dataset_id`。
- 文件上传与解析在更前面的接口完成；接口5不直接接文件路径，PDF 仅是上游可能处理的一种原始文件类型。
