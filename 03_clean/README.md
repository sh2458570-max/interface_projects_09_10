# 03 clean

主接口：`POST /api/data/clean`

## 上下游

- 上游：`02_upload_split`
- 下游：`04_semantic_chunk`

## 启动

```bash
python app.py
```

默认端口：`6103`

## 说明

- 本项目复制自 `api_02_clean`。
- 项目自带一份 `shared/` 副本，不依赖仓库根目录共享层。
