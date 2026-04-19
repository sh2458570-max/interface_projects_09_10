# 02 upload_split

主接口：`POST /api/data/upload_split`

## 上下游

- 上游：`01_validate_protocol_files`
- 下游：`03_clean`

## 启动

```bash
python app.py
```

默认端口：`6102`

## 说明

- 本项目复制自 `api_01_upload_split`，并在根入口只暴露上传拆分接口。
- 项目自带一份 `shared/` 副本，不依赖仓库根目录共享层。
