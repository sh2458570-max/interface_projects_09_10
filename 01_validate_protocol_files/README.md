# 01 validate_protocol_files

主接口：`POST /api/data/validate_protocol_files`

## 上下游

- 上游：无
- 下游：`02_upload_split`

## 启动

```bash
python app.py
```

默认端口：`6101`

## 说明

- 本项目复制自 `api_01_upload_split`，并在根入口只暴露文件校验接口。
- 项目自带一份 `shared/` 副本，不依赖仓库根目录共享层。
