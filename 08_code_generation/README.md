# 08 code_generation

主接口：`POST /api/code_generation/generate`

## 上下游

- 上游：`07_protocol_generate_rules`
- 下游：无

## 启动

```bash
python app.py
```

默认端口：`6108`

## 说明

- 本项目为独立代码生成项目，直接复用本地 `project_generator` 生成 Qt/C++ 协议转换工程。
- 项目不依赖仓库根目录的 `shared/` 或 `code_generate/`，内部已携带所需副本。
