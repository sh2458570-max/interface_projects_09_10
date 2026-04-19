# Python 协议转换项目生成器

## 用法

```bash
python -m project_generator build --protocol-dir input/protocols --mappings input/mappings.json --output output/demo_project
```

联合转换模式：

```bash
python -m project_generator build --protocol-dir input/protocols --mappings input/mappings.json --choreography input/choreography.json --output output/demo_project
```

## 输入说明

- XML: 协议结构与 `MessCode` 序列
- `mappings.json`: 字段公式、运行时抓取策略、端口配置
- `choreography.json`: 联合转换目标窗口、时序矩阵、缓存发送策略

## 当前能力

- 解析 `Item/StructMess/Field/Group/MessCode`
- 生成 `*_def.h`、`codec.*`、`messageconvert.*`、映射文件、`main.cpp`、`config.xml`、`peach.pro`
- 联合模式生成 `to_code_Choreography.*`
- `codec.cpp` 按 AST 递归生成分支和循环读写逻辑
- `process_method / message_name / display_name / cache_name / cache_num` 支持自动推导或默认补齐
