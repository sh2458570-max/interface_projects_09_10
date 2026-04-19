# 10 rule_evaluate

主接口：`POST /api/knowledge/rule_evaluate`

## 上下游

- 上游：`07_protocol_generate_rules`
- 下游：无

## 启动

```bash
python app.py
```

默认端口：`6110`

## 说明

- 本项目是规则级评估接口的自包含版本，直接评估接口 7 输出的 `conversion_rules`。
- 评估输入只依赖源协议 XML 目录、目标协议 XML 目录和规则 JSON，不依赖接口 8 生成出的工程。
- 评估过程包含粗召回、精排序、结构校验和综合评分，输出字段匹配正确率、语义保真度、结构完整性、量纲物理一致性转换准确率、字段覆盖率和最终转换率。
