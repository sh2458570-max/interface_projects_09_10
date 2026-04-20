# k1.6 -> k1.7 示例

这个示例直接使用仓库根目录下的 `K/` 作为协议 XML 输入目录，重点展示：

- `k1.6.xml -> k1.7.xml` 的字段级映射
- `Field` 分支按顺序展开
- `Group` 循环按 `max` 展开；没有 `max` 时按 1 次展开

## 生成命令

```bash
python -m project_generator build --protocol-dir K --mappings project_generator/examples/k1_6_to_k1_7/mappings.json --output out_k1_6_to_k1_7
```

## 示例数据

- 示例输入：`sample_input.json`
- 期望输出：`expected_output.json`

说明：

- `expected_output.json` 只列出本例显式映射的目标字段。
- 其他未映射字段保持目标协议默认值 `0`。
