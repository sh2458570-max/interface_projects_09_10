# 测试目录说明

本目录统一存放 `interface_projects` 的测试脚本和测试数据。

## 目录结构

- `run_smoke_tests.py`：主测试脚本
- `run_smoke_tests.sh`：Shell 包装脚本
- `data/`：样例 XML、规则文件、端口配置
- `output/`：测试输出目录

## 当前测试能力

### 1. 健康检查

对所有接口的 `GET /health` 发起请求，确认服务已启动。

### 2. 合同检查

对部分接口发送空 JSON，验证服务已正确挂载且必填参数校验生效。

当前覆盖：

- 接口 6
- 接口 7
- 接口 8
- 接口 10

### 3. 接口 8 功能测试

使用 `data/codegen/` 中的最小 XML、规则和端口配置，调用：

- `POST /api/code_generation/generate`

### 4. 接口 10 功能测试

使用同一批 XML 与规则，调用：

- `POST /api/knowledge/rule_evaluate`

默认启用回退评估模式，不强依赖模型。
