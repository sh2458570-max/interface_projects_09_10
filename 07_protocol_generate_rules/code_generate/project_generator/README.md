# 协议转换项目生成器

这是一个 Python 命令行生成器，用于根据协议 XML、转换公式 `mappings.json`、联合编排 `choreography.json` 生成 Qt/C++ 协议转换工程。

## 文档

- `输入输出接口规格说明.md`：面向外部集成的完整输入输出接口规格，定义 4 类输入、成功输出、失败输出、工程产物和兼容关系。
- `输入说明与现有工程配置对照.md`：对照当前仓库实现与 `newB/newC/newD` 工程，说明旧版配置项如何落在现有文件中。

> 当前代码实现仍兼容 `mappings.json + choreography.json + runtime.endpoints` 这套输入方式；`输入输出接口规格说明.md` 描述的是对外收敛后的目标输入输出契约。

## 命令

```bash
python -m project_generator build --protocol-dir input/protocols --mappings input/mappings.json --output output/project
```

联合转换模式：

```bash
python -m project_generator build --protocol-dir input/protocols --mappings input/mappings.json --choreography input/choreography.json --output output/project
```

## 当前实现

- XML 解析支持 `Dimen`、`Item`、`StructMess`、`Field`、`Group`、`MessCode`
- 同时输出两套模型：
  - AST：用于生成带分支/循环的 `codec.cpp`
  - 平铺字段清单：用于 `*_def.h`、映射校验和 manifest
- `mappings.json` 除字段公式外，还支持运行时配置：
  - 顶层 `runtime.endpoints`
  - 顶层 `runtime.loop_sleep_ms`
  - 顶层 `runtime.check_data_interval_ms`
  - 顶层 `runtime.protocol_verifies`
  - 每条 conversion 的 `runtime.sources[].fetches / response_enabled / send_mode`
  - 命名类字段 `runtime.process_method / runtime.sources[].message_name / runtime.sources[].display_name / runtime.cache_name / runtime.cache_num` 可省略，按 `newB` 风格或默认值自动补齐
- 联合模式支持：
  - `to_code_Choreography.*`
  - 目标缓存窗口
  - 多源时序矩阵
  - `messageconvert.cpp` 中的联合判定与缓存发送
- 内建三套完整 reference profile：
  - `newB`
  - `newC`
  - `newD`
  在 `mappings.json` 顶层 `runtime.reference_profile` 指定后，生成器会直接输出与现有项目一致的完整实现文件

## 产物

- `main.cpp`
- `config.xml`
- `peach.pro`
- `messageconvert.h/.cpp`
- `codec.h/.cpp`
- `*_def.h`
- `xxx_to_yyy.h/.cpp`
- `to_code_Choreography.h/.cpp`（联合模式）
- `protocol_manifest.json`

## 示例

- 简单转换：`project_generator/examples/simple`
- 联合转换：`project_generator/examples/joint`
- K 协议映射示例：`project_generator/examples/k1_6_to_k1_7`

## `protocol_verifies` 输入

用于按协议生成 `Verify...Seq(...)`、`checkObjMaps(...)` 和应答分支状态机。
其中命名类字段支持省略，生成器会按 `newB` 风格自动推导：

- `constraints[].name` 省略时自动生成为 `Constraint1`、`Constraint2` ...
- `verify_rules[].name` 省略时自动生成为 `verify1`、`verify2` ...
- `response_actions[].on_verify` 省略时按顺序引用对应的 `verify_rules`

运行时命名字段同样支持省略：

- `conversions[].runtime.process_method` 省略时，单源默认 `{源协议名}dataPro`，多源默认 `{源1}_{源2}_...dataPro`
- `conversions[].runtime.sources[].message_name` 省略时，简单模式默认源协议名；联合模式优先取 `choreography.json` 中对应 source 的 `cache_key`
- `conversions[].runtime.sources[].display_name` 省略时，简单模式默认源协议名；联合模式优先取 `choreography.json` 中对应 source 的 `protocol`
- `conversions[].runtime.cache_name` 省略时默认目标协议名
- `conversions[].runtime.cache_num` 省略时默认 `3`

```json
{
  "runtime": {
    "protocol_verifies": {
      "Temp_Sensor": {
        "constraints": [
          {
            "check": "value.temperature > 80",
            "set": [
              {"field": "status", "value": "1"}
            ]
          }
        ],
        "verify_rules": [
          {
            "when_seq": "Seq_1"
          }
        ],
        "response_actions": [
          {
            "set_constraint": "Constraint1",
            "encode_seq": "Seq_1",
            "return_code": 1
          }
        ],
        "default_return_code": -1
      }
    }
  }
}
```

字段含义：

- `constraints[].name`: 可省略，默认 `Constraint{序号}`
- `constraints[].check`: C++ 条件表达式，作用对象固定为 `value`
- `constraints[].set[]`: 命中应答动作前要写回的字段和值
- `verify_rules[].name`: 可省略，默认 `verify{序号}`
- `verify_rules[]`: `seq + constraint -> verify 标签`
- `response_actions[].on_verify`: 可省略，默认按顺序绑定到对应 `verify_rules`
- `response_actions[]`: `verify 标签 -> 应答动作`
- `default_verify`: 未命中 verify 规则时返回的默认标签
- `default_return_code`: `checkObjMaps(...)` 未命中动作时返回值
