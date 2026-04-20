# QA Prompt配置占位符说明

本文档说明 `qa_prompt_config.json` 中 `user_prompt_template` 使用的所有占位符。

---

## 占位符列表

### 1. `{count}`

**用途**: 指定要生成的QA对数量

**数据类型**: `int`

**取值范围**:
- 最小值: 1
- 最大值: 20
- 推荐值: 3-10

**说明**:
- 控制LLM在单次调用中生成的问答对数量
- 数量过少可能导致覆盖不全面
- 数量过多可能导致质量下降或超出token限制

**使用示例**:
```python
# 生成5个问答对
count = 5
prompt = template.format(count=count, content=doc_content, user_instruction="")
```

**实际替换效果**:
```
原始: "生成 {count} 个协议理解类问答对"
替换后: "生成 5 个协议理解类问答对"
```

---

### 2. `{content}`

**用途**: 提供协议文档的原始内容，作为QA生成的信息源

**数据类型**: `str`

**内容要求**:
- 必须是协议文档的文本内容
- 应包含字段定义、参数规格、转换公式等结构化信息
- 建议长度: 500-4000字符（过长会被截断）

**内容特征**（理想情况）:
- **协议理解类**: 包含字段名、位宽、位段、范围、单位、枚举值等
- **协议转换类**: 包含转换公式、系数、偏移量、映射关系等

**说明**:
- 这是最核心的占位符，直接决定生成QA的质量
- 内容应尽量保持原始格式（表格、列表等）
- 如果内容过长，会在模板中被截断到4000字符（见 `prompt_templates.py:266`）

**使用示例**:
```python
# 从数据库或文件读取协议文档内容
content = """
TRACK_NUMBER | 1-16 | 16位 | 目标航迹编号，范围0-65535
LATITUDE | 17-33 | 17位 | 纬度，分辨率0.0013°，范围-90°到+90°
MISSION_TYPE | 34-37 | 4位 | 任务类型，0=IDLE, 1=REFUEL, 5=ENGAGE
"""

prompt = template.format(count=5, content=content, user_instruction="")
```

**实际替换效果**:
```
原始: "## 文档内容\n{content}"
替换后: "## 文档内容\nTRACK_NUMBER | 1-16 | 16位 | 目标航迹编号..."
```

**注意事项**:
- 避免包含无关信息（如页眉、页脚、版权声明）
- 如果是从PDF提取，需要先清洗格式
- 表格结构应保持清晰（使用 | 分隔符或保持原始格式）

---

### 3. `{user_instruction}`

**用途**: 用户自定义的额外生成指令或约束

**数据类型**: `str`

**默认值**: `""` (空字符串)

**使用场景**:
- 强调特定字段或主题
- 添加额外的格式要求
- 指定特殊的生成策略
- 排除某些内容

**说明**:
- 这是可选占位符，可以为空
- 如果为空，模板中会直接显示空行
- 用于在标准prompt基础上添加个性化需求

**使用示例**:

**示例1: 强调特定字段**
```python
user_instruction = "请重点关注LATITUDE和LONGITUDE字段，生成至少3个与位置相关的问答。"
prompt = template.format(count=5, content=content, user_instruction=user_instruction)
```

**示例2: 指定问题类型**
```python
user_instruction = "优先生成参数规格类问题，确保每个答案都包含数值范围和单位信息。"
prompt = template.format(count=5, content=content, user_instruction=user_instruction)
```

**示例3: 排除某些内容**
```python
user_instruction = "不要生成关于RESERVED字段的问答，这些字段暂未使用。"
prompt = template.format(count=5, content=content, user_instruction=user_instruction)
```

**示例4: 空指令（默认情况）**
```python
user_instruction = ""
prompt = template.format(count=5, content=content, user_instruction=user_instruction)
# 模板中 {user_instruction} 位置会显示为空行
```

**实际替换效果**:
```
原始: "{user_instruction}\n\n请直接输出JSON数组"
替换后（有指令）: "请重点关注LATITUDE和LONGITUDE字段...\n\n请直接输出JSON数组"
替换后（无指令）: "\n\n请直接输出JSON数组"
```

---

## 完整使用示例

### Python代码示例

```python
import json

# 1. 加载配置
with open('shared/llm/qa_prompt_config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

# 2. 选择QA类型
qa_type = "protocol_understanding"  # 或 "protocol_conversion"
qa_config = config['qa_generation'][qa_type]

# 3. 准备占位符数据
count = 5
content = """
TRACK_NUMBER | 1-16 | 16位 | 目标航迹编号，范围0-65535
LATITUDE | 17-33 | 17位 | 纬度，分辨率0.0013°，范围-90°到+90°
LONGITUDE | 34-51 | 18位 | 经度，分辨率0.0013°，范围-180°到+180°
"""
user_instruction = "请重点生成与位置字段相关的问答。"

# 4. 格式化prompt
system_prompt = qa_config['system_prompt']
user_prompt = qa_config['user_prompt_template'].format(
    count=count,
    content=content,
    user_instruction=user_instruction
)

# 5. 调用LLM
# response = llm.generate(prompt=user_prompt, system_prompt=system_prompt)
```

### 在现有代码中的使用

参考 `api_04_generate_qa/app.py` 中的 `generate_qa_pairs` 函数：

```python
def generate_qa_pairs(
    content: str,
    count: int,
    system_prompt: str = None,
    user_instruction: str = None,
    task_spec: str = None,
) -> List[Dict[str, Any]]:
    """调用LLM生成QA对"""
    llm = get_llm_client()

    # 使用模板格式化prompt
    system, user = PromptTemplates.format_qa_generate(
        content=content,           # {content} 占位符
        count=count,               # {count} 占位符
        system_prompt=system_prompt,
        user_instruction=user_instruction,  # {user_instruction} 占位符
        task_spec=task_spec,
    )

    # 调用LLM生成
    response = llm.generate(
        prompt=user,
        system_prompt=system,
        max_new_tokens=2048,
        temperature=0.7,
    )
    return parse_qa_response(response)
```

---

## 占位符验证规则

### 必填占位符
- `{count}`: 必须提供，不能为空
- `{content}`: 必须提供，不能为空

### 可选占位符
- `{user_instruction}`: 可以为空字符串

### 数据验证
```python
def validate_placeholders(count: int, content: str, user_instruction: str = ""):
    """验证占位符数据"""
    errors = []

    # 验证 count
    if not isinstance(count, int):
        errors.append("count必须是整数")
    elif count < 1 or count > 20:
        errors.append("count必须在1-20之间")

    # 验证 content
    if not content or not content.strip():
        errors.append("content不能为空")
    elif len(content.strip()) < 50:
        errors.append("content内容过短，至少需要50字符")

    # 验证 user_instruction（可选）
    if user_instruction is None:
        user_instruction = ""

    return errors, user_instruction
```

---

## 常见问题

### Q1: content太长怎么办？
A: 模板会自动截断到4000字符。建议预处理时：
- 优先保留包含字段定义的部分
- 移除冗余的描述性文字
- 保持表格和结构化信息的完整性

### Q2: user_instruction应该写什么？
A: 根据实际需求：
- 如果文档质量好，可以留空
- 如果需要特定类型的问答，明确指出
- 如果某些字段更重要，可以强调

### Q3: count设置多少合适？
A: 建议：
- 小文档（<1000字符）: 3-5个
- 中等文档（1000-3000字符）: 5-10个
- 大文档（>3000字符）: 10-15个

### Q4: 如何确保生成的QA质量？
A: 关键在于content的质量：
- 确保包含完整的字段信息
- 保持原始格式（表格、列表）
- 包含数值、单位、公式等关键信息
- 避免模糊或不完整的描述

---

## 更新日志

- **v1.0.0** (2026-03-03): 初始版本，定义三个核心占位符
