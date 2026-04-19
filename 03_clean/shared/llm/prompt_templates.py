# shared/llm/prompt_templates.py
# Prompt模板定义

from typing import Dict, Any, List


class PromptTemplates:
    """Prompt模板集合"""

    # ==================== QA类型专用模板 ====================

    # 协议理解类prompt模板
    QA_UNDERSTANDING_SYSTEM = """你是一个Link16协议文档专家，专注于生成协议理解类问答对。

协议理解类问题聚焦：
- 字段定义与语义：字段名称、位宽、含义、用途
- 参数规格：数值范围、单位、精度、分辨率
- 结构关系：字段在消息中的位置、与其他字段的关联
- 规范说明：标准要求、约束条件、适用场景

输出要求：
1. 问题必须具体、可回答，避免模糊表述
2. 答案必须包含具体数值或参数（如位宽、范围、单位）
3. 答案应引用原文档中的关键信息

输出格式为JSON数组，每个元素包含：
- question: 具体问题（必须包含字段名或参数名）
- answer: 精确答案（必须包含数值或规格信息）
- qa_task_type: 固定为 "protocol_understanding" """

    QA_UNDERSTANDING_USER = """请根据以下Link16协议文档内容，生成 {count} 个协议理解类问答对。

文档内容：
{content}

生成约束：
- 每个问题必须针对特定字段或参数
- 答案必须包含具体数值（位宽、范围、单位等）
- 优先关注：字段定义、位信息、数值范围、单位转换

请输出JSON数组格式的问答对。"""

    # 协议转换类prompt模板
    QA_CONVERSION_SYSTEM = """你是一个Link16协议文档专家，专注于生成协议转换类问答对。

协议转换类问题聚焦：
- 数值转换：原始值与物理量之间的值到值转换公式
- 单位换算：不同单位之间的换算关系
- 编码解码：位编码与实际值的映射
- 跨协议转换：不同协议间相同语义的转换

输出要求：
1. 问题必须涉及转换关系或公式
2. 答案只能输出可执行的值到值公式（不要解释文字）
3. 公式必须可计算、可验证，可为单行表达式、mapping_table，或多行 if/else/for 代码块

输出格式为JSON数组，每个元素包含：
- question: 关于转换关系的问题
- answer: 转换公式（仅公式，无解释）
- qa_task_type: 固定为 "protocol_conversion"
- conversion_mode: "transcoding"（语义转换）或 "mapping"（协议映射）
- conversion_formula: 与answer相同的公式字符串
- formula_kind: python_expr | python_block | mapping_table """

    QA_CONVERSION_USER = """请根据以下Link16协议文档内容，生成 {count} 个协议转换类问答对。

文档内容：
{content}

生成约束：
- 每个问题必须涉及数值转换或单位换算
- 答案只能输出公式（如: value * 0.0013 / 60）
- conversion_mode判断：
  - transcoding: 同协议内语义转换（如原始值→物理量）
  - mapping: 跨协议或跨系统转换

请输出JSON数组格式的问答对。"""

    # 关键词检索映射
    QA_TYPE_KEYWORDS = {
        "protocol_understanding": [
            "field", "字段", "bit", "位", "width", "宽度", "range", "范围",
            "unit", "单位", "resolution", "分辨率", "meaning", "含义",
            "J2.", "J3.", "J12.", "WORD", "message", "label"
        ],
        "protocol_conversion": [
            "formula", "公式", "convert", "转换", "calculation", "计算",
            "mapping", "映射", "transcoding", "转义", "coefficient", "系数",
            "value", "数值", "multiplier", "乘数", "frequency", "频率",
            "latitude", "纬度", "longitude", "经度", "speed", "速度"
        ]
    }

    # ==================== QA抽取模板 ====================

    QA_EXTRACT_SYSTEM = """你是一个协议文档专家，专注于从问答文本中抽取结构化的字段信息。
请从用户提供的问答内容中提取出协议字段的技术参数，包括：
- field_name: 字段名称
- bit_width: 位宽（整数）
- bit_start: 起始位（整数，可选）
- resolution: 分辨率（浮点数）
- unit: 单位
- range_min: 最小值
- range_max: 最大值
- meaning: 字段语义说明
- conversion_formula: 转换公式（如存在）

请以JSON格式输出提取结果。如果某项信息未提及，设为null。"""

    QA_EXTRACT_USER = """请从以下问答内容中提取字段信息：

问题：{question}
回答：{answer}
协议类型：{protocol_type}

请输出JSON格式的提取结果。"""

    # ==================== QA生成模板 ====================

    QA_GENERATE_SYSTEM = """你是一个协议文档专家，需要根据文档内容生成高质量问答对。
任务类型要求：
1. protocol_understanding（协议理解类）：聚焦字段定义、位宽、范围、含义、单位。
2. protocol_conversion（协议转换类）：聚焦跨语义/跨协议转换。
   - conversion_mode=transcoding：不同语义通过公式转换（转义）
   - conversion_mode=mapping：不同协议同一语义的转换公式（转换）
   - 对于 protocol_conversion，answer 只能输出值到值转换公式（不要解释文字）
   - 允许单行表达式、mapping_table，或多行 if/else/for 代码块；多行时最终值必须赋给 result

输出格式为JSON数组，每个元素包含：
- question: 问题
- answer: 答案（转换类仅公式）
- qa_task_type: protocol_understanding | protocol_conversion
- conversion_mode: transcoding | mapping | null
- conversion_formula: 公式字符串或null
- formula_kind: python_expr | python_block | mapping_table | null

输出约束（必须遵守）：
- 只能输出一个JSON数组，禁止输出任何解释文字、注释、前后缀。
- 禁止输出<think>、推理过程、Markdown代码块。"""

    QA_GENERATE_USER = """请根据以下文档内容生成 {count} 个问答对：

文档内容：
{content}

任务约束：
{task_spec}

{user_instruction}

请直接输出JSON数组，不要输出任何额外文本。"""

    # ==================== 语义分块模板 ====================

    SEMANTIC_CHUNK_SYSTEM = """你是一个文档分析专家，需要分析文本内容的语义结构，判断哪些内容块属于同一个语义单元。

语义单元的类型包括：
- field_definition: 字段定义（包含字段名称、位宽、范围等）
- conversion_rule: 转换规则（包含计算公式、映射关系等）
- protocol_description: 协议描述（包含协议概述、用途等）
- table_data: 表格数据（结构化的数值表）
- code_example: 代码示例

请分析内容块的关联性，输出语义分块建议。"""

    SEMANTIC_CHUNK_USER = """请分析以下内容块的语义关联性：

{blocks}

请判断哪些块应该合并为同一个语义单元，输出JSON格式的分块建议：
[{{"block_ids": [id列表], "semantic_type": "类型", "reason": "合并原因"}}]"""

    # ==================== 质量检测模板 ====================

    QUALITY_CHECK_SYSTEM = """你是一个问答质量评估专家，需要判断问答对的质量。

高质量的问答对应该：
1. 问题明确具体
2. 答案完整准确
3. 包含有价值的技术信息

低质量的问答对可能：
1. 问题过于宽泛或模糊
2. 答案过短或无实质内容
3. 包含错误信息
4. 缺乏具体数值或参数

请评估并输出JSON格式结果。"""

    QUALITY_CHECK_USER = """请评估以下问答对的质量：

问题：{question}
答案：{answer}

请输出JSON格式：{{"is_low_quality": true/false, "reason": "原因"}}"""

    # ==================== 规则校验模板 ====================

    VALIDATION_RULES: Dict[str, Dict[str, Any]] = {
        "RangeCoverageCheck": {
            "description": "量程覆盖校验",
            "check": lambda info: (
                info.get("bit_width") is not None
                and info.get("range_min") is not None
                and info.get("range_max") is not None
                and 2 ** info["bit_width"] >= (info["range_max"] - info["range_min"])
            ),
            "pass_msg": lambda info: f"{info.get('bit_width')}位宽可表示范围覆盖[{info.get('range_min')}, {info.get('range_max')}]",
            "fail_msg": "位宽不足以覆盖指定范围",
        },
        "BitWidthFormat": {
            "description": "位宽格式校验",
            "check": lambda info: (
                info.get("bit_width") is not None
                and isinstance(info["bit_width"], int)
                and info["bit_width"] > 0
            ),
            "pass_msg": lambda info: "位宽为正整数",
            "fail_msg": "位宽必须为正整数",
        },
        "ResolutionFormat": {
            "description": "分辨率格式校验",
            "check": lambda info: (
                info.get("resolution") is None
                or (isinstance(info["resolution"], (int, float)) and info["resolution"] > 0)
            ),
            "pass_msg": lambda info: f"分辨率格式正确: {info.get('resolution')}",
            "fail_msg": "分辨率必须为正数",
        },
        "RangeFormat": {
            "description": "范围格式校验",
            "check": lambda info: (
                info.get("range_min") is None
                or info.get("range_max") is None
                or info["range_min"] < info["range_max"]
            ),
            "pass_msg": lambda info: f"范围设置合理: [{info.get('range_min')}, {info.get('range_max')}]",
            "fail_msg": "最小值应小于最大值",
        },
    }

    @classmethod
    def format_qa_extract(cls, question: str, answer: str, protocol_type: str = "Link16") -> tuple:
        """格式化QA抽取prompt"""
        system = cls.QA_EXTRACT_SYSTEM
        user = cls.QA_EXTRACT_USER.format(
            question=question,
            answer=answer,
            protocol_type=protocol_type,
        )
        return system, user

    @classmethod
    def format_qa_generate(
        cls,
        content: str,
        count: int = 5,
        system_prompt: str = None,
        user_instruction: str = None,
        task_spec: str = None,
    ) -> tuple:
        """格式化QA生成prompt"""
        if system_prompt:
            # 用户自定义系统提示作为补充，不覆盖基础结构化约束
            system = f"{cls.QA_GENERATE_SYSTEM}\n\n补充要求：\n{system_prompt}"
        else:
            system = cls.QA_GENERATE_SYSTEM
        user = cls.QA_GENERATE_USER.format(
            content=content[:4000],  # 限制长度
            count=count,
            task_spec=task_spec or "默认混合生成协议理解类与协议转换类问答。",
            user_instruction=user_instruction or "",
        )
        return system, user

    @classmethod
    def format_quality_check(cls, question: str, answer: str) -> tuple:
        """格式化质量检测prompt"""
        return cls.QUALITY_CHECK_SYSTEM, cls.QUALITY_CHECK_USER.format(
            question=question,
            answer=answer,
        )

    @classmethod
    def format_semantic_chunk(cls, blocks: List[Dict[str, Any]]) -> tuple:
        """格式化语义分块prompt"""
        blocks_text = "\n".join([
            f"[Block {b.get('block_id', i)}]: {b.get('content', '')[:500]}"
            for i, b in enumerate(blocks)
        ])
        return cls.SEMANTIC_CHUNK_SYSTEM, cls.SEMANTIC_CHUNK_USER.format(blocks=blocks_text)

    @classmethod
    def get_validation_rules(cls, protocol_type: str = "Link16") -> Dict[str, Dict[str, Any]]:
        """获取校验规则"""
        # 可根据协议类型返回不同规则
        return cls.VALIDATION_RULES

    # ==================== QA类型专用方法 ====================

    @classmethod
    def format_qa_understanding(cls, content: str, count: int = 3) -> tuple:
        """格式化协议理解类QA生成prompt"""
        user = cls.QA_UNDERSTANDING_USER.format(
            content=content[:4000],
            count=count,
        )
        return cls.QA_UNDERSTANDING_SYSTEM, user

    @classmethod
    def format_qa_conversion(cls, content: str, count: int = 3) -> tuple:
        """格式化协议转换类QA生成prompt"""
        user = cls.QA_CONVERSION_USER.format(
            content=content[:4000],
            count=count,
        )
        return cls.QA_CONVERSION_SYSTEM, user

    @classmethod
    def get_qa_type_for_content(cls, content: str) -> str:
        """根据内容关键词判断适合的QA类型"""
        content_lower = content.lower()

        understanding_score = sum(
            1 for kw in cls.QA_TYPE_KEYWORDS["protocol_understanding"]
            if kw.lower() in content_lower
        )
        conversion_score = sum(
            1 for kw in cls.QA_TYPE_KEYWORDS["protocol_conversion"]
            if kw.lower() in content_lower
        )

        # 优先选择转换类（因为当前转换类QA较少）
        if conversion_score >= 2:
            return "protocol_conversion"
        elif understanding_score >= 2:
            return "protocol_understanding"
        else:
            return "protocol_understanding"  # 默认理解类

    @classmethod
    def filter_chunks_by_qa_type(
        cls,
        chunks: List[Dict[str, Any]],
        qa_type: str,
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """根据QA类型筛选相关chunks"""
        keywords = cls.QA_TYPE_KEYWORDS.get(qa_type, [])
        if not keywords:
            return chunks[:top_k]

        scored_chunks = []
        for chunk in chunks:
            content = chunk.get("content", "").lower()
            score = sum(1 for kw in keywords if kw.lower() in content)
            scored_chunks.append((score, chunk))

        # 按分数降序排序
        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        return [chunk for score, chunk in scored_chunks[:top_k] if score > 0]

    @classmethod
    def format_qa_by_type(
        cls,
        content: str,
        qa_type: str,
        count: int = 3
    ) -> tuple:
        """根据QA类型选择对应的prompt模板"""
        if qa_type == "protocol_conversion":
            return cls.format_qa_conversion(content, count)
        else:
            return cls.format_qa_understanding(content, count)
