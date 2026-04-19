# 接口2: 数据自动化清洗
# POST /api/data/clean

import re
import sys
import os
from typing import List, Dict, Any, Tuple
from flask import Flask, request, jsonify

# 添加项目根目录到路径以导入shared模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.database import MySQLClient
from shared.database.models import Block, CleaningIssue

app = Flask(__name__)

# 初始化数据库客户端
db_client = MySQLClient()


# ==================== 清洗规则实现 ====================

class DataCleaner:
    """数据清洗器 - 实现多种清洗规则"""

    # OCR乱码检测正则模式
    GARBLED_PATTERNS = [
        # 连续特殊符号（排除常见标点组合）
        r'[#$%^&*]{3,}',
        # 随机符号组合（如 @#$%, *&^%）
        r'[@#$%^&*]{4,}',
        # 乱码字符序列（非正常文本）
        r'[^\w\s\u4e00-\u9fff\u3000-\u303f\uff00-\uffef.,;:!?\'"()\[\]{}\-–—…·]{5,}',
        # 重复无意义符号
        r'([#$%&*^@!~])\1{3,}',
        # OCR常见乱码模式
        r'[|\\\/]{4,}',
        # 混合乱码符号
        r'[#@$%^&*()]{6,}',
    ]

    # 编译正则表达式
    GARBLED_REGEX = re.compile('|'.join(GARBLED_PATTERNS))

    @classmethod
    def detect_garbled_text(cls, content: str) -> Tuple[bool, str, str]:
        """
        检测并移除OCR乱码字符

        Returns:
            Tuple[has_issue, cleaned_content, description]
        """
        if not content:
            return False, content, ""

        matches = cls.GARBLED_REGEX.findall(content)
        if not matches:
            return False, content, ""

        # 记录发现的乱码
        found_garbled = list(set(matches))[:5]  # 最多记录5种

        # 移除乱码字符
        cleaned = cls.GARBLED_REGEX.sub('', content)

        # 清理多余的空白
        cleaned = re.sub(r' {2,}', ' ', cleaned)
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)

        garbled_str = ', '.join(repr(g) for g in found_garbled[:3])
        description = f"检测到OCR乱码字符: {garbled_str}"
        return True, cleaned.strip(), description

    @classmethod
    def fix_encoding(cls, content: str) -> Tuple[bool, str, str]:
        """
        修复编码问题

        常见的编码问题：
        - 全角/半角标点混用
        - 异常Unicode字符
        - 常见乱码替换
        """
        if not content:
            return False, content, ""

        original = content
        fixed = False
        issues = []

        # 常见编码错误映射
        encoding_fixes = {
            '锟斤拷': '',  # 经典乱码
            '烫烫烫': '',  # 未初始化内存
            '屯屯屯': '',  # 未初始化内存
            '\ufffd': '',  # 替换字符
            '\u0000': '',  # 空字符
        }

        for wrong, correct in encoding_fixes.items():
            if wrong in content:
                content = content.replace(wrong, correct)
                fixed = True
                issues.append(f"修复编码错误: {repr(wrong)}")

        # 检查并修复异常控制字符（保留换行、制表符）
        control_pattern = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')
        if control_pattern.search(content):
            content = control_pattern.sub('', content)
            fixed = True
            issues.append("移除异常控制字符")

        if fixed:
            return True, content, '; '.join(issues)
        return False, original, ""

    @classmethod
    def normalize_whitespace(cls, content: str) -> Tuple[bool, str, str]:
        """
        规范化空白字符

        处理：
        - 多个连续空格压缩为单个
        - 多个连续换行压缩为最多两个
        - 行首行尾空白去除
        - 制表符转空格
        """
        if not content:
            return False, content, ""

        original = content

        # 制表符转空格
        content = content.replace('\t', '    ')

        # 压缩连续空格（不在行首）
        content = re.sub(r'(?<!\n) {2,}', ' ', content)

        # 压缩连续换行为最多两个
        content = re.sub(r'\n{3,}', '\n\n', content)

        # 去除每行首尾空白
        lines = [line.strip() for line in content.split('\n')]
        content = '\n'.join(lines)

        # 去除整体首尾空白
        content = content.strip()

        if content != original:
            return True, content, "规范化空白字符"
        return False, content, ""

    @classmethod
    def fix_broken_table(cls, content: str) -> Tuple[bool, str, str]:
        """
        修复破损的表格格式

        处理：
        - 表格分隔线断裂
        - 单元格对齐问题
        - 表格行合并错误
        """
        if not content:
            return False, content, ""

        original = content
        issues = []

        # 检测是否包含表格特征
        table_indicators = [
            r'\|.*\|',  # 管道符分隔
            r'[-+]{3,}',  # 分隔线
            r'^\s*\+[-+]+\+',  # 表格边框
        ]

        has_table = any(re.search(p, content, re.MULTILINE) for p in table_indicators)
        if not has_table:
            return False, content, ""

        # 修复管道符表格
        lines = content.split('\n')
        fixed_lines = []

        for line in lines:
            fixed_line = line

            # 修复断开的表格行（以|开头但不以|结尾）
            if line.strip().startswith('|') and not line.strip().endswith('|'):
                fixed_line = line.rstrip() + ' |'
                issues.append("修复表格行结尾")

            # 修复多余的管道符
            if re.search(r'\|{3,}', line):
                fixed_line = re.sub(r'\|{3,}', '||', line)
                issues.append("修复多余管道符")

            # 修复表格分隔线
            if re.match(r'^\s*\|[\s\-:]+\|\s*$', line):
                # 确保分隔线完整
                if '|' in line:
                    parts = line.split('|')
                    if len(parts) > 2:
                        # 标准化分隔线格式
                        fixed_line = '|' + '|'.join(p.strip() or '---' for p in parts[1:-1]) + '|'
                        issues.append("修复表格分隔线")

            fixed_lines.append(fixed_line)

        content = '\n'.join(fixed_lines)

        if content != original:
            unique_issues = list(set(issues))[:3]
            return True, content, '; '.join(unique_issues)
        return False, original, ""

    @classmethod
    def detect_duplicate(cls, content: str, content_hash_dict: Dict[str, int] = None) -> Tuple[bool, str, str]:
        """
        检测重复内容

        检测：
        - 行级重复
        - 段落级重复
        - 与其他块的内容重复
        """
        if not content:
            return False, content, ""

        issues = []
        original = content

        # 检测连续重复行
        lines = content.split('\n')
        seen_lines = {}
        dedup_lines = []

        for i, line in enumerate(lines):
            line_stripped = line.strip()
            if not line_stripped:  # 保留空行
                dedup_lines.append(line)
                continue

            if line_stripped in seen_lines:
                # 检查是否是连续重复
                if i > 0 and lines[i-1].strip() == line_stripped:
                    issues.append(f"检测到连续重复行")
                    continue  # 跳过连续重复行
            else:
                seen_lines[line_stripped] = i

            dedup_lines.append(line)

        content = '\n'.join(dedup_lines)

        # 检测段落级重复
        paragraphs = re.split(r'\n\s*\n', content)
        seen_paras = {}
        dedup_paras = []

        for para in paragraphs:
            para_stripped = para.strip()
            if not para_stripped:
                dedup_paras.append(para)
                continue

            # 简单哈希用于检测相似段落
            para_hash = hash(para_stripped[:100])  # 使用前100字符做哈希

            if para_hash in seen_paras:
                issues.append("检测到重复段落")
                continue

            seen_paras[para_hash] = True
            dedup_paras.append(para)

        if issues:
            content = '\n\n'.join(dedup_paras)
            return True, content, '; '.join(list(set(issues))[:3])
        return False, original, ""


def clean_block(block: Block) -> Tuple[Block, List[CleaningIssue]]:
    """
    对单个Block执行所有清洗规则

    Args:
        block: 待清洗的文档块

    Returns:
        Tuple[清洗后的Block, 清洗问题列表]
    """
    issues = []
    content = block.content or ""

    if not content.strip():
        return block, issues

    # 按顺序执行清洗规则
    cleaning_rules = [
        ("GARBLED_TEXT", DataCleaner.detect_garbled_text),
        ("ENCODING_FIX", DataCleaner.fix_encoding),
        ("WHITESPACE", DataCleaner.normalize_whitespace),
        ("BROKEN_TABLE", DataCleaner.fix_broken_table),
        ("DUPLICATE", DataCleaner.detect_duplicate),
    ]

    current_content = content

    for rule_name, rule_func in cleaning_rules:
        has_issue, cleaned_content, description = rule_func(current_content)

        if has_issue:
            issue = CleaningIssue(
                block_id=block.block_id,
                issue_type=rule_name,
                description=description,
                original=current_content[:200] + "..." if len(current_content) > 200 else current_content,
                cleaned=cleaned_content[:200] + "..." if len(cleaned_content) > 200 else cleaned_content
            )
            issues.append(issue)
            current_content = cleaned_content

    # 更新block的cleaned_content
    if issues:
        block.cleaned_content = current_content
    else:
        # 无问题则cleaned_content等于原始内容
        block.cleaned_content = content

    return block, issues


@app.route("/api/data/clean", methods=["POST"])
def clean_data():
    """
    数据自动化清洗接口

    输入参数:
    {
        "dataset_id": "ds_001",
        "block_ids": [101, 102, 103, 104, 105]
    }

    响应格式:
    {
        "code": 200,
        "message": "success",
        "data": {
            "cleaning_rate": "20.0%",
            "total_count": 5,
            "modified_count": 1,
            "modified_block_ids": [102],
            "issues": [...]
        }
    }
    """
    data = request.json

    # 参数校验
    if not data:
        return jsonify({
            "code": 400,
            "message": "请求体不能为空",
            "data": None
        }), 400

    dataset_id = data.get("dataset_id")
    block_ids = data.get("block_ids", [])

    if not dataset_id:
        return jsonify({
            "code": 400,
            "message": "dataset_id不能为空",
            "data": None
        }), 400

    if not block_ids:
        return jsonify({
            "code": 400,
            "message": "block_ids不能为空",
            "data": None
        }), 400

    try:
        # 1. 从数据库获取待清洗的Block数据
        blocks = db_client.get_blocks_by_ids(block_ids)

        if not blocks:
            return jsonify({
                "code": 404,
                "message": "未找到指定的Block数据",
                "data": {
                    "cleaning_rate": "0%",
                    "total_count": len(block_ids),
                    "modified_count": 0,
                    "modified_block_ids": [],
                    "issues": []
                }
            })

        # 2. 执行清洗规则
        all_issues = []
        modified_block_ids = []
        content_hash_dict = {}  # 用于跨块重复检测

        for block in blocks:
            # 执行清洗
            cleaned_block, issues = clean_block(block)

            # 记录内容哈希用于重复检测
            if cleaned_block.content:
                content_hash_dict[hash(cleaned_block.content[:100])] = block.block_id

            # 如果有清洗问题，更新数据库
            if issues:
                modified_block_ids.append(block.block_id)
                all_issues.extend(issues)

                # 更新数据库中的cleaned_content字段
                db_client.update_block_content(block.block_id, cleaned_block.cleaned_content)

        # 3. 计算清洗统计
        total_count = len(blocks)
        modified_count = len(modified_block_ids)
        cleaning_rate = f"{(modified_count / total_count * 100):.1f}%" if total_count > 0 else "0%"

        # 4. 返回清洗结果
        return jsonify({
            "code": 200,
            "message": "success",
            "data": {
                "cleaning_rate": cleaning_rate,
                "total_count": total_count,
                "modified_count": modified_count,
                "modified_block_ids": modified_block_ids,
                "issues": [issue.to_dict() for issue in all_issues]
            }
        })

    except Exception as e:
        return jsonify({
            "code": 500,
            "message": f"清洗过程发生错误: {str(e)}",
            "data": None
        }), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
