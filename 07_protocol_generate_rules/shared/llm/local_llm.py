# shared/llm/local_llm.py
# 本地LLM推理客户端

import os
import json
import torch
import re
from typing import Optional, List, Dict, Any, Generator
from dataclasses import dataclass


@dataclass
class LLMConfig:
    """LLM配置"""
    model_name: str = "Qwen/Qwen3-4B"
    device: str = "auto"
    max_length: int = 2048
    temperature: float = 0.7
    top_p: float = 0.9
    use_vllm: bool = False
    vllm_url: str = "http://localhost:8000"


class LocalLLM:
    """本地LLM推理客户端"""

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig()
        self._model = None
        self._tokenizer = None
        self._vllm_client = None
        self._vllm_model_name: Optional[str] = None

        # 从环境变量覆盖配置
        self.config.model_name = os.getenv("LLM_MODEL_NAME", self.config.model_name)
        self.config.use_vllm = os.getenv("USE_VLLM", "false").lower() == "true"
        self.config.vllm_url = os.getenv("VLLM_URL", self.config.vllm_url)

    def _load_model(self):
        """加载模型（懒加载）"""
        if self.config.use_vllm and self._vllm_client is not None and self._vllm_model_name:
            return

        if self._model is not None:
            return

        if self.config.use_vllm:
            # 使用vLLM服务
            try:
                import requests
                self._vllm_client = requests.Session()
                # 测试连接
                response = self._vllm_client.get(f"{self.config.vllm_url}/health")
                if response.status_code == 200:
                    models_resp = self._vllm_client.get(f"{self.config.vllm_url}/v1/models")
                    if models_resp.status_code == 200:
                        data = models_resp.json().get("data") or []
                        model_ids = [item.get("id") for item in data if isinstance(item, dict) and item.get("id")]
                        if model_ids:
                            if self.config.model_name in model_ids:
                                self._vllm_model_name = self.config.model_name
                            else:
                                self._vllm_model_name = model_ids[0]
                    if not self._vllm_model_name:
                        self._vllm_model_name = self.config.model_name
                    print(f"已连接到vLLM服务: {self.config.vllm_url}")
                    print(f"vLLM模型: {self._vllm_model_name}")
                    return
            except Exception as e:
                print(f"vLLM连接失败: {e}，将使用本地模型")

        # 本地加载模型
        from transformers import AutoModelForCausalLM, AutoTokenizer

        model_path = os.path.expanduser(self.config.model_name)
        if not os.path.exists(model_path):
            # 如果不是本地路径，尝试从HuggingFace加载
            model_path = self.config.model_name

        print(f"正在加载模型: {model_path}")

        self._tokenizer = AutoTokenizer.from_pretrained(
            model_path,
            trust_remote_code=True,
        )

        self._model = AutoModelForCausalLM.from_pretrained(
            model_path,
            torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
            device_map=self.config.device,
            trust_remote_code=True,
        )
        self._model.eval()

        print("模型加载完成")

    def generate(
        self,
        prompt: str,
        system_prompt: str = None,
        max_new_tokens: int = 512,
        temperature: float = None,
        top_p: float = None,
        **kwargs,
    ) -> str:
        """生成文本"""
        self._load_model()

        temperature = temperature or self.config.temperature
        top_p = top_p or self.config.top_p

        if self.config.use_vllm and self._vllm_client:
            return self._generate_vllm(
                prompt,
                system_prompt,
                max_new_tokens,
                temperature,
                top_p,
                **kwargs,
            )
        else:
            return self._generate_local(
                prompt,
                system_prompt,
                max_new_tokens,
                temperature,
                top_p,
                **kwargs,
            )

    def _generate_vllm(
        self,
        prompt: str,
        system_prompt: str = None,
        max_new_tokens: int = 512,
        temperature: float = 0.7,
        top_p: float = 0.9,
        enable_thinking: Optional[bool] = None,
    ) -> str:
        """使用vLLM服务生成"""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": self._vllm_model_name or self.config.model_name,
            "messages": messages,
            "max_tokens": max_new_tokens,
            "temperature": temperature,
            "top_p": top_p,
        }
        if enable_thinking is not None:
            payload["chat_template_kwargs"] = {"enable_thinking": enable_thinking}

        response = self._vllm_client.post(
            f"{self.config.vllm_url}/v1/chat/completions",
            json=payload,
        )
        result = response.json()
        if response.status_code != 200:
            raise RuntimeError(f"vLLM请求失败({response.status_code}): {result}")
        choices = result.get("choices")
        if not choices:
            raise RuntimeError(f"vLLM返回异常: {result}")
        return choices[0]["message"]["content"]

    def _generate_local(
        self,
        prompt: str,
        system_prompt: str = None,
        max_new_tokens: int = 512,
        temperature: float = 0.7,
        top_p: float = 0.9,
        enable_thinking: Optional[bool] = None,
    ) -> str:
        """使用本地模型生成"""
        # 构建消息格式
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        # 应用聊天模板
        if hasattr(self._tokenizer, "apply_chat_template"):
            apply_kwargs = {
                "tokenize": False,
                "add_generation_prompt": True,
            }
            if enable_thinking is not None:
                apply_kwargs["enable_thinking"] = enable_thinking
            try:
                input_text = self._tokenizer.apply_chat_template(messages, **apply_kwargs)
            except TypeError:
                apply_kwargs.pop("enable_thinking", None)
                input_text = self._tokenizer.apply_chat_template(messages, **apply_kwargs)
        else:
            input_text = "\n".join([f"{m['role']}: {m['content']}" for m in messages])
            input_text += "\nassistant:"

        inputs = self._tokenizer(input_text, return_tensors="pt")
        inputs = {k: v.to(self._model.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self._model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                temperature=temperature,
                top_p=top_p,
                do_sample=temperature > 0,
                pad_token_id=self._tokenizer.eos_token_id,
            )

        generated_text = self._tokenizer.decode(outputs[0], skip_special_tokens=True)

        # 提取assistant的回复
        if "assistant:" in generated_text:
            generated_text = generated_text.split("assistant:")[-1].strip()

        return generated_text

    def generate_stream(
        self,
        prompt: str,
        system_prompt: str = None,
        max_new_tokens: int = 512,
        temperature: float = None,
        top_p: float = None,
    ) -> Generator[str, None, None]:
        """流式生成文本"""
        self._load_model()

        temperature = temperature or self.config.temperature
        top_p = top_p or self.config.top_p

        if self.config.use_vllm and self._vllm_client:
            yield from self._generate_stream_vllm(prompt, system_prompt, max_new_tokens, temperature, top_p)
        else:
            yield self._generate_local(prompt, system_prompt, max_new_tokens, temperature, top_p)

    def _generate_stream_vllm(
        self,
        prompt: str,
        system_prompt: str = None,
        max_new_tokens: int = 512,
        temperature: float = 0.7,
        top_p: float = 0.9,
    ) -> Generator[str, None, None]:
        """使用vLLM服务流式生成"""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = self._vllm_client.post(
            f"{self.config.vllm_url}/v1/chat/completions",
            json={
                "model": self._vllm_model_name or self.config.model_name,
                "messages": messages,
                "max_tokens": max_new_tokens,
                "temperature": temperature,
                "top_p": top_p,
                "stream": True,
            },
            stream=True,
        )
        if response.status_code != 200:
            raise RuntimeError(f"vLLM流式请求失败({response.status_code}): {response.text}")

        for line in response.iter_lines():
            if line:
                line = line.decode("utf-8")
                if line.startswith("data: "):
                    data = line[6:]
                    if data == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                        if chunk["choices"][0]["delta"].get("content"):
                            yield chunk["choices"][0]["delta"]["content"]
                    except json.JSONDecodeError:
                        continue

    @staticmethod
    def _sanitize_response_text(response: str) -> str:
        """移除常见推理噪声标记，保留结构化内容。"""
        if not response:
            return ""

        cleaned = re.sub(r"<think>[\s\S]*?</think>", "", response, flags=re.IGNORECASE)
        cleaned = re.sub(r"<\|im_start\|>assistant\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"<\|im_end\|>\s*$", "", cleaned, flags=re.IGNORECASE)
        return cleaned.strip()

    @staticmethod
    def _extract_fenced_blocks(text: str) -> List[str]:
        """提取Markdown代码块候选。"""
        candidates: List[str] = []
        for match in re.finditer(r"```(?:json)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE):
            block = match.group(1).strip()
            if block:
                candidates.append(block)
        return candidates

    @staticmethod
    def _extract_balanced_json_snippets(text: str) -> List[str]:
        """
        从混合文本中抽取平衡的JSON对象/数组片段。
        通过括号栈处理嵌套结构，避免简单正则的贪婪匹配问题。
        """
        snippets: List[str] = []
        stack: List[str] = []
        start_idx: Optional[int] = None
        in_string = False
        escaped = False

        for idx, ch in enumerate(text):
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
                continue

            if ch in "{[":
                if not stack:
                    start_idx = idx
                stack.append(ch)
                continue

            if ch in "}]":
                if not stack:
                    continue
                top = stack[-1]
                if (top == "{" and ch == "}") or (top == "[" and ch == "]"):
                    stack.pop()
                    if not stack and start_idx is not None:
                        snippets.append(text[start_idx: idx + 1].strip())
                        start_idx = None
                else:
                    stack.clear()
                    start_idx = None

        return snippets

    @classmethod
    def parse_json_from_response(
        cls,
        response: str,
        prefer: Optional[type] = None,
    ) -> Optional[Any]:
        """从模型响应中提取首个可解析JSON。"""
        cleaned = cls._sanitize_response_text(response)
        if not cleaned:
            return None

        candidates: List[str] = [cleaned]
        candidates.extend(cls._extract_fenced_blocks(cleaned))
        candidates.extend(cls._extract_balanced_json_snippets(cleaned))

        seen = set()
        unique_candidates: List[str] = []
        for candidate in candidates:
            stripped = candidate.strip()
            if not stripped or stripped in seen:
                continue
            seen.add(stripped)
            unique_candidates.append(stripped)

        if prefer is not None:
            ordered: List[str] = []
            deferred: List[str] = []
            for candidate in unique_candidates:
                starts_with = candidate[0]
                if prefer is list and starts_with == "[":
                    ordered.append(candidate)
                elif prefer is dict and starts_with == "{":
                    ordered.append(candidate)
                else:
                    deferred.append(candidate)
            unique_candidates = ordered + deferred

        for candidate in unique_candidates:
            try:
                parsed = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            if prefer is None or isinstance(parsed, prefer):
                return parsed
        return None

    def extract_json(self, prompt: str, system_prompt: str = None) -> Optional[Any]:
        """生成并解析JSON输出"""
        response = self.generate(prompt, system_prompt=system_prompt)
        return self.parse_json_from_response(response)


# 全局实例
_llm_instance: Optional[LocalLLM] = None


def get_llm(config: Optional[LLMConfig] = None) -> LocalLLM:
    """获取LLM单例"""
    global _llm_instance
    if _llm_instance is None:
        _llm_instance = LocalLLM(config)
    return _llm_instance
