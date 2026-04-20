# shared/llm/local_llm.py
# 本地LLM推理客户端

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional

import torch


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
    api_key: Optional[str] = None
    api_base_url: Optional[str] = None


def _get_env_value(*names: str) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _get_env_flag(*names: str, default: bool = False) -> bool:
    value = _get_env_value(*names)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


class LocalLLM:
    """本地LLM推理客户端"""

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig()
        self._model = None
        self._tokenizer = None
        self._vllm_client = None
        self._vllm_model_name: Optional[str] = None
        self._api_base_url: Optional[str] = None
        self._service_root_url: Optional[str] = None

        openai_model = _get_env_value("OPENAI_MODEL", "openai_model")
        openai_base_url = _get_env_value("OPENAI_BASE_URL", "openai_base_url")
        openai_api_key = _get_env_value("OPENAI_API_KEY", "openai_api_key")
        legacy_model = _get_env_value("LLM_MODEL_NAME")
        legacy_vllm_url = _get_env_value("VLLM_URL", "vllm_url")
        use_vllm_flag = _get_env_flag("USE_VLLM", "use_vllm", default=self.config.use_vllm)

        self.config.model_name = openai_model or legacy_model or self.config.model_name
        self.config.api_key = openai_api_key or self.config.api_key
        self.config.api_base_url = openai_base_url or self.config.api_base_url

        if self.config.api_base_url:
            self.config.use_vllm = True
            self.config.vllm_url = self.config.api_base_url
        else:
            self.config.use_vllm = bool(use_vllm_flag or legacy_vllm_url)
            self.config.vllm_url = legacy_vllm_url or self.config.vllm_url

        self._api_base_url, self._service_root_url = self._normalize_remote_urls(
            self.config.vllm_url if self.config.use_vllm else None
        )

    @staticmethod
    def _normalize_remote_urls(raw_url: Optional[str]) -> tuple[Optional[str], Optional[str]]:
        if not raw_url:
            return None, None
        normalized = str(raw_url).strip().rstrip("/")
        if not normalized:
            return None, None
        if normalized.endswith("/v1"):
            service_root = normalized[:-3].rstrip("/") or normalized
            return normalized, service_root
        return f"{normalized}/v1", normalized

    def _build_remote_headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        return headers

    def _load_model(self):
        """加载模型（懒加载）"""
        if self.config.use_vllm and self._vllm_client is not None and self._api_base_url:
            return

        if self._model is not None:
            return

        if self.config.use_vllm and self._api_base_url:
            try:
                import requests

                self._vllm_client = requests.Session()
                self._vllm_client.headers.update(self._build_remote_headers())

                models_resp = self._vllm_client.get(f"{self._api_base_url}/models", timeout=15)
                if models_resp.status_code == 200:
                    data = models_resp.json().get("data") or []
                    model_ids = [item.get("id") for item in data if isinstance(item, dict) and item.get("id")]
                    if model_ids:
                        if self.config.model_name in model_ids:
                            self._vllm_model_name = self.config.model_name
                        else:
                            self._vllm_model_name = model_ids[0]
                elif self._service_root_url:
                    health_resp = self._vllm_client.get(f"{self._service_root_url}/health", timeout=10)
                    if health_resp.status_code != 200:
                        raise RuntimeError(
                            f"模型服务不可用: models={models_resp.status_code}, health={health_resp.status_code}"
                        )

                if not self._vllm_model_name:
                    self._vllm_model_name = self.config.model_name

                print(f"已连接到OpenAI兼容模型服务: {self._api_base_url}")
                print(f"远程模型: {self._vllm_model_name}")
                return
            except Exception as exc:
                self._vllm_client = None
                self._vllm_model_name = None
                print(f"远程模型服务连接失败: {exc}，将使用本地模型")
        elif self.config.use_vllm:
            print("远程模型服务地址未配置，将使用本地模型")
            self.config.use_vllm = False

        from transformers import AutoModelForCausalLM, AutoTokenizer

        model_path = os.path.expanduser(self.config.model_name)
        if not os.path.exists(model_path):
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

        if temperature is None:
            temperature = self.config.temperature
        if top_p is None:
            top_p = self.config.top_p

        if self.config.use_vllm and self._vllm_client and self._api_base_url:
            return self._generate_vllm(
                prompt,
                system_prompt,
                max_new_tokens,
                temperature,
                top_p,
                **kwargs,
            )
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
        """使用OpenAI兼容服务生成"""
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
            f"{self._api_base_url}/chat/completions",
            json=payload,
            timeout=120,
        )
        try:
            result = response.json()
        except ValueError:
            result = {"raw": response.text}
        if response.status_code != 200:
            raise RuntimeError(f"远程模型请求失败({response.status_code}): {result}")
        choices = result.get("choices")
        if not choices:
            raise RuntimeError(f"远程模型返回异常: {result}")
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
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

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
        inputs = {key: value.to(self._model.device) for key, value in inputs.items()}

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

        if temperature is None:
            temperature = self.config.temperature
        if top_p is None:
            top_p = self.config.top_p

        if self.config.use_vllm and self._vllm_client and self._api_base_url:
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
        """使用OpenAI兼容服务流式生成"""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = self._vllm_client.post(
            f"{self._api_base_url}/chat/completions",
            json={
                "model": self._vllm_model_name or self.config.model_name,
                "messages": messages,
                "max_tokens": max_new_tokens,
                "temperature": temperature,
                "top_p": top_p,
                "stream": True,
            },
            stream=True,
            timeout=120,
        )
        if response.status_code != 200:
            raise RuntimeError(f"远程模型流式请求失败({response.status_code}): {response.text}")

        for line in response.iter_lines():
            if not line:
                continue
            text = line.decode("utf-8")
            if not text.startswith("data: "):
                continue
            data = text[6:]
            if data == "[DONE]":
                break
            try:
                chunk = json.loads(data)
            except json.JSONDecodeError:
                continue
            content = chunk["choices"][0]["delta"].get("content")
            if content:
                yield content

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
        """从混合文本中抽取平衡的JSON对象/数组片段。"""
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


_llm_instance: Optional[LocalLLM] = None


def get_llm(config: Optional[LLMConfig] = None) -> LocalLLM:
    """获取LLM单例"""
    global _llm_instance
    if _llm_instance is None:
        _llm_instance = LocalLLM(config)
    return _llm_instance
