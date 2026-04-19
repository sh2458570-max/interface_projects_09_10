"""LoRA protocol finetuning entrypoint with CLI arguments."""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any, Dict

import torch
from datasets import Dataset, load_dataset
from peft import LoraConfig
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainerCallback,
    TrainingArguments,
)
try:
    from trl import SFTTrainer
except ModuleNotFoundError:  # pragma: no cover - 依赖检查
    SFTTrainer = None  # type: ignore[assignment]


class ProgressLogger(TrainerCallback):
    """Emit parser-friendly training logs for progress and metrics."""

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def on_log(self, args, state, control, logs=None, **kwargs):  # pylint: disable=unused-argument
        logs = logs or {}
        max_steps = max(1, int(getattr(state, "max_steps", 0) or 0))
        step = int(getattr(state, "global_step", 0) or 0)
        percent = min(100, int(step / max_steps * 100))
        epoch = self._to_float(getattr(state, "epoch", 0.0), default=0.0)
        loss = self._to_float(logs.get("loss"), default=0.0)
        lr = self._to_float(logs.get("learning_rate"), default=0.0)
        acc = logs.get("acc")
        if acc is None:
            acc = logs.get("accuracy")
        if acc is None:
            acc = logs.get("train_acc")
        if acc is None:
            acc = max(0.0, min(1.0, 1.0 / (1.0 + max(loss, 0.0))))
        acc = self._to_float(acc, default=0.0)

        print(
            (
                f"progress={percent}% step={step}/{max_steps} "
                f"epoch={epoch:.4f} loss={loss:.6f} acc={acc:.6f} lr={lr:.8f}"
            ),
            flush=True,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LoRA finetune for protocol conversion")
    parser.add_argument("--model_name", required=True, help="Base model path or model id")
    parser.add_argument("--dataset", required=True, help="Training dataset file path (json/jsonl)")
    parser.add_argument("--output_dir", required=True, help="Output directory for LoRA adapters")
    parser.add_argument("--epochs", type=float, default=3.0, help="Training epochs")
    parser.add_argument("--learning_rate", type=float, default=2e-4, help="Learning rate")
    parser.add_argument("--batch_size", type=int, default=1, help="Per-device train batch size")
    parser.add_argument("--lora_rank", type=int, default=16, help="LoRA rank")
    parser.add_argument("--lora_alpha", type=int, default=32, help="LoRA alpha")
    parser.add_argument("--lora_dropout", type=float, default=0.05, help="LoRA dropout")
    parser.add_argument("--max_length", type=int, default=1024, help="Max sequence length")
    parser.add_argument("--save_steps", type=int, default=500, help="Checkpoint save interval")
    parser.add_argument(
        "--resume_from_checkpoint",
        default="",
        help="Optional checkpoint path for resuming training",
    )
    return parser.parse_args()


def _require_positive(name: str, value: float | int) -> None:
    if value <= 0:
        raise ValueError(f"参数 {name} 必须大于 0，当前值: {value}")


def _build_text(example: Dict[str, Any], has_text: bool) -> str:
    if has_text:
        text = str(example.get("text", "")).strip()
        if not text:
            raise ValueError("字段 text 为空")
        return text

    instruction = str(example.get("instruction", "")).strip()
    output = str(example.get("output", "")).strip()
    user_input = str(example.get("input", "")).strip()

    if not instruction:
        raise ValueError("字段 instruction 为空")
    if not output:
        raise ValueError("字段 output 为空")

    return (
        "<|im_start|>system\n"
        "你是一个专业的网络消息协议转换助手，严格按照指令将原始协议消息转换为指定目标协议消息。\n"
        "<|im_end|>\n"
        "<|im_start|>user\n"
        f"{instruction}\n"
        f"{user_input}\n"
        "<|im_end|>\n"
        "<|im_start|>assistant\n"
        f"{output}\n"
        "<|im_end|>"
    )


def load_and_validate_dataset(dataset_path: str) -> Dataset:
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"数据集不存在: {dataset_path}")
    if not os.path.isfile(dataset_path):
        raise ValueError(f"数据集路径不是文件: {dataset_path}")

    try:
        dataset = load_dataset("json", data_files=dataset_path, split="train")
    except Exception as exc:  # pylint: disable=broad-except
        raise ValueError(f"数据集加载失败，请确认是合法 JSON/JSONL: {dataset_path}") from exc

    if dataset.num_rows <= 0:
        raise ValueError(f"数据集为空: {dataset_path}")

    columns = set(dataset.column_names or [])
    has_text = "text" in columns
    has_structured = "instruction" in columns and "output" in columns
    if not has_text and not has_structured:
        raise ValueError(
            "数据集字段不合法：需要 text 字段，或 instruction + output 字段（input 可选）"
        )

    sample_count = min(20, dataset.num_rows)
    for idx in range(sample_count):
        record = dataset[idx]
        try:
            _build_text(record, has_text=has_text)
        except ValueError as exc:
            raise ValueError(f"数据集第 {idx + 1} 条样本不合法: {exc}") from exc

    def _map_record(example: Dict[str, Any]) -> Dict[str, str]:
        return {"text": _build_text(example, has_text=has_text)}

    return dataset.map(_map_record, remove_columns=list(columns))


def _build_quant_config() -> BitsAndBytesConfig | None:
    if not torch.cuda.is_available():
        return None
    try:
        import bitsandbytes  # pylint: disable=unused-import,import-outside-toplevel
    except Exception:  # pylint: disable=broad-except
        return None
    return BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float16,
    )


def run_training(args: argparse.Namespace) -> None:
    if SFTTrainer is None:
        raise ModuleNotFoundError("缺少依赖 trl，请先安装 api_05_finetune/requirements.txt")

    _require_positive("epochs", args.epochs)
    _require_positive("learning_rate", args.learning_rate)
    _require_positive("batch_size", args.batch_size)
    _require_positive("lora_rank", args.lora_rank)
    _require_positive("lora_alpha", args.lora_alpha)
    _require_positive("max_length", args.max_length)
    _require_positive("save_steps", args.save_steps)
    resume_checkpoint = str(args.resume_from_checkpoint or "").strip()
    if resume_checkpoint and not os.path.exists(resume_checkpoint):
        raise FileNotFoundError(f"恢复点不存在: {resume_checkpoint}")

    os.makedirs(args.output_dir, exist_ok=True)

    dataset = load_and_validate_dataset(args.dataset)
    tokenizer = AutoTokenizer.from_pretrained(args.model_name, trust_remote_code=True, use_fast=False)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token or tokenizer.unk_token
    tokenizer.padding_side = "right"

    quantization_config = _build_quant_config()
    dtype = torch.float16 if torch.cuda.is_available() else torch.float32
    if torch.cuda.is_available() and torch.cuda.is_bf16_supported():
        dtype = torch.bfloat16

    model_kwargs: Dict[str, Any] = {
        "trust_remote_code": True,
        "torch_dtype": dtype,
    }
    if quantization_config is not None:
        model_kwargs["quantization_config"] = quantization_config
        model_kwargs["device_map"] = "auto"

    model = AutoModelForCausalLM.from_pretrained(args.model_name, **model_kwargs)

    lora_config = LoraConfig(
        r=args.lora_rank,
        lora_alpha=args.lora_alpha,
        lora_dropout=args.lora_dropout,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj"],
        bias="none",
        task_type="CAUSAL_LM",
    )

    # TRL 0.29+ 使用 SFTConfig
    try:
        from trl import SFTConfig
        training_args = SFTConfig(
            output_dir=args.output_dir,
            num_train_epochs=args.epochs,
            per_device_train_batch_size=args.batch_size,
            gradient_accumulation_steps=1,
            learning_rate=args.learning_rate,
            logging_strategy="steps",
            logging_steps=1,
            save_strategy="steps",
            save_steps=args.save_steps,
            save_total_limit=2,
            remove_unused_columns=False,
            report_to="none",
            optim="adamw_torch",
            bf16=torch.cuda.is_available() and torch.cuda.is_bf16_supported(),
            fp16=torch.cuda.is_available() and not torch.cuda.is_bf16_supported(),
            max_length=args.max_length,
            packing=False,
            dataset_text_field="text",
        )
    except ImportError:
        training_args = TrainingArguments(
            output_dir=args.output_dir,
            num_train_epochs=args.epochs,
            per_device_train_batch_size=args.batch_size,
            gradient_accumulation_steps=1,
            learning_rate=args.learning_rate,
            logging_strategy="steps",
            logging_steps=1,
            save_strategy="steps",
            save_steps=args.save_steps,
            save_total_limit=2,
            remove_unused_columns=False,
            report_to="none",
            optim="adamw_torch",
            fp16=torch.cuda.is_available() and not torch.cuda.is_bf16_supported(),
            bf16=torch.cuda.is_available() and torch.cuda.is_bf16_supported(),
        )

    # formatting_func for TRL 0.29+
    def format_example(example):
        return example["text"]

    trainer = SFTTrainer(
        model=model,
        train_dataset=dataset,
        peft_config=lora_config,
        args=training_args,
        formatting_func=format_example,
        callbacks=[ProgressLogger()],
    )

    if resume_checkpoint:
        print(f"training start resume_from_checkpoint={resume_checkpoint}", flush=True)
        trainer.train(resume_from_checkpoint=resume_checkpoint)
    else:
        print("training start", flush=True)
        trainer.train()
    trainer.save_model(args.output_dir)
    print(f"training done output_dir={args.output_dir}", flush=True)


def main() -> int:
    args = parse_args()
    try:
        run_training(args)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[ERROR] {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
