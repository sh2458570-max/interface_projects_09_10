"""LoRA preference finetuning entrypoint with an ORPO-style objective."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List

import torch
import torch.nn.functional as F
from datasets import Dataset, load_dataset
from peft import LoraConfig, get_peft_model
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    Trainer,
    TrainerCallback,
    TrainingArguments,
)

try:
    from peft import prepare_model_for_kbit_training
except ImportError:  # pragma: no cover - optional peft helper
    prepare_model_for_kbit_training = None  # type: ignore[assignment]


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
        acc = max(0.0, min(1.0, 1.0 / (1.0 + max(loss, 0.0))))

        print(
            (
                f"progress={percent}% step={step}/{max_steps} "
                f"epoch={epoch:.4f} loss={loss:.6f} acc={acc:.6f} lr={lr:.8f}"
            ),
            flush=True,
        )


@dataclass
class PreferenceBatchCollator:
    """Pad chosen and rejected sequences into one batch."""

    pad_token_id: int
    label_pad_token_id: int = -100

    def _pad(self, sequences: List[List[int]], pad_value: int) -> torch.Tensor:
        max_length = max(len(item) for item in sequences)
        padded = [item + [pad_value] * (max_length - len(item)) for item in sequences]
        return torch.tensor(padded, dtype=torch.long)

    def __call__(self, features: List[Dict[str, Any]]) -> Dict[str, torch.Tensor]:
        """Convert a feature list into a model batch.

        Args:
            features: Tokenized preference examples.

        Returns:
            A padded batch for chosen and rejected sequences.
        """
        batch: Dict[str, torch.Tensor] = {}
        for prefix in ("chosen", "rejected"):
            batch[f"{prefix}_input_ids"] = self._pad(
                [list(feature[f"{prefix}_input_ids"]) for feature in features],
                self.pad_token_id,
            )
            batch[f"{prefix}_attention_mask"] = self._pad(
                [list(feature[f"{prefix}_attention_mask"]) for feature in features],
                0,
            )
            batch[f"{prefix}_labels"] = self._pad(
                [list(feature[f"{prefix}_labels"]) for feature in features],
                self.label_pad_token_id,
            )
        return batch


class ORPOTrainerLite(Trainer):
    """A lightweight ORPO-style trainer built on top of ``Trainer``."""

    def __init__(self, *args, orpo_alpha: float = 1.0, orpo_beta: float = 0.1, **kwargs):
        super().__init__(*args, **kwargs)
        self.orpo_alpha = float(orpo_alpha)
        self.orpo_beta = float(orpo_beta)

    @staticmethod
    def _sequence_logps(logits: torch.Tensor, labels: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        shift_logits = logits[:, :-1, :].contiguous()
        shift_labels = labels[:, 1:].contiguous()
        valid_mask = shift_labels.ne(-100)
        safe_labels = shift_labels.masked_fill(~valid_mask, 0)

        log_probs = F.log_softmax(shift_logits, dim=-1)
        token_logps = log_probs.gather(-1, safe_labels.unsqueeze(-1)).squeeze(-1)
        token_logps = token_logps * valid_mask
        token_count = valid_mask.sum(dim=-1).clamp(min=1)
        avg_logp = token_logps.sum(dim=-1) / token_count
        nll = -(token_logps.sum(dim=-1) / token_count)
        return avg_logp, nll

    @staticmethod
    def _log_odds(avg_logp: torch.Tensor) -> torch.Tensor:
        probs = avg_logp.exp().clamp(min=1e-7, max=1 - 1e-7)
        return torch.log(probs) - torch.log1p(-probs)

    def compute_loss(self, model, inputs, return_outputs=False, **kwargs):  # pylint: disable=unused-argument
        chosen_outputs = model(
            input_ids=inputs["chosen_input_ids"],
            attention_mask=inputs["chosen_attention_mask"],
        )
        rejected_outputs = model(
            input_ids=inputs["rejected_input_ids"],
            attention_mask=inputs["rejected_attention_mask"],
        )

        chosen_logp, chosen_nll = self._sequence_logps(chosen_outputs.logits, inputs["chosen_labels"])
        rejected_logp, _ = self._sequence_logps(rejected_outputs.logits, inputs["rejected_labels"])
        log_odds_gap = self._log_odds(chosen_logp) - self._log_odds(rejected_logp)
        preference_loss = -F.logsigmoid(self.orpo_beta * log_odds_gap)
        loss = chosen_nll.mean() + self.orpo_alpha * preference_loss.mean()

        if return_outputs:
            return loss, {
                "chosen_logp": chosen_logp.detach(),
                "rejected_logp": rejected_logp.detach(),
            }
        return loss


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for ORPO training."""
    parser = argparse.ArgumentParser(description="LoRA ORPO finetune for protocol conversion")
    parser.add_argument("--model_name", required=True, help="Base model path or model id")
    parser.add_argument("--dataset", required=True, help="Preference dataset file path (json/jsonl)")
    parser.add_argument("--output_dir", required=True, help="Output directory for LoRA adapters")
    parser.add_argument("--epochs", type=float, default=2.0, help="Training epochs")
    parser.add_argument("--learning_rate", type=float, default=5e-6, help="Learning rate")
    parser.add_argument("--batch_size", type=int, default=1, help="Per-device train batch size")
    parser.add_argument("--lora_rank", type=int, default=16, help="LoRA rank")
    parser.add_argument("--lora_alpha", type=int, default=32, help="LoRA alpha")
    parser.add_argument("--lora_dropout", type=float, default=0.05, help="LoRA dropout")
    parser.add_argument("--max_length", type=int, default=1024, help="Max sequence length")
    parser.add_argument("--save_steps", type=int, default=200, help="Checkpoint save interval")
    parser.add_argument("--orpo_alpha", type=float, default=1.0, help="Weight of the preference loss")
    parser.add_argument("--orpo_beta", type=float, default=0.1, help="Scale of the log-odds margin")
    parser.add_argument(
        "--resume_from_checkpoint",
        default="",
        help="Optional checkpoint path for resuming training",
    )
    return parser.parse_args()


def _require_positive(name: str, value: float | int) -> None:
    """Validate one positive numeric argument.

    Args:
        name: Argument name.
        value: Numeric argument value.

    Raises:
        ValueError: If the value is not positive.
    """
    if value <= 0:
        raise ValueError(f"参数 {name} 必须大于 0，当前值: {value}")


def _build_chat_prompt(example: Dict[str, Any]) -> str:
    """Convert one preference example into a chat prompt prefix.

    Args:
        example: Raw dataset record.

    Returns:
        A prompt string that ends with the assistant prefix.
    """
    prompt = str(example.get("prompt", "")).strip()
    if prompt:
        return prompt

    system_prompt = str(example.get("system_prompt", "")).strip()
    user_prompt = str(example.get("user_prompt", "")).strip()
    if not user_prompt:
        raise ValueError("字段 user_prompt 或 prompt 不能为空")
    if not system_prompt:
        system_prompt = "你是一个专业的网络消息协议转换助手，严格按照指令输出可执行规则。"
    return (
        "<|im_start|>system\n"
        f"{system_prompt}\n"
        "<|im_end|>\n"
        "<|im_start|>user\n"
        f"{user_prompt}\n"
        "<|im_end|>\n"
        "<|im_start|>assistant\n"
    )


def load_and_validate_dataset(dataset_path: str) -> Dataset:
    """Load and validate one ORPO preference dataset.

    Args:
        dataset_path: Preference dataset path.

    Returns:
        A validated dataset with prompt/chosen/rejected columns.
    """
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
    if "chosen" not in columns or "rejected" not in columns:
        raise ValueError("偏好数据集字段不合法：至少需要 chosen + rejected，且需提供 prompt 或 system_prompt+user_prompt")

    sample_count = min(20, dataset.num_rows)
    for idx in range(sample_count):
        record = dataset[idx]
        prompt = _build_chat_prompt(record)
        chosen = str(record.get("chosen", "")).strip()
        rejected = str(record.get("rejected", "")).strip()
        if not prompt:
            raise ValueError(f"数据集第 {idx + 1} 条样本 prompt 为空")
        if not chosen:
            raise ValueError(f"数据集第 {idx + 1} 条样本 chosen 为空")
        if not rejected:
            raise ValueError(f"数据集第 {idx + 1} 条样本 rejected 为空")

    return dataset


def _build_quant_config() -> BitsAndBytesConfig | None:
    """Create a 4-bit quantization config when available."""
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


def _truncate_pair(prompt_ids: List[int], response_ids: List[int], max_length: int) -> tuple[List[int], List[int]]:
    """Trim one prompt/response pair to ``max_length`` tokens."""
    response_ids = list(response_ids[:max_length])
    available_prompt = max(1, max_length - len(response_ids))
    prompt_ids = list(prompt_ids[-available_prompt:])
    if len(prompt_ids) + len(response_ids) > max_length:
        response_ids = response_ids[: max_length - len(prompt_ids)]
    if not response_ids:
        response_ids = prompt_ids[-1:]
        prompt_ids = prompt_ids[:-1] or prompt_ids
    return prompt_ids, response_ids


def _tokenize_record(example: Dict[str, Any], tokenizer: AutoTokenizer, max_length: int) -> Dict[str, Any]:
    """Tokenize one prompt/chosen/rejected record for ORPO training."""
    prompt_text = _build_chat_prompt(example)
    chosen_text = str(example.get("chosen", "")).strip()
    rejected_text = str(example.get("rejected", "")).strip()
    eos_text = tokenizer.eos_token or ""

    prompt_ids = tokenizer(prompt_text, add_special_tokens=False)["input_ids"]
    chosen_ids = tokenizer(chosen_text + eos_text, add_special_tokens=False)["input_ids"]
    rejected_ids = tokenizer(rejected_text + eos_text, add_special_tokens=False)["input_ids"]
    chosen_prompt_ids, chosen_response_ids = _truncate_pair(prompt_ids, chosen_ids, max_length=max_length)
    rejected_prompt_ids, rejected_response_ids = _truncate_pair(prompt_ids, rejected_ids, max_length=max_length)

    chosen_input_ids = chosen_prompt_ids + chosen_response_ids
    rejected_input_ids = rejected_prompt_ids + rejected_response_ids
    return {
        "chosen_input_ids": chosen_input_ids,
        "chosen_attention_mask": [1] * len(chosen_input_ids),
        "chosen_labels": [-100] * len(chosen_prompt_ids) + chosen_response_ids,
        "rejected_input_ids": rejected_input_ids,
        "rejected_attention_mask": [1] * len(rejected_input_ids),
        "rejected_labels": [-100] * len(rejected_prompt_ids) + rejected_response_ids,
    }


def run_training(args: argparse.Namespace) -> None:
    """Run one ORPO training job."""
    _require_positive("epochs", args.epochs)
    _require_positive("learning_rate", args.learning_rate)
    _require_positive("batch_size", args.batch_size)
    _require_positive("lora_rank", args.lora_rank)
    _require_positive("lora_alpha", args.lora_alpha)
    _require_positive("max_length", args.max_length)
    _require_positive("save_steps", args.save_steps)
    _require_positive("orpo_alpha", args.orpo_alpha)
    _require_positive("orpo_beta", args.orpo_beta)
    resume_checkpoint = str(args.resume_from_checkpoint or "").strip()
    if resume_checkpoint and not os.path.exists(resume_checkpoint):
        raise FileNotFoundError(f"恢复点不存在: {resume_checkpoint}")

    os.makedirs(args.output_dir, exist_ok=True)

    dataset = load_and_validate_dataset(args.dataset)
    tokenizer = AutoTokenizer.from_pretrained(args.model_name, trust_remote_code=True, use_fast=False)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token or tokenizer.unk_token
    tokenizer.padding_side = "right"

    tokenized_dataset = dataset.map(
        lambda example: _tokenize_record(example, tokenizer, args.max_length),
        remove_columns=list(dataset.column_names or []),
    )

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
    if quantization_config is not None and prepare_model_for_kbit_training is not None:
        model = prepare_model_for_kbit_training(model)
    model.config.use_cache = False

    lora_config = LoraConfig(
        r=args.lora_rank,
        lora_alpha=args.lora_alpha,
        lora_dropout=args.lora_dropout,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj"],
        bias="none",
        task_type="CAUSAL_LM",
    )
    model = get_peft_model(model, lora_config)

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

    trainer = ORPOTrainerLite(
        model=model,
        args=training_args,
        train_dataset=tokenized_dataset,
        data_collator=PreferenceBatchCollator(pad_token_id=tokenizer.pad_token_id),
        callbacks=[ProgressLogger()],
        orpo_alpha=args.orpo_alpha,
        orpo_beta=args.orpo_beta,
    )

    if resume_checkpoint:
        print(f"training start resume_from_checkpoint={resume_checkpoint}", flush=True)
        trainer.train(resume_from_checkpoint=resume_checkpoint)
    else:
        print("training start", flush=True)
        trainer.train()
    trainer.save_model(args.output_dir)
    tokenizer.save_pretrained(args.output_dir)
    print(f"training done output_dir={args.output_dir}", flush=True)


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    try:
        run_training(args)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[ERROR] {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
