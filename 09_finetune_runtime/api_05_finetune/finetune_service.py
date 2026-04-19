"""微调任务执行与控制服务。"""

from __future__ import annotations

import os
import json
import re
import sys
import time
import signal
import zipfile
import threading
import subprocess
from typing import Any, Dict, Optional, Tuple

from shared.utils.file_store import FileStore


class FinetuneService:
    """训练任务调度器（支持启动/暂停/恢复/终止）"""

    def __init__(self) -> None:
        self.models_dir = os.path.abspath(os.getenv("FINETUNE_OUTPUT_DIR", "models"))
        os.makedirs(self.models_dir, exist_ok=True)
        default_script = os.path.join(os.path.dirname(__file__), "lora_finetune_protocol.py")
        default_orpo_script = os.path.join(os.path.dirname(__file__), "orpo_finetune_protocol.py")
        self.script_path = os.getenv("FINETUNE_SCRIPT", default_script)
        self.orpo_script_path = os.getenv("FINETUNE_ORPO_SCRIPT", default_orpo_script)
        self.task_state_file = "task_state.json"
        self.processes: Dict[str, subprocess.Popen] = {}
        self._lock = threading.Lock()
        self._persist_lock = threading.Lock()
        self._file_store = FileStore()

    @staticmethod
    def _normalize_model_name(raw: Any) -> str:
        name = str(raw or "").strip()
        if not name:
            return ""
        lower = name.lower()
        if lower in {"qwen2.5-0.5b", "qwen2.5-0.5b-instruct", "qwen2.5-0___5b", "qwen2.5-0___5b-instruct"}:
            return "Qwen/Qwen2.5-0.5B-Instruct"
        if lower in {"qwen3"}:
            return os.getenv("LLM_MODEL_NAME", "Qwen/Qwen3-4B")
        return name

    def _resolve_dataset_path(self, raw_dataset: Any, output_dir: str, train_mode: str = "sft") -> str:
        dataset = str(raw_dataset or "").strip()
        if not dataset:
            return ""
        if os.path.isfile(dataset):
            return dataset

        dataset_id = dataset
        if train_mode == "orpo":
            export_path = os.path.join(output_dir, f"preference_{dataset_id}.jsonl")
            exported = self._file_store.export_preference_data(dataset_id, output_path=export_path)
        else:
            export_path = os.path.join(output_dir, f"train_{dataset_id}.jsonl")
            exported = self._file_store.export_training_data(dataset_id, output_path=export_path)
        try:
            if os.path.getsize(exported) <= 0:
                raise RuntimeError("训练数据为空（可能未生成数据或全部被过滤）")
        except OSError as exc:
            raise RuntimeError(f"训练数据导出失败: {exported}") from exc
        return exported

    def _resolve_script_path(self, config: Dict[str, Any]) -> str:
        train_mode = str(config.get("train_mode") or "sft").strip().lower()
        if config.get("script_path"):
            return str(config["script_path"])
        if train_mode == "orpo":
            return self.orpo_script_path
        return self.script_path

    def load_persisted_tasks(self) -> Dict[str, Dict[str, Any]]:
        """加载落盘任务状态（用于服务重启恢复上下文）"""
        tasks: Dict[str, Dict[str, Any]] = {}
        for name in os.listdir(self.models_dir):
            state_path = os.path.join(self.models_dir, name, self.task_state_file)
            if not os.path.isfile(state_path):
                continue
            try:
                with open(state_path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            job_id = str(payload.get("job_id") or name)
            payload["job_id"] = job_id
            tasks[job_id] = payload
        return tasks

    def persist_task_snapshot(self, job_id: str, task: Dict[str, Any]) -> None:
        """落盘任务状态"""
        self._persist_task(job_id, task)

    def start_training(
        self,
        job_id: str,
        config: Dict[str, Any],
        log_queue: Any,
        task_store: Dict[str, Dict[str, Any]],
    ) -> None:
        """异步启动训练线程"""
        thread = threading.Thread(
            target=self._run_training,
            args=(job_id, config, log_queue, task_store),
            daemon=True,
        )
        thread.start()

    def pause_job(self, job_id: str) -> Tuple[bool, str]:
        """暂停训练任务"""
        with self._lock:
            process = self.processes.get(job_id)
        if process is None or process.poll() is not None:
            return False, "任务未在运行"
        if not hasattr(signal, "SIGSTOP"):
            return False, "当前平台不支持暂停信号"
        os.kill(process.pid, signal.SIGSTOP)
        return True, "任务已暂停"

    def resume_job(self, job_id: str) -> Tuple[bool, str]:
        """恢复训练任务"""
        with self._lock:
            process = self.processes.get(job_id)
        if process is None or process.poll() is not None:
            return False, "任务未在运行"
        if not hasattr(signal, "SIGCONT"):
            return False, "当前平台不支持恢复信号"
        os.kill(process.pid, signal.SIGCONT)
        return True, "任务已恢复"

    def stop_job(self, job_id: str) -> Tuple[bool, str]:
        """终止训练任务"""
        with self._lock:
            process = self.processes.get(job_id)
        if process is None or process.poll() is not None:
            return False, "任务未在运行"
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
        return True, "任务已终止"

    def _run_training(
        self,
        job_id: str,
        config: Dict[str, Any],
        log_queue: Any,
        task_store: Dict[str, Dict[str, Any]],
    ) -> None:
        """训练主流程"""
        existing_progress = task_store.get(job_id, {}).get("progress", {})
        parameters = config.get("parameters", {}) if isinstance(config, dict) else {}
        resume_checkpoint = config.get("resume_from_checkpoint")
        if resume_checkpoint and isinstance(existing_progress, dict):
            current_step = int(existing_progress.get("current_step") or 0)
            total_steps = max(1, int(existing_progress.get("total_steps") or 100))
            epoch = float(existing_progress.get("epoch") or 0.0)
            progress = {
                "current_step": current_step,
                "total_steps": total_steps,
                "epoch": epoch,
                "percent": min(100, int(current_step / total_steps * 100)),
            }
        else:
            progress = {"current_step": 0, "total_steps": 100, "epoch": 0, "percent": 0}

        task_store[job_id]["status"] = "running"
        task_store[job_id]["started_at"] = time.time()
        task_store[job_id]["updated_at"] = time.time()
        task_store[job_id]["progress"] = progress
        task_store[job_id].setdefault("history_points", [])
        task_store[job_id].setdefault("best_metric", None)
        task_store[job_id].setdefault("best_checkpoint", None)
        if resume_checkpoint:
            task_store[job_id]["resume_from_checkpoint"] = resume_checkpoint
        else:
            task_store[job_id].pop("resume_from_checkpoint", None)
        save_steps = self._normalize_save_steps(parameters.get("save_steps", 500))
        current_step = int(progress.get("current_step") or 0)
        next_checkpoint_step = max(save_steps, ((current_step // save_steps) + 1) * save_steps)
        latest_metrics = {"loss": None, "acc": None, "lr": None}

        command, output_dir = self._build_command(job_id, config)
        process_env = self._build_process_env(config)
        task_store[job_id]["output_dir"] = output_dir
        self._persist_task(job_id, task_store[job_id])
        log_queue.put(
            {
                "type": "status",
                "job_id": job_id,
                "status": "running",
                "message": "训练任务已启动",
                "time": time.time(),
            }
        )
        log_queue.put(
            {
                "type": "log",
                "message": f"启动命令: {' '.join(command)}",
                "time": time.time(),
                "progress": progress,
            }
        )

        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=os.path.dirname(__file__),
                env=process_env,
            )
            with self._lock:
                self.processes[job_id] = process

            for raw_line in process.stdout or []:
                line = raw_line.strip()
                if not line:
                    continue
                progress = self._parse_progress(line, progress)
                latest_metrics = self._parse_metrics(line, latest_metrics)
                task_store[job_id]["progress"] = progress
                task_store[job_id]["updated_at"] = time.time()
                self._append_history_point(task_store[job_id], progress, latest_metrics)
                log_queue.put(
                    {
                        "type": "log",
                        "message": line,
                        "time": time.time(),
                        "progress": progress,
                        "loss": latest_metrics.get("loss"),
                        "acc": latest_metrics.get("acc"),
                        "lr": latest_metrics.get("lr"),
                    }
                )
                next_checkpoint_step = self._emit_checkpoint_events(
                    job_id=job_id,
                    progress=progress,
                    metrics=latest_metrics,
                    output_dir=output_dir,
                    save_steps=save_steps,
                    next_checkpoint_step=next_checkpoint_step,
                    task_store=task_store,
                    log_queue=log_queue,
                )
                self._persist_task(job_id, task_store[job_id])

            return_code = process.wait()
            with self._lock:
                self.processes.pop(job_id, None)

            # 被stop动作提前标记为stopped时，不再覆盖状态
            if task_store[job_id].get("status") == "stopped":
                duration_seconds = max(0.0, time.time() - float(task_store[job_id].get("started_at", time.time())))
                log_queue.put(
                    {
                        "type": "finish",
                        "success": False,
                        "status": "stopped",
                        "job_id": job_id,
                        "message": "任务已终止",
                        "metrics": {
                            "final_loss": latest_metrics.get("loss"),
                            "final_acc": latest_metrics.get("acc"),
                            "total_epoch": progress.get("epoch"),
                            "total_time": self._format_duration(duration_seconds),
                        },
                        "time": time.time(),
                    }
                )
                self._persist_task(job_id, task_store[job_id])
                return

            if return_code == 0:
                duration_seconds = max(0.0, time.time() - float(task_store[job_id].get("started_at", time.time())))
                model_zip = self._compress_model(output_dir, job_id)
                total_steps = max(1, int(progress.get("total_steps") or 100))
                final_epoch = float(
                    progress.get(
                        "epoch",
                        parameters.get("epochs", parameters.get("epoch", 0)),
                    )
                )
                finish_metrics = {
                    "final_loss": latest_metrics.get("loss"),
                    "final_acc": latest_metrics.get("acc"),
                    "total_epoch": final_epoch,
                    "total_time": self._format_duration(duration_seconds),
                }
                task_store[job_id].update(
                    {
                        "status": "completed",
                        "progress": {
                            "current_step": total_steps,
                            "total_steps": total_steps,
                            "epoch": final_epoch,
                            "percent": 100,
                        },
                        "completed_at": time.time(),
                        "model_path": model_zip,
                        "metrics": finish_metrics,
                    }
                )
                if not task_store[job_id].get("best_checkpoint"):
                    task_store[job_id]["best_checkpoint"] = task_store[job_id].get("last_checkpoint", {}).get("path")
                if not task_store[job_id].get("best_metric"):
                    task_store[job_id]["best_metric"] = {
                        "loss": latest_metrics.get("loss"),
                        "accuracy": latest_metrics.get("acc"),
                    }
                log_queue.put(
                    {
                        "type": "finish",
                        "success": True,
                        "status": "success",
                        "job_id": job_id,
                        "message": "微调任务已完成，模型权重已固化",
                        "model_path": model_zip,
                        "metrics": finish_metrics,
                        "time": time.time(),
                    }
                )
                self._persist_task(job_id, task_store[job_id])
            else:
                duration_seconds = max(0.0, time.time() - float(task_store[job_id].get("started_at", time.time())))
                fail_metrics = {
                    "final_loss": latest_metrics.get("loss"),
                    "final_acc": latest_metrics.get("acc"),
                    "total_epoch": progress.get("epoch"),
                    "total_time": self._format_duration(duration_seconds),
                }
                task_store[job_id].update(
                    {
                        "status": "failed",
                        "error": f"训练进程退出码: {return_code}",
                        "completed_at": time.time(),
                        "metrics": fail_metrics,
                    }
                )
                log_queue.put(
                    {
                        "type": "finish",
                        "success": False,
                        "status": "failed",
                        "job_id": job_id,
                        "message": f"训练失败，退出码: {return_code}",
                        "metrics": fail_metrics,
                        "time": time.time(),
                    }
                )
                self._persist_task(job_id, task_store[job_id])
        except Exception as exc:  # pylint: disable=broad-except
            with self._lock:
                self.processes.pop(job_id, None)
            task_store[job_id].update(
                {
                    "status": "failed",
                    "error": str(exc),
                    "completed_at": time.time(),
                }
            )
            log_queue.put(
                {
                    "type": "finish",
                    "success": False,
                    "job_id": job_id,
                    "message": f"训练异常: {exc}",
                    "time": time.time(),
                }
            )
            self._persist_task(job_id, task_store[job_id])

    def _build_command(self, job_id: str, config: Dict[str, Any]) -> Tuple[list, str]:
        """构造训练命令"""
        output_dir = os.path.join(self.models_dir, job_id)
        os.makedirs(output_dir, exist_ok=True)
        parameters = config.get("parameters", {})
        train_mode = str(config.get("train_mode") or "sft").strip().lower()
        script_path = self._resolve_script_path(config)
        resume_checkpoint = config.get("resume_from_checkpoint")
        resume_step = self._parse_checkpoint_step(resume_checkpoint)

        simulate = config.get("simulate", False) or os.getenv("FINETUNE_SIMULATE", "false").lower() == "true"
        model_name = self._normalize_model_name(config.get("model_name", config.get("base_model", "")))
        if simulate or not os.path.exists(script_path):
            simulation_code = (
                "import os,sys,time,json;"
                "out=sys.argv[1];base=sys.argv[2];start=max(0,int(sys.argv[3]));mode=sys.argv[4];os.makedirs(out,exist_ok=True);"
                "total=max(start,100);"
                "print('simulation start');"
                "[(print(f'progress={int(i/max(1,total)*100)}% step={i}/{total} epoch=1 loss={max(0.01,1.0-i/130):.4f} "
                "acc={min(0.99,0.5+i/220):.4f} lr=0.000200') or time.sleep(0.05)) for i in range(start,total+1,5)];"
                "cfg={'simulated':True,'base_model_name_or_path':base,'train_mode':mode};"
                "open(os.path.join(out,'adapter_config.json'),'w',encoding='utf-8').write(json.dumps(cfg,ensure_ascii=False));"
                "print('simulation done')"
            )
            return [
                sys.executable,
                "-u",
                "-c",
                simulation_code,
                output_dir,
                str(model_name),
                str(resume_step),
                train_mode,
            ], output_dir

        epochs = parameters.get("epochs", parameters.get("epoch", 3))
        raw_dataset = config.get("dataset", config.get("dataset_id", ""))
        dataset = self._resolve_dataset_path(raw_dataset, output_dir=output_dir, train_mode=train_mode)
        cmd = [
            sys.executable,
            "-u",
            script_path,
            "--model_name",
            str(model_name),
            "--dataset",
            str(dataset),
            "--epochs",
            str(epochs),
            "--learning_rate",
            str(parameters.get("learning_rate", 2e-4)),
            "--batch_size",
            str(parameters.get("batch_size", 4)),
            "--lora_rank",
            str(parameters.get("lora_rank", 16)),
            "--lora_alpha",
            str(parameters.get("lora_alpha", 32)),
            "--lora_dropout",
            str(parameters.get("lora_dropout", 0.05)),
            "--max_length",
            str(parameters.get("max_length", 2048)),
            "--save_steps",
            str(self._normalize_save_steps(parameters.get("save_steps", 500))),
            "--output_dir",
            output_dir,
        ]
        if train_mode == "orpo":
            cmd.extend(
                [
                    "--orpo_alpha",
                    str(parameters.get("orpo_alpha", 1.0)),
                    "--orpo_beta",
                    str(parameters.get("orpo_beta", 0.1)),
                ]
            )
        if resume_checkpoint:
            cmd.extend(["--resume_from_checkpoint", str(resume_checkpoint)])
        return cmd, output_dir

    @staticmethod
    def _build_process_env(config: Dict[str, Any]) -> Dict[str, str]:
        env = os.environ.copy()
        cuda_visible_devices = str(config.get("cuda_visible_devices") or "").strip()
        if cuda_visible_devices:
            env["CUDA_VISIBLE_DEVICES"] = cuda_visible_devices
        return env

    def _parse_progress(self, line: str, prev: Dict[str, Any]) -> Dict[str, Any]:
        """从日志行中提取进度信息，仅接受显式进度标记并避免回跳。"""
        progress = dict(prev)
        step_match = re.search(r"step\s*=\s*(\d+)\s*/\s*(\d+)", line, flags=re.IGNORECASE)
        percent_match = re.search(r"progress\s*=\s*(\d+)\s*%", line, flags=re.IGNORECASE)

        candidate: Dict[str, Any] = {}
        if step_match:
            current = int(step_match.group(1))
            total = max(1, int(step_match.group(2)))
            candidate.update(
                {
                    "current_step": current,
                    "total_steps": total,
                    "percent": int(current / total * 100),
                }
            )
        elif percent_match:
            percent = max(0, min(100, int(percent_match.group(1))))
            candidate.update(
                {
                    "percent": percent,
                    "current_step": percent,
                    "total_steps": 100,
                }
            )

        prev_percent = int(progress.get("percent") or 0)
        candidate_percent = int(candidate.get("percent") or 0)
        if candidate and candidate_percent >= prev_percent:
            progress.update(candidate)

        epoch_match = re.search(r"epoch[:=\s]+(\d+(?:\.\d+)?)", line, flags=re.IGNORECASE)
        if epoch_match:
            try:
                epoch_value = float(epoch_match.group(1))
            except ValueError:
                epoch_value = progress.get("epoch", 0)
            if epoch_value >= float(progress.get("epoch") or 0):
                progress["epoch"] = epoch_value
        return progress

    def _extract_float_metric(self, line: str, names: tuple[str, ...]) -> Optional[float]:
        pattern = "|".join(re.escape(name) for name in names)
        match = re.search(
            rf"(?:^|[\s,;])(?:{pattern})\s*[:=]\s*(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)",
            line,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        try:
            return float(match.group(1))
        except ValueError:
            return None

    def _parse_metrics(self, line: str, prev: Dict[str, Any]) -> Dict[str, Any]:
        metrics = dict(prev)
        loss = self._extract_float_metric(line, ("loss", "train_loss"))
        acc = self._extract_float_metric(line, ("acc", "accuracy", "train_acc"))
        lr = self._extract_float_metric(line, ("lr", "learning_rate"))

        if loss is not None:
            metrics["loss"] = loss
        if acc is not None:
            metrics["acc"] = acc
        if lr is not None:
            metrics["lr"] = lr
        return metrics

    def _normalize_save_steps(self, value: Any) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return 500
        return parsed if parsed > 0 else 500

    def _parse_checkpoint_step(self, checkpoint_path: Any) -> int:
        if not checkpoint_path:
            return 0
        match = re.search(r"checkpoint-(\d+)", str(checkpoint_path))
        if not match:
            return 0
        try:
            return max(0, int(match.group(1)))
        except ValueError:
            return 0

    def _format_duration(self, seconds: float) -> str:
        """格式化时长（h/m/s）"""
        total = int(max(0, seconds))
        hours, remainder = divmod(total, 3600)
        minutes, sec = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m"
        if minutes > 0:
            return f"{minutes}m {sec}s"
        return f"{sec}s"

    def _append_history_point(
        self,
        task: Dict[str, Any],
        progress: Dict[str, Any],
        metrics: Dict[str, Any],
    ) -> None:
        history = task.setdefault("history_points", [])
        point = {
            "step": int(progress.get("current_step") or 0),
            "loss": metrics.get("loss"),
            "lr": metrics.get("lr"),
            "acc": metrics.get("acc"),
        }
        if history and history[-1].get("step") == point["step"]:
            history[-1] = point
        else:
            history.append(point)
        if len(history) > 2000:
            del history[0 : len(history) - 2000]

    def _to_float(self, value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _should_replace_best(
        self,
        candidate: Dict[str, Any],
        current_best: Optional[Dict[str, Any]],
    ) -> bool:
        if not current_best:
            return True

        cand_acc = self._to_float(candidate.get("accuracy"))
        best_acc = self._to_float(current_best.get("accuracy"))
        cand_loss = self._to_float(candidate.get("loss"))
        best_loss = self._to_float(current_best.get("loss"))

        if cand_acc is not None and best_acc is None:
            return True
        if cand_acc is None and best_acc is not None:
            return False
        if cand_acc is not None and best_acc is not None:
            if cand_acc > best_acc:
                return True
            if cand_acc < best_acc:
                return False

        if cand_loss is not None and best_loss is None:
            return True
        if cand_loss is None and best_loss is not None:
            return False
        if cand_loss is not None and best_loss is not None:
            return cand_loss < best_loss

        return False

    def _update_best_checkpoint(self, task: Dict[str, Any], checkpoint: Dict[str, Any]) -> None:
        metrics = checkpoint.get("metrics") or {}
        candidate_metric = {
            "loss": metrics.get("loss"),
            "accuracy": metrics.get("acc"),
        }
        if self._should_replace_best(candidate_metric, task.get("best_metric")):
            task["best_metric"] = candidate_metric
            task["best_checkpoint"] = checkpoint.get("checkpoint_path")

    def _emit_checkpoint_events(
        self,
        job_id: str,
        progress: Dict[str, Any],
        metrics: Dict[str, Any],
        output_dir: str,
        save_steps: int,
        next_checkpoint_step: int,
        task_store: Dict[str, Dict[str, Any]],
        log_queue: Any,
    ) -> int:
        if save_steps <= 0:
            return save_steps

        current_step = int(progress.get("current_step") or 0)
        while current_step >= next_checkpoint_step:
            checkpoint = self._build_checkpoint_payload(
                job_id=job_id,
                step=next_checkpoint_step,
                output_dir=output_dir,
                progress=progress,
                metrics=metrics,
            )
            task_store[job_id]["last_checkpoint"] = {
                "path": checkpoint["checkpoint_path"],
                "timestamp": checkpoint["timestamp"],
                "metrics": checkpoint["metrics"],
            }
            self._update_best_checkpoint(task_store[job_id], checkpoint)
            task_store[job_id]["updated_at"] = time.time()
            log_queue.put(checkpoint)
            next_checkpoint_step += save_steps
        return next_checkpoint_step

    def _build_checkpoint_payload(
        self,
        job_id: str,
        step: int,
        output_dir: str,
        progress: Dict[str, Any],
        metrics: Dict[str, Any],
    ) -> Dict[str, Any]:
        checkpoint_root = os.path.join(output_dir, "checkpoints")
        checkpoint_path = os.path.join(checkpoint_root, f"checkpoint-{step}")
        os.makedirs(checkpoint_path, exist_ok=True)

        payload = {
            "type": "checkpoint",
            "job_id": job_id,
            "step": step,
            "checkpoint_path": checkpoint_path,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "metrics": {
                "loss": metrics.get("loss"),
                "acc": metrics.get("acc"),
                "lr": metrics.get("lr"),
                "epoch": progress.get("epoch"),
            },
            "message": f"Checkpoint-{step} saved.",
        }
        meta_path = os.path.join(checkpoint_path, "checkpoint.meta.json")
        with open(meta_path, "w", encoding="utf-8") as meta_file:
            json.dump(payload["metrics"], meta_file, ensure_ascii=False, indent=2)
        return payload

    def _compress_model(self, model_dir: str, job_id: str) -> str:
        """压缩模型目录"""
        zip_path = os.path.join(self.models_dir, f"{job_id}.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for root, _, files in os.walk(model_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, model_dir)
                    zip_file.write(file_path, arcname)
        return zip_path

    def _state_path(self, job_id: str, task: Dict[str, Any]) -> str:
        output_dir = task.get("output_dir")
        if not output_dir:
            output_dir = os.path.join(self.models_dir, job_id)
        os.makedirs(output_dir, exist_ok=True)
        return os.path.join(output_dir, self.task_state_file)

    def _persist_task(self, job_id: str, task: Dict[str, Any]) -> None:
        state_path = self._state_path(job_id, task)
        payload = {
            "job_id": job_id,
            "status": task.get("status"),
            "config": task.get("config", {}),
            "progress": task.get("progress", {}),
            "created_at": task.get("created_at"),
            "started_at": task.get("started_at"),
            "updated_at": task.get("updated_at"),
            "completed_at": task.get("completed_at"),
            "metrics": task.get("metrics"),
            "last_checkpoint": task.get("last_checkpoint"),
            "history_points": task.get("history_points", []),
            "best_metric": task.get("best_metric"),
            "best_checkpoint": task.get("best_checkpoint"),
            "model_path": task.get("model_path"),
            "output_dir": task.get("output_dir"),
            "error": task.get("error"),
            "resume_from_checkpoint": task.get("resume_from_checkpoint"),
        }
        # 多线程并发写同一任务状态时，使用唯一临时文件并串行替换，避免竞态导致FileNotFoundError。
        tmp_path = f"{state_path}.{threading.get_ident()}.{time.time_ns()}.tmp"
        with self._persist_lock:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, state_path)
