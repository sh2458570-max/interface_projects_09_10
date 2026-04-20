"""Lightweight mapping evaluation helpers for examples and tests."""

from __future__ import annotations

import math
from types import SimpleNamespace

from project_generator.models import ConversionSpec


def clamp(value: float, low: float, high: float) -> float:
    """Clamps a numeric value into a closed interval."""

    return max(low, min(value, high))


def evaluate_conversion(
    conversion: ConversionSpec,
    inputs: dict[str, dict[str, int | float]],
) -> dict[str, int | float]:
    """Evaluates one conversion against sample input payloads.

    Args:
        conversion: The conversion definition to evaluate.
        inputs: Mapping from source alias to field-value dictionary.

    Returns:
        A dictionary containing the evaluated target fields.
    """

    namespace = {
        "abs": abs,
        "min": min,
        "max": max,
        "pow": pow,
        "round": round,
        "floor": math.floor,
        "ceil": math.ceil,
        "clamp": clamp,
    }
    for alias, payload in inputs.items():
        namespace[alias] = SimpleNamespace(**payload)

    target: dict[str, int | float] = {}
    for rule in conversion.rules:
        if rule.when:
            matched = bool(eval(rule.when, {"__builtins__": {}}, namespace))
            if not matched:
                if rule.default_value is not None:
                    target[rule.target_field] = eval(
                        str(rule.default_value),
                        {"__builtins__": {}},
                        namespace,
                    )
                continue
        target[rule.target_field] = eval(rule.formula, {"__builtins__": {}}, namespace)
    return target
