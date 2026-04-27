"""Validate pipeline YAML after ``safe_load`` (fail fast, clear errors).

Hand-written rules (no Pydantic) to keep dependencies minimal. Adjust
``_ALLOWED_TOP_LEVEL_KEYS`` when the schema grows.
"""

from __future__ import annotations

_ALLOWED_TOP_LEVEL_KEYS = frozenset(
    {
        "tickers",
        "thresholds",
        "thresholds_wow",
        "default_threshold_dod",
        "default_threshold_wow",
        "anomaly_warning_limit",
        "checks",
    }
)
_ALLOWED_CHECK_KEYS = frozenset({"DoD", "WoW"})


def _is_real_number(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _validate_fraction(label: str, value, *, upper: float = 1.0) -> None:
    if not _is_real_number(value):
        raise ValueError(f"Invalid config: {label} must be a number (not bool)")
    v = float(value)
    if not (0 < v <= upper):
        raise ValueError(f"Invalid config: {label} must be in (0, {upper}], got {value!r}")


def validate_pipeline_config(config: object) -> None:
    """Raise ``ValueError`` with an ``Invalid config:`` message if rules fail."""
    if config is None:
        raise ValueError("Invalid config: file is empty or YAML is null")
    if not isinstance(config, dict):
        raise ValueError("Invalid config: top level must be a mapping (object)")

    unknown = set(config) - _ALLOWED_TOP_LEVEL_KEYS
    if unknown:
        keys = ", ".join(sorted(unknown))
        raise ValueError(f"Invalid config: unknown top-level key(s): {keys}")

    tickers = config.get("tickers")
    if tickers is None:
        raise ValueError("Invalid config: tickers is required")
    if not isinstance(tickers, list) or len(tickers) == 0:
        raise ValueError("Invalid config: tickers must be a non-empty list")
    for i, t in enumerate(tickers):
        if not isinstance(t, str) or not t.strip():
            raise ValueError(f"Invalid config: tickers[{i}] must be a non-empty string")

    for name in ("thresholds", "thresholds_wow"):
        block = config.get(name)
        if block is None:
            continue
        if not isinstance(block, dict):
            raise ValueError(f"Invalid config: {name} must be a mapping")
        for key, val in block.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(f"Invalid config: {name} has an invalid key {key!r}")
            if key not in tickers:
                raise ValueError(f"Invalid config: {name} has an invalid key {key!r}")
            _validate_fraction(f"{name}[{key!r}]", val)

    if "default_threshold_dod" in config:
        _validate_fraction("default_threshold_dod", config["default_threshold_dod"])
    if "default_threshold_wow" in config:
        _validate_fraction("default_threshold_wow", config["default_threshold_wow"])
    if "anomaly_warning_limit" in config:
        _validate_fraction("anomaly_warning_limit", config["anomaly_warning_limit"])

    checks = config.get("checks")
    if checks is None:
        return
    if not isinstance(checks, dict):
        raise ValueError("Invalid config: checks must be a mapping")
    bad_check_keys = set(checks) - _ALLOWED_CHECK_KEYS
    if bad_check_keys:
        keys = ", ".join(sorted(bad_check_keys))
        raise ValueError(f"Invalid config: checks may only contain DoD and WoW, not: {keys}")
    for key in _ALLOWED_CHECK_KEYS:
        if key not in checks:
            continue
        val = checks[key]
        if not isinstance(val, bool):
            raise ValueError(
                f"Invalid config: checks.{key} must be true or false (boolean), got {type(val).__name__}"
            )
