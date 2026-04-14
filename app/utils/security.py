"""
Security helpers for masking sensitive values.
"""
from __future__ import annotations

def mask_secret(value: str | None, prefix: int = 6, suffix: int = 4) -> str:
    """Return a masked representation of a sensitive value."""
    if not value:
        return ""

    value = value.strip()
    if not value:
        return ""

    if len(value) <= prefix + suffix:
        if len(value) <= 2:
            return "*" * len(value)
        return f"{value[0]}{'*' * (len(value) - 2)}{value[-1]}"

    return f"{value[:prefix]}{'*' * 8}{value[-suffix:]}"


def has_masked_placeholder(value: str | None) -> bool:
    """Return True when the supplied value looks like a masked preview."""
    return bool(value and "*" in value)
