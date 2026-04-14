"""Email normalization helpers used across redemption and warranty flows."""
from __future__ import annotations

from typing import Iterable, Optional

from sqlalchemy import func


def normalize_email(email: Optional[str]) -> Optional[str]:
    """Normalize an email for storage and comparisons."""
    if email is None:
        return None

    normalized = str(email).strip()
    if not normalized:
        return None
    return normalized.lower()


def emails_match(left: Optional[str], right: Optional[str]) -> bool:
    """Return whether two email values match after normalization."""
    left_normalized = normalize_email(left)
    right_normalized = normalize_email(right)
    return left_normalized is not None and left_normalized == right_normalized


def normalize_email_set(emails: Iterable[Optional[str]]) -> set[str]:
    """Normalize a collection of email values into a deduplicated set."""
    return {
        normalized
        for email in emails
        if (normalized := normalize_email(email)) is not None
    }


def email_equals(column, email: Optional[str]):
    """Build a case-insensitive SQL equality expression for email columns."""
    normalized = normalize_email(email)
    if normalized is None:
        raise ValueError("email must be provided")
    return func.lower(func.trim(column)) == normalized
