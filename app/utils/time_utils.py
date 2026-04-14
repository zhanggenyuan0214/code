from __future__ import annotations

from datetime import datetime, timezone, tzinfo
from typing import Any
from zoneinfo import ZoneInfo

import pytz
from sqlalchemy import String
from sqlalchemy.types import TypeDecorator

from app.config import settings


UTC = timezone.utc


def _resolve_timezone(value: tzinfo | str | None) -> tzinfo:
    if value is None:
        return UTC
    if isinstance(value, str):
        try:
            return ZoneInfo(value)
        except Exception:
            return pytz.timezone(value)
    return value


def get_app_timezone() -> tzinfo:
    return _resolve_timezone(settings.timezone)


def get_now() -> datetime:
    """Return the current time as an aware UTC datetime."""
    return datetime.now(UTC)


def get_local_now(timezone_name: tzinfo | str | None = None) -> datetime:
    return get_now().astimezone(_resolve_timezone(timezone_name or settings.timezone))


def parse_datetime(
    value: Any,
    *,
    assume_timezone: tzinfo | str | None = UTC,
) -> datetime | None:
    """Normalize supported datetime inputs into an aware UTC datetime."""
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        dt = datetime.fromisoformat(raw)
    else:
        raise TypeError(f"Unsupported datetime value: {type(value)!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_resolve_timezone(assume_timezone))

    return dt.astimezone(UTC)


def parse_unix_timestamp(value: int | float | str | None) -> datetime | None:
    if value in (None, ""):
        return None
    return datetime.fromtimestamp(float(value), tz=UTC)


def to_timezone(
    value: datetime | str | None,
    timezone_name: tzinfo | str | None = None,
    *,
    assume_timezone: tzinfo | str | None = UTC,
) -> datetime | None:
    dt = parse_datetime(value, assume_timezone=assume_timezone)
    if dt is None:
        return None
    return dt.astimezone(_resolve_timezone(timezone_name or settings.timezone))


def is_expired(
    value: datetime | str | None,
    *,
    now: datetime | None = None,
    assume_timezone: tzinfo | str | None = UTC,
) -> bool:
    dt = parse_datetime(value, assume_timezone=assume_timezone)
    if dt is None:
        return False

    reference = parse_datetime(now, assume_timezone=assume_timezone) if now is not None else get_now()
    return dt <= reference


class UTCDateTime(TypeDecorator):
    """
    Persist datetimes as ISO-8601 strings and always return aware UTC datetimes.

    Legacy naive values are interpreted with the configured fallback timezone.
    New writes are always stored with an explicit UTC offset, removing future
    ambiguity.
    """

    impl = String(40)
    cache_ok = True

    def __init__(self, naive_assume_timezone: tzinfo | str | None = UTC, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.naive_assume_timezone = naive_assume_timezone

    @property
    def python_type(self) -> type[datetime]:
        return datetime

    def process_bind_param(self, value: Any, dialect: Any) -> str | None:
        dt = parse_datetime(value, assume_timezone=self.naive_assume_timezone)
        if dt is None:
            return None
        return dt.isoformat(sep=" ")

    def process_result_value(self, value: Any, dialect: Any) -> datetime | None:
        return parse_datetime(value, assume_timezone=self.naive_assume_timezone)

    def copy(self, **kw: Any) -> "UTCDateTime":
        return UTCDateTime(self.naive_assume_timezone)
