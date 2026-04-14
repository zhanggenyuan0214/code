"""
Shared application bootstrap helpers.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import AsyncSessionLocal, init_db
from app.db_migrations import run_auto_migration
from app.models import Setting
from app.services.auth import auth_service
from app.services.settings import settings_service

logger = logging.getLogger(__name__)

DEFAULT_SETTING_SPECS = (
    (
        "proxy",
        lambda: settings.proxy or "",
        "代理地址 (支持 http:// 和 socks5://)",
    ),
    (
        "proxy_enabled",
        lambda: str(settings.proxy_enabled).lower(),
        "是否启用代理",
    ),
    (
        "log_level",
        lambda: settings.log_level,
        "日志级别",
    ),
    (
        "homepage_announcement",
        lambda: "",
        "用户首页顶部公告",
    ),
    (
        "homepage_usage_notice",
        lambda: "",
        "用户首页使用须知",
    ),
)
LEGACY_SETTING_KEYS = {"initialized"}


@dataclass
class BootstrapResult:
    """Summary of the bootstrap work performed."""

    persisted_log_level: str | None
    created_settings: list[str] = field(default_factory=list)
    removed_legacy_settings: list[str] = field(default_factory=list)


async def ensure_default_settings(session: AsyncSession) -> tuple[list[str], list[str]]:
    """Insert required default settings when they are missing."""
    tracked_keys = {key for key, _, _ in DEFAULT_SETTING_SPECS} | LEGACY_SETTING_KEYS
    result = await session.execute(select(Setting).where(Setting.key.in_(tracked_keys)))
    existing_settings = {setting.key: setting for setting in result.scalars().all()}

    created_settings: list[str] = []
    for key, value_factory, description in DEFAULT_SETTING_SPECS:
        if key in existing_settings:
            continue

        session.add(
            Setting(
                key=key,
                value=value_factory(),
                description=description,
            )
        )
        created_settings.append(key)

    removed_legacy_settings: list[str] = []
    for legacy_key in LEGACY_SETTING_KEYS:
        legacy_setting = existing_settings.get(legacy_key)
        if legacy_setting is None:
            continue

        await session.delete(legacy_setting)
        removed_legacy_settings.append(legacy_key)

    if created_settings or removed_legacy_settings:
        await session.commit()
        for key in [*created_settings, *removed_legacy_settings]:
            settings_service._cache.pop(key, None)

    return created_settings, removed_legacy_settings


async def bootstrap_application() -> BootstrapResult:
    """Run the shared database/application bootstrap sequence."""
    if settings.database_path is not None:
        settings.database_path.parent.mkdir(parents=True, exist_ok=True)

    await init_db()
    run_auto_migration()

    async with AsyncSessionLocal() as session:
        created_settings, removed_legacy_settings = await ensure_default_settings(session)
        await auth_service.initialize_admin_password(session)
        persisted_log_level = await settings_service.get_setting(session, "log_level")

    return BootstrapResult(
        persisted_log_level=persisted_log_level,
        created_settings=created_settings,
        removed_legacy_settings=removed_legacy_settings,
    )
