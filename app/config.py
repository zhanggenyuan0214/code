"""
Application settings.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_URL = "sqlite+aiosqlite:///./data/team_manage.db"
DEFAULT_SECRET_KEY = "your-secret-key-here-change-in-production"
DEFAULT_ADMIN_PASSWORD = "admin123"
WINDOWS_SQLITE_ABSOLUTE_PATH = re.compile(r"^/[A-Za-z]:[/\\]")
logger = logging.getLogger(__name__)


def _build_sqlite_url(path: Path, scheme: str = "sqlite+aiosqlite") -> str:
    return f"{scheme}:///{path.as_posix()}"


def _normalize_database_url(value: str) -> str:
    database_url = (value or DEFAULT_SQLITE_URL).strip()
    parsed = urlsplit(database_url)

    if not parsed.scheme.startswith("sqlite"):
        return database_url

    if parsed.path in {":memory:", "/:memory:"}:
        return database_url

    if parsed.path.startswith("//") or WINDOWS_SQLITE_ABSOLUTE_PATH.match(parsed.path):
        return database_url

    relative_path = Path(parsed.path.lstrip("/"))
    absolute_path = (BASE_DIR / relative_path).resolve()
    suffix = ""
    if parsed.query:
        suffix += f"?{parsed.query}"
    if parsed.fragment:
        suffix += f"#{parsed.fragment}"
    return f"{_build_sqlite_url(absolute_path, parsed.scheme)}{suffix}"


def _database_path_from_url(database_url: str) -> Path | None:
    parsed = urlsplit(database_url)
    if not parsed.scheme.startswith("sqlite"):
        return None

    if parsed.path in {":memory:", "/:memory:"}:
        return None

    raw_path = parsed.path
    if WINDOWS_SQLITE_ABSOLUTE_PATH.match(raw_path):
        raw_path = raw_path[1:]
    return Path(raw_path)


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    app_name: str = "GPT Team 管理系统"
    app_version: str = "0.1.0"
    app_host: str = "0.0.0.0"
    app_port: int = 8008
    debug: bool = True
    environment: str = "development"

    database_url: str = DEFAULT_SQLITE_URL

    secret_key: str = DEFAULT_SECRET_KEY
    admin_password: str = DEFAULT_ADMIN_PASSWORD
    session_https_only: bool | None = None
    tls_ca_bundle: str = ""

    log_level: str = "INFO"
    database_echo: bool = False

    proxy: str = ""
    proxy_enabled: bool = False

    timezone: str = "Asia/Shanghai"

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    @field_validator("database_url", mode="before")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        return _normalize_database_url(value)

    @property
    def is_production(self) -> bool:
        return self.environment.strip().lower() in {"prod", "production"}

    @property
    def database_path(self) -> Path | None:
        return _database_path_from_url(self.database_url)

    @property
    def session_https_only_enabled(self) -> bool:
        if self.session_https_only is not None:
            return self.session_https_only
        return self.is_production

    @property
    def tls_verify(self) -> bool | str:
        if not self.tls_ca_bundle:
            return True

        bundle_path = Path(self.tls_ca_bundle).expanduser()
        if not bundle_path.is_absolute():
            bundle_path = (BASE_DIR / bundle_path).resolve()
        return str(bundle_path)

    def validate_security(self) -> None:
        errors: list[str] = []
        has_default_credentials = False

        if self.secret_key == DEFAULT_SECRET_KEY:
            has_default_credentials = True
            logger.warning("Security warning: current configuration is still using default SECRET_KEY.")
            if self.is_production:
                errors.append("SECRET_KEY must be changed in production")

        if self.admin_password == DEFAULT_ADMIN_PASSWORD:
            has_default_credentials = True
            logger.warning("Security warning: current configuration is still using default ADMIN_PASSWORD.")
            if self.is_production:
                errors.append("ADMIN_PASSWORD must be changed in production")

        if has_default_credentials and not self.is_production:
            logger.warning("WARNING: Running with default credentials, NOT suitable for public deployment")

        if self.tls_ca_bundle:
            bundle_path = Path(self.tls_verify)
            if not bundle_path.exists():
                errors.append(f"TLS_CA_BUNDLE does not exist: {bundle_path}")
            elif not bundle_path.is_file():
                errors.append(f"TLS_CA_BUNDLE is not a file: {bundle_path}")

        if errors:
            raise ValueError("; ".join(errors))


settings = Settings()
