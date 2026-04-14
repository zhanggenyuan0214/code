"""
数据库连接模块，提供异步数据库连接配置和会话管理。
"""
from typing import Any

from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import StaticPool

from app.config import settings


def _build_engine_kwargs(database_url: str) -> dict[str, Any]:
    """Return dialect-aware engine options."""
    url = make_url(database_url)
    engine_kwargs: dict[str, Any] = {
        "echo": settings.database_echo,
        "future": True,
        "connect_args": {"timeout": 60},
    }

    if url.get_backend_name() == "sqlite":
        if url.database in {None, "", ":memory:"}:
            engine_kwargs["poolclass"] = StaticPool
        else:
            engine_kwargs.update(
                {
                    "pool_size": 5,
                    "max_overflow": 0,
                    "pool_recycle": 3600,
                    "pool_pre_ping": True,
                }
            )
        return engine_kwargs

    engine_kwargs.update(
        {
            "pool_size": 50,
            "max_overflow": 100,
            "pool_recycle": 3600,
            "pool_pre_ping": True,
        }
    )
    return engine_kwargs


engine = create_async_engine(
    settings.database_url,
    **_build_engine_kwargs(settings.database_url),
)


def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
    if engine.sync_engine.dialect.name != "sqlite":
        return

    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
    finally:
        cursor.close()


event.listen(engine.sync_engine, "connect", _enable_sqlite_foreign_keys)


AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


Base = declarative_base()


async def get_db() -> AsyncSession:
    """
    获取数据库会话，用于 FastAPI 依赖注入。
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """
    初始化数据库并创建所有表。
    """
    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA journal_mode=WAL"))
        await conn.run_sync(Base.metadata.create_all)


async def close_db():
    """
    关闭数据库连接。
    """
    await engine.dispose()
