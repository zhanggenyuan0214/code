"""
认证服务
处理管理员登录、密码验证和 Session 管理
"""
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import bcrypt
from typing import Optional, Dict, Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Setting
from app.config import DEFAULT_ADMIN_PASSWORD, settings

logger = logging.getLogger(__name__)


@dataclass
class LoginAttemptState:
    """Track failed admin login attempts for a single client identifier."""

    failed_attempts: int = 0
    window_started_at: datetime | None = None
    locked_until: datetime | None = None


class AuthService:
    """认证服务类"""

    LOGIN_FAILURE_WINDOW = timedelta(minutes=10)
    LOGIN_LOCK_DURATION = timedelta(minutes=15)
    MAX_LOGIN_FAILURES = 5

    def __init__(self):
        """初始化认证服务"""
        self._login_attempts: Dict[str, LoginAttemptState] = {}
        self._login_attempts_lock = asyncio.Lock()

    def _utcnow(self) -> datetime:
        """Return the current UTC time for lockout calculations."""
        return datetime.now(timezone.utc)

    def _normalize_login_attempt_state(
        self,
        state: LoginAttemptState,
        now: datetime
    ) -> LoginAttemptState:
        """Reset expired windows and lockouts to keep state bounded."""
        if state.locked_until and state.locked_until <= now:
            return LoginAttemptState()

        if state.window_started_at and (now - state.window_started_at) >= self.LOGIN_FAILURE_WINDOW:
            return LoginAttemptState()

        return state

    def _is_empty_login_attempt_state(self, state: LoginAttemptState) -> bool:
        return (
            state.failed_attempts == 0
            and state.window_started_at is None
            and state.locked_until is None
        )

    def _build_lockout_response(self, retry_after: int) -> Dict[str, Any]:
        return {
            "success": False,
            "message": None,
            "error": f"登录尝试过于频繁，请在 {retry_after} 秒后重试",
            "status_code": 429,
            "retry_after": retry_after,
        }

    async def check_login_rate_limit(self, client_identifier: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Check whether the client is currently rate limited.

        Args:
            client_identifier: 客户端标识（通常为 IP）

        Returns:
            若已被锁定则返回错误响应，否则返回 None
        """
        if not client_identifier:
            return None

        now = self._utcnow()
        async with self._login_attempts_lock:
            state = self._login_attempts.get(client_identifier)
            if not state:
                return None

            state = self._normalize_login_attempt_state(state, now)
            if self._is_empty_login_attempt_state(state):
                self._login_attempts.pop(client_identifier, None)
                return None

            self._login_attempts[client_identifier] = state
            if not state.locked_until:
                return None

            retry_after = max(1, int((state.locked_until - now).total_seconds()))
            return self._build_lockout_response(retry_after)

    async def record_login_failure(self, client_identifier: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Record a failed login attempt and apply a temporary lock if needed.

        Args:
            client_identifier: 客户端标识（通常为 IP）

        Returns:
            若达到锁定阈值则返回错误响应，否则返回 None
        """
        if not client_identifier:
            return None

        now = self._utcnow()
        async with self._login_attempts_lock:
            state = self._login_attempts.get(client_identifier, LoginAttemptState())
            state = self._normalize_login_attempt_state(state, now)

            if state.locked_until:
                retry_after = max(1, int((state.locked_until - now).total_seconds()))
                return self._build_lockout_response(retry_after)

            if state.window_started_at is None:
                state.window_started_at = now
                state.failed_attempts = 0

            state.failed_attempts += 1
            if state.failed_attempts >= self.MAX_LOGIN_FAILURES:
                state.locked_until = now + self.LOGIN_LOCK_DURATION

            self._login_attempts[client_identifier] = state

            if not state.locked_until:
                return None

            retry_after = max(1, int((state.locked_until - now).total_seconds()))
            return self._build_lockout_response(retry_after)

    async def reset_login_failures(self, client_identifier: Optional[str]) -> None:
        """
        Clear failed login tracking after a successful login.

        Args:
            client_identifier: 客户端标识（通常为 IP）
        """
        if not client_identifier:
            return

        async with self._login_attempts_lock:
            self._login_attempts.pop(client_identifier, None)

    def hash_password(self, password: str) -> str:
        """
        哈希密码

        Args:
            password: 明文密码

        Returns:
            哈希后的密码
        """
        password_bytes = password.encode('utf-8')
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password_bytes, salt)
        return hashed.decode('utf-8')

    def verify_password(self, password: str, hashed_password: str) -> bool:
        """
        验证密码

        Args:
            password: 明文密码
            hashed_password: 哈希后的密码

        Returns:
            是否匹配
        """
        try:
            password_bytes = password.encode('utf-8')
            hashed_bytes = hashed_password.encode('utf-8')
            return bcrypt.checkpw(password_bytes, hashed_bytes)
        except Exception as e:
            logger.error(f"密码验证失败: {e}")
            return False

    async def get_admin_password_hash(self, db_session: AsyncSession) -> Optional[str]:
        """
        从数据库获取管理员密码哈希

        Args:
            db_session: 数据库会话

        Returns:
            密码哈希，如果不存在则返回 None
        """
        try:
            stmt = select(Setting).where(Setting.key == "admin_password_hash")
            result = await db_session.execute(stmt)
            setting = result.scalar_one_or_none()

            if setting:
                return setting.value
            return None

        except Exception as e:
            logger.error(f"获取管理员密码哈希失败: {e}")
            return None

    async def set_admin_password_hash(
        self,
        password_hash: str,
        db_session: AsyncSession
    ) -> bool:
        """
        设置管理员密码哈希到数据库

        Args:
            password_hash: 密码哈希
            db_session: 数据库会话

        Returns:
            是否成功
        """
        try:
            # 查询是否已存在
            stmt = select(Setting).where(Setting.key == "admin_password_hash")
            result = await db_session.execute(stmt)
            setting = result.scalar_one_or_none()

            if setting:
                # 更新
                setting.value = password_hash
            else:
                # 创建
                setting = Setting(
                    key="admin_password_hash",
                    value=password_hash,
                    description="管理员密码哈希"
                )
                db_session.add(setting)

            await db_session.commit()
            logger.info("管理员密码哈希已更新")
            return True

        except Exception as e:
            await db_session.rollback()
            logger.error(f"设置管理员密码哈希失败: {e}")
            return False

    async def initialize_admin_password(self, db_session: AsyncSession) -> bool:
        """
        初始化管理员密码
        如果数据库中没有密码哈希，则从配置文件读取并哈希后存储

        Args:
            db_session: 数据库会话

        Returns:
            是否成功
        """
        try:
            # 检查是否已存在
            existing_hash = await self.get_admin_password_hash(db_session)

            if existing_hash:
                logger.info("管理员密码已存在，跳过初始化")
                return True

            # 从配置读取密码
            admin_password = settings.admin_password

            if not admin_password or admin_password == DEFAULT_ADMIN_PASSWORD:
                logger.warning("使用默认密码，建议修改！")

            # 哈希密码
            password_hash = self.hash_password(admin_password)

            # 存储到数据库
            success = await self.set_admin_password_hash(password_hash, db_session)

            if success:
                logger.info("管理员密码初始化成功")
            else:
                logger.error("管理员密码初始化失败")

            return success

        except Exception as e:
            logger.error(f"初始化管理员密码失败: {e}")
            return False

    async def verify_admin_login(
        self,
        password: str,
        db_session: AsyncSession,
        client_identifier: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        验证管理员登录

        Args:
            password: 密码
            db_session: 数据库会话
            client_identifier: 客户端标识（用于登录限速）

        Returns:
            结果字典，包含 success, message, error
        """
        try:
            lockout_result = await self.check_login_rate_limit(client_identifier)
            if lockout_result:
                logger.warning(f"管理员登录已被临时锁定: {client_identifier}")
                return lockout_result

            # 获取密码哈希
            password_hash = await self.get_admin_password_hash(db_session)

            if not password_hash:
                # 尝试初始化
                await self.initialize_admin_password(db_session)
                password_hash = await self.get_admin_password_hash(db_session)

                if not password_hash:
                    return {
                        "success": False,
                        "message": None,
                        "error": "系统错误：无法获取管理员密码"
                    }

            # 验证密码
            if self.verify_password(password, password_hash):
                await self.reset_login_failures(client_identifier)
                logger.info("管理员登录成功")
                return {
                    "success": True,
                    "message": "登录成功",
                    "error": None
                }
            else:
                lockout_result = await self.record_login_failure(client_identifier)
                if lockout_result:
                    logger.warning(f"管理员登录失败次数过多，已临时锁定: {client_identifier}")
                    return lockout_result

                logger.warning("管理员登录失败：密码错误")
                return {
                    "success": False,
                    "message": None,
                    "error": "密码错误"
                }

        except Exception as e:
            logger.error(f"验证管理员登录失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"登录失败: {str(e)}"
            }

    async def change_admin_password(
        self,
        old_password: str,
        new_password: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        修改管理员密码

        Args:
            old_password: 旧密码
            new_password: 新密码
            db_session: 数据库会话

        Returns:
            结果字典，包含 success, message, error
        """
        try:
            # 验证旧密码
            verify_result = await self.verify_admin_login(old_password, db_session)

            if not verify_result["success"]:
                return {
                    "success": False,
                    "message": None,
                    "error": "旧密码错误"
                }

            # 哈希新密码
            new_password_hash = self.hash_password(new_password)

            # 更新密码
            success = await self.set_admin_password_hash(new_password_hash, db_session)

            if success:
                logger.info("管理员密码修改成功")
                return {
                    "success": True,
                    "message": "密码修改成功",
                    "error": None
                }
            else:
                return {
                    "success": False,
                    "message": None,
                    "error": "密码修改失败"
                }

        except Exception as e:
            logger.error(f"修改管理员密码失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"密码修改失败: {str(e)}"
            }


# 创建全局实例
auth_service = AuthService()
