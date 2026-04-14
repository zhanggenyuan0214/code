"""
Team 管理服务模块
处理 Team 的增删改查及同步操作
"""
import asyncio
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional, Dict, Any
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import TEAM_MEMBER_LIMIT, Team, TeamAccount, RedemptionCode, RedemptionRecord
from app.services.encryption import encryption_service
from app.utils.email_utils import normalize_email, normalize_email_set
from app.utils.token_parser import TokenParser
from app.utils.jwt_parser import JWTParser
from app.utils.security import has_masked_placeholder, mask_secret
from app.utils.time_utils import get_now, is_expired, parse_datetime

logger = logging.getLogger(__name__)

_DEFER_SYNC_COMMIT = ContextVar(
    "team_service_defer_sync_commit",
    default=False,
)

_FORCE_LOCAL_REPAIR_ERROR_CODES = {
    "team_access_unavailable",
    "token_invalidated",
    "session_invalidated",
    "token_expired",
    "account_deactivated",
    "account_suspended",
    "account_not_found",
    "user_not_found",
    "deactivated_workspace",
    "banned",
    "expired",
}


class TeamService:

    def __init__(self):
        """Initialize the Team management service."""
        from app.services.chatgpt import chatgpt_service
        self.chatgpt_service = chatgpt_service
        self.token_parser = TokenParser()
        self.jwt_parser = JWTParser()

    @contextmanager
    def defer_sync_commit(self):
        token = _DEFER_SYNC_COMMIT.set(True)
        try:
            yield
        finally:
            _DEFER_SYNC_COMMIT.reset(token)

    @staticmethod
    def _compute_team_status(current_members: int, max_members: int, expires_at) -> str:
        if current_members >= max_members:
            return "full"
        if is_expired(expires_at):
            return "expired"
        return "active"

    @staticmethod
    def _classify_sync_presence(sync_res: Dict[str, Any], email: str) -> str:
        normalized_email = normalize_email(email)
        if normalized_email is None:
            return "none"

        joined_emails = normalize_email_set(sync_res.get("joined_member_emails", []))
        invited_emails = normalize_email_set(sync_res.get("invited_emails", []))
        if normalized_email in joined_emails:
            return "joined"
        if normalized_email in invited_emails:
            return "invited"
        return "none"

    def _build_pending_invite_result(self, email: str, team: Team) -> Dict[str, Any]:
        team_label = team.team_name or team.email or f"Team {team.id}"
        return {
            "success": True,
            "pending": True,
            "message": (
                f"邀请已提交到 {email}，但 Team {team_label} 的成员列表尚未完成同步确认。"
                " 请稍后查收邮件，若后台仍未刷新可再次提交相同邮箱确认。"
            ),
            "error": None,
        }

    async def _mark_team_state_uncertain(
        self,
        team: Team,
        db_session: AsyncSession,
    ) -> None:
        if team.status not in {"banned", "expired"}:
            team.status = "error"
        team.error_count = max(team.error_count or 0, 1)
        await db_session.commit()

    async def _build_post_mutation_sync_result(
        self,
        *,
        team: Team,
        team_id: int,
        db_session: AsyncSession,
        success_message: str,
        partial_message: str,
    ) -> Dict[str, Any]:
        with self.defer_sync_commit():
            sync_res = await self.sync_team_info(team_id, db_session)

        if not sync_res.get("success"):
            await self._mark_team_state_uncertain(team, db_session)
            sync_error = sync_res.get("error") or "本地同步失败"
            return {
                "success": False,
                "partial": True,
                "upstream_success": True,
                "local_team_consistent": False,
                "message": partial_message,
                "error": (
                    f"{partial_message} 当前 Team 已标记为异常，请手动刷新确认。"
                    f"同步错误: {sync_error}"
                ),
            }

        await self._reset_error_status(team, db_session)
        await db_session.commit()

        return {
            "success": True,
            "partial": False,
            "upstream_success": True,
            "local_team_consistent": True,
            "message": success_message,
            "error": None,
        }

    @staticmethod
    def _allow_force_local_repair(result: Dict[str, Any]) -> bool:
        if result.get("force_local_repair_allowed"):
            return True

        error_code = str(result.get("error_code") or "").strip().lower()
        return error_code in _FORCE_LOCAL_REPAIR_ERROR_CODES

    def _mask_encrypted_token(self, encrypted_token: Optional[str]) -> str:
        """Return a masked preview for an encrypted token."""
        if not encrypted_token:
            return ""

        try:
            return mask_secret(encryption_service.decrypt_token(encrypted_token))
        except Exception as exc:
            logger.error(f"Failed to build token preview: {exc}")
            return ""

    def _format_upstream_error(
        self,
        result: Dict[str, Any],
        fallback: str = "请求失败，请稍后重试",
    ) -> str:
        error_code = str(result.get("error_code") or "").strip().lower()
        error_msg = str(result.get("error") or "").strip()
        error_msg_lower = error_msg.lower()

        if error_code in {"token_invalidated", "session_invalidated"}:
            return "Token 已失效"
        if error_code == "token_expired":
            return "Token 已过期"
        if error_code in {
            "account_deactivated",
            "account_suspended",
            "account_not_found",
            "user_not_found",
            "banned",
        }:
            return "账号已停用"
        if error_code in {"deactivated_workspace", "expired"}:
            return "Team 已过期"
        if error_code == "ghost_success":
            return "上游返回成功但未生效，请稍后重试"

        if any(keyword in error_msg_lower for keyword in ["token_invalidated", "token has been invalidated", "session_invalidated"]):
            return "Token 已失效"
        if any(
            keyword in error_msg_lower
            for keyword in [
                "account_deactivated",
                "account has been deactivated",
                "account is deactivated",
                "account_suspended",
                "account is suspended",
                "account was deleted",
                "user_not_found",
            ]
        ):
            return "账号已停用"
        if any(keyword in error_msg_lower for keyword in ["token_expired", "token is expired"]):
            return "Token 已过期"
        if any(
            keyword in error_msg_lower
            for keyword in ["maximum number of seats", "reached maximum number of seats", "no seats available"]
        ):
            return "该 Team 已满"

        return error_msg or fallback

    def _build_team_response(self, team: Team) -> Dict[str, Any]:
        """Serialize a Team model without exposing raw tokens."""
        return {
            "id": team.id,
            "email": team.email,
            "account_id": team.account_id,
            "access_token_masked": self._mask_encrypted_token(team.access_token_encrypted),
            "refresh_token_masked": self._mask_encrypted_token(team.refresh_token_encrypted),
            "session_token_masked": self._mask_encrypted_token(team.session_token_encrypted),
            "has_access_token": bool(team.access_token_encrypted),
            "has_refresh_token": bool(team.refresh_token_encrypted),
            "has_session_token": bool(team.session_token_encrypted),
            "client_id": team.client_id or "",
            "team_name": team.team_name,
            "plan_type": team.plan_type,
            "subscription_plan": team.subscription_plan,
            "expires_at": team.expires_at.isoformat() if team.expires_at else None,
            "current_members": team.current_members,
            "max_members": team.max_members,
            "status": team.status,
            "account_role": team.account_role,
            "device_code_auth_enabled": team.device_code_auth_enabled,
            "last_sync": team.last_sync.isoformat() if team.last_sync else None,
            "created_at": team.created_at.isoformat() if team.created_at else None,
        }

    async def get_team_public_by_id(
        self,
        team_id: int,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """Return team details with masked token previews only."""
        try:
            stmt = select(Team).where(Team.id == team_id).options(
                selectinload(Team.team_accounts)
            )
            result = await db_session.execute(stmt)
            team = result.scalar_one_or_none()

            if not team:
                return {
                    "success": False,
                    "team": None,
                    "team_accounts": [],
                    "error": f"Team ID {team_id} does not exist",
                }

            team_accounts_data = []
            for acc in team.team_accounts:
                team_accounts_data.append(
                    {
                        "id": acc.id,
                        "account_id": acc.account_id,
                        "account_name": acc.account_name,
                        "is_primary": acc.is_primary,
                    }
                )

            return {
                "success": True,
                "team": self._build_team_response(team),
                "team_accounts": team_accounts_data,
                "error": None,
            }
        except Exception as exc:
            logger.error(f"Failed to get masked team details: {exc}")
            return {
                "success": False,
                "team": None,
                "team_accounts": [],
                "error": f"Failed to get team details: {str(exc)}",
            }

    async def _handle_api_error(self, result: Dict[str, Any], team: Team, db_session: AsyncSession) -> bool:
        """
        处理 API 错误，检测 Token 或 Team 状态异常并自动更新

        Returns:
            bool: 是否已处理该错误（True 表示已处理，调用方应中止操作）
        """
        error_code = str(result.get("error_code") or "").strip().lower()
        error_msg = str(result.get("error", "")).lower()
        
        # 1. 检测账号封禁类错误
        # 已知的封禁错误码
        ban_codes = {
            "account_deactivated", 
            "token_invalidated", 
            "session_invalidated",
            "account_suspended", 
            "account_not_found",
            "user_not_found",
            "deactivated_workspace",
            "banned",
            "expired",
        }
        is_banned = error_code in ban_codes
        
        # 关键词匹配（有些 Token 返回 description 而非 code）
        if not is_banned:
            ban_keywords = [
                "token has been invalidated", 
                "account_deactivated",
                "account has been deactivated",
                "account is deactivated",
                "account_suspended",
                "account is suspended",
                "account was deleted",
                "user_not_found",
                "session_invalidated",
                "this account is deactivated",
                "deactivated_workspace",
                "workspace expired",
                "account banned",
            ]
            if any(kw in error_msg for kw in ban_keywords):
                is_banned = True
                
        # 1.1 检测幽灵成功（Ghost Success）
        if error_code == "ghost_success":
            logger.error(f"检测到 Team {team.id} ({team.email}) 幽灵成功（返回 200 但无实际效果），标记为 error")
            team.status = "error"
            return True

        if is_banned:
            if error_code in {"deactivated_workspace", "expired"} or "workspace" in error_msg:
                status_desc = "expired"
                team.status = "expired"
            elif error_code in {
                "account_deactivated",
                "token_invalidated",
                "session_invalidated",
                "account_suspended",
                "account_not_found",
                "user_not_found",
                "banned",
            } or any(x in error_msg for x in ["deactivated", "suspended", "not found", "deleted", "banned"]):
                status_desc = "banned"
                team.status = "banned"
            else:
                status_desc = "invalid"
                team.status = "banned"
                
            logger.warning(
                f"账号状态异常: {status_desc} (code={error_code}, msg={error_msg})，标记 Team {team.id} ({team.email}) 为 {team.status}"
            )
            return True

        full_keywords = ["maximum number of seats", "reached maximum number of seats", "no seats available"]
        if any(kw in error_msg for kw in full_keywords):
            logger.warning(f"检测到 Team 已满 (msg={error_msg})，标记 Team {team.id} ({team.email}) 为 full")
            team.status = "full"
            if team.max_members:
                team.current_members = max(team.current_members or 0, team.max_members)

            return True

        # 2.5 检测用户已在团队中（非错误）
        already_in_keywords = ["already in workspace", "already in team", "already a member"]
        if any(kw in error_msg for kw in already_in_keywords):
            logger.info(f"Team {team.id} 用户已在团队中: {error_msg}")
            # 不视为错误
            return False # 返回 False 让调用方继续处理
        # 3. 其他错误（含 Token 过期等）
        is_token_expired = error_code == "token_expired" or "token_expired" in error_msg or "token is expired" in error_msg
        
        logger.warning(f"Team {team.id} ({team.email}) request failed (code={error_code}, msg={error_msg})")
        
        team.error_count = (team.error_count or 0) + 1
        if team.error_count >= 3:
            if is_token_expired:
                logger.error(f"Team {team.id} 连续失败且 Token 已过期，标记为 expired")
                team.status = "expired"
            else:
                logger.error(f"Team {team.id} 连续失败 {team.error_count} 次，标记为 error")
                team.status = "error"

        if is_token_expired:
            logger.info(f"Team {team.id} Token 已过期，尝试刷新..")
            # 尝试刷新 Token
            await self.ensure_access_token(team, db_session)

        return True
        
    async def _reset_error_status(self, team: Team, db_session: AsyncSession) -> None:
        """
        重置错误状态，清零错误计数并重新计算状态
        """
        team.error_count = 0
        if team.status == "error":
            # 重新计算状态
            if team.current_members >= team.max_members:
                logger.info(f"Team {team.id} ({team.email}) 请求成功，从 error 恢复为 full")
                team.status = "full"
            elif is_expired(team.expires_at):
                logger.info(f"Team {team.id} ({team.email}) 请求成功，从 error 恢复为 expired")
                team.status = "expired"
            else:
                logger.info(f"Team {team.id} ({team.email}) 请求成功，从 error 恢复为 active")
                team.status = "active"

    async def ensure_access_token(self, team: Team, db_session: AsyncSession, force_refresh: bool = False) -> Optional[str]:
        """
        确保获取有效的 AT Token，必要时自动刷新

        Args:
            team: Team 实例
            db_session: 数据库会话
            force_refresh: 是否强制刷新（跳过过期检查）

        Returns:
            有效的 AT Token，失败返回 None
        """
        try:
            # 1. 解密 Token
            access_token = encryption_service.decrypt_token(team.access_token_encrypted)

            # 2. 检查是否过期（非强制刷新时）
            if not force_refresh and not self.jwt_parser.is_token_expired(access_token):
                return access_token

            if force_refresh:
                logger.info(f"Team {team.id} ({team.email}) 强制刷新 Token")
            else:
                logger.info(f"Team {team.id} ({team.email}) Token 已过期，尝试刷新")
        except Exception as e:
            logger.error(f"解密 Token 失败: {e}")
            access_token = None # 标记为无效
        # 3. 尝试用 session_token 刷新
        if team.session_token_encrypted:
            session_token = encryption_service.decrypt_token(team.session_token_encrypted)
            refresh_result = await self.chatgpt_service.refresh_access_token_with_session_token(
                session_token, db_session, account_id=team.account_id, identifier=team.email
            )
            if refresh_result["success"]:
                new_at = refresh_result["access_token"]
                new_st = refresh_result.get("session_token")
                logger.info(f"Team {team.id} 通过 session_token 刷新 AT 成功")
                team.access_token_encrypted = encryption_service.encrypt_token(new_at)

                # 更新 session_token，如果有变化
                if new_st and new_st != session_token:
                    logger.info(f"Team {team.id} session token updated")
                    team.session_token_encrypted = encryption_service.encrypt_token(new_st)

                await self._reset_error_status(team, db_session)
                return new_at
            else:
                # 处理刷新失败（如 token_invalidated）
                if await self._handle_api_error(refresh_result, team, db_session):
                    return None

        # 4. 尝试用 refresh_token 刷新
        if team.refresh_token_encrypted and team.client_id:
            refresh_token = encryption_service.decrypt_token(team.refresh_token_encrypted)
            refresh_result = await self.chatgpt_service.refresh_access_token_with_refresh_token(
                refresh_token, team.client_id, db_session, identifier=team.email
            )
            if refresh_result["success"]:
                new_at = refresh_result["access_token"]
                new_rt = refresh_result.get("refresh_token")
                logger.info(f"Team {team.id} 通过 refresh_token 刷新 AT 成功")
                team.access_token_encrypted = encryption_service.encrypt_token(new_at)
                if new_rt:
                    team.refresh_token_encrypted = encryption_service.encrypt_token(new_rt)
                # 重置错误状态
                await self._reset_error_status(team, db_session)
                return new_at
            else:
                # 处理刷新失败（如 account_deactivated）
                if await self._handle_api_error(refresh_result, team, db_session):
                    return None

        if team.status != "banned":
            logger.error(f"Team {team.id} Token 刷新失败，标记为 expired")
            team.status = "expired"
            team.error_count = (team.error_count or 0) + 1

        return None

    async def import_team_single(
        self,
        access_token: Optional[str],
        db_session: AsyncSession,
        email: Optional[str] = None,
        account_id: Optional[str] = None,
        refresh_token: Optional[str] = None,
        session_token: Optional[str] = None,
        client_id: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            is_at_valid = False
            if access_token:
                try:
                    is_at_valid = not self.jwt_parser.is_token_expired(access_token)
                except Exception:
                    is_at_valid = False

            if not is_at_valid:
                logger.info("提供的 AT 无效或已过期，尝试用 ST/RT 刷新")
                if session_token:
                    refresh_result = await self.chatgpt_service.refresh_access_token_with_session_token(
                        session_token,
                        db_session,
                        account_id=account_id,
                        identifier=email or "import",
                    )
                    if refresh_result["success"]:
                        access_token = refresh_result["access_token"]
                        if refresh_result.get("session_token"):
                            session_token = refresh_result["session_token"]
                        is_at_valid = True
                        logger.info("通过 session_token 刷新 AT 成功")

                if not is_at_valid and refresh_token and client_id:
                    refresh_result = await self.chatgpt_service.refresh_access_token_with_refresh_token(
                        refresh_token,
                        client_id,
                        db_session,
                        identifier=email or "import",
                    )
                    if refresh_result["success"]:
                        access_token = refresh_result["access_token"]
                        if refresh_result.get("refresh_token"):
                            refresh_token = refresh_result["refresh_token"]
                        is_at_valid = True
                        logger.info("通过 refresh_token 刷新 AT 成功")

            if not access_token or not is_at_valid:
                return {
                    "success": False,
                    "team_id": None,
                    "email": email,
                    "message": None,
                    "error": "无有效的 Access Token，且 Session/Refresh Token 刷新失败",
                }

            token_email = self.jwt_parser.extract_email(access_token)
            if not email:
                email = token_email
                if not email:
                    return {
                        "success": False,
                        "team_id": None,
                        "email": None,
                        "message": None,
                        "error": "Unable to extract email from token. Please provide the email explicitly.",
                    }
            elif token_email and token_email.lower() != email.lower():
                logger.error(f"Token email mismatch during import: expected {email}, got {token_email}")
                return {
                    "success": False,
                    "team_id": None,
                    "email": email,
                    "message": None,
                    "error": f"Token identity ({token_email}) does not match the provided email ({email}).",
                }

            account_result = await self.chatgpt_service.get_account_info(
                access_token,
                db_session,
                identifier=email,
            )
            if not account_result["success"]:
                return {
                    "success": False,
                    "team_id": None,
                    "email": email,
                    "message": None,
                    "error": f"Unable to verify upstream account metadata: {account_result['error']}",
                }

            team_accounts = account_result.get("accounts", [])
            if account_id:
                selected_account = next((acc for acc in team_accounts if acc["account_id"] == account_id), None)
                if not selected_account:
                    return {
                        "success": False,
                        "team_id": None,
                        "email": email,
                        "message": None,
                        "error": f"Specified account_id {account_id} was not confirmed by upstream metadata.",
                    }
                accounts_to_import = [selected_account]
            else:
                accounts_to_import = [acc for acc in team_accounts if acc.get("has_active_subscription")]
                if not accounts_to_import:
                    if not team_accounts:
                        return {
                            "success": False,
                            "team_id": None,
                            "email": email,
                            "message": None,
                            "error": "该 Token 下未找到 Team 账户",
                        }
                    accounts_to_import = [team_accounts[0]]

            encrypted_token = encryption_service.encrypt_token(access_token)
            encrypted_rt = encryption_service.encrypt_token(refresh_token) if refresh_token else None
            encrypted_st = encryption_service.encrypt_token(session_token) if session_token else None

            verified_accounts = []
            skipped_ids = []
            imported_ids = []

            for selected_account in accounts_to_import:
                stmt = select(Team).where(Team.account_id == selected_account["account_id"])
                result = await db_session.execute(stmt)
                existing_team = result.scalar_one_or_none()
                if existing_team:
                    skipped_ids.append(selected_account["account_id"])
                    continue

                members_result = await self.chatgpt_service.get_members(
                    access_token,
                    selected_account["account_id"],
                    db_session,
                    identifier=email,
                )
                invites_result = await self.chatgpt_service.get_invites(
                    access_token,
                    selected_account["account_id"],
                    db_session,
                    identifier=email,
                )

                if not members_result["success"] or not invites_result["success"]:
                    errors = []
                    if not members_result["success"]:
                        errors.append(f"members: {members_result.get('error', 'unknown error')}")
                    if not invites_result["success"]:
                        errors.append(f"invites: {invites_result.get('error', 'unknown error')}")
                    return {
                        "success": False,
                        "team_id": None,
                        "email": email,
                        "message": None,
                        "error": (
                            f"Unable to verify upstream membership state for account "
                            f"{selected_account['account_id']}: {'; '.join(errors)}"
                        ),
                    }

                expires_at = None
                if selected_account.get("expires_at"):
                    try:
                        expires_at = parse_datetime(selected_account["expires_at"])
                    except Exception as exc:
                        logger.warning(f"Failed to parse upstream expires_at for account {selected_account['account_id']}: {exc}")

                device_code_auth_enabled = False
                settings_result = await self.chatgpt_service.get_account_settings(
                    access_token,
                    selected_account["account_id"],
                    db_session,
                    identifier=email,
                )
                if settings_result["success"]:
                    beta_settings = settings_result.get("data", {}).get("beta_settings", {})
                    device_code_auth_enabled = beta_settings.get("codex_device_code_auth", False)

                verified_accounts.append(
                    {
                        "account_id": selected_account["account_id"],
                        "name": selected_account.get("name", ""),
                        "plan_type": selected_account.get("plan_type", ""),
                        "subscription_plan": selected_account.get("subscription_plan", ""),
                        "account_user_role": selected_account.get("account_user_role"),
                        "expires_at": expires_at,
                        "current_members": (members_result.get("total") or 0) + (invites_result.get("total") or 0),
                        "device_code_auth_enabled": device_code_auth_enabled,
                    }
                )

            if not verified_accounts and skipped_ids:
                return {
                    "success": False,
                    "team_id": None,
                    "email": email,
                    "message": None,
                    "error": f"Skipped {len(skipped_ids)} existing Team records; no new Team was imported.",
                }

            if not verified_accounts:
                return {
                    "success": False,
                    "team_id": None,
                    "email": email,
                    "message": None,
                    "error": "No Team account was imported.",
                }

            for verified_account in verified_accounts:
                team = Team(
                    email=email,
                    access_token_encrypted=encrypted_token,
                    refresh_token_encrypted=encrypted_rt,
                    session_token_encrypted=encrypted_st,
                    client_id=client_id,
                    encryption_key_id="default",
                    account_id=verified_account["account_id"],
                    team_name=verified_account["name"],
                    plan_type=verified_account["plan_type"],
                    subscription_plan=verified_account["subscription_plan"],
                    expires_at=verified_account["expires_at"],
                    current_members=verified_account["current_members"],
                    max_members=TEAM_MEMBER_LIMIT,
                    status=self._compute_team_status(
                        verified_account["current_members"],
                        TEAM_MEMBER_LIMIT,
                        verified_account["expires_at"],
                    ),
                    account_role=verified_account["account_user_role"],
                    device_code_auth_enabled=verified_account["device_code_auth_enabled"],
                    last_sync=get_now(),
                )
                db_session.add(team)
                await db_session.flush()

                for acc in team_accounts:
                    team_account = TeamAccount(
                        team_id=team.id,
                        account_id=acc["account_id"],
                        account_name=acc.get("name", ""),
                        is_primary=(acc["account_id"] == verified_account["account_id"]),
                    )
                    db_session.add(team_account)

                imported_ids.append(team.id)

            await db_session.commit()

            message = f"Imported {len(imported_ids)} Team account(s)."
            if skipped_ids:
                message += f" (跳过 {len(skipped_ids)} 个已存在的)"

            logger.info(f"Imported Team accounts for {email}: {len(imported_ids)} created")
            return {
                "success": True,
                "team_id": imported_ids[0],
                "email": email,
                "message": message,
                "error": None,
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"Team 导入失败: {e}")
            return {
                "success": False,
                "team_id": None,
                "email": email,
                "message": None,
                "error": f"导入失败: {str(e)}",
            }


    async def update_team(
        self,
        team_id: int,
        db_session: AsyncSession,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        session_token: Optional[str] = None,
        client_id: Optional[str] = None,
        email: Optional[str] = None,
        account_id: Optional[str] = None,
        max_members: Optional[int] = None,
        team_name: Optional[str] = None,
        status: Optional[str] = None,
        manual_status_override: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        更新 Team 信息

        Args:
            team_id: Team ID
            db_session: 数据库会话
            access_token: 新的 AT Token（可选）
            refresh_token: 新的 RT Token（可选）
            session_token: 新的 ST Token（可选）
            client_id: 新的 Client ID（可选）
            email: 邮箱（可选）
            account_id: 新的 Account ID（可选）
            max_members: 最大人数（可选）
            team_name: Team 名称（可选）
            status: 状态（可选）

        Returns:
            操作结果
        """
        try:
            # 1. 查询 Team（含关联 team_accounts）
            stmt = select(Team).where(Team.id == team_id).options(
                selectinload(Team.team_accounts)
            )
            result = await db_session.execute(stmt)
            team = result.scalar_one_or_none()

            if not team:
                return {"success": False, "error": f"Team ID {team_id} does not exist"}

            if email:
                team.email = email
            
            if team_name is not None:
                team.team_name = team_name

            if account_id:
                team.account_id = account_id
                for acc in team.team_accounts:
                    if acc.account_id == account_id:
                        acc.is_primary = True
                    else:
                        acc.is_primary = False

            # 3. 更新 Token
            if access_token and not has_masked_placeholder(access_token):
                team.access_token_encrypted = encryption_service.encrypt_token(access_token)
            if refresh_token and not has_masked_placeholder(refresh_token):
                team.refresh_token_encrypted = encryption_service.encrypt_token(refresh_token)
            if session_token and not has_masked_placeholder(session_token):
                team.session_token_encrypted = encryption_service.encrypt_token(session_token)
            if client_id:
                team.client_id = client_id

            # 4. 更新最大人数
            if max_members is not None:
                if not 1 <= max_members <= TEAM_MEMBER_LIMIT:
                    return {
                        "success": False,
                        "error": f"最大成员数必须在 1 到 {TEAM_MEMBER_LIMIT} 之间",
                    }
                team.max_members = max_members

            manual_override_requested = bool(manual_status_override)
            if manual_status_override is None and status is not None:
                manual_override_requested = True

            # 5. 手动指定状态
            if manual_override_requested:
                if status is None:
                    return {
                        "success": False,
                        "error": "手动覆盖状态时必须提供状态值",
                    }
                team.status = status

            # 自动重算 active/full/expired 状态（管理员未显式要求手动覆盖时）
            if not manual_override_requested and team.status in ["active", "full", "expired"]:
                team.status = self._compute_team_status(
                    team.current_members,
                    team.max_members,
                    team.expires_at,
                )

            await db_session.commit()


            logger.info(f"Team {team_id} 更新成功")
            return {"success": True, "message": "Team 更新成功"}

        except Exception as e:
            await db_session.rollback()
            logger.error(f"更新 Team 失败: {e}")
            return {"success": False, "error": f"更新失败: {str(e)}"}

    async def import_team_batch(
        self,
        text: str,
        db_session: AsyncSession
    ):
        """
        批量导入 Team（流式返回）

        Args:
            text: 包含 Token、Account ID 等信息的文本
            db_session: 数据库会话
        Yields:
            进度 Dict
        """
        try:
            # 1. 解析文本
            parsed_data = self.token_parser.parse_team_import_text(text)

            if not parsed_data:
                yield {
                    "type": "error",
                    "error": "未解析到有效 Token"
                }
                return

            # 1.1 去重（仅去掉完全重复的输入，避免提前合并同邮箱不同 account_id）
            seen_inputs = set()
            unique_data = []
            for item in parsed_data:
                token = item.get("token")
                email = normalize_email(item.get("email"))
                account_id = str(item.get("account_id") or "").strip().lower()
                refresh_token = item.get("refresh_token") or ""
                session_token = item.get("session_token") or ""
                client_id = str(item.get("client_id") or "").strip()
                
                if not email and token:
                    try:
                        extracted = self.jwt_parser.extract_email(token)
                        if extracted:
                            email = normalize_email(extracted)
                            item["email"] = email
                    except:
                        pass

                dedup_key = (
                    email or "",
                    account_id,
                    token or "",
                    refresh_token,
                    session_token,
                    client_id,
                )

                if any(dedup_key) and dedup_key not in seen_inputs:
                    seen_inputs.add(dedup_key)
                    unique_data.append(item)
            
            parsed_data = unique_data
            total = len(parsed_data)
            yield {
                "type": "start",
                "total": total
            }

            # 2. 逐条导入
            success_count = 0
            failed_count = 0

            for i, data in enumerate(parsed_data):
                result = await self.import_team_single(
                    access_token=data.get("token"),
                    db_session=db_session,
                    email=data.get("email"),
                    account_id=data.get("account_id"),
                    refresh_token=data.get("refresh_token"),
                    session_token=data.get("session_token"),
                    client_id=data.get("client_id")
                )

                if result["success"]:
                    success_count += 1
                else:
                    failed_count += 1

                yield {
                    "type": "progress",
                    "current": i + 1,
                    "total": total,
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "last_result": {
                        "email": result.get("email") or data.get("email") or "",
                        "account_id": data.get("account_id", "unknown"),
                        "success": result["success"],
                        "team_id": result["team_id"],
                        "message": result["message"],
                        "error": result["error"]
                    }
                }

            logger.info(f"批量导入完成: 共 {total}，成功 {success_count}，失败 {failed_count}")

            yield {
                "type": "finish",
                "total": total,
                "success_count": success_count,
                "failed_count": failed_count
            }

        except Exception as e:
            logger.error(f"批量导入异常: {e}")
            yield {
                "type": "error",
                "error": f"批量导入异常: {str(e)}"
            }

    async def sync_team_info(
        self,
        team_id: int,
        db_session: AsyncSession,
        force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        同步 Team 信息（从上游拉取最新数据）

        Args:
            team_id: Team ID
            db_session: 数据库会话
            force_refresh: 是否强制刷新 Token

        Returns:
            操作结果，包含 success, message, error
        """
        try:
            # 1. 查询 Team
            stmt = select(Team).where(Team.id == team_id)
            result = await db_session.execute(stmt)
            team = result.scalar_one_or_none()

            if not team:
                return {
                    "success": False,
                    "message": None,
                    "error": f"Team ID {team_id} does not exist",
                }

            # 2. 获取有效 AT Token
            access_token = await self.ensure_access_token(team, db_session, force_refresh=force_refresh)
            if not access_token:
                await db_session.commit()
                if team.status == "banned":
                    return {
                        "success": False,
                        "message": None,
                        "error": "Token 已失效"
                    }
                return {
                    "success": False,
                    "message": None,
                    "error": "Token 无效或已过期"
                }

            # 2.5 校验 Token 邮箱一致性（防止串号）
            token_email = self.jwt_parser.extract_email(access_token)
            if token_email and team.email and token_email.lower() != team.email.lower():
                logger.error(f"Team {team_id} Token 邮箱不匹配: 期望 {team.email}, 实际 {token_email}")
                return {
                    "success": False,
                    "message": None,
                    "error": f"Token identity ({token_email}) does not match Team email ({team.email}).",
                }

            # 3. 获取账户信息
            account_result = await self.chatgpt_service.get_account_info(
                access_token,
                db_session,
                identifier=team.email
            )

            if not account_result["success"]:
                # 检查是否是 Token 过期错误
                error_msg_raw = str(account_result.get("error", "")).lower()
                is_token_expired = account_result.get("error_code") == "token_expired" or "token_expired" in error_msg_raw or "token is expired" in error_msg_raw

                # 处理 API 错误（含状态更新）
                await self._handle_api_error(account_result, team, db_session)

                if is_token_expired:
                    logger.info(f"Team {team.id} 检测到 Token 过期，尝试刷新...")
                    new_token = await self.ensure_access_token(team, db_session, force_refresh=True)
                    if new_token:
                        # 2.6 校验刷新后 AT 邮箱一致性（防止串号）
                        new_token_email = self.jwt_parser.extract_email(new_token)
                        if new_token_email and team.email and new_token_email.lower() != team.email.lower():
                            logger.error(f"Team {team_id} 刷新后 Token 邮箱不匹配: 期望 {team.email}, 实际 {new_token_email}")
                            return {
                                "success": False,
                                "message": None,
                                "error": f"Refreshed token identity ({new_token_email}) does not match Team email ({team.email}).",
                            }

                        # 用新 Token 重试获取账户信息
                        account_result = await self.chatgpt_service.get_account_info(new_token, db_session, identifier=team.email)
                        if account_result["success"]:
                            access_token = new_token
                            logger.info(f"Team {team.id} token refresh sync succeeded")
                        else:
                            await self._handle_api_error(account_result, team, db_session)
                            await db_session.commit()
                            return {
                                "success": False,
                                "message": None,
                                "error": self._format_upstream_error(account_result, fallback="Token 刷新后仍无效"),
                            }
                    else:
                        await db_session.commit()
                        return {
                            "success": False,
                            "message": None,
                            "error": "Token 刷新失败"
                        }
                else:
                    await db_session.commit()
                    return {
                        "success": False,
                        "message": None,
                        "error": self._format_upstream_error(account_result, fallback="请求失败次数过多" if team.status == "error" else "同步失败"),
                    }

            # 4. 匹配当前 account
            team_accounts = account_result["accounts"]
            current_account = None

            for acc in team_accounts:
                if acc["account_id"] == team.account_id:
                    current_account = acc
                    break

            if not current_account:
                team.status = "error"
                await db_session.commit()
                return {
                    "success": False,
                    "message": None,
                    "error": "原始 account_id 在上游已不存在，请管理员手动确认"
                }

            # 5. 获取成员列表（含邀请）
            members_result = await self.chatgpt_service.get_members(
                access_token,
                current_account["account_id"],
                db_session,
                identifier=team.email
            )
            
            invites_result = await self.chatgpt_service.get_invites(
                access_token,
                current_account["account_id"],
                db_session,
                identifier=team.email
            )

            if not members_result["success"] or not invites_result["success"]:
                failed_result = members_result if not members_result["success"] else invites_result
                if await self._handle_api_error(failed_result, team, db_session):
                    await db_session.commit()
                    return {
                        "success": False,
                        "message": None,
                        "error": self._format_upstream_error(failed_result, fallback="获取成员信息失败"),
                    }

                team.error_count = (team.error_count or 0) + 1
                if team.error_count >= 3:
                    logger.error(f"Team {team.id} 连续失败 {team.error_count} 次，标记为 error")
                    team.status = "error"
                await db_session.commit()
                return {
                    "success": False,
                    "message": None,
                    "error": f"获取成员信息失败: {failed_result.get('error', '')} (失败次数: {team.error_count})",
                }

            joined_member_emails = normalize_email_set(
                member.get("email")
                for member in members_result.get("members", [])
            )
            invited_emails = normalize_email_set(
                invite.get("email_address")
                for invite in invites_result.get("items", [])
            )
            all_member_emails = joined_member_emails | invited_emails
            current_members = (members_result.get("total") or 0) + (invites_result.get("total") or 0)

            # 6. 解析过期时间
            expires_at = None
            if current_account["expires_at"]:
                try:
                    expires_at = parse_datetime(current_account["expires_at"])
                except Exception as e:
                    logger.warning(f"解析过期时间失败: {e}")

            # 7.5 获取账户设置（含 beta_settings）
            settings_result = await self.chatgpt_service.get_account_settings(
                access_token,
                current_account["account_id"],
                db_session,
                identifier=team.email
            )
            device_code_auth_enabled = team.device_code_auth_enabled
            if settings_result["success"]:
                beta_settings = settings_result["data"].get("beta_settings", {})
                device_code_auth_enabled = beta_settings.get("codex_device_code_auth", False)

            # 7. 计算状态
            status = self._compute_team_status(current_members, team.max_members, expires_at)

            # 8. 更新 Team 信息
            team.account_id = current_account["account_id"]
            team.team_name = current_account["name"]
            team.plan_type = current_account["plan_type"]
            team.subscription_plan = current_account["subscription_plan"]
            team.account_role = current_account.get("account_user_role")
            team.expires_at = expires_at
            team.current_members = current_members
            team.status = status
            team.device_code_auth_enabled = device_code_auth_enabled
            team.error_count = 0
            team.last_sync = get_now()

            if _DEFER_SYNC_COMMIT.get():
                await db_session.flush()
            else:
                await db_session.commit()

            logger.info(f"Team 同步成功: ID {team_id}, 当前成员 {current_members}")

            return {
                "success": True,
                "message": f"同步成功，当前成员 {current_members}",
                "member_emails": list(all_member_emails),
                "joined_member_emails": list(joined_member_emails),
                "invited_emails": list(invited_emails),
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"Team 同步失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"同步失败: {str(e)}"
            }

    async def get_team_members(
        self,
        team_id: int,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        获取 Team 成员列表

        Args:
            team_id: Team ID
            db_session: 数据库会话
        Returns:
            操作结果，包含 success, members, total, error
        """
        try:
            # 1. 查询 Team
            stmt = select(Team).where(Team.id == team_id)
            result = await db_session.execute(stmt)
            team = result.scalar_one_or_none()

            if not team:
                return {
                    "success": False,
                    "members": [],
                    "total": 0,
                    "error_code": "team_not_found",
                    "force_local_repair_allowed": False,
                    "error": f"Team ID {team_id} does not exist",
                }

            # 2. 获取有效 AT Token
            access_token = await self.ensure_access_token(team, db_session)
            if not access_token:
                await db_session.commit()
                return {
                    "success": False,
                    "members": [],
                    "total": 0,
                    "error_code": "team_access_unavailable",
                    "force_local_repair_allowed": True,
                    "error": "Token 无效或已过期"
                }

            # 3. 调用 ChatGPT API 获取成员
            members_result = await self.chatgpt_service.get_members(
                access_token,
                team.account_id,
                db_session
            )

            if not members_result["success"]:
                # 处理 Token 或账号异常
                if await self._handle_api_error(members_result, team, db_session):
                    await db_session.commit()
                    return {
                        "success": False,
                        "members": [],
                        "total": 0,
                        "error_code": members_result.get("error_code") or "team_member_fetch_failed",
                        "force_local_repair_allowed": self._allow_force_local_repair(members_result),
                        "error": self._format_upstream_error(members_result, fallback="获取成员失败"),
                    }

                return {
                    "success": False,
                    "members": [],
                    "total": 0,
                    "error_code": members_result.get("error_code") or "team_member_fetch_failed",
                    "force_local_repair_allowed": False,
                    "error": f"获取成员失败: {members_result['error']}"
                }

            invites_result = await self.chatgpt_service.get_invites(
                access_token,
                team.account_id,
                db_session
            )
            
            if not invites_result["success"]:
                # 处理 Token 或账号异常
                if await self._handle_api_error(invites_result, team, db_session):
                    await db_session.commit()
                    return {
                        "success": False,
                        "members": [],
                        "total": 0,
                        "error_code": invites_result.get("error_code") or "team_member_fetch_failed",
                        "force_local_repair_allowed": self._allow_force_local_repair(invites_result),
                        "error": self._format_upstream_error(invites_result, fallback="获取邀请列表失败"),
                    }

            # 5. 组装返回数据
            all_members = []

            for m in members_result["members"]:
                all_members.append({
                    "user_id": m.get("id"),
                    "email": m.get("email"),
                    "name": m.get("name"),
                    "role": m.get("role"),
                    "added_at": m.get("created_time"),
                    "status": "joined"
                })
            
            if invites_result["success"]:
                for inv in invites_result["items"]:
                    all_members.append({
                        "user_id": None, # 邀请中无 user_id
                        "email": inv.get("email_address"),
                        "name": None,
                        "role": inv.get("role"),
                        "added_at": inv.get("created_time"),
                        "status": "invited"
                    })

            logger.info(f"获取 Team {team_id} 成员列表: 共 {len(all_members)} 人(已加入 {members_result['total']})")

            team.current_members = len(all_members)
            team.status = self._compute_team_status(
                team.current_members,
                team.max_members,
                team.expires_at,
            )
            team.last_sync = get_now()
            team.error_count = 0
            await self._reset_error_status(team, db_session)
            await db_session.commit()

            return {
                "success": True,
                "members": all_members,
                "total": len(all_members),
                "error_code": None,
                "force_local_repair_allowed": False,
                "error": None
            }

        except Exception as e:
            logger.error(f"获取成员列表失败: {e}")
            return {
                "success": False,
                "members": [],
                "total": 0,
                "error_code": "team_member_fetch_failed",
                "force_local_repair_allowed": False,
                "error": f"获取成员列表失败: {str(e)}"
            }

    async def revoke_team_invite(
        self,
        team_id: int,
        email: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        撤销 Team 邀请

        Args:
            team_id: Team ID
            email: 要撤销的邮箱
            db_session: 数据库会话

        Returns:
            操作结果，包含 success, message, error
        """
        try:
            # 1. 查询 Team
            stmt = select(Team).where(Team.id == team_id)
            result = await db_session.execute(stmt)
            team = result.scalar_one_or_none()

            if not team:
                return {
                    "success": False,
                    "message": None,
                    "error": f"Team ID {team_id} does not exist",
                }

            # 2. 获取有效 AT Token
            access_token = await self.ensure_access_token(team, db_session)
            if not access_token:
                await db_session.commit()
                return {
                    "success": False,
                    "message": None,
                    "error": "Token 无效或已过期"
                }

            revoke_result = await self.chatgpt_service.delete_invite(
                access_token,
                team.account_id,
                email,
                db_session,
                identifier=team.email
            )

            if not revoke_result["success"]:
                # 处理 Token 或账号异常
                if await self._handle_api_error(revoke_result, team, db_session):
                    await db_session.commit()
                    return {
                        "success": False,
                        "message": None,
                        "error": self._format_upstream_error(revoke_result, fallback="撤销邀请失败"),
                    }

                return {
                    "success": False,
                    "message": None,
                    "error": f"撤销邀请失败: {self._format_upstream_error(revoke_result, fallback='撤销邀请失败')}"
                }

            logger.info(f"已撤销邀请 {email} from Team {team_id}")

            return await self._build_post_mutation_sync_result(
                team=team,
                team_id=team_id,
                db_session=db_session,
                success_message=f"Invite revoked for {email}.",
                partial_message=f"{email} 的邀请已在上游撤销，但本地 Team 状态未能同步刷新。",
            )

        except Exception as e:
            await db_session.rollback()
            logger.error(f"撤销邀请失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"撤销邀请失败: {str(e)}"
            }

    async def add_team_member(
        self,
        team_id: int,
        email: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        添加 Team 成员

        Args:
            team_id: Team ID
            email: 邮箱
            db_session: 数据库会话

        Returns:
            操作结果，包含 success, message, error
        """
        try:
            email = normalize_email(email)
            if not email:
                return {
                    "success": False,
                    "pending": False,
                    "message": None,
                    "error": "邮箱不能为空",
                }

            # 1. 查询 Team
            stmt = select(Team).where(Team.id == team_id)
            result = await db_session.execute(stmt)
            team = result.scalar_one_or_none()

            if not team:
                return {
                    "success": False,
                    "message": None,
                    "error": f"Team ID {team_id} does not exist",
                }

            if team.status == "full":
                return {
                    "success": False,
                    "message": None,
                    "error": "该 Team 已满"
                }

            if team.status == "expired":
                return {
                    "success": False,
                    "message": None,
                    "error": "该 Team 已过期"
                }

            # 3. 获取有效 AT Token
            access_token = await self.ensure_access_token(team, db_session)
            if not access_token:
                await db_session.commit()
                return {
                    "success": False,
                    "message": None,
                    "error": "Token 无效或已过期"
                }

            with self.defer_sync_commit():
                pre_sync_res = await self.sync_team_info(team_id, db_session)
            if pre_sync_res.get("success"):
                presence = self._classify_sync_presence(pre_sync_res, email)
                if presence == "joined":
                    await self._reset_error_status(team, db_session)
                    await db_session.commit()
                    return {
                        "success": True,
                        "pending": False,
                        "message": f"{email} 已在 Team 中",
                        "error": None,
                    }
                if presence == "invited":
                    await self._reset_error_status(team, db_session)
                    await db_session.commit()
                    return {
                        "success": True,
                        "pending": False,
                        "message": f"{email} 已有待确认邀请，请提醒用户查收邮箱。",
                        "error": None,
                    }

            invite_result = await self.chatgpt_service.send_invite(
                access_token,
                team.account_id,
                email,
                db_session,
                identifier=team.email
            )

            if not invite_result["success"]:
                # 处理 Token 或账号异常
                if await self._handle_api_error(invite_result, team, db_session):
                    await db_session.commit()
                    return {
                        "success": False,
                        "message": None,
                        "error": self._format_upstream_error(invite_result, fallback="添加成员失败"),
                    }

                return {
                    "success": False,
                    "message": None,
                    "error": f"添加成员失败: {self._format_upstream_error(invite_result, fallback='添加成员失败')}"
                }

            invite_data = invite_result.get("data", {})
            if "account_invites" in invite_data and not invite_data.get("account_invites"):
                await self._handle_api_error(
                    {"success": False, "error": "", "error_code": "ghost_success"},
                    team,
                    db_session,
                )
                await db_session.commit()
                return {
                    "success": False,
                    "message": None,
                    "error": "Team membership verification failed after invite submission.",
                }

            # 5. 验证邀请是否生效（最多重试 3 次，每次间隔 200 毫秒）
            is_verified = False
            for i in range(3):
                await asyncio.sleep(5)
                with self.defer_sync_commit():
                    sync_res = await self.sync_team_info(team_id, db_session)
                member_emails = normalize_email_set(sync_res.get("member_emails", []))
                if email in member_emails:
                    is_verified = True
                    logger.info(f"Team {team_id} [add_member] 验证成功(第 {i+1} 次)")
                    break
                if i < 2:
                    logger.warning(f"Team {team_id} [add_member] 未找到 {email}，第 {i+2} 次重试..")
            
            if not is_verified:
                logger.warning(
                    "Team %s invite for %s was accepted upstream but is still not visible after retries",
                    team_id,
                    email,
                )
                await db_session.commit()
                return self._build_pending_invite_result(email, team)

            await self._reset_error_status(team, db_session)
            await db_session.commit()

            logger.info(f"添加成员成功: {email} -> Team {team_id}")

            return {
                "success": True,
                "pending": False,
                "message": f"已邀请 {email}",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"添加成员失败: {e}")
            return {
                "success": False,
                "pending": False,
                "message": None,
                "error": f"添加成员失败: {str(e)}"
            }

    async def delete_team_member(
        self,
        team_id: int,
        user_id: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        删除 Team 成员

        Args:
            team_id: Team ID
            user_id: 用户 ID (格式: user-xxx)
            db_session: 数据库会话

        Returns:
            操作结果，包含 success, message, error
        """
        try:
            # 1. 查询 Team
            stmt = select(Team).where(Team.id == team_id)
            result = await db_session.execute(stmt)
            team = result.scalar_one_or_none()

            if not team:
                return {
                    "success": False,
                    "message": None,
                    "error": f"Team ID {team_id} does not exist",
                }

            # 2. 获取有效 AT Token
            access_token = await self.ensure_access_token(team, db_session)
            if not access_token:
                await db_session.commit()
                return {
                    "success": False,
                    "message": None,
                    "error": "Token 无效或已过期"
                }

            # 3. 调用 ChatGPT API 删除成员
            delete_result = await self.chatgpt_service.delete_member(
                access_token,
                team.account_id,
                user_id,
                db_session,
                identifier=team.email
            )

            if not delete_result["success"]:
                # 处理 Token 或账号异常
                if await self._handle_api_error(delete_result, team, db_session):
                    await db_session.commit()
                    return {
                        "success": False,
                        "message": None,
                        "error": self._format_upstream_error(delete_result, fallback="删除成员失败"),
                    }

                return {
                    "success": False,
                    "message": None,
                    "error": f"删除成员失败: {self._format_upstream_error(delete_result, fallback='删除成员失败')}"
                }

            logger.info(f"已删除成员: {user_id} from Team {team_id}")

            return await self._build_post_mutation_sync_result(
                team=team,
                team_id=team_id,
                db_session=db_session,
                success_message="Member deleted successfully.",
                partial_message="成员已从上游删除，但本地 Team 状态未能同步刷新。",
            )

        except Exception as e:
            await db_session.rollback()
            logger.error(f"删除成员失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"删除成员失败: {str(e)}"
            }

    async def enable_device_code_auth(
        self,
        team_id: int,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        开启 Team 设备代码认证
        """
        try:
            # 1. 查询 Team
            stmt = select(Team).where(Team.id == team_id)
            result = await db_session.execute(stmt)
            team = result.scalar_one_or_none()

            if not team:
                return {"success": False, "error": f"Team ID {team_id} does not exist"}

            # 2. 获取有效 AT Token
            access_token = await self.ensure_access_token(team, db_session)
            if not access_token:
                await db_session.commit()
                return {"success": False, "error": "Token 无效或已过期"}

            result = await self.chatgpt_service.toggle_beta_feature(
                access_token,
                team.account_id,
                "codex_device_code_auth",
                True,
                db_session,
                identifier=team.email
            )

            if not result["success"]:
                if await self._handle_api_error(result, team, db_session):
                    await db_session.commit()
                return {
                    "success": False,
                    "error": self._format_upstream_error(result, fallback="开启设备代码验证失败"),
                }

            team.device_code_auth_enabled = True
            await db_session.commit()

            logger.info(f"Device code auth enabled for Team {team_id} ({team.email})")
            return {"success": True, "message": "Device code auth enabled."}

        except Exception as e:
            logger.error(f"开启设备代码认证失败: {e}")
            return {"success": False, "error": f"开启设备代码认证失败: {str(e)}"}

    async def get_all_teams(
        self,
        db_session: AsyncSession,
        page: int = 1,
        per_page: int = 20,
        search: Optional[str] = None,
        status: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取所有 Team 列表（分页）

        Args:
            db_session: 数据库会话
            page: 页码
            per_page: 每页数量
            search: 搜索关键词
            status: 状态筛选（可选）

        Returns:
            操作结果，包含 success, teams, total, total_pages, current_page, error
        """
        try:
            # 1. 基础查询
            stmt = select(Team)

            # 2. 搜索过滤
            if search:
                from sqlalchemy import or_, cast, String
                search_filter = f"%{search}%"
                stmt = stmt.where(
                    or_(
                        Team.email.ilike(search_filter),
                        Team.account_id.ilike(search_filter),
                        Team.team_name.ilike(search_filter),
                        cast(Team.id, String).ilike(search_filter)
                    )
                )

            # 3. 状态筛选
            if status:
                stmt = stmt.where(Team.status == status)

            # 4. 统计总数
            count_stmt = select(func.count()).select_from(stmt.subquery())
            count_result = await db_session.execute(count_stmt)
            total = count_result.scalar() or 0

            # 4. 计算分页
            import math
            total_pages = math.ceil(total / per_page) if total > 0 else 1
            if page < 1:
                page = 1
            if total_pages > 0 and page > total_pages:
                page = total_pages

            offset = (page - 1) * per_page

            # 5. 查询数据
            final_stmt = stmt.order_by(Team.created_at.desc()).limit(per_page).offset(offset)
            result = await db_session.execute(final_stmt)
            teams = result.scalars().all()

            # 组装返回数据
            team_list = []
            for team in teams:
                team_list.append({
                    "id": team.id,
                    "email": team.email,
                    "account_id": team.account_id,
                    "team_name": team.team_name,
                    "plan_type": team.plan_type,
                    "subscription_plan": team.subscription_plan,
                    "expires_at": team.expires_at.isoformat() if team.expires_at else None,
                    "current_members": team.current_members,
                    "max_members": team.max_members,
                    "status": team.status,
                    "device_code_auth_enabled": getattr(team, 'device_code_auth_enabled', False),
                    "last_sync": team.last_sync.isoformat() if team.last_sync else None,
                    "created_at": team.created_at.isoformat() if team.created_at else None
                })

            logger.info(f"获取 Team 列表: 第 {page} 页，共 {len(team_list)} 条 / 总计 {total}")

            return {
                "success": True,
                "teams": team_list,
                "total": total,
                "total_pages": total_pages,
                "current_page": page,
                "error": None
            }

        except Exception as e:
            logger.error(f"获取 Team 列表失败: {e}")
            return {
                "success": False,
                "teams": [],
                "error": f"获取 Team 列表失败: {str(e)}"
            }

    async def remove_invite_or_member(
        self,
        team_id: int,
        email: str,
        db_session: AsyncSession,
        force_local: bool = False,
    ) -> Dict[str, Any]:
        """
        移除成员或撤销邀请（自动判断）

        Args:
            team_id: Team ID
            email: 邮箱
            db_session: 数据库会话

        Returns:
            操作结果
        """
        try:
            members_result = await self.get_team_members(team_id, db_session)
            if not members_result["success"]:
                if force_local and self._allow_force_local_repair(members_result):
                    return {
                        "success": True,
                        "partial": True,
                        "local_only": True,
                        "upstream_success": False,
                        "local_team_consistent": False,
                        "message": "当前 Team 无法访问，已按管理员强制修复模式跳过上游移除。",
                        "error": None,
                    }

                members_result["can_force_local_repair"] = self._allow_force_local_repair(members_result)
                return members_result

            all_members = members_result["members"]

            normalized_email = email.strip().lower()
            target = next(
                (
                    member
                    for member in all_members
                    if isinstance(member.get("email"), str)
                    and member["email"].strip().lower() == normalized_email
                ),
                None,
            )

            if not target:
                logger.warning(f"No member or invite matched email {email} in Team {team_id}")
                return {
                    "success": True,
                    "partial": False,
                    "local_only": False,
                    "upstream_success": False,
                    "local_team_consistent": True,
                    "upstream_action": "already_absent",
                    "message": "成员或邀请在上游已不存在，已按当前上游状态继续处理本地记录。",
                    "error": None,
                }

            if target["status"] == "joined":
                return await self.delete_team_member(team_id, target["user_id"], db_session)
            return await self.revoke_team_invite(team_id, target["email"], db_session)

        except Exception as e:
            logger.error(f"移除成员或撤销邀请失败: {e}")
            return {"success": False, "error": str(e)}

    async def delete_team(
        self,
        team_id: int,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        删除 Team

        Args:
            team_id: Team ID
            db_session: 数据库会话

        Returns:
            操作结果，包含 success, message, error
        """
        try:
            # 1. 查询 Team
            stmt = select(Team).where(Team.id == team_id)
            result = await db_session.execute(stmt)
            team = result.scalar_one_or_none()

            if not team:
                return {
                    "success": False,
                    "message": None,
                    "error": f"Team ID {team_id} does not exist",
                }

            history_stmt = (
                select(RedemptionRecord.id)
                .where(RedemptionRecord.team_id == team_id)
                .limit(1)
            )
            history_result = await db_session.execute(history_stmt)
            if history_result.scalar_one_or_none() is not None:
                return {
                    "success": False,
                    "message": None,
                    "error": "该 Team 存在历史兑换记录，无法删除，否则会影响质保查询和复兑判断。",
                }

            pending_stmt = (
                select(RedemptionCode.code, RedemptionCode.used_by_email)
                .where(
                    RedemptionCode.used_team_id == team_id,
                    RedemptionCode.status == "invite_pending",
                )
                .limit(1)
            )
            pending_result = await db_session.execute(pending_stmt)
            pending_code = pending_result.first()
            if pending_code is not None:
                pending_code_value, pending_email = pending_code
                return {
                    "success": False,
                    "message": None,
                    "error": (
                        "该 Team 存在待确认兑换，删除会导致兑换码状态卡死。"
                        f" 请先处理兑换码 {pending_code_value}"
                        f"{f'（{pending_email}）' if pending_email else ''} 后再删除。"
                    ),
                }

            # 1.5 清除 RedemptionCode 关联（解除外键）
            update_stmt = update(RedemptionCode).where(RedemptionCode.used_team_id == team_id).values(used_team_id=None)
            await db_session.execute(update_stmt)

            # 2. 删除 Team（级联删除 team_accounts 和 redemption_records）
            await db_session.delete(team)
            await db_session.commit()

            logger.info(f"删除 Team {team_id} 成功")

            return {
                "success": True,
                "message": "Team deleted successfully.",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"删除 Team 失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"删除 Team 失败: {str(e)}"
            }

    async def get_total_available_seats(
        self,
        db_session: AsyncSession
    ) -> int:
        """
        获取所有 Team 的总剩余车位数
        """
        try:
            stmt = select(func.sum(Team.max_members - Team.current_members)).where(
                Team.status == "active",
                Team.current_members < Team.max_members
            )
            result = await db_session.execute(stmt)
            return result.scalar() or 0
        except Exception as e:
            logger.error(f"获取总剩余车位失败: {e}")
            return 0

    async def get_stats(
        self,
        db_session: AsyncSession
    ) -> Dict[str, int]:
        """获取 Team 统计信息"""
        try:
            # 总数
            total_stmt = select(func.count(Team.id))
            total_result = await db_session.execute(total_stmt)
            total = total_result.scalar() or 0

            # 可用 Team 数（状态为 active 且有空位）
            available_stmt = select(func.count(Team.id)).where(
                Team.status == "active",
                Team.current_members < Team.max_members
            )
            available_result = await db_session.execute(available_stmt)
            available = available_result.scalar() or 0

            return {
                "total": total,
                "available": available
            }
        except Exception as e:
            logger.error(f"获取 Team 统计失败: {e}")
            return {"total": 0, "available": 0}


# 全局 Team 服务实例
team_service = TeamService()
