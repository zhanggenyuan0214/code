"""
兑换流程服务。
"""
import asyncio
import logging
from collections import defaultdict
from datetime import timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.redemption import RedemptionService
from app.services.team import TeamService
from app.services.warranty import warranty_service
from app.utils.email_utils import email_equals, emails_match, normalize_email, normalize_email_set
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)

ALREADY_IN_TEAM_ERROR_KEYWORDS = (
    "already in workspace",
    "already in team",
    "already a member",
)
FULL_TEAM_ERROR_KEYWORDS = (
    "maximum number of seats",
    "full",
    "no seats",
)
CODE_CONFLICT_ERROR = "兑换码状态已变化，请重试"
PENDING_REDEMPTION_STATUS = "invite_pending"
WARRANTY_ACTIVE_STATUS = "warranty_active"

# 针对 code 的全局锁，防止同一兑换码并发请求。
_code_locks = defaultdict(asyncio.Lock)
# 针对 team 的全局锁，防止同一 Team 并发拉人导致状态冲突。
_team_locks = defaultdict(asyncio.Lock)


class RedeemFlowService:
    """兑换流程场景服务类。"""

    def __init__(self):
        from app.services.chatgpt import chatgpt_service

        self.redemption_service = RedemptionService()
        self.warranty_service = warranty_service
        self.team_service = TeamService()
        self.chatgpt_service = chatgpt_service

    async def select_team_auto(
        self,
        db_session: AsyncSession,
        exclude_team_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """自动选择一个可用 Team。"""
        try:
            stmt = select(Team).where(
                Team.status == "active",
                Team.current_members < Team.max_members,
            )

            if exclude_team_ids:
                stmt = stmt.where(Team.id.not_in(exclude_team_ids))

            stmt = stmt.order_by(Team.current_members.asc(), Team.created_at.desc())

            result = await db_session.execute(stmt)
            team = result.scalars().first()
            if not team:
                reason = "没有可用的 Team"
                if exclude_team_ids:
                    reason = "您已加入所有可用 Team"
                return {"success": False, "team_id": None, "error": reason}

            logger.info("自动选择 Team: %s", team.id)
            return {"success": True, "team_id": team.id, "error": None}
        except Exception as exc:
            logger.error("自动选择 Team 失败: %s", exc)
            return {
                "success": False,
                "team_id": None,
                "error": f"自动选择 Team 失败: {exc}",
            }

    def _build_success_result(self, target_team: Team) -> Dict[str, Any]:
        return {
            "success": True,
            "pending": False,
            "message": "兑换成功！邀请链接已发送至您的邮箱，请及时查收。",
            "team_info": {
                "id": target_team.id,
                "team_name": target_team.team_name,
                "email": target_team.email,
                "expires_at": target_team.expires_at.isoformat() if target_team.expires_at else None,
            },
        }

    def _build_existing_membership_error(self, email: str, target_team: Team) -> str:
        team_label = target_team.team_name or target_team.email or f"Team {target_team.id}"
        return f"邮箱 {email} 已在 Team {team_label} 中，本次兑换未新增成员，兑换码未消耗"

    def _build_existing_invite_error(self, email: str, target_team: Team) -> str:
        team_label = target_team.team_name or target_team.email or f"Team {target_team.id}"
        return (
            f"邮箱 {email} 在 Team {team_label} 中已有待确认邀请，"
            "请先查收邮件，本次兑换未新增成员，兑换码未消耗"
        )

    def _build_pending_result(self, target_team: Team) -> Dict[str, Any]:
        return {
            "success": True,
            "pending": True,
            "message": (
                "邀请已提交成功，但上游成员列表尚未完成同步确认。"
                " 系统已保留本次兑换，请稍后查收邮件或使用相同邮箱与兑换码再次确认。"
            ),
            "team_info": {
                "id": target_team.id,
                "team_name": target_team.team_name,
                "email": target_team.email,
                "expires_at": target_team.expires_at.isoformat() if target_team.expires_at else None,
            },
        }

    def _build_pending_conflict_error(self) -> str:
        return "该兑换码已有待确认的邀请正在处理，请使用原兑换邮箱稍后重试，勿重复分配到其他 Team"

    @staticmethod
    def _snapshot_redemption_code(redemption_code: RedemptionCode) -> Dict[str, Any]:
        return {
            "status": redemption_code.status,
            "used_by_email": redemption_code.used_by_email,
            "used_team_id": redemption_code.used_team_id,
            "used_at": redemption_code.used_at,
            "warranty_expires_at": redemption_code.warranty_expires_at,
        }

    @staticmethod
    def _apply_redemption_code_snapshot(
        redemption_code: RedemptionCode,
        snapshot: Dict[str, Any],
    ) -> None:
        redemption_code.status = snapshot["status"]
        redemption_code.used_by_email = snapshot["used_by_email"]
        redemption_code.used_team_id = snapshot["used_team_id"]
        redemption_code.used_at = snapshot["used_at"]
        redemption_code.warranty_expires_at = snapshot["warranty_expires_at"]

    @staticmethod
    def _reservation_matches(
        redemption_code: Optional[RedemptionCode],
        email: str,
        team_id: Optional[int],
    ) -> bool:
        return (
            redemption_code is not None
            and emails_match(redemption_code.used_by_email, email)
            and redemption_code.used_team_id == team_id
        )

    def _is_pending_reservation(
        self,
        redemption_code: Optional[RedemptionCode],
        email: str,
        team_id: Optional[int],
    ) -> bool:
        return (
            redemption_code is not None
            and redemption_code.status == PENDING_REDEMPTION_STATUS
            and self._reservation_matches(redemption_code, email, team_id)
        )

    @staticmethod
    def _classify_sync_presence(sync_res: Dict[str, Any], email: str) -> str:
        normalized_email = normalize_email(email)
        if normalized_email is None:
            return "none"

        joined_member_emails = normalize_email_set(
            sync_res.get("joined_member_emails", sync_res.get("member_emails", []))
        )
        invited_emails = normalize_email_set(sync_res.get("invited_emails", []))
        if normalized_email in joined_member_emails:
            return "joined"
        if normalized_email in invited_emails:
            return "invited"
        if normalized_email in normalize_email_set(sync_res.get("member_emails", [])):
            return "joined"
        return "none"

    async def _load_existing_redemption_record(
        self,
        db_session: AsyncSession,
        code: str,
        email: str,
        team_id: int,
    ) -> Optional[RedemptionRecord]:
        stmt = (
            select(RedemptionRecord)
            .where(
                RedemptionRecord.code == code,
                email_equals(RedemptionRecord.email, email),
                RedemptionRecord.team_id == team_id,
            )
            .order_by(RedemptionRecord.redeemed_at.desc(), RedemptionRecord.id.desc())
        )
        result = await db_session.execute(stmt)
        return result.scalars().first()

    def _is_idempotent_success(
        self,
        redemption_code: Optional[RedemptionCode],
        existing_record: Optional[RedemptionRecord],
        email: str,
        team_id: int,
    ) -> bool:
        return (
            existing_record is not None
            and redemption_code is not None
            and redemption_code.status in {"used", WARRANTY_ACTIVE_STATUS}
            and emails_match(redemption_code.used_by_email, email)
            and redemption_code.used_team_id == team_id
        )

    async def _load_redemption_code(
        self,
        db_session: AsyncSession,
        code: str,
    ) -> Optional[RedemptionCode]:
        result = await db_session.execute(select(RedemptionCode).where(RedemptionCode.code == code))
        return result.scalar_one_or_none()

    async def _has_existing_success_record(
        self,
        db_session: AsyncSession,
        code: str,
        email: str,
        team_id: int,
    ) -> bool:
        existing_record = await self._load_existing_redemption_record(db_session, code, email, team_id)
        if not existing_record:
            return False

        result = await db_session.execute(select(RedemptionCode).where(RedemptionCode.code == code))
        redemption_code = result.scalar_one_or_none()
        return self._is_idempotent_success(redemption_code, existing_record, email, team_id)

    async def _claim_redemption_code(
        self,
        db_session: AsyncSession,
        code: str,
        email: str,
        team_id: int,
        redeemed_at,
        has_warranty: bool,
        warranty_days: Optional[int],
        expected_statuses: Optional[set[str]] = None,
        expected_email: Optional[str] = None,
        expected_team_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        warranty_expires_at = None
        if has_warranty:
            warranty_expires_at = redeemed_at + timedelta(days=warranty_days or 30)

        final_status = WARRANTY_ACTIVE_STATUS if has_warranty else "used"
        allowed_statuses = expected_statuses or {"unused", WARRANTY_ACTIVE_STATUS}

        update_stmt = (
            update(RedemptionCode)
            .where(
                RedemptionCode.code == code,
                RedemptionCode.status.in_(list(allowed_statuses)),
            )
            .values(
                status=final_status,
                used_by_email=email,
                used_team_id=team_id,
                used_at=func.coalesce(RedemptionCode.used_at, redeemed_at) if has_warranty else redeemed_at,
                warranty_expires_at=(
                    func.coalesce(RedemptionCode.warranty_expires_at, warranty_expires_at)
                    if has_warranty
                    else None
                ),
            )
        )
        if expected_email is not None:
            update_stmt = update_stmt.where(email_equals(RedemptionCode.used_by_email, expected_email))
        if expected_team_id is not None:
            update_stmt = update_stmt.where(RedemptionCode.used_team_id == expected_team_id)

        claim_result = await db_session.execute(update_stmt)
        if claim_result.rowcount == 1:
            return {"success": True, "claimed": True, "error": None}

        refreshed_code_result = await db_session.execute(
            select(RedemptionCode).where(RedemptionCode.code == code)
        )
        refreshed_code = refreshed_code_result.scalar_one_or_none()
        existing_record = await self._load_existing_redemption_record(db_session, code, email, team_id)
        if self._is_idempotent_success(refreshed_code, existing_record, email, team_id):
            return {"success": True, "claimed": False, "error": None}

        return {"success": False, "claimed": False, "error": CODE_CONFLICT_ERROR}

    async def _restore_pending_reservation(
        self,
        db_session: AsyncSession,
        code: str,
        email: str,
        team_id: int,
        snapshot: Optional[Dict[str, Any]],
    ) -> None:
        if snapshot is None:
            return

        started_transaction = False
        try:
            if not db_session.in_transaction():
                await db_session.begin()
                started_transaction = True

            code_result = await db_session.execute(
                select(RedemptionCode)
                .where(RedemptionCode.code == code)
                .with_for_update()
            )
            redemption_code = code_result.scalar_one_or_none()
            if self._is_pending_reservation(redemption_code, email, team_id):
                self._apply_redemption_code_snapshot(redemption_code, snapshot)

            if started_transaction or db_session.in_transaction():
                await db_session.commit()
        except Exception:
            if db_session.in_transaction():
                await db_session.rollback()
            raise

    def _reserve_redemption_code(
        self,
        redemption_code: RedemptionCode,
        email: str,
        team_id: int,
    ) -> None:
        redemption_code.status = PENDING_REDEMPTION_STATUS
        redemption_code.used_by_email = email
        redemption_code.used_team_id = team_id

    async def _sync_team_members(
        self,
        team_id: int,
        db_session: AsyncSession,
    ) -> Dict[str, Any]:
        with self.team_service.defer_sync_commit():
            sync_res = await self.team_service.sync_team_info(team_id, db_session)
        if not sync_res.get("success"):
            return {
                "success": False,
                "member_emails": set(),
                "joined_member_emails": set(),
                "invited_emails": set(),
                "error": sync_res.get("error") or f"Team {team_id} sync failed",
            }

        member_emails = normalize_email_set(sync_res.get("member_emails", []))
        joined_member_emails = normalize_email_set(
            sync_res.get("joined_member_emails", sync_res.get("member_emails", []))
        )
        invited_emails = normalize_email_set(sync_res.get("invited_emails", []))
        return {
            "success": True,
            "member_emails": member_emails,
            "joined_member_emails": joined_member_emails,
            "invited_emails": invited_emails,
            "error": None,
        }

    async def _confirm_membership(
        self,
        team_id: int,
        email: str,
        db_session: AsyncSession,
        max_attempts: int = 3,
        delay_seconds: int = 5,
    ) -> Dict[str, Any]:
        normalized_email = normalize_email(email)
        last_error = f"Invite verification failed for {email}"

        for attempt in range(max_attempts):
            sync_res = await self._sync_team_members(team_id, db_session)
            if sync_res["success"]:
                if normalized_email in sync_res["member_emails"]:
                    logger.info(
                        "Team %s membership confirmed for %s on attempt %s/%s",
                        team_id,
                        email,
                        attempt + 1,
                        max_attempts,
                    )
                    return {"success": True, "error": None}
                last_error = (
                    f"Invite verification failed after sync attempt {attempt + 1}: "
                    f"{email} not present"
                )
            else:
                last_error = sync_res["error"]
                logger.warning(
                    "Team %s membership confirmation sync failed for %s on attempt %s/%s: %s",
                    team_id,
                    email,
                    attempt + 1,
                    max_attempts,
                    last_error,
                )

            if attempt < max_attempts - 1:
                await asyncio.sleep(delay_seconds)

        return {"success": False, "error": last_error}

    async def _finalize_redemption_record(
        self,
        db_session: AsyncSession,
        code: str,
        email: str,
        team_id: int,
    ) -> Dict[str, Any]:
        if not db_session.in_transaction():
            await db_session.begin()

        try:
            code_result = await db_session.execute(
                select(RedemptionCode)
                .where(RedemptionCode.code == code)
                .with_for_update()
            )
            redemption_code = code_result.scalar_one_or_none()
            team_result = await db_session.execute(
                select(Team).where(Team.id == team_id).with_for_update()
            )
            target_team = team_result.scalar_one_or_none()
            existing_record = await self._load_existing_redemption_record(
                db_session,
                code,
                email,
                team_id,
            )

            if self._is_idempotent_success(
                redemption_code,
                existing_record,
                email,
                team_id,
            ):
                await db_session.commit()
                return self._build_success_result(target_team)

            if not redemption_code:
                await db_session.rollback()
                return {"success": False, "error": "兑换码不存在"}
            if not target_team:
                raise Exception(f"目标 Team {team_id} 不存在")

            redeemed_at = get_now()
            claim_result = await self._claim_redemption_code(
                db_session,
                code,
                email,
                team_id,
                redeemed_at,
                redemption_code.has_warranty,
                redemption_code.warranty_days,
                expected_statuses={PENDING_REDEMPTION_STATUS},
                expected_email=email,
                expected_team_id=team_id,
            )
            if not claim_result["success"]:
                await db_session.rollback()
                return {"success": False, "error": claim_result["error"]}

            if claim_result["claimed"]:
                db_session.add(
                    RedemptionRecord(
                        email=email,
                        code=code,
                        team_id=team_id,
                        account_id=target_team.account_id,
                        redeemed_at=redeemed_at,
                        is_warranty_redemption=redemption_code.has_warranty,
                    )
                )
            await db_session.commit()
            return self._build_success_result(target_team)
        except Exception:
            if db_session.in_transaction():
                await db_session.rollback()
            raise

    async def redeem_and_join_team(
        self,
        email: str,
        code: str,
        team_id: Optional[int],
        db_session: AsyncSession,
    ) -> Dict[str, Any]:
        """完整的兑换流程。"""
        email = normalize_email(email)
        code = code.strip()
        if not email or not code:
            return {"success": False, "error": "请同时提供邮箱和兑换码"}

        last_error = "未知错误"
        max_retries = 3
        attempt = 0
        current_target_team_id = team_id
        explicit_team_requested = team_id is not None
        excluded_team_ids = set()

        async with _code_locks[code]:
            while attempt < max_retries:
                team_id_final = current_target_team_id
                logger.info(
                    "兑换尝试 %s/%s (Code: %s, Email: %s, Team: %s)",
                    attempt + 1,
                    max_retries,
                    code,
                    email,
                    team_id_final,
                )

                try:
                    if not team_id_final:
                        pending_code = await self._load_redemption_code(db_session, code)
                        if pending_code and pending_code.status == PENDING_REDEMPTION_STATUS:
                            if not emails_match(pending_code.used_by_email, email):
                                return {
                                    "success": False,
                                    "error": self._build_pending_conflict_error(),
                                }
                            if not pending_code.used_team_id:
                                return {"success": False, "error": CODE_CONFLICT_ERROR}
                            team_id_final = pending_code.used_team_id
                            current_target_team_id = team_id_final
                        else:
                            select_res = await self.select_team_auto(
                                db_session,
                                exclude_team_ids=list(excluded_team_ids) or None,
                            )
                            if not select_res["success"]:
                                if excluded_team_ids:
                                    return {
                                        "success": False,
                                        "error": "该邮箱已在所有可用 Team 中，本次兑换未新增成员，兑换码未消耗",
                                    }
                                return {"success": False, "error": select_res["error"]}
                            team_id_final = select_res["team_id"]
                            current_target_team_id = team_id_final

                    async with _team_locks[team_id_final]:
                        logger.info("锁定 Team %s 执行兑换", team_id_final)

                        if db_session.in_transaction():
                            await db_session.rollback()

                        sync_res = await self._sync_team_members(team_id_final, db_session)
                        if not sync_res["success"]:
                            raise Exception(sync_res["error"])

                        target_team = await db_session.get(Team, team_id_final)
                        if not target_team:
                            raise Exception(f"目标 Team {team_id_final} 不存在")

                        if not db_session.in_transaction():
                            await db_session.begin()
                        reservation_snapshot: Optional[Dict[str, Any]] = None
                        try:
                            code_result = await db_session.execute(
                                select(RedemptionCode)
                                .where(RedemptionCode.code == code)
                                .with_for_update()
                            )
                            redemption_code = code_result.scalar_one_or_none()
                            if not redemption_code:
                                await db_session.rollback()
                                return {"success": False, "error": "兑换码不存在"}

                            existing_record = await self._load_existing_redemption_record(
                                db_session,
                                code,
                                email,
                                team_id_final,
                            )
                            if self._is_idempotent_success(
                                redemption_code,
                                existing_record,
                                email,
                                team_id_final,
                            ):
                                await db_session.commit()
                                return self._build_success_result(target_team)

                            if (
                                redemption_code.status == PENDING_REDEMPTION_STATUS
                                and not self._reservation_matches(redemption_code, email, team_id_final)
                            ):
                                await db_session.rollback()
                                return {
                                    "success": False,
                                    "error": self._build_pending_conflict_error(),
                                }

                            presence_state = self._classify_sync_presence(sync_res, email)
                            if presence_state in {"joined", "invited"}:
                                if self._is_pending_reservation(redemption_code, email, team_id_final):
                                    result = await self._finalize_redemption_record(
                                        db_session,
                                        code,
                                        email,
                                        team_id_final,
                                    )
                                    return result

                                if presence_state == "invited":
                                    error_message = self._build_existing_invite_error(email, target_team)
                                    await db_session.rollback()
                                    return {
                                        "success": False,
                                        "error": error_message,
                                    }
                                if explicit_team_requested:
                                    error_message = self._build_existing_membership_error(email, target_team)
                                    await db_session.rollback()
                                    return {
                                        "success": False,
                                        "error": error_message,
                                    }
                                await db_session.rollback()
                                excluded_team_ids.add(team_id_final)
                                current_target_team_id = None
                                continue

                            if self._is_pending_reservation(redemption_code, email, team_id_final):
                                pending_result = self._build_pending_result(target_team)
                                await db_session.rollback()
                                return pending_result

                            if redemption_code.status not in ["unused", WARRANTY_ACTIVE_STATUS, "used"]:
                                await db_session.rollback()
                                return {
                                    "success": False,
                                    "error": f"兑换码状态无效: {redemption_code.status}",
                                }

                            if redemption_code.status in {WARRANTY_ACTIVE_STATUS, "used"}:
                                warranty_check = await self.warranty_service.validate_warranty_reuse(
                                    db_session,
                                    code,
                                    email,
                                )
                                if not warranty_check.get("success"):
                                    await db_session.rollback()
                                    return {
                                        "success": False,
                                        "error": warranty_check.get("error")
                                        or warranty_check.get("reason")
                                        or "无法确认质保状态",
                                    }
                                if not warranty_check.get("can_reuse"):
                                    await db_session.rollback()
                                    return {
                                        "success": False,
                                        "error": warranty_check.get("reason") or "兑换码已使用",
                                    }

                            team_result = await db_session.execute(
                                select(Team).where(Team.id == team_id_final).with_for_update()
                            )
                            target_team = team_result.scalar_one_or_none()
                            if not target_team or target_team.status != "active":
                                raise Exception(
                                    f"目标 Team {team_id_final} 不可用 "
                                    f"({target_team.status if target_team else 'None'})"
                                )

                            if target_team.current_members >= target_team.max_members:
                                target_team.status = "full"
                                raise Exception("该 Team 已满, 请选择其他 Team 重试")

                            account_id_to_use = target_team.account_id
                            team_email_to_use = target_team.email
                            reservation_snapshot = self._snapshot_redemption_code(redemption_code)
                            self._reserve_redemption_code(redemption_code, email, team_id_final)
                            await db_session.commit()
                        except Exception:
                            if db_session.in_transaction():
                                await db_session.rollback()
                            raise

                        target_team = await db_session.get(Team, team_id_final)
                        access_token = await self.team_service.ensure_access_token(target_team, db_session)
                        if not access_token:
                            await self._restore_pending_reservation(
                                db_session,
                                code,
                                email,
                                team_id_final,
                                reservation_snapshot,
                            )
                            raise Exception("获取 Team 访问权限失败，账户状态异常")

                        invite_res = await self.chatgpt_service.send_invite(
                            access_token,
                            account_id_to_use,
                            email,
                            db_session,
                            identifier=team_email_to_use,
                        )
                        if not invite_res["success"]:
                            err = invite_res.get("error", "邀请失败")
                            err_str = str(err).lower()

                            if any(keyword in err_str for keyword in ALREADY_IN_TEAM_ERROR_KEYWORDS):
                                sync_res = await self._sync_team_members(team_id_final, db_session)
                                if not sync_res["success"]:
                                    return self._build_pending_result(target_team)

                                target_team = await db_session.get(Team, team_id_final)
                                if self._classify_sync_presence(sync_res, email) in {"joined", "invited"}:
                                    result = await self._finalize_redemption_record(
                                        db_session,
                                        code,
                                        email,
                                        team_id_final,
                                    )
                                    return result
                                return self._build_pending_result(target_team)

                            if any(keyword in err_str for keyword in FULL_TEAM_ERROR_KEYWORDS):
                                await self._restore_pending_reservation(
                                    db_session,
                                    code,
                                    email,
                                    team_id_final,
                                    reservation_snapshot,
                                )
                                target_team = await db_session.get(Team, team_id_final)
                                if target_team:
                                    target_team.status = "full"
                                    await db_session.commit()
                                raise Exception(f"该 Team 席位已满 (API Error: {err})")

                            await self._restore_pending_reservation(
                                db_session,
                                code,
                                email,
                                team_id_final,
                                reservation_snapshot,
                            )
                            raise Exception(err)

                        invite_data = invite_res.get("data", {})
                        if "account_invites" in invite_data and not invite_data.get("account_invites"):
                            await self._restore_pending_reservation(
                                db_session,
                                code,
                                email,
                                team_id_final,
                                reservation_snapshot,
                            )
                            raise Exception(
                                "Team账号受限: 官方拦截下发(响应空列表)，请检查账号风控状态"
                            )

                        confirm_res = await self._confirm_membership(team_id_final, email, db_session)
                        if not confirm_res["success"]:
                            return self._build_pending_result(target_team)

                        result = await self._finalize_redemption_record(
                            db_session,
                            code,
                            email,
                            team_id_final,
                        )
                        return result

                except Exception as exc:
                    last_error = str(exc)
                    logger.error("兑换重试失败 (%s): %s", attempt + 1, last_error)

                    try:
                        if db_session.in_transaction():
                            await db_session.rollback()
                    except Exception:
                        pass

                    if any(
                        keyword in last_error
                        for keyword in [
                            "不存在",
                            "已使用",
                            "已有正在使用",
                            "质保已过期",
                            "状态无效",
                            "待确认邀请",
                            "原兑换邮箱",
                        ]
                    ):
                        return {"success": False, "error": last_error}

                    if any(keyword in last_error.lower() for keyword in ["已满", "seats", "full"]):
                        try:
                            if not explicit_team_requested and team_id_final:
                                from sqlalchemy import update as sqlalchemy_update

                                await db_session.execute(
                                    sqlalchemy_update(Team)
                                    .where(Team.id == team_id_final)
                                    .values(status="full")
                                )
                                await db_session.commit()
                            current_target_team_id = None
                        except Exception:
                            pass

                    attempt += 1
                    if attempt < max_retries:
                        await asyncio.sleep(1.5 * attempt)

            return {
                "success": False,
                "error": f"兑换失败次数过多。最后报错: {last_error}",
            }


redeem_flow_service = RedeemFlowService()
