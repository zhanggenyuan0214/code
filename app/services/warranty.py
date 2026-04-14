"""
Warranty service.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import RedemptionCode, RedemptionRecord, Team
from app.utils.email_utils import email_equals, emails_match, normalize_email, normalize_email_set
from app.utils.time_utils import get_now, is_expired


logger = logging.getLogger(__name__)

_query_rate_limit: dict[tuple[str, ...], Any] = {}
UNABLE_TO_CONFIRM_MESSAGE = "无法确认质保关联 Team 状态，请稍后重试"
OWNER_BINDING_MESSAGE = "该质保码已绑定原兑换邮箱，仅支持原兑换邮箱复兑"
OWNER_BINDING_UNKNOWN_MESSAGE = "无法确认原兑换归属，请联系管理员处理"


class WarrantyService:
    def __init__(self):
        from app.services.team import TeamService

        self.team_service = TeamService()

    async def _confirm_membership(
        self,
        db_session: AsyncSession,
        team: Team,
        email: str,
    ) -> Dict[str, Any]:
        with self.team_service.defer_sync_commit():
            sync_res = await self.team_service.sync_team_info(team.id, db_session)
        if not sync_res.get("success"):
            return {
                "confirmed": None,
                "reason": UNABLE_TO_CONFIRM_MESSAGE,
                "details": sync_res.get("error"),
            }

        member_emails = sync_res.get("member_emails")
        if member_emails is None:
            return {
                "confirmed": None,
                "reason": UNABLE_TO_CONFIRM_MESSAGE,
                "details": "sync_team_info did not return member_emails",
            }

        normalized = normalize_email_set(member_emails)
        return {
            "confirmed": normalize_email(email) in normalized,
            "reason": None,
            "details": None,
        }

    async def _confirm_membership_cached(
        self,
        db_session: AsyncSession,
        team: Team,
        email: str,
        membership_cache: dict[tuple[int, str], Dict[str, Any]],
    ) -> Dict[str, Any]:
        cache_key = (team.id, normalize_email(email) or "")
        if cache_key not in membership_cache:
            membership_cache[cache_key] = await self._confirm_membership(db_session, team, email)
        return membership_cache[cache_key]

    @staticmethod
    def _get_bound_owner_email(
        code_obj: RedemptionCode,
        records: list[RedemptionRecord],
    ) -> Optional[str]:
        for record in reversed(records):
            owner_email = normalize_email(record.email)
            if owner_email:
                return owner_email
        return normalize_email(code_obj.used_by_email)

    @staticmethod
    def _build_warranty_expiry(
        code_obj: RedemptionCode,
        record: Optional[RedemptionRecord],
    ):
        if not code_obj.has_warranty:
            return None

        expiry = code_obj.warranty_expires_at
        if not expiry and record is not None:
            start_time = code_obj.used_at or record.redeemed_at
            if start_time:
                expiry = start_time + timedelta(days=code_obj.warranty_days or 30)
        return expiry

    @classmethod
    def _evaluate_warranty_validity(
        cls,
        code_obj: RedemptionCode,
        record: Optional[RedemptionRecord],
    ) -> tuple[bool, Any]:
        expiry = cls._build_warranty_expiry(code_obj, record)
        if not code_obj.has_warranty:
            return False, expiry
        if expiry is None and code_obj.status == "unused":
            return True, expiry
        return not is_expired(expiry), expiry

    @staticmethod
    def _build_uncertain_response(original_code: Optional[str] = None) -> Dict[str, Any]:
        return {
            "success": True,
            "has_warranty": False,
            "warranty_valid": False,
            "warranty_expires_at": None,
            "banned_teams": [],
            "can_reuse": False,
            "original_code": original_code,
            "records": [],
            "message": UNABLE_TO_CONFIRM_MESSAGE,
        }

    async def _load_code_records_with_teams(
        self,
        db_session: AsyncSession,
        code: str,
    ) -> list[tuple[RedemptionRecord, Optional[Team]]]:
        stmt = (
            select(RedemptionRecord, Team)
            .join(Team, RedemptionRecord.team_id == Team.id, isouter=True)
            .where(RedemptionRecord.code == code)
            .order_by(RedemptionRecord.redeemed_at.desc(), RedemptionRecord.id.desc())
        )
        result = await db_session.execute(stmt)
        return result.all()

    async def _find_active_reuse_blocker(
        self,
        db_session: AsyncSession,
        records_with_teams: list[tuple[RedemptionRecord, Optional[Team]]],
        email: str,
        membership_cache: dict[tuple[int, str], Dict[str, Any]],
    ) -> Dict[str, Any]:
        for record, team in records_with_teams:
            if not team:
                continue
            if team.status not in {"active", "full"} or is_expired(team.expires_at):
                continue

            membership_check = await self._confirm_membership_cached(
                db_session,
                team,
                record.email,
                membership_cache,
            )
            if membership_check["confirmed"] is None:
                logger.warning(
                    "Warranty reuse check could not confirm Team %s membership for %s: %s",
                    team.id,
                    record.email,
                    membership_check.get("details"),
                )
                return {
                    "blocked": False,
                    "uncertain": True,
                    "reason": UNABLE_TO_CONFIRM_MESSAGE,
                }

            if membership_check["confirmed"] is False:
                logger.warning(
                    "Warranty reuse check found non-confirmed membership without mutating data "
                    "(Email: %s, Team: %s)",
                    record.email,
                    team.id,
                )
                continue

            if emails_match(record.email, email):
                return {
                    "blocked": True,
                    "uncertain": False,
                    "reason": f"您已在有效 Team 中 ({team.team_name or team.id})，不可重复兑换",
                }

            return {
                "blocked": True,
                "uncertain": False,
                "reason": "该兑换码当前已被其他账号绑定并正在使用中",
            }

        return {"blocked": False, "uncertain": False, "reason": None}

    async def check_warranty_status(
        self,
        db_session: AsyncSession,
        email: Optional[str] = None,
        code: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            if not email or not code:
                return {"success": False, "error": "请同时提供邮箱和兑换码"}

            email = normalize_email(email)
            code = code.strip()
            if not email or not code:
                return {"success": False, "error": "请同时提供邮箱和兑换码"}

            now = get_now()
            limit_key = ("email_code", email, code)
            last_time = _query_rate_limit.get(limit_key)
            if last_time and (now - last_time).total_seconds() < 30:
                wait_time = int(30 - (now - last_time).total_seconds())
                return {
                    "success": False,
                    "error": f"查询过于频繁，请 {wait_time} 秒后再试",
                    "status_code": 429,
                    "retry_after": wait_time,
                }
            _query_rate_limit[limit_key] = now

            stmt = (
                select(RedemptionRecord, RedemptionCode, Team)
                .join(RedemptionCode, RedemptionRecord.code == RedemptionCode.code)
                .join(Team, RedemptionRecord.team_id == Team.id)
                .where(
                    email_equals(RedemptionRecord.email, email),
                    RedemptionRecord.code == code,
                )
                .order_by(RedemptionRecord.redeemed_at.desc())
            )
            result = await db_session.execute(stmt)
            records_data = result.all()

            if not records_data:
                return {
                    "success": True,
                    "has_warranty": False,
                    "warranty_valid": False,
                    "warranty_expires_at": None,
                    "banned_teams": [],
                    "can_reuse": False,
                    "original_code": None,
                    "records": [],
                    "message": "未找到匹配记录",
                }

            final_records = []
            banned_teams_info = []
            has_any_warranty = False
            primary_warranty_valid = False
            primary_expiry = None
            primary_code = None
            membership_cache: dict[tuple[int, str], Dict[str, Any]] = {}
            code_records_with_teams = await self._load_code_records_with_teams(db_session, code)
            all_records_for_code = [record for record, _ in code_records_with_teams]
            bound_owner_email = self._get_bound_owner_email(
                records_data[0][1],
                all_records_for_code,
            )
            owner_matches = bound_owner_email is not None and emails_match(bound_owner_email, email)

            for record, code_obj, team in records_data:
                if team.status not in {"banned", "expired"}:
                    membership_check = await self._confirm_membership_cached(
                        db_session,
                        team,
                        record.email,
                        membership_cache,
                    )
                    if membership_check["confirmed"] is None:
                        logger.warning(
                            "Warranty status check could not confirm Team %s membership for %s: %s",
                            team.id,
                            record.email,
                            membership_check.get("details"),
                        )
                        return self._build_uncertain_response(code_obj.code)

                    if membership_check["confirmed"] is False:
                        logger.warning(
                            "Warranty status check found non-confirmed membership without mutating data "
                            "(Email: %s, Team: %s)",
                            record.email,
                            team.id,
                        )

                is_valid, expiry_date = self._evaluate_warranty_validity(code_obj, record)

                if code_obj.has_warranty:
                    has_any_warranty = True
                    if primary_code is None:
                        primary_code = code_obj.code
                        primary_warranty_valid = is_valid
                        primary_expiry = expiry_date

                if team.status == "banned":
                    banned_teams_info.append(
                        {
                            "team_id": team.id,
                            "team_name": team.team_name,
                            "email": team.email,
                            "banned_at": team.last_sync.isoformat() if team.last_sync else None,
                        }
                        )

                final_records.append(
                    {
                        "code": code_obj.code,
                        "has_warranty": code_obj.has_warranty,
                        "warranty_valid": is_valid,
                        "warranty_expires_at": expiry_date.isoformat() if expiry_date else None,
                        "status": code_obj.status,
                        "used_at": record.redeemed_at.isoformat() if record.redeemed_at else None,
                        "team_id": team.id,
                        "team_name": team.team_name,
                        "team_status": team.status,
                        "team_expires_at": team.expires_at.isoformat() if team.expires_at else None,
                        "email": record.email,
                        "device_code_auth_enabled": bool(team.device_code_auth_enabled),
                    }
                )

            active_reuse_blocker = await self._find_active_reuse_blocker(
                db_session,
                code_records_with_teams,
                email,
                membership_cache,
            )
            if active_reuse_blocker["uncertain"]:
                return self._build_uncertain_response(primary_code or code)

            can_reuse = bool(
                owner_matches
                and has_any_warranty
                and primary_warranty_valid
                and banned_teams_info
                and not active_reuse_blocker["blocked"]
            )

            message = "查询成功"
            if bound_owner_email is None:
                message = OWNER_BINDING_UNKNOWN_MESSAGE
            elif not owner_matches:
                message = OWNER_BINDING_MESSAGE

            return {
                "success": True,
                "has_warranty": has_any_warranty,
                "warranty_valid": primary_warranty_valid,
                "warranty_expires_at": primary_expiry.isoformat() if primary_expiry else None,
                "banned_teams": banned_teams_info,
                "can_reuse": can_reuse,
                "original_code": primary_code,
                "records": final_records,
                "message": message,
            }

        except Exception as exc:
            logger.error("检查质保状态失败: %s", exc)
            return {"success": False, "error": f"检查质保状态失败: {str(exc)}"}

    async def validate_warranty_reuse(
        self,
        db_session: AsyncSession,
        code: str,
        email: str,
    ) -> Dict[str, Any]:
        try:
            email = normalize_email(email)
            code = code.strip()
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            if not redemption_code:
                return {"success": True, "can_reuse": False, "reason": "兑换码不存在", "error": None}

            if not redemption_code.has_warranty:
                return {
                    "success": True,
                    "can_reuse": False,
                    "reason": "该兑换码不是质保兑换码",
                    "error": None,
                }

            records_with_teams = await self._load_code_records_with_teams(db_session, code)
            all_records_for_code = [record for record, _ in records_with_teams]
            latest_record = all_records_for_code[0] if all_records_for_code else None
            bound_owner_email = self._get_bound_owner_email(redemption_code, all_records_for_code)

            warranty_valid, _ = self._evaluate_warranty_validity(redemption_code, latest_record)
            if not warranty_valid:
                return {"success": True, "can_reuse": False, "reason": "质保已过期", "error": None}
            if bound_owner_email is None:
                return {
                    "success": True,
                    "can_reuse": False,
                    "reason": OWNER_BINDING_UNKNOWN_MESSAGE,
                    "error": None,
                }
            if not emails_match(bound_owner_email, email):
                return {
                    "success": True,
                    "can_reuse": False,
                    "reason": OWNER_BINDING_MESSAGE,
                    "error": None,
                }

            active_reuse_blocker = await self._find_active_reuse_blocker(
                db_session,
                records_with_teams,
                email,
                membership_cache={},
            )
            if active_reuse_blocker["uncertain"]:
                return {
                    "success": True,
                    "can_reuse": False,
                    "reason": UNABLE_TO_CONFIRM_MESSAGE,
                    "error": None,
                }
            if active_reuse_blocker["blocked"]:
                return {
                    "success": True,
                    "can_reuse": False,
                    "reason": active_reuse_blocker["reason"],
                    "error": None,
                }

            records_for_email = [
                record for record in all_records_for_code if emails_match(record.email, email)
            ]
            if not records_for_email:
                return {
                    "success": True,
                    "can_reuse": False,
                    "reason": "未找到原兑换记录，无法确认质保复兑条件",
                    "error": None,
                }

            has_banned_team = False
            for record, team in records_with_teams:
                if not emails_match(record.email, email):
                    continue
                if team and team.status == "banned":
                    has_banned_team = True
                    break

            if has_banned_team:
                return {
                    "success": True,
                    "can_reuse": True,
                    "reason": "之前加入的 Team 已封号，可使用质保重复兑换",
                    "error": None,
                }

            return {
                "success": True,
                "can_reuse": False,
                "reason": "未找到封号记录，且质保不支持正常过期或异常提示下的重复兑换",
                "error": None,
            }

        except Exception as exc:
            logger.error("验证质保码重复使用失败: %s", exc)
            return {
                "success": False,
                "can_reuse": False,
                "reason": None,
                "error": f"验证失败: {str(exc)}",
            }


warranty_service = WarrantyService()
