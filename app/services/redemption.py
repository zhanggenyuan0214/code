"""
兑换码管理服务
用于管理兑换码的生成、验证、使用和查询
"""
import math
import logging
import secrets
import string
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import RedemptionCode, RedemptionRecord, Team
from app.utils.time_utils import get_local_now, get_now

logger = logging.getLogger(__name__)


class RedemptionService:
    """兑换码管理服务类"""

    def __init__(self):
        """初始化兑换码管理服务"""
        pass

    def _generate_random_code(self, length: int = 16) -> str:
        """
        生成随机兑换码

        Args:
            length: 兑换码长度

        Returns:
            随机兑换码字符串
        """
        # 使用大写字母和数字,排除容易混淆的字符 (0, O, I, 1)
        alphabet = string.ascii_uppercase + string.digits
        alphabet = alphabet.replace('0', '').replace('O', '').replace('I', '').replace('1', '')

        # 生成随机码
        code = ''.join(secrets.choice(alphabet) for _ in range(length))

        # 格式化为 XXXX-XXXX-XXXX-XXXX
        if length == 16:
            code = f"{code[0:4]}-{code[4:8]}-{code[8:12]}-{code[12:16]}"

        return code

    @staticmethod
    def _calculate_warranty_expires_at(
        redemption_code: RedemptionCode,
        *,
        has_warranty: Optional[bool] = None,
        warranty_days: Optional[int] = None,
    ):
        effective_has_warranty = redemption_code.has_warranty if has_warranty is None else has_warranty
        effective_warranty_days = redemption_code.warranty_days if warranty_days is None else warranty_days

        if not effective_has_warranty or redemption_code.used_at is None:
            return None

        return redemption_code.used_at + timedelta(days=effective_warranty_days or 30)

    async def _code_exists(self, db_session: AsyncSession, code: str) -> bool:
        stmt = select(RedemptionCode).where(RedemptionCode.code == code)
        result = await db_session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def _generate_unique_code(
        self,
        db_session: AsyncSession,
        *,
        reserved_codes: set[str] | None = None,
        max_attempts: int = 10,
    ) -> Optional[str]:
        reserved = reserved_codes or set()

        for _ in range(max_attempts):
            code = self._generate_random_code()
            if code in reserved:
                continue

            if not await self._code_exists(db_session, code):
                return code

        return None

    def _calculate_expires_at(self, expires_days: Optional[int]):
        if not expires_days:
            return None
        return get_now() + timedelta(days=expires_days)

    @staticmethod
    def _derive_status_from_warranty(current_status: Optional[str], has_warranty: bool) -> Optional[str]:
        if current_status in {"used", "warranty_active"}:
            return "warranty_active" if has_warranty else "used"
        return current_status

    def _apply_code_warranty_settings(
        self,
        redemption_code: RedemptionCode,
        *,
        has_warranty: Optional[bool] = None,
        warranty_days: Optional[int] = None,
    ) -> None:
        if has_warranty is not None:
            redemption_code.has_warranty = has_warranty
        if warranty_days is not None:
            redemption_code.warranty_days = warranty_days
        if redemption_code.warranty_days is None:
            redemption_code.warranty_days = 30

        redemption_code.status = self._derive_status_from_warranty(
            redemption_code.status,
            bool(redemption_code.has_warranty),
        )
        redemption_code.warranty_expires_at = self._calculate_warranty_expires_at(redemption_code)

    async def _get_code_history_exists(
        self,
        db_session: AsyncSession,
        code: str,
    ) -> bool:
        stmt = select(RedemptionRecord.id).where(RedemptionRecord.code == code)
        result = await db_session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def _get_delete_block_reason(
        self,
        db_session: AsyncSession,
        redemption_code: RedemptionCode,
    ) -> Optional[str]:
        if redemption_code.status == "invite_pending":
            return "待确认中的兑换码不能删除"

        if redemption_code.status in {"used", "warranty_active"}:
            return "已使用或质保中的兑换码不能删除"

        if await self._get_code_history_exists(db_session, redemption_code.code):
            return "存在兑换记录的兑换码不能删除"

        return None

    @staticmethod
    def _build_batch_code_action_response(
        *,
        action_name: str,
        completion_message: str,
        success_count: int,
        failed_count: int,
        failure_details: list[str],
    ) -> Dict[str, Any]:
        response: Dict[str, Any] = {
            "success": success_count > 0,
            "partial": success_count > 0 and failed_count > 0,
            "message": f"{completion_message}: 成功 {success_count}, 失败 {failed_count}",
            "success_count": success_count,
            "failed_count": failed_count,
        }

        if failure_details:
            preview = "；".join(failure_details[:3])
            if len(failure_details) > 3:
                preview += f"；另有 {len(failure_details) - 3} 个失败"
            response["errors"] = failure_details
            if success_count == 0:
                response["error"] = preview
            else:
                response["message"] = f"{response['message']}。{preview}"

        return response

    @staticmethod
    def _build_record_filters(
        *,
        email: Optional[str] = None,
        code: Optional[str] = None,
        team_id: Optional[int] = None,
        redeemed_from: Optional[datetime] = None,
        redeemed_until: Optional[datetime] = None,
    ) -> list[Any]:
        filters = []
        if email:
            filters.append(RedemptionRecord.email.ilike(f"%{email}%"))
        if code:
            filters.append(RedemptionRecord.code.ilike(f"%{code}%"))
        if team_id is not None:
            filters.append(RedemptionRecord.team_id == team_id)
        if redeemed_from is not None:
            filters.append(RedemptionRecord.redeemed_at >= redeemed_from)
        if redeemed_until is not None:
            filters.append(RedemptionRecord.redeemed_at < redeemed_until)
        return filters

    async def _count_records(
        self,
        db_session: AsyncSession,
        *,
        filters: list[Any],
        redeemed_from: Optional[datetime] = None,
    ) -> int:
        stmt = select(func.count(RedemptionRecord.id)).select_from(RedemptionRecord)

        all_filters = list(filters)
        if redeemed_from is not None:
            all_filters.append(RedemptionRecord.redeemed_at >= redeemed_from)
        if all_filters:
            stmt = stmt.where(and_(*all_filters))

        result = await db_session.execute(stmt)
        return result.scalar() or 0

    def _build_redemption_code(
        self,
        *,
        code: str,
        expires_at,
        has_warranty: bool,
        warranty_days: int,
    ) -> RedemptionCode:
        return RedemptionCode(
            code=code,
            status="unused",
            expires_at=expires_at,
            has_warranty=has_warranty,
            warranty_days=warranty_days,
        )

    async def _prepare_code_for_insert(
        self,
        db_session: AsyncSession,
        *,
        code: Optional[str] = None,
        expires_at=None,
        expires_days: Optional[int] = None,
        has_warranty: bool = False,
        warranty_days: int = 30,
        reserved_codes: set[str] | None = None,
    ) -> tuple[Optional[RedemptionCode], Optional[str]]:
        reserved = reserved_codes or set()
        resolved_code = code

        if not resolved_code:
            resolved_code = await self._generate_unique_code(
                db_session,
                reserved_codes=reserved,
            )
            if not resolved_code:
                return None, "生成唯一兑换码失败,请重试"
        elif resolved_code in reserved or await self._code_exists(db_session, resolved_code):
            return None, f"兑换码 {resolved_code} 已存在"

        effective_expires_at = expires_at
        if effective_expires_at is None:
            effective_expires_at = self._calculate_expires_at(expires_days)

        return (
            self._build_redemption_code(
                code=resolved_code,
                expires_at=effective_expires_at,
                has_warranty=has_warranty,
                warranty_days=warranty_days,
            ),
            None,
        )

    async def generate_code_single(
        self,
        db_session: AsyncSession,
        code: Optional[str] = None,
        expires_days: Optional[int] = None,
        has_warranty: bool = False,
        warranty_days: int = 30
    ) -> Dict[str, Any]:
        """
        生成单个兑换码

        Args:
            db_session: 数据库会话
            code: 自定义兑换码 (可选,如果不提供则自动生成)
            expires_days: 有效期天数 (可选,如果不提供则永久有效)
            has_warranty: 是否为质保兑换码 (默认 False)

        Returns:
            结果字典,包含 success, code, message, error
        """
        try:
            redemption_code, error = await self._prepare_code_for_insert(
                db_session,
                code=code,
                expires_days=expires_days,
                has_warranty=has_warranty,
                warranty_days=warranty_days,
            )
            if redemption_code is None:
                return {
                    "success": False,
                    "code": None,
                    "message": None,
                    "error": error,
                }

            db_session.add(redemption_code)
            await db_session.commit()

            logger.info(f"生成兑换码成功: {redemption_code.code}")

            return {
                "success": True,
                "code": redemption_code.code,
                "message": f"兑换码生成成功: {redemption_code.code}",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"生成兑换码失败: {e}")
            return {
                "success": False,
                "code": None,
                "message": None,
                "error": f"生成兑换码失败: {str(e)}"
            }

    async def generate_code_batch(
        self,
        db_session: AsyncSession,
        count: int,
        expires_days: Optional[int] = None,
        has_warranty: bool = False,
        warranty_days: int = 30
    ) -> Dict[str, Any]:
        """
        批量生成兑换码

        Args:
            db_session: 数据库会话
            count: 生成数量
            expires_days: 有效期天数 (可选)
            has_warranty: 是否为质保兑换码 (默认 False)

        Returns:
            结果字典,包含 success, codes, total, message, error
        """
        try:
            if count <= 0 or count > 1000:
                return {
                    "success": False,
                    "codes": [],
                    "total": 0,
                    "message": None,
                    "error": "生成数量必须在 1-1000 之间"
                }

            codes = []
            pending_codes: set[str] = set()
            pending_records: list[RedemptionCode] = []
            expires_at = self._calculate_expires_at(expires_days)

            for i in range(count):
                redemption_code, error = await self._prepare_code_for_insert(
                    db_session,
                    expires_at=expires_at,
                    has_warranty=has_warranty,
                    warranty_days=warranty_days,
                    reserved_codes=pending_codes,
                )
                if redemption_code is None:
                    logger.warning(f"生成第 {i+1} 个兑换码失败: {error}")
                    continue

                pending_codes.add(redemption_code.code)
                pending_records.append(redemption_code)
                codes.append(redemption_code.code)

            db_session.add_all(pending_records)

            await db_session.commit()

            logger.info(f"批量生成兑换码成功: {len(codes)} 个")

            return {
                "success": True,
                "codes": codes,
                "total": len(codes),
                "message": f"成功生成 {len(codes)} 个兑换码",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"批量生成兑换码失败: {e}")
            return {
                "success": False,
                "codes": [],
                "total": 0,
                "message": None,
                "error": f"批量生成兑换码失败: {str(e)}"
            }

    async def get_all_codes(
        self,
        db_session: AsyncSession,
        page: int = 1,
        per_page: int = 50,
        search: Optional[str] = None,
        status: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取所有兑换码

        Args:
            db_session: 数据库会话
            page: 页码
            per_page: 每页数量
            search: 搜索关键词 (兑换码或邮箱)
            status: 状态筛选

        Returns:
            结果字典,包含 success, codes, total, total_pages, current_page, error
        """
        try:
            # 1. 构建基础查询
            count_stmt = select(func.count(RedemptionCode.id))
            stmt = select(RedemptionCode).order_by(RedemptionCode.created_at.desc())

            # 2. 如果提供了筛选条件,添加过滤条件
            filters = []
            if search:
                filters.append(or_(
                    RedemptionCode.code.ilike(f"%{search}%"),
                    RedemptionCode.used_by_email.ilike(f"%{search}%")
                ))
            
            if status:
                if status == 'used':
                    # "已使用" 在查询中通常指窄义的 used, 但如果要包含质保中, 逻辑如下
                    filters.append(RedemptionCode.status.in_(['used', 'warranty_active', 'invite_pending']))
                else:
                    filters.append(RedemptionCode.status == status)
            
            if filters:
                count_stmt = count_stmt.where(and_(*filters))
                stmt = stmt.where(and_(*filters))

            # 3. 获取总数
            count_result = await db_session.execute(count_stmt)
            total = count_result.scalar() or 0

            # 4. 计算分页
            import math
            total_pages = math.ceil(total / per_page) if total > 0 else 1
            if page < 1:
                page = 1
            if page > total_pages and total_pages > 0:
                page = total_pages
            
            offset = (page - 1) * per_page

            # 5. 查询分页数据
            stmt = stmt.limit(per_page).offset(offset)
            result = await db_session.execute(stmt)
            codes = result.scalars().all()

            # 构建返回数据
            code_list = []
            for code in codes:
                code_list.append({
                    "id": code.id,
                    "code": code.code,
                    "status": code.status,
                    "created_at": code.created_at.isoformat() if code.created_at else None,
                    "expires_at": code.expires_at.isoformat() if code.expires_at else None,
                    "used_by_email": code.used_by_email,
                    "used_team_id": code.used_team_id,
                    "used_at": code.used_at.isoformat() if code.used_at else None,
                    "has_warranty": code.has_warranty,
                    "warranty_days": code.warranty_days,
                    "warranty_expires_at": code.warranty_expires_at.isoformat() if code.warranty_expires_at else None
                })

            logger.info(f"获取所有兑换码成功: 第 {page} 页, 共 {len(code_list)} 个 / 总数 {total}")

            return {
                "success": True,
                "codes": code_list,
                "total": total,
                "total_pages": total_pages,
                "current_page": page,
                "error": None
            }

        except Exception as e:
            logger.error(f"获取所有兑换码失败: {e}")
            return {
                "success": False,
                "codes": [],
                "total": 0,
                "error": f"获取所有兑换码失败: {str(e)}"
            }

    async def get_all_records(
        self,
        db_session: AsyncSession,
        email: Optional[str] = None,
        code: Optional[str] = None,
        team_id: Optional[int] = None,
        redeemed_from: Optional[datetime] = None,
        redeemed_until: Optional[datetime] = None,
        page: Optional[int] = None,
        per_page: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        获取所有兑换记录 (支持筛选)

        Args:
            db_session: 数据库会话
            email: 邮箱模糊搜索
            code: 兑换码模糊搜索
            team_id: Team ID 筛选

        Returns:
            结果字典,包含 success, records, total, error
        """
        try:
            filters = self._build_record_filters(
                email=email,
                code=code,
                team_id=team_id,
                redeemed_from=redeemed_from,
                redeemed_until=redeemed_until,
            )

            base_stmt = (
                select(
                    RedemptionRecord.id,
                    RedemptionRecord.email,
                    RedemptionRecord.code,
                    RedemptionRecord.team_id,
                    RedemptionRecord.account_id,
                    RedemptionRecord.redeemed_at,
                    Team.team_name,
                )
                .select_from(RedemptionRecord)
                .outerjoin(Team, RedemptionRecord.team_id == Team.id)
                .order_by(RedemptionRecord.redeemed_at.desc())
            )
            if filters:
                base_stmt = base_stmt.where(and_(*filters))

            total = await self._count_records(db_session, filters=filters)

            effective_page = page if page and page > 0 else 1
            total_pages = 1
            if per_page and per_page > 0:
                total_pages = math.ceil(total / per_page) if total > 0 else 1
                if effective_page > total_pages:
                    effective_page = total_pages
                base_stmt = base_stmt.limit(per_page).offset((effective_page - 1) * per_page)

            result = await db_session.execute(base_stmt)
            rows = result.all()

            record_list = []
            for row in rows:
                record_list.append({
                    "id": row.id,
                    "email": row.email,
                    "code": row.code,
                    "team_id": row.team_id,
                    "team_name": row.team_name,
                    "account_id": row.account_id,
                    "redeemed_at": row.redeemed_at.isoformat() if row.redeemed_at else None,
                })

            now = get_local_now()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            week_start = today_start - timedelta(days=today_start.weekday())
            month_start = today_start.replace(day=1)

            stats = {
                "total": total,
                "today": await self._count_records(db_session, filters=filters, redeemed_from=today_start),
                "this_week": await self._count_records(db_session, filters=filters, redeemed_from=week_start),
                "this_month": await self._count_records(db_session, filters=filters, redeemed_from=month_start),
            }

            logger.info(f"获取所有兑换记录成功: 共 {len(record_list)} 条 / 总数 {total}")

            return {
                "success": True,
                "records": record_list,
                "total": total,
                "stats": stats,
                "total_pages": total_pages,
                "current_page": effective_page,
                "error": None,
            }

        except Exception as e:
            logger.error(f"获取所有兑换记录失败: {e}")
            return {
                "success": False,
                "records": [],
                "total": 0,
                "error": f"获取所有兑换记录失败: {str(e)}"
            }

    async def delete_code(
        self,
        code: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        删除兑换码。
        """
        try:
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            if not redemption_code:
                return {
                    "success": False,
                    "message": None,
                    "error": f"兑换码 {code} 不存在"
                }

            delete_block_reason = await self._get_delete_block_reason(db_session, redemption_code)
            if delete_block_reason:
                return {
                    "success": False,
                    "message": None,
                    "error": delete_block_reason
                }

            await db_session.delete(redemption_code)
            await db_session.commit()

            logger.info(f"删除兑换码成功: {code}")

            return {
                "success": True,
                "message": f"兑换码 {code} 已删除",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"删除兑换码失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"删除兑换码失败: {str(e)}"
            }

    async def update_code(
        self,
        code: str,
        db_session: AsyncSession,
        has_warranty: Optional[bool] = None,
        warranty_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """更新兑换码信息"""
        return await self.bulk_update_codes([code], db_session, has_warranty, warranty_days)

    async def withdraw_record(
        self,
        record_id: int,
        db_session: AsyncSession,
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        撤回使用记录 (删除记录,恢复兑换码,并在 Team 中移除成员/邀请)

        Args:
            record_id: 记录 ID
            db_session: 数据库会话

        Returns:
            结果字典
        """
        try:
            from app.services.team import team_service
            
            # 1. 查询记录
            stmt = select(RedemptionRecord).where(RedemptionRecord.id == record_id).options(
                selectinload(RedemptionRecord.redemption_code)
            )
            result = await db_session.execute(stmt)
            record = result.scalar_one_or_none()

            if not record:
                return {"success": False, "error": f"记录 ID {record_id} 不存在"}

            # 2. 调用 TeamService 移除成员/邀请
            logger.info(f"正在从 Team {record.team_id} 中移除成员 {record.email}")
            team_result = await team_service.remove_invite_or_member(
                record.team_id,
                record.email,
                db_session,
                force_local=force,
            )

            remaining_record_stmt = (
                select(RedemptionRecord)
                .where(
                    RedemptionRecord.code == record.code,
                    RedemptionRecord.id != record.id,
                )
                .order_by(RedemptionRecord.redeemed_at.desc(), RedemptionRecord.id.desc())
            )
            remaining_record_result = await db_session.execute(remaining_record_stmt)
            remaining_record = remaining_record_result.scalars().first()

            if not team_result["success"]:
                if team_result.get("can_force_local_repair") and not force:
                    return {
                        "success": False,
                        "requires_force": True,
                        "can_force": True,
                        "error_code": team_result.get("error_code"),
                        "error": (
                            "当前 Team 已不可访问，普通撤回无法确认上游成员状态。"
                            "如果这是修复历史错误，请使用强制撤回，仅修复本地记录和兑换码。"
                        ),
                    }

                if not team_result.get("upstream_success"):
                    return {
                        "success": False,
                        "error": f"从 Team 移除成员失败: {team_result.get('error') or team_result.get('message')}"
                    }

            # 3. 恢复兑换码状态
            code = record.redemption_code
            if code:
                # 如果是质保兑换，且还有其他记录，状态可能不应该直接回 unused
                # 但根据逻辑，目前一个码一个记录（除了质保补发可能产生新记录，但那是两个不同的码吧？）
                # 查了一下模型，RedemptionCode 有 used_by_email 等字段，说明它是单次使用的设计
                code.status = "unused"
                code.used_by_email = None
                code.used_team_id = None
                code.used_at = None
                code.warranty_expires_at = None
                # 特殊处理质保字段
                if code.has_warranty:
                    code.warranty_expires_at = None
                if remaining_record:
                    code.status = "warranty_active" if code.has_warranty else "used"
                    code.used_by_email = remaining_record.email
                    code.used_team_id = remaining_record.team_id
                    code.used_at = remaining_record.redeemed_at
                code.warranty_expires_at = self._calculate_warranty_expires_at(code)

            # 4. 删除使用记录
            await db_session.delete(record)
            await db_session.commit()

            logger.info(f"撤回记录成功: {record_id}, 邮箱: {record.email}, 兑换码: {record.code}")

            message = f"成功撤回记录并恢复兑换码 {record.code}"
            if team_result.get("local_only"):
                message = (
                    f"已强制撤回记录并恢复兑换码 {record.code}；"
                    "当前 Team 已不可访问，本次仅修复本地记录和兑换码。"
                )
            elif team_result.get("upstream_action") == "already_absent":
                message = f"上游成员或邀请已不存在，已撤回记录并恢复兑换码 {record.code}。"
            elif team_result.get("partial"):
                message = (
                    f"已撤回记录并恢复兑换码 {record.code}，"
                    "但 Team 本地状态未能同步刷新，请手动复核。"
                )

            return {
                "success": True,
                "partial": bool(team_result.get("partial") or team_result.get("local_only")),
                "local_only": bool(team_result.get("local_only")),
                "message": message
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"撤回记录失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "error": f"撤回失败: {str(e)}"}

    async def bulk_update_codes(
        self,
        codes: List[str],
        db_session: AsyncSession,
        has_warranty: Optional[bool] = None,
        warranty_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        批量更新兑换码信息

        Args:
            codes: 兑换码列表
            db_session: 数据库会话
            has_warranty: 是否为质保兑换码 (可选)
            warranty_days: 质保天数 (可选)

        Returns:
            结果字典
        """
        try:
            if not codes:
                return {"success": True, "message": "没有需要更新的兑换码"}

            if has_warranty is None and warranty_days is None:
                return {"success": True, "message": "没有提供更新内容"}

            stmt = select(RedemptionCode).where(RedemptionCode.code.in_(codes))
            result = await db_session.execute(stmt)
            code_rows = result.scalars().all()

            for code_row in code_rows:
                self._apply_code_warranty_settings(
                    code_row,
                    has_warranty=has_warranty,
                    warranty_days=warranty_days,
                )

            await db_session.commit()

            logger.info(f"成功批量更新 {len(codes)} 个兑换码")

            return {
                "success": True,
                "message": f"成功批量更新 {len(codes)} 个兑换码",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"批量更新兑换码失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"批量更新失败: {str(e)}"
            }

    async def bulk_delete_codes(
        self,
        codes: List[str],
        db_session: AsyncSession,
    ) -> Dict[str, Any]:
        """批量删除兑换码。"""
        try:
            requested_codes = list(dict.fromkeys(code for code in codes if code))
            if not requested_codes:
                return {
                    "success": False,
                    "partial": False,
                    "message": "批量删除完成: 成功 0, 失败 0",
                    "success_count": 0,
                    "failed_count": 0,
                    "error": "请选择需要删除的兑换码",
                }

            stmt = select(RedemptionCode).where(RedemptionCode.code.in_(requested_codes))
            result = await db_session.execute(stmt)
            code_rows = result.scalars().all()
            code_map = {code_row.code: code_row for code_row in code_rows}

            deletable_rows: list[RedemptionCode] = []
            success_count = 0
            failed_count = 0
            failure_details: list[str] = []

            for code in requested_codes:
                code_row = code_map.get(code)
                if code_row is None:
                    failed_count += 1
                    failure_details.append(f"{code}: 兑换码不存在")
                    continue

                delete_block_reason = await self._get_delete_block_reason(db_session, code_row)
                if delete_block_reason:
                    failed_count += 1
                    failure_details.append(f"{code}: {delete_block_reason}")
                    continue

                deletable_rows.append(code_row)

            for code_row in deletable_rows:
                await db_session.delete(code_row)
                success_count += 1

            await db_session.commit()

            response = self._build_batch_code_action_response(
                action_name="删除",
                completion_message="批量删除完成",
                success_count=success_count,
                failed_count=failed_count,
                failure_details=failure_details,
            )
            response["deleted_codes"] = [code_row.code for code_row in deletable_rows]
            return response

        except Exception as e:
            await db_session.rollback()
            logger.error(f"批量删除兑换码失败: {e}")
            return {
                "success": False,
                "partial": False,
                "message": None,
                "success_count": 0,
                "failed_count": len(codes),
                "error": f"批量删除失败: {str(e)}",
            }

    async def get_stats(
        self,
        db_session: AsyncSession
    ) -> Dict[str, int]:
        """
        获取兑换码统计信息
        
        Returns:
            统计字典, 包含 total, unused, used, expired
        """
        try:
            # 使用 SQL 聚合统计各状态数量
            stmt = select(
                RedemptionCode.status,
                func.count(RedemptionCode.id)
            ).group_by(RedemptionCode.status)
            
            result = await db_session.execute(stmt)
            status_counts = dict(result.all())
            
            # 由于 "used" 和 "warranty_active" 都属于广义上的 "已使用"
            # 这里的 used 统计需要合并这两个状态
            used_count = (
                status_counts.get("used", 0)
                + status_counts.get("warranty_active", 0)
                + status_counts.get("invite_pending", 0)
            )
            
            # 计算总数
            total_stmt = select(func.count(RedemptionCode.id))
            total_result = await db_session.execute(total_stmt)
            total = total_result.scalar() or 0
            
            return {
                "total": total,
                "unused": status_counts.get("unused", 0),
                "used": used_count,
                "warranty_active": status_counts.get("warranty_active", 0),
                "invite_pending": status_counts.get("invite_pending", 0),
                "expired": status_counts.get("expired", 0)
            }
        except Exception as e:
            logger.error(f"获取兑换码统计信息失败: {e}")
            return {
                "total": 0,
                "unused": 0,
                "used": 0,
                "expired": 0
            }


# 创建全局兑换码服务实例
redemption_service = RedemptionService()
