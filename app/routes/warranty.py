"""
质保相关路由
处理用户质保查询请求
"""
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.auth import require_admin
from app.routes.error_utils import raise_internal_http_error
from app.services.warranty import warranty_service
from app.utils.email_utils import email_equals, emails_match, normalize_email

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/warranty",
    tags=["warranty"],
)


class WarrantyCheckRequest(BaseModel):
    """质保查询请求"""

    email: Optional[EmailStr] = None
    code: Optional[str] = None


class WarrantyCheckRecord(BaseModel):
    """质保查询单条记录"""

    code: str
    has_warranty: bool
    warranty_valid: bool
    warranty_expires_at: Optional[str]
    status: str
    used_at: Optional[str]
    team_name: Optional[str]
    team_status: Optional[str]
    team_expires_at: Optional[str]


class WarrantyCheckResponse(BaseModel):
    """质保查询响应"""

    success: bool
    has_warranty: bool
    warranty_valid: bool
    warranty_expires_at: Optional[str]
    banned_teams: list[dict[str, Any]] = Field(default_factory=list)
    can_reuse: bool
    original_code: Optional[str]
    records: list[WarrantyCheckRecord] = Field(default_factory=list)
    message: Optional[str]
    error: Optional[str]


def _mask_team_name(team_name: Optional[str]) -> Optional[str]:
    if not team_name:
        return None
    return f"{team_name[:2]}***"


def _build_record_response(record: dict[str, Any]) -> WarrantyCheckRecord:
    return WarrantyCheckRecord(
        code=record["code"],
        has_warranty=record["has_warranty"],
        warranty_valid=record["warranty_valid"],
        warranty_expires_at=record.get("warranty_expires_at"),
        status=record["status"],
        used_at=record.get("used_at"),
        team_name=_mask_team_name(record.get("team_name")),
        team_status=record.get("team_status"),
        team_expires_at=record.get("team_expires_at"),
    )


def _build_banned_team_response(team_info: dict[str, Any]) -> dict[str, Any]:
    return {
        "team_name": _mask_team_name(team_info.get("team_name")),
        "banned_at": team_info.get("banned_at"),
    }


@router.post("/check", response_model=WarrantyCheckResponse)
async def check_warranty(
    request: WarrantyCheckRequest,
    db_session: AsyncSession = Depends(get_db),
):
    """
    检查质保状态。
    匿名用户必须同时提供邮箱和兑换码，只能查询这组精确匹配的数据。
    """
    try:
        if not request.email or not request.code:
            raise HTTPException(status_code=400, detail="请同时提供邮箱和兑换码")

        normalized_email = normalize_email(request.email)
        result = await warranty_service.check_warranty_status(
            db_session,
            email=normalized_email,
            code=request.code,
        )

        if not result.get("success"):
            headers = None
            if result.get("retry_after") is not None:
                headers = {"Retry-After": str(result["retry_after"])}
            raise HTTPException(
                status_code=result.get("status_code", status.HTTP_500_INTERNAL_SERVER_ERROR),
                detail=result.get("error", "查询失败"),
                headers=headers,
            )

        raw_records = result.get("records", [])
        exact_records = raw_records
        if raw_records:
            exact_records = [
                record
                for record in raw_records
                if emails_match(record.get("email"), normalized_email)
                and record.get("code") == request.code
            ]
            if not exact_records:
                return WarrantyCheckResponse(
                    success=True,
                    has_warranty=False,
                    warranty_valid=False,
                    warranty_expires_at=None,
                    banned_teams=[],
                    can_reuse=False,
                    original_code=None,
                    records=[],
                    message="未找到匹配记录",
                    error=None,
                )

        sanitized_records = [_build_record_response(record) for record in exact_records]
        sanitized_banned_teams = []
        if exact_records:
            sanitized_banned_teams = [
                _build_banned_team_response(team_info)
                for team_info in result.get("banned_teams", [])
            ]

        return WarrantyCheckResponse(
            success=True,
            has_warranty=result.get("has_warranty", False),
            warranty_valid=result.get("warranty_valid", False),
            warranty_expires_at=result.get("warranty_expires_at"),
            banned_teams=sanitized_banned_teams,
            can_reuse=result.get("can_reuse", False) if exact_records else False,
            original_code=(
                result.get("original_code")
                if exact_records and result.get("can_reuse", False)
                else None
            ),
            records=sanitized_records,
            message=result.get("message"),
            error=None,
        )

    except HTTPException:
        raise
    except Exception as exc:
        raise_internal_http_error(logger, exc, log_message="查询质保状态失败")


class EnableDeviceAuthRequest(BaseModel):
    """开启设备身份验证请求"""

    code: str
    email: str
    team_id: int


@router.post("/enable-device-auth")
async def enable_device_auth(
    request: EnableDeviceAuthRequest,
    db_session: AsyncSession = Depends(get_db),
    current_user: dict = Depends(require_admin),
):
    """
    管理员开启设备身份验证。
    """
    from sqlalchemy import select

    from app.models import RedemptionRecord
    from app.services.team import team_service

    _ = current_user

    try:
        normalized_email = normalize_email(request.email)
        if not normalized_email:
            raise HTTPException(status_code=400, detail="邮箱不能为空")

        stmt = select(RedemptionRecord).where(
            RedemptionRecord.code == request.code,
            email_equals(RedemptionRecord.email, normalized_email),
            RedemptionRecord.team_id == request.team_id,
        )
        result = await db_session.execute(stmt)
        record = result.scalar_one_or_none()

        if not record:
            raise HTTPException(
                status_code=403,
                detail="未找到相关的兑换记录，无法进行该操作",
            )

        res = await team_service.enable_device_code_auth(request.team_id, db_session)

        if not res.get("success"):
            raise HTTPException(status_code=500, detail=res.get("error", "开启失败"))

        return {"success": True, "message": "设备代码身份验证开启成功"}

    except HTTPException:
        raise
    except Exception as exc:
        raise_internal_http_error(
            logger,
            exc,
            log_message="开启设备身份验证失败",
            detail_prefix="开启失败",
        )
