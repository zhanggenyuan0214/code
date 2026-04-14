"""
兑换路由。
"""
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.routes.error_utils import raise_internal_http_error
from app.services.redeem_flow import CODE_CONFLICT_ERROR, redeem_flow_service
from app.utils.email_utils import normalize_email

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/redeem", tags=["redeem"])

CONFLICT_ERROR_KEYWORDS = [
    "已满",
    "席位",
    "maximum number of seats",
    "已在 Team",
    "already in workspace",
    "already in team",
    "already a member",
    "待确认邀请",
    CODE_CONFLICT_ERROR,
]
BAD_REQUEST_ERROR_KEYWORDS = [
    "不存在",
    "已使用",
    "已过期",
    "截止时间",
    "质保",
    "原兑换邮箱",
    "归属",
    "无效",
    "失效",
    *CONFLICT_ERROR_KEYWORDS,
]


class RedeemRequest(BaseModel):
    """兑换请求。"""

    email: EmailStr = Field(..., description="用户邮箱")
    code: str = Field(..., min_length=1, description="兑换码")
    team_id: Optional[int] = Field(None, description="Team ID，不传则自动选择")


class RedeemResponse(BaseModel):
    """兑换响应。"""

    success: bool
    pending: bool = False
    message: Optional[str] = None
    team_info: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@router.post("/confirm", response_model=RedeemResponse)
async def confirm_redeem(
    request: RedeemRequest,
    db: AsyncSession = Depends(get_db),
):
    """确认兑换并加入 Team。"""
    try:
        logger.info(
            "兑换请求: %s -> Team %s (兑换码: %s)",
            request.email,
            request.team_id,
            request.code,
        )

        result = await redeem_flow_service.redeem_and_join_team(
            normalize_email(request.email) or str(request.email),
            request.code,
            request.team_id,
            db,
        )

        if not result["success"]:
            error_msg = result.get("error") or "未知原因"
            if any(keyword in error_msg for keyword in BAD_REQUEST_ERROR_KEYWORDS):
                status_code = status.HTTP_400_BAD_REQUEST
                if any(keyword in error_msg for keyword in CONFLICT_ERROR_KEYWORDS):
                    status_code = status.HTTP_409_CONFLICT
                raise HTTPException(status_code=status_code, detail=error_msg)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=error_msg,
            )

        return RedeemResponse(
            success=result.get("success", False),
            pending=result.get("pending", False),
            message=result.get("message"),
            team_info=result.get("team_info"),
            error=result.get("error"),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal_http_error(logger, exc, log_message="兑换失败")
