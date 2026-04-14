"""
用户路由
处理用户兑换页面
"""
import logging
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.routes.error_utils import html_internal_error

logger = logging.getLogger(__name__)

# 创建路由器
router = APIRouter(
    tags=["user"]
)


@router.get("/", response_class=HTMLResponse)
async def redeem_page(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """
    用户兑换页面

    Args:
        request: FastAPI Request 对象
        db: 数据库会话

    Returns:
        用户兑换页面 HTML
    """
    try:
        from app.main import templates
        from app.services.settings import settings_service
        from app.services.team import TeamService
        
        team_service = TeamService()
        remaining_spots = await team_service.get_total_available_seats(db)
        homepage_announcement = (
            await settings_service.get_setting(db, "homepage_announcement", "")
            or ""
        ).strip()
        homepage_usage_notice = (
            await settings_service.get_setting(db, "homepage_usage_notice", "")
            or ""
        ).strip()

        logger.info(f"用户访问兑换页面，剩余车位: {remaining_spots}")

        return templates.TemplateResponse(
            request,
            "user/redeem.html",
            {
                "remaining_spots": remaining_spots,
                "homepage_announcement": homepage_announcement,
                "homepage_usage_notice": homepage_usage_notice,
            }
        )

    except Exception as e:
        return html_internal_error(
            logger,
            e,
            log_message="渲染兑换页面失败",
            include_prefix=False,
        )
