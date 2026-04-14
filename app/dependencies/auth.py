"""
Authentication dependencies.
"""
import logging
from typing import Any

from fastapi import HTTPException, Request, status

logger = logging.getLogger(__name__)


def get_current_user(request: Request) -> dict[str, Any]:
    """Return the current authenticated session user."""
    user = request.session.get("user")
    if not user:
        logger.warning("Unauthenticated request attempted to access a protected resource")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )
    return user


def get_optional_admin_user(request: Request) -> dict[str, Any] | None:
    """Return the current admin session user when available."""
    user = request.session.get("user")
    if user and user.get("is_admin"):
        admin_user = dict(user)
        admin_user.setdefault("auth_type", "session")
        admin_user.setdefault("permissions", ["admin"])
        return admin_user
    return None


def require_session_admin(request: Request) -> dict[str, Any]:
    """Require an authenticated admin session."""
    admin_user = get_optional_admin_user(request)
    if admin_user:
        return admin_user

    logger.warning("Admin session authentication failed")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Admin session required",
    )
# Backward compatible alias for admin-only routes.
require_admin = require_session_admin
