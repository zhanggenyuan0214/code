"""
Small helpers for translating unexpected route exceptions.
"""
from __future__ import annotations

import logging

from fastapi import HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse


def _format_error_message(exc: Exception, *, prefix: str | None, include_prefix: bool) -> str:
    if not include_prefix or not prefix:
        return str(exc)
    return f"{prefix}: {exc}"


def json_internal_error(
    logger: logging.Logger,
    exc: Exception,
    *,
    log_message: str,
    error_prefix: str | None = None,
    include_prefix: bool = True,
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
) -> JSONResponse:
    """Return the standard JSON 500 payload used by many routes."""
    logger.exception("%s", log_message)
    return JSONResponse(
        status_code=status_code,
        content={
            "success": False,
            "error": _format_error_message(
                exc,
                prefix=error_prefix or log_message,
                include_prefix=include_prefix,
            ),
        },
    )


def raise_internal_http_error(
    logger: logging.Logger,
    exc: Exception,
    *,
    log_message: str,
    detail_prefix: str | None = None,
    include_prefix: bool = True,
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
) -> None:
    """Raise the standard HTTP 500 used by API-style routes."""
    logger.exception("%s", log_message)
    raise HTTPException(
        status_code=status_code,
        detail=_format_error_message(
            exc,
            prefix=detail_prefix or log_message,
            include_prefix=include_prefix,
        ),
    ) from exc


def html_internal_error(
    logger: logging.Logger,
    exc: Exception,
    *,
    log_message: str,
    title: str = "页面加载失败",
    detail_prefix: str | None = None,
    include_prefix: bool = True,
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
) -> HTMLResponse:
    """Return the standard HTML 500 payload used by page routes."""
    logger.exception("%s", log_message)
    detail = _format_error_message(
        exc,
        prefix=detail_prefix or log_message,
        include_prefix=include_prefix,
    )
    return HTMLResponse(
        content=f"<h1>{title}</h1><p>{detail}</p>",
        status_code=status_code,
    )
