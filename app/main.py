"""
FastAPI application entrypoint.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware

from app.bootstrap import bootstrap_application
from app.config import settings
from app.database import close_db
from app.routes import admin, api, auth, redeem, user, warranty
from app.utils.time_utils import to_timezone


BASE_DIR = Path(__file__).resolve().parent.parent
APP_DIR = BASE_DIR / "app"

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def apply_persisted_log_level(raw_level: str | None, root_logger: logging.Logger | None = None) -> None:
    effective_root_logger = root_logger or logging.getLogger()
    current_level_name = logging.getLevelName(effective_root_logger.level)

    if raw_level is None or not str(raw_level).strip():
        logger.warning(
            f"No persisted log_level found in database; using environment log level {current_level_name}"
        )
        return

    normalized_level = str(raw_level).strip().upper()
    if normalized_level not in VALID_LOG_LEVELS:
        logger.warning(
            f"Invalid persisted log_level '{raw_level}' in database; using environment log level {current_level_name}"
        )
        return

    target_level = getattr(logging, normalized_level)
    if effective_root_logger.level != target_level:
        effective_root_logger.setLevel(target_level)
        logger.info(f"Applied persisted log level from database: {normalized_level}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize resources at startup and dispose them at shutdown."""
    logger.info("Application startup: validating security configuration")
    settings.validate_security()

    try:
        bootstrap_result = await bootstrap_application()
        apply_persisted_log_level(bootstrap_result.persisted_log_level)

        logger.info("Application startup completed")
        yield
    finally:
        await close_db()
        logger.info("Application shutdown completed")


app = FastAPI(
    title=settings.app_name,
    description="ChatGPT Team account management and redemption service",
    version=settings.app_version,
    lifespan=lifespan,
)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Redirect browser clients to login for auth failures."""
    response_headers = dict(exc.headers or {})

    if exc.status_code in [401, 403]:
        accept = request.headers.get("accept", "")
        if "text/html" in accept:
            return RedirectResponse(url="/login", headers=response_headers or None)

    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=response_headers or None,
    )


app.add_middleware(
    SessionMiddleware,
    secret_key=settings.secret_key,
    session_cookie="session",
    max_age=14 * 24 * 60 * 60,
    same_site="lax",
    https_only=settings.session_https_only_enabled,
)

app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")

templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


def format_datetime(value):
    """Render datetimes in the configured application timezone."""
    if not value:
        return "-"

    try:
        dt = to_timezone(value, settings.timezone)
    except Exception:
        return value

    if dt is None:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M")


def escape_js(value):
    """Escape values embedded into inline JavaScript."""
    if not value:
        return ""
    return (
        value.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


templates.env.filters["format_datetime"] = format_datetime
templates.env.filters["escape_js"] = escape_js

app.include_router(user.router)
app.include_router(redeem.router)
app.include_router(warranty.router)
app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(api.router)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(request, "auth/login.html", {"user": None})


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(APP_DIR / "static" / "favicon.png")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.debug,
    )
