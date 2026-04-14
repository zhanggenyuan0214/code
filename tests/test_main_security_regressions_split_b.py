import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi.testclient import TestClient

from app.config import Settings
from app.database import get_db
from app.main import app as main_app


async def fake_db_dependency():
    yield object()


class MainHttpExceptionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.app = main_app
        self.app.dependency_overrides[get_db] = fake_db_dependency

        self.validate_security_patcher = patch("app.config.Settings.validate_security", new=Mock())
        self.bootstrap_patcher = patch(
            "app.main.bootstrap_application",
            new=AsyncMock(
                return_value=SimpleNamespace(
                    persisted_log_level=None,
                    created_settings=[],
                    removed_legacy_settings=[],
                )
            ),
        )
        self.close_db_patcher = patch("app.main.close_db", new=AsyncMock())

        self.validate_security_patcher.start()
        self.bootstrap_patcher.start()
        self.close_db_patcher.start()

    def tearDown(self):
        self.app.dependency_overrides.clear()
        self.close_db_patcher.stop()
        self.bootstrap_patcher.stop()
        self.validate_security_patcher.stop()

    def test_main_app_login_rate_limit_preserves_retry_after_header(self):
        with patch(
            "app.routes.auth.auth_service.verify_admin_login",
            new=AsyncMock(
                return_value={
                    "success": False,
                    "error": "登录尝试过于频繁，请在 19 秒后重试",
                    "status_code": 429,
                    "retry_after": 19,
                }
            ),
        ):
            with TestClient(self.app) as client:
                response = client.post("/auth/login", json={"password": "wrong-password"})

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers.get("retry-after"), "19")
        self.assertEqual(response.json()["detail"], "登录尝试过于频繁，请在 19 秒后重试")

    def test_main_app_warranty_rate_limit_preserves_retry_after_header(self):
        with patch(
            "app.routes.warranty.warranty_service.check_warranty_status",
            new=AsyncMock(
                return_value={
                    "success": False,
                    "error": "查询过于频繁，请 17 秒后再试",
                    "status_code": 429,
                    "retry_after": 17,
                }
            ),
        ):
            with TestClient(self.app) as client:
                response = client.post(
                    "/warranty/check",
                    json={"email": "user@example.com", "code": "WARRANTY-CODE"},
                )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers.get("retry-after"), "17")
        self.assertEqual(response.json()["detail"], "查询过于频繁，请 17 秒后再试")

    def test_main_app_html_auth_failures_still_redirect_to_login(self):
        with TestClient(self.app) as client:
            response = client.get(
                "/admin/codes",
                headers={"accept": "text/html"},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers.get("location"), "/login")


class SessionCookieSecurityConfigTests(unittest.TestCase):
    def test_production_defaults_session_cookie_to_secure_when_not_overridden(self):
        config = Settings(
            environment="production",
            session_https_only=None,
            secret_key="strong-secret",
            admin_password="strong-password",
        )

        self.assertTrue(config.session_https_only_enabled)

    def test_development_keeps_session_cookie_non_secure_by_default(self):
        config = Settings(
            environment="development",
            session_https_only=None,
            secret_key="strong-secret",
            admin_password="strong-password",
        )

        self.assertFalse(config.session_https_only_enabled)


if __name__ == "__main__":
    unittest.main()
