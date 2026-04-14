import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool
from starlette.middleware.sessions import SessionMiddleware

from app.config import DEFAULT_ADMIN_PASSWORD, DEFAULT_SECRET_KEY, Settings
from app.database import Base, _build_engine_kwargs, get_db
from app.models import TEAM_MEMBER_LIMIT, RedemptionCode, RedemptionRecord, Team
from app.routes import admin, auth, redeem, warranty
from app.services.auth import AuthService
from app.services.chatgpt import ChatGPTService
from app.services.encryption import encryption_service
from app.services.redeem_flow import PENDING_REDEMPTION_STATUS, RedeemFlowService
from app.services.team import TeamService
from app.services.warranty import UNABLE_TO_CONFIRM_MESSAGE, WarrantyService, _query_rate_limit
from app.utils.jwt_parser import JWTParser
from app.utils.security import mask_secret
from app.utils.time_utils import UTC, get_app_timezone, parse_datetime


async def fake_db_dependency():
    yield object()


class AuthServiceRateLimitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = AuthService()
        self.password_hash = self.service.hash_password("correct-password")
        self.service.get_admin_password_hash = AsyncMock(return_value=self.password_hash)

    async def test_repeated_failed_logins_trigger_temporary_lock_and_success_resets_state(self):
        client_identifier = "127.0.0.1"
        base_time = datetime(2026, 4, 4, 0, 0, tzinfo=timezone.utc)

        self.service._utcnow = Mock(return_value=base_time)
        for _ in range(self.service.MAX_LOGIN_FAILURES - 1):
            result = await self.service.verify_admin_login("wrong-password", AsyncMock(), client_identifier)
            self.assertFalse(result["success"])
            self.assertNotIn("status_code", result)

        locked_result = await self.service.verify_admin_login("wrong-password", AsyncMock(), client_identifier)
        self.assertFalse(locked_result["success"])
        self.assertEqual(locked_result["status_code"], 429)
        self.assertIn("retry_after", locked_result)

        still_locked = await self.service.verify_admin_login("correct-password", AsyncMock(), client_identifier)
        self.assertFalse(still_locked["success"])
        self.assertEqual(still_locked["status_code"], 429)

        unlocked_time = base_time + self.service.LOGIN_LOCK_DURATION + timedelta(seconds=1)
        self.service._utcnow = Mock(return_value=unlocked_time)
        success_result = await self.service.verify_admin_login("correct-password", AsyncMock(), client_identifier)
        self.assertTrue(success_result["success"])

        self.service._utcnow = Mock(return_value=unlocked_time + timedelta(seconds=1))
        post_reset_failure = await self.service.verify_admin_login("wrong-password", AsyncMock(), client_identifier)
        self.assertFalse(post_reset_failure["success"])
        self.assertNotIn("status_code", post_reset_failure)


class AuthRouteSessionTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.add_middleware(SessionMiddleware, secret_key="test-secret", https_only=False)
        self.app.include_router(auth.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def test_successful_login_preserves_existing_session_flow(self):
        with patch(
            "app.routes.auth.auth_service.verify_admin_login",
            new=AsyncMock(return_value={"success": True, "message": "登录成功", "error": None}),
        ):
            with TestClient(self.app) as client:
                login_response = client.post("/auth/login", json={"password": "correct-password"})
                status_response = client.get("/auth/status")

        self.assertEqual(login_response.status_code, 200)
        self.assertTrue(status_response.json()["authenticated"])
        self.assertEqual(status_response.json()["user"]["username"], "admin")

    def test_rate_limited_login_returns_retry_after_header(self):
        with patch(
            "app.routes.auth.auth_service.verify_admin_login",
            new=AsyncMock(
                return_value={
                    "success": False,
                    "message": None,
                    "error": "登录尝试过于频繁，请在 30 秒后重试",
                    "status_code": 429,
                    "retry_after": 30,
                }
            ),
        ):
            with TestClient(self.app) as client:
                response = client.post("/auth/login", json={"password": "wrong-password"})

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers.get("retry-after"), "30")
        self.assertEqual(response.json()["detail"], "登录尝试过于频繁，请在 30 秒后重试")


class DatabaseEngineConfigTests(unittest.TestCase):
    def test_sqlite_file_database_uses_conservative_pool_settings(self):
        engine_kwargs = _build_engine_kwargs("sqlite+aiosqlite:///./data/team_manage.db")

        self.assertEqual(engine_kwargs["pool_size"], 5)
        self.assertEqual(engine_kwargs["max_overflow"], 0)
        self.assertTrue(engine_kwargs["pool_pre_ping"])
        self.assertNotIn("poolclass", engine_kwargs)

    def test_sqlite_memory_database_uses_static_pool(self):
        engine_kwargs = _build_engine_kwargs("sqlite+aiosqlite:///:memory:")

        self.assertIs(engine_kwargs["poolclass"], StaticPool)
        self.assertNotIn("pool_size", engine_kwargs)
        self.assertNotIn("max_overflow", engine_kwargs)

    def test_non_sqlite_database_keeps_existing_large_pool_settings(self):
        engine_kwargs = _build_engine_kwargs("postgresql+asyncpg://user:pass@localhost/dbname")

        self.assertEqual(engine_kwargs["pool_size"], 50)
        self.assertEqual(engine_kwargs["max_overflow"], 100)
        self.assertTrue(engine_kwargs["pool_pre_ping"])


class ImportAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.add_middleware(SessionMiddleware, secret_key="test-secret", https_only=False)
        self.app.include_router(auth.router)
        self.app.include_router(admin.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def test_admin_session_can_access_import_endpoint(self):
        with patch(
            "app.routes.auth.auth_service.verify_admin_login",
            new=AsyncMock(return_value={"success": True, "message": "登录成功", "error": None}),
        ), patch(
            "app.routes.admin.team_service.import_team_single",
            new=AsyncMock(return_value={"success": True, "team_id": 1, "message": "ok"}),
        ):
            with TestClient(self.app) as client:
                login_response = client.post("/auth/login", json={"password": "correct-password"})
                self.assertEqual(login_response.status_code, 200)

                import_response = client.post(
                    "/admin/teams/import",
                    json={
                        "import_type": "single",
                        "email": "team@example.com",
                        "access_token": "token-value",
                    },
                )

        self.assertEqual(import_response.status_code, 200)
        self.assertTrue(import_response.json()["success"])

    def test_x_api_key_alone_cannot_access_import_endpoint(self):
        with TestClient(self.app) as client:
            response = client.post(
                "/admin/teams/import",
                headers={"X-API-Key": "import-only-key"},
                json={
                    "import_type": "single",
                    "email": "team@example.com",
                    "access_token": "token-value",
                },
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "Admin session required")


class TeamMaskingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with self.session_factory() as session:
            session.add(
                Team(
                    email="owner@example.com",
                    account_id="acc-1",
                    access_token_encrypted=encryption_service.encrypt_token("access-token-123456"),
                    refresh_token_encrypted=encryption_service.encrypt_token("refresh-token-abcdef"),
                    session_token_encrypted=encryption_service.encrypt_token("session-token-xyz987"),
                    client_id="client-1",
                    team_name="Example Team",
                    status="active",
                    max_members=TEAM_MEMBER_LIMIT,
                )
            )
            await session.commit()

        self.service = TeamService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_get_team_public_by_id_masks_sensitive_tokens(self):
        async with self.session_factory() as session:
            result = await self.service.get_team_public_by_id(1, session)

        self.assertTrue(result["success"])
        team = result["team"]
        self.assertEqual(team["access_token_masked"], mask_secret("access-token-123456"))
        self.assertEqual(team["refresh_token_masked"], mask_secret("refresh-token-abcdef"))
        self.assertEqual(team["session_token_masked"], mask_secret("session-token-xyz987"))
        self.assertNotIn("access_token", team)
        self.assertNotIn("refresh_token", team)
        self.assertNotIn("session_token", team)


class UtcDateTimeCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_legacy_naive_datetimes_are_normalized_with_column_specific_assumptions(self):
        legacy_local_expiry = datetime(2026, 1, 1, 8, 0, 0)
        legacy_upstream_expiry = datetime(2026, 1, 1, 12, 0, 0)

        async with self.session_factory() as session:
            session.add(
                Team(
                    email="owner@example.com",
                    account_id="acc-1",
                    access_token_encrypted=encryption_service.encrypt_token("token"),
                    team_name="Example Team",
                    status="active",
                    expires_at=legacy_upstream_expiry,
                )
            )
            session.add(
                RedemptionCode(
                    code="TIME-CODE",
                    status="unused",
                    expires_at=legacy_local_expiry,
                )
            )
            await session.commit()

        async with self.session_factory() as session:
            team = await session.get(Team, 1)
            code = await session.get(RedemptionCode, 1)

        self.assertEqual(team.expires_at, datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC))
        self.assertEqual(
            code.expires_at,
            parse_datetime(legacy_local_expiry, assume_timezone=get_app_timezone()),
        )
        self.assertEqual(team.expires_at.tzinfo, timezone.utc)
        self.assertEqual(code.expires_at.tzinfo, timezone.utc)


class JwtParserTimeTests(unittest.TestCase):
    def test_decode_token_uses_unsigned_decode(self):
        parser = JWTParser()

        with patch("app.utils.jwt_parser.jwt.decode", return_value={"exp": 1700000000}) as mock_decode:
            payload = parser.decode_token("token")

        self.assertEqual(payload, {"exp": 1700000000})
        self.assertEqual(mock_decode.call_args.args[0], "token")
        self.assertFalse(mock_decode.call_args.kwargs["options"]["verify_signature"])
        self.assertFalse(mock_decode.call_args.kwargs["options"]["verify_exp"])

    def test_expiration_time_is_utc_aware(self):
        parser = JWTParser()
        parser.decode_token = Mock(return_value={"exp": 1700000000})

        exp_time = parser.get_expiration_time("ignored")

        self.assertEqual(exp_time, datetime.fromtimestamp(1700000000, tz=timezone.utc))
        self.assertEqual(exp_time.tzinfo, timezone.utc)


class WarrantyServiceSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _query_rate_limit.clear()
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with self.session_factory() as session:
            team = Team(
                email="owner@example.com",
                account_id="acc-1",
                access_token_encrypted=encryption_service.encrypt_token("token"),
                team_name="Warranty Team",
                status="active",
                current_members=1,
                max_members=TEAM_MEMBER_LIMIT,
            )
            session.add(team)
            await session.flush()

            session.add(
                RedemptionCode(
                    code="WARRANTY-CODE",
                    status="used",
                    has_warranty=True,
                    warranty_days=30,
                    used_by_email="user@example.com",
                    used_team_id=team.id,
                )
            )
            session.add(
                RedemptionRecord(
                    email="user@example.com",
                    code="WARRANTY-CODE",
                    team_id=team.id,
                    account_id=team.account_id,
                    is_warranty_redemption=True,
                )
            )
            await session.commit()

    async def asyncTearDown(self):
        _query_rate_limit.clear()
        await self.engine.dispose()

    async def test_check_warranty_status_does_not_delete_records_when_sync_fails(self):
        service = WarrantyService()
        service.team_service.sync_team_info = AsyncMock(return_value={"success": False, "error": "upstream down"})

        async with self.session_factory() as session:
            result = await service.check_warranty_status(
                session,
                email="user@example.com",
                code="WARRANTY-CODE",
            )
            record_count = await session.scalar(select(func.count(RedemptionRecord.id)))

        self.assertTrue(result["success"])
        self.assertEqual(result["message"], UNABLE_TO_CONFIRM_MESSAGE)
        self.assertFalse(result["can_reuse"])
        self.assertEqual(record_count, 1)

    async def test_validate_warranty_reuse_blocks_when_sync_cannot_confirm_members(self):
        service = WarrantyService()
        service.team_service.sync_team_info = AsyncMock(return_value={"success": True})

        async with self.session_factory() as session:
            result = await service.validate_warranty_reuse(session, "WARRANTY-CODE", "user@example.com")
            record_count = await session.scalar(select(func.count(RedemptionRecord.id)))

        self.assertTrue(result["success"])
        self.assertFalse(result["can_reuse"])
        self.assertEqual(result["reason"], UNABLE_TO_CONFIRM_MESSAGE)
        self.assertEqual(record_count, 1)

    async def test_check_warranty_status_repeated_requests_return_rate_limit_metadata(self):
        service = WarrantyService()
        service.team_service.sync_team_info = AsyncMock(
            return_value={"success": True, "member_emails": ["user@example.com"]}
        )

        async with self.session_factory() as session:
            first_result = await service.check_warranty_status(
                session,
                email="user@example.com",
                code="WARRANTY-CODE",
            )
            second_result = await service.check_warranty_status(
                session,
                email="user@example.com",
                code="WARRANTY-CODE",
            )

        self.assertTrue(first_result["success"])
        self.assertFalse(second_result["success"])
        self.assertEqual(second_result["status_code"], 429)
        self.assertIn("查询过于频繁", second_result["error"])
        self.assertGreaterEqual(second_result["retry_after"], 0)


class WarrantyRouteStatusCodeTests(unittest.TestCase):
    def setUp(self):
        _query_rate_limit.clear()
        self.app = FastAPI()
        self.app.include_router(warranty.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency

    def tearDown(self):
        _query_rate_limit.clear()
        self.app.dependency_overrides.clear()

    def test_rate_limited_warranty_check_returns_429_instead_of_500(self):
        with patch(
            "app.routes.warranty.warranty_service.check_warranty_status",
            new=AsyncMock(
                return_value={
                    "success": False,
                    "error": "查询过于频繁，请 29 秒后再试",
                    "status_code": 429,
                    "retry_after": 29,
                }
            ),
        ):
            with TestClient(self.app) as client:
                response = client.post(
                    "/warranty/check",
                    json={"email": "user@example.com", "code": "WARRANTY-CODE"},
                )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers.get("retry-after"), "29")
        self.assertEqual(response.json()["detail"], "查询过于频繁，请 29 秒后再试")


class TeamImportFailClosedTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_unknown_account_id_is_rejected_without_creating_placeholder_team(self):
        service = TeamService()
        service.jwt_parser.is_token_expired = Mock(return_value=False)
        service.jwt_parser.extract_email = Mock(return_value="owner@example.com")
        service.chatgpt_service.get_account_info = AsyncMock(
            return_value={
                "success": True,
                "accounts": [
                    {
                        "account_id": "real-account",
                        "name": "Real Team",
                        "plan_type": "team",
                        "subscription_plan": "team",
                        "expires_at": "2026-01-01T00:00:00+00:00",
                        "has_active_subscription": True,
                    }
                ],
                "error": None,
            }
        )
        service.chatgpt_service.get_members = AsyncMock()
        service.chatgpt_service.get_invites = AsyncMock()

        async with self.session_factory() as session:
            result = await service.import_team_single(
                access_token="access-token",
                db_session=session,
                email="owner@example.com",
                account_id="missing-account",
            )
            team_count = await session.scalar(select(func.count(Team.id)))

        self.assertFalse(result["success"])
        self.assertIn("not confirmed", result["error"])
        self.assertEqual(team_count, 0)
        service.chatgpt_service.get_members.assert_not_awaited()
        service.chatgpt_service.get_invites.assert_not_awaited()



class RedeemFlowSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = RedeemFlowService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _create_team(
        self,
        session,
        *,
        email,
        account_id,
        team_name,
        current_members=0,
        max_members=TEAM_MEMBER_LIMIT,
        status="active",
    ):
        team = Team(
            email=email,
            account_id=account_id,
            access_token_encrypted=encryption_service.encrypt_token("token"),
            team_name=team_name,
            status=status,
            current_members=current_members,
            max_members=max_members,
        )
        session.add(team)
        await session.flush()
        return team

    async def test_same_code_email_team_existing_record_returns_idempotent_success(self):
        async with self.session_factory() as session:
            team = await self._create_team(
                session,
                email="owner@example.com",
                account_id="acc-1",
                team_name="Idempotent Team",
                current_members=1,
            )
            session.add(
                RedemptionCode(
                    code="USED-CODE",
                    status="used",
                    used_by_email="user@example.com",
                    used_team_id=team.id,
                )
            )
            session.add(
                RedemptionRecord(
                    email="user@example.com",
                    code="USED-CODE",
                    team_id=team.id,
                    account_id=team.account_id,
                )
            )
            await session.commit()
            team_id = team.id

        self.service.team_service.sync_team_info = AsyncMock(
            return_value={"success": True, "member_emails": ["user@example.com"]}
        )
        self.service.chatgpt_service.send_invite = AsyncMock()

        async with self.session_factory() as session:
            result = await self.service.redeem_and_join_team(
                "user@example.com",
                "USED-CODE",
                team_id,
                session,
            )
            code_row = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "USED-CODE")
            )
            record_count = await session.scalar(select(func.count(RedemptionRecord.id)))
            team = await session.get(Team, team_id)

        self.assertTrue(result["success"])
        self.assertEqual(record_count, 1)
        self.assertEqual(code_row.status, "used")
        self.assertEqual(code_row.used_by_email, "user@example.com")
        self.assertEqual(code_row.used_team_id, team_id)
        self.assertEqual(team.current_members, 1)
        self.service.chatgpt_service.send_invite.assert_not_awaited()

    async def test_existing_member_with_new_code_returns_clear_error_without_consuming(self):
        async with self.session_factory() as session:
            team = await self._create_team(
                session,
                email="owner@example.com",
                account_id="acc-1",
                team_name="Existing Member Team",
                current_members=1,
            )
            session.add(RedemptionCode(code="FRESH-CODE", status="unused"))
            await session.commit()
            team_id = team.id

        self.service.team_service.sync_team_info = AsyncMock(
            return_value={"success": True, "member_emails": ["user@example.com"]}
        )
        self.service.chatgpt_service.send_invite = AsyncMock()

        async with self.session_factory() as session:
            result = await self.service.redeem_and_join_team(
                "user@example.com",
                "FRESH-CODE",
                team_id,
                session,
            )
            code_row = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "FRESH-CODE")
            )
            record_count = await session.scalar(select(func.count(RedemptionRecord.id)))
            team = await session.get(Team, team_id)

        self.assertFalse(result["success"])
        self.assertIn("兑换码未消耗", result["error"])
        self.assertEqual(code_row.status, "unused")
        self.assertIsNone(code_row.used_by_email)
        self.assertIsNone(code_row.used_team_id)
        self.assertEqual(record_count, 0)
        self.assertEqual(team.current_members, 1)
        self.service.chatgpt_service.send_invite.assert_not_awaited()

    async def test_auto_select_skips_team_that_already_contains_email(self):
        async with self.session_factory() as session:
            first_team = await self._create_team(
                session,
                email="owner1@example.com",
                account_id="acc-1",
                team_name="First Team",
                current_members=0,
            )
            second_team = await self._create_team(
                session,
                email="owner2@example.com",
                account_id="acc-2",
                team_name="Second Team",
                current_members=1,
            )
            session.add(RedemptionCode(code="AUTO-CODE", status="unused"))
            await session.commit()
            first_team_id = first_team.id
            second_team_id = second_team.id

        second_team_sync_count = 0

        async def fake_sync(team_id, db_session, force_refresh=False):
            nonlocal second_team_sync_count
            team = await db_session.get(Team, team_id)
            if team_id == first_team_id:
                team.current_members = 1
                team.status = "active"
                await db_session.commit()
                return {"success": True, "member_emails": ["user@example.com"]}
            if team_id == second_team_id:
                second_team_sync_count += 1
                team.status = "active"
                team.current_members = 1 if second_team_sync_count == 1 else 2
                await db_session.commit()
                member_emails = [] if second_team_sync_count == 1 else ["user@example.com"]
                return {"success": True, "member_emails": member_emails}
            raise AssertionError(f"unexpected team_id: {team_id}")

        self.service.team_service.sync_team_info = AsyncMock(side_effect=fake_sync)
        self.service.team_service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.send_invite = AsyncMock(
            return_value={
                "success": True,
                "data": {"account_invites": [{"email_address": "user@example.com"}]},
            }
        )

        async with self.session_factory() as session:
            result = await self.service.redeem_and_join_team(
                "user@example.com",
                "AUTO-CODE",
                None,
                session,
            )
            await asyncio.sleep(0)
            code_row = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "AUTO-CODE")
            )
            record = await session.scalar(
                select(RedemptionRecord).where(RedemptionRecord.code == "AUTO-CODE")
            )
            first_team = await session.get(Team, first_team_id)
            second_team = await session.get(Team, second_team_id)

        self.assertTrue(result["success"])
        self.assertEqual(code_row.used_team_id, second_team_id)
        self.assertEqual(record.team_id, second_team_id)
        self.assertEqual(first_team.current_members, 1)
        self.assertEqual(second_team.current_members, 2)
        self.service.chatgpt_service.send_invite.assert_awaited_once()

    async def test_confirmation_failure_keeps_pending_reservation_instead_of_losing_state(self):
        async with self.session_factory() as session:
            team = await self._create_team(
                session,
                email="owner@example.com",
                account_id="acc-1",
                team_name="Confirm Team",
                current_members=0,
            )
            session.add(RedemptionCode(code="CONFIRM-CODE", status="unused"))
            await session.commit()
            team_id = team.id

        async def fake_sync(team_id_value, db_session, force_refresh=False):
            team = await db_session.get(Team, team_id_value)
            team.current_members = 0
            team.status = "active"
            await db_session.commit()
            return {"success": True, "member_emails": []}

        self.service.team_service.sync_team_info = AsyncMock(side_effect=fake_sync)
        self.service.team_service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.send_invite = AsyncMock(
            return_value={
                "success": True,
                "data": {"account_invites": [{"email_address": "user@example.com"}]},
            }
        )

        with patch("app.services.redeem_flow.asyncio.sleep", new=AsyncMock()):
            async with self.session_factory() as session:
                result = await self.service.redeem_and_join_team(
                    "user@example.com",
                    "CONFIRM-CODE",
                    team_id,
                    session,
                )
                code_row = await session.scalar(
                    select(RedemptionCode).where(RedemptionCode.code == "CONFIRM-CODE")
                )
                record_count = await session.scalar(select(func.count(RedemptionRecord.id)))
                team = await session.get(Team, team_id)

        self.assertTrue(result["success"])
        self.assertTrue(result["pending"])
        self.assertEqual(code_row.status, PENDING_REDEMPTION_STATUS)
        self.assertEqual(code_row.used_by_email, "user@example.com")
        self.assertEqual(code_row.used_team_id, team_id)
        self.assertEqual(record_count, 0)
        self.assertEqual(team.current_members, 0)
        self.service.chatgpt_service.send_invite.assert_awaited_once()

    async def test_pending_redemption_can_be_confirmed_by_retry_without_resending_invite(self):
        async with self.session_factory() as session:
            team = await self._create_team(
                session,
                email="owner@example.com",
                account_id="acc-1",
                team_name="Retry Team",
                current_members=0,
            )
            session.add(RedemptionCode(code="PENDING-RETRY-CODE", status="unused"))
            await session.commit()
            team_id = team.id

        sync_call_count = 0

        async def fake_sync(team_id_value, db_session, force_refresh=False):
            nonlocal sync_call_count
            sync_call_count += 1
            team = await db_session.get(Team, team_id_value)
            if sync_call_count <= 4:
                team.current_members = 0
                team.status = "active"
                await db_session.commit()
                return {"success": True, "member_emails": []}

            team.current_members = 1
            team.status = "active"
            await db_session.commit()
            return {
                "success": True,
                "member_emails": ["user@example.com"],
                "invited_emails": ["user@example.com"],
            }

        self.service.team_service.sync_team_info = AsyncMock(side_effect=fake_sync)
        self.service.team_service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.send_invite = AsyncMock(
            return_value={
                "success": True,
                "data": {"account_invites": [{"email_address": "user@example.com"}]},
            }
        )

        with patch("app.services.redeem_flow.asyncio.sleep", new=AsyncMock()):
            async with self.session_factory() as session:
                first_result = await self.service.redeem_and_join_team(
                    "user@example.com",
                    "PENDING-RETRY-CODE",
                    team_id,
                    session,
                )
                second_result = await self.service.redeem_and_join_team(
                    "user@example.com",
                    "PENDING-RETRY-CODE",
                    None,
                    session,
                )
                code_row = await session.scalar(
                    select(RedemptionCode).where(RedemptionCode.code == "PENDING-RETRY-CODE")
                )
                record_count = await session.scalar(select(func.count(RedemptionRecord.id)))

        self.assertTrue(first_result["success"])
        self.assertTrue(first_result["pending"])
        self.assertTrue(second_result["success"])
        self.assertFalse(second_result["pending"])
        self.assertEqual(code_row.status, "used")
        self.assertEqual(code_row.used_team_id, team_id)
        self.assertEqual(record_count, 1)
        self.service.chatgpt_service.send_invite.assert_awaited_once()


class RedeemRouteConflictTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(redeem.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def test_confirm_redeem_returns_conflict_for_existing_member(self):
        with patch(
            "app.routes.redeem.redeem_flow_service.redeem_and_join_team",
            new=AsyncMock(
                return_value={
                    "success": False,
                    "error": "邮箱 user@example.com 已在 Team Demo 中，本次兑换未新增成员，兑换码未消耗",
                }
            ),
        ):
            with TestClient(self.app) as client:
                response = client.post(
                    "/redeem/confirm",
                    json={"email": "user@example.com", "code": "ABC-123", "team_id": 1},
                )

        self.assertEqual(response.status_code, 409)
        self.assertIn("兑换码未消耗", response.json()["detail"])


class TeamStatusOverrideTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = TeamService()

        async with self.session_factory() as session:
            team = Team(
                email="owner@example.com",
                account_id="acc-1",
                access_token_encrypted=encryption_service.encrypt_token("token"),
                team_name="Status Team",
                status="active",
                current_members=TEAM_MEMBER_LIMIT,
                max_members=TEAM_MEMBER_LIMIT,
            )
            session.add(team)
            await session.commit()
            self.team_id = team.id

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_update_team_preserves_explicit_status_and_recomputes_when_omitted(self):
        async with self.session_factory() as session:
            result = await self.service.update_team(self.team_id, session, status="active")
            team = await session.get(Team, self.team_id)

        self.assertTrue(result["success"])
        self.assertEqual(team.status, "active")

        async with self.session_factory() as session:
            result = await self.service.update_team(self.team_id, session)
            team = await session.get(Team, self.team_id)

        self.assertTrue(result["success"])
        self.assertEqual(team.status, "full")

class SettingsSecurityTests(unittest.TestCase):
    def test_production_rejects_default_secret_key(self):
        config = Settings(
            environment="production",
            secret_key=DEFAULT_SECRET_KEY,
            admin_password="strong-password",
        )
        with self.assertRaises(ValueError):
            config.validate_security()

    def test_production_rejects_default_admin_password(self):
        config = Settings(
            environment="production",
            secret_key="very-secret-value",
            admin_password=DEFAULT_ADMIN_PASSWORD,
        )
        with self.assertRaises(ValueError):
            config.validate_security()


class ChatGPTErrorPropagationTests(unittest.IsolatedAsyncioTestCase):
    async def test_make_request_prefers_nested_error_message_for_account_info_errors(self):
        service = ChatGPTService()
        response = Mock(
            status_code=401,
            text='{"error":{"message":"Token revoked","code":"token_invalidated"}}',
        )
        response.json = Mock(
            return_value={"error": {"message": "Token revoked", "code": "token_invalidated"}}
        )
        session = Mock(get=AsyncMock(return_value=response))

        service._get_session = AsyncMock(return_value=session)
        service.clear_session = AsyncMock()

        result = await service.get_account_info(
            "access-token",
            AsyncMock(),
            identifier="owner@example.com",
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "Token revoked")
        self.assertEqual(result["error_code"], "token_invalidated")
        service.clear_session.assert_awaited_once_with("owner@example.com")

    async def test_wrappers_preserve_error_code_from_make_request(self):
        service = ChatGPTService()
        service._make_request = AsyncMock(
            return_value={
                "success": False,
                "error": "Token revoked",
                "error_code": "token_invalidated",
            }
        )

        calls = [
            ("account_info", service.get_account_info, ("access-token", AsyncMock()), {"identifier": "owner@example.com"}),
            ("members", service.get_members, ("access-token", "acc-1", AsyncMock()), {"identifier": "owner@example.com"}),
            ("invites", service.get_invites, ("access-token", "acc-1", AsyncMock()), {"identifier": "owner@example.com"}),
        ]

        for name, method, args, kwargs in calls:
            with self.subTest(method=name):
                result = await method(*args, **kwargs)
                self.assertFalse(result["success"])
                self.assertEqual(result["error"], "Token revoked")
                self.assertEqual(result["error_code"], "token_invalidated")


class ChatGPTTlsTests(unittest.IsolatedAsyncioTestCase):
    async def test_chatgpt_session_uses_configured_tls_verification(self):
        service = ChatGPTService()

        with tempfile.NamedTemporaryFile(suffix=".pem", delete=False) as pem_file:
            pem_file.write(b"dummy")
            pem_path = Path(pem_file.name)

        try:
            with patch("app.services.chatgpt.settings.tls_ca_bundle", str(pem_path)), patch(
                "app.services.chatgpt.settings_service.get_proxy_config",
                new=AsyncMock(return_value={"enabled": False, "proxy": ""}),
            ), patch("app.services.chatgpt.AsyncSession") as mock_session:
                await service._create_session(AsyncMock())

            self.assertEqual(mock_session.call_args.kwargs["verify"], str(pem_path))
        finally:
            pem_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()




