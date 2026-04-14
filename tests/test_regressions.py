import logging
import unittest
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.middleware.sessions import SessionMiddleware

from app.config import DEFAULT_ADMIN_PASSWORD, DEFAULT_SECRET_KEY, Settings
from app.database import Base, get_db
from app.models import TEAM_MEMBER_LIMIT, RedemptionCode, RedemptionRecord, Setting, Team
from app.routes import admin, api, redeem, user, warranty
from app.services.encryption import encryption_service
from app.services.redeem_flow import CODE_CONFLICT_ERROR, RedeemFlowService
from app.services.redemption import RedemptionService
from app.services.team import TeamService
from app.services.warranty import WarrantyService, _query_rate_limit
from app.utils.token_parser import TokenParser
from app.utils.time_utils import get_now


async def fake_db_dependency():
    yield object()


def fake_admin_dependency():
    return {"username": "admin", "is_admin": True}


class WithdrawRecordRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = RedemptionService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _create_team(self, session, email: str, account_id: str, team_name: str) -> Team:
        team = Team(
            email=email,
            account_id=account_id,
            access_token_encrypted=encryption_service.encrypt_token("token"),
            team_name=team_name,
            status="active",
            max_members=TEAM_MEMBER_LIMIT,
        )
        session.add(team)
        await session.flush()
        return team

    async def test_withdraw_record_restores_latest_remaining_usage_instead_of_unused(self):
        older_time = datetime(2026, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
        newer_time = datetime(2026, 2, 20, 1, 30, 0, tzinfo=timezone.utc)

        async with self.session_factory() as session:
            older_team = await self._create_team(session, "owner1@example.com", "acc-1", "Older Team")
            newer_team = await self._create_team(session, "owner2@example.com", "acc-2", "Newer Team")

            code = RedemptionCode(
                code="WARRANTY-RESTORE",
                status="used",
                has_warranty=True,
                warranty_days=14,
                used_by_email="newer@example.com",
                used_team_id=newer_team.id,
                used_at=newer_time,
                warranty_expires_at=newer_time + timedelta(days=14),
            )
            session.add(code)
            await session.flush()

            older_record = RedemptionRecord(
                email="older@example.com",
                code=code.code,
                team_id=older_team.id,
                account_id=older_team.account_id,
                redeemed_at=older_time,
                is_warranty_redemption=True,
            )
            newer_record = RedemptionRecord(
                email="newer@example.com",
                code=code.code,
                team_id=newer_team.id,
                account_id=newer_team.account_id,
                redeemed_at=newer_time,
                is_warranty_redemption=True,
            )
            session.add_all([older_record, newer_record])
            await session.commit()
            newer_record_id = newer_record.id

        with patch(
            "app.services.team.team_service.remove_invite_or_member",
            new=AsyncMock(return_value={"success": True}),
        ):
            async with self.session_factory() as session:
                result = await self.service.withdraw_record(newer_record_id, session)
                code_row = await session.scalar(
                    select(RedemptionCode).where(RedemptionCode.code == "WARRANTY-RESTORE")
                )
                records = (
                    await session.execute(
                        select(RedemptionRecord)
                        .where(RedemptionRecord.code == "WARRANTY-RESTORE")
                        .order_by(RedemptionRecord.redeemed_at.asc())
                    )
                ).scalars().all()

        self.assertTrue(result["success"])
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].email, "older@example.com")
        self.assertEqual(code_row.status, "warranty_active")
        self.assertEqual(code_row.used_by_email, "older@example.com")
        self.assertEqual(code_row.used_team_id, records[0].team_id)
        self.assertEqual(code_row.used_at, older_time)
        self.assertEqual(code_row.warranty_expires_at, older_time + timedelta(days=14))


class WarrantyExpiryConsistencyRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _query_rate_limit.clear()
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.redemption_service = RedemptionService()
        self.warranty_service = WarrantyService()

    async def asyncTearDown(self):
        _query_rate_limit.clear()
        await self.engine.dispose()

    async def _create_team(
        self,
        session,
        *,
        email: str,
        account_id: str,
        team_name: str,
        status: str,
        expires_at=None,
    ) -> Team:
        team = Team(
            email=email,
            account_id=account_id,
            access_token_encrypted=encryption_service.encrypt_token("token"),
            team_name=team_name,
            status=status,
            max_members=TEAM_MEMBER_LIMIT,
            expires_at=expires_at,
        )
        session.add(team)
        await session.flush()
        return team

    async def test_bulk_update_recomputes_used_code_warranty_and_reuse_matches_status(self):
        used_at = get_now() - timedelta(days=10)

        async with self.session_factory() as session:
            team = await self._create_team(
                session,
                email="banned-owner@example.com",
                account_id="acc-banned",
                team_name="Banned Team",
                status="banned",
            )
            session.add(
                RedemptionCode(
                    code="WARRANTY-EDIT-CONSISTENT",
                    status="used",
                    has_warranty=True,
                    warranty_days=7,
                    used_by_email="user@example.com",
                    used_team_id=team.id,
                    used_at=used_at,
                    warranty_expires_at=used_at + timedelta(days=7),
                )
            )
            await session.flush()
            session.add(
                RedemptionRecord(
                    email="user@example.com",
                    code="WARRANTY-EDIT-CONSISTENT",
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=used_at,
                    is_warranty_redemption=True,
                )
            )
            await session.commit()

        async with self.session_factory() as session:
            update_result = await self.redemption_service.bulk_update_codes(
                ["WARRANTY-EDIT-CONSISTENT"],
                session,
                warranty_days=20,
            )
            code_row = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "WARRANTY-EDIT-CONSISTENT")
            )
            warranty_status = await self.warranty_service.check_warranty_status(
                session,
                email="user@example.com",
                code="WARRANTY-EDIT-CONSISTENT",
            )
            reuse_validation = await self.warranty_service.validate_warranty_reuse(
                session,
                "WARRANTY-EDIT-CONSISTENT",
                "user@example.com",
            )

        self.assertTrue(update_result["success"])
        self.assertEqual(code_row.warranty_days, 20)
        self.assertEqual(code_row.warranty_expires_at, used_at + timedelta(days=20))
        self.assertTrue(warranty_status["success"])
        self.assertTrue(warranty_status["warranty_valid"])
        self.assertTrue(warranty_status["can_reuse"])
        self.assertTrue(reuse_validation["success"])
        self.assertTrue(reuse_validation["can_reuse"])

    async def test_check_warranty_status_matches_reuse_validation_when_banned_and_active_records_coexist(self):
        banned_time = get_now() - timedelta(days=8)
        active_time = get_now() - timedelta(days=2)
        team_expires_at = get_now() + timedelta(days=20)

        async with self.session_factory() as session:
            banned_team = await self._create_team(
                session,
                email="banned-owner@example.com",
                account_id="acc-banned",
                team_name="Banned Team",
                status="banned",
            )
            active_team = await self._create_team(
                session,
                email="active-owner@example.com",
                account_id="acc-active",
                team_name="Active Team",
                status="active",
                expires_at=team_expires_at,
            )
            session.add(
                RedemptionCode(
                    code="WARRANTY-BANNED-ACTIVE",
                    status="used",
                    has_warranty=True,
                    warranty_days=30,
                    used_by_email="user@example.com",
                    used_team_id=active_team.id,
                    used_at=active_time,
                    warranty_expires_at=active_time + timedelta(days=30),
                )
            )
            await session.flush()
            session.add_all(
                [
                    RedemptionRecord(
                        email="user@example.com",
                        code="WARRANTY-BANNED-ACTIVE",
                        team_id=banned_team.id,
                        account_id=banned_team.account_id,
                        redeemed_at=banned_time,
                        is_warranty_redemption=True,
                    ),
                    RedemptionRecord(
                        email="user@example.com",
                        code="WARRANTY-BANNED-ACTIVE",
                        team_id=active_team.id,
                        account_id=active_team.account_id,
                        redeemed_at=active_time,
                        is_warranty_redemption=True,
                    ),
                ]
            )
            await session.commit()

        self.warranty_service.team_service.sync_team_info = AsyncMock(
            return_value={"success": True, "member_emails": ["user@example.com"]}
        )

        async with self.session_factory() as session:
            warranty_status = await self.warranty_service.check_warranty_status(
                session,
                email="user@example.com",
                code="WARRANTY-BANNED-ACTIVE",
            )
            reuse_validation = await self.warranty_service.validate_warranty_reuse(
                session,
                "WARRANTY-BANNED-ACTIVE",
                "user@example.com",
            )

        self.assertTrue(warranty_status["success"])
        self.assertTrue(warranty_status["warranty_valid"])
        self.assertEqual(len(warranty_status["banned_teams"]), 1)
        self.assertFalse(warranty_status["can_reuse"])
        self.assertTrue(reuse_validation["success"])
        self.assertFalse(reuse_validation["can_reuse"])
        self.assertIn("有效 Team", reuse_validation["reason"])


class WarrantyReuseFlowRegressionTests(unittest.IsolatedAsyncioTestCase):
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
        email: str,
        account_id: str,
        team_name: str,
        status: str = "active",
    ) -> Team:
        team = Team(
            email=email,
            account_id=account_id,
            access_token_encrypted=encryption_service.encrypt_token("token"),
            team_name=team_name,
            status=status,
            max_members=TEAM_MEMBER_LIMIT,
        )
        session.add(team)
        await session.flush()
        return team

    async def test_valid_warranty_reuse_succeeds_and_preserves_original_warranty_window(self):
        first_redeemed_at = get_now() - timedelta(days=5)
        first_expiry = first_redeemed_at + timedelta(days=30)

        async with self.session_factory() as session:
            banned_team = await self._create_team(
                session,
                email="banned-owner@example.com",
                account_id="acc-banned",
                team_name="Banned Team",
                status="banned",
            )
            replacement_team = await self._create_team(
                session,
                email="active-owner@example.com",
                account_id="acc-active",
                team_name="Replacement Team",
                status="active",
            )
            session.add(
                RedemptionCode(
                    code="WARRANTY-REUSE-SUCCESS",
                    status="used",
                    has_warranty=True,
                    warranty_days=30,
                    used_by_email="Owner@Example.com",
                    used_team_id=banned_team.id,
                    used_at=first_redeemed_at,
                    warranty_expires_at=first_expiry,
                )
            )
            await session.flush()
            session.add(
                RedemptionRecord(
                    email="Owner@Example.com",
                    code="WARRANTY-REUSE-SUCCESS",
                    team_id=banned_team.id,
                    account_id=banned_team.account_id,
                    redeemed_at=first_redeemed_at,
                    is_warranty_redemption=True,
                )
            )
            await session.commit()
            replacement_team_id = replacement_team.id

        sync_results = [
            {"success": True, "member_emails": []},
            {
                "success": True,
                "member_emails": ["owner@example.com"],
                "invited_emails": ["owner@example.com"],
            },
        ]

        self.service.team_service.sync_team_info = AsyncMock(side_effect=sync_results)
        self.service.team_service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.send_invite = AsyncMock(
            return_value={"success": True, "data": {"account_invites": [{"id": "invite-1"}]}}
        )

        async with self.session_factory() as session:
            result = await self.service.redeem_and_join_team(
                "owner@example.com",
                "WARRANTY-REUSE-SUCCESS",
                replacement_team_id,
                session,
            )
            code_row = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "WARRANTY-REUSE-SUCCESS")
            )
            records = (
                await session.execute(
                    select(RedemptionRecord)
                    .where(RedemptionRecord.code == "WARRANTY-REUSE-SUCCESS")
                    .order_by(RedemptionRecord.redeemed_at.asc(), RedemptionRecord.id.asc())
                )
            ).scalars().all()

        self.assertTrue(result["success"])
        self.assertFalse(result["pending"])
        self.assertEqual(code_row.status, "warranty_active")
        self.assertEqual(code_row.used_by_email, "owner@example.com")
        self.assertEqual(code_row.used_team_id, replacement_team_id)
        self.assertEqual(code_row.used_at, first_redeemed_at)
        self.assertEqual(code_row.warranty_expires_at, first_expiry)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[-1].team_id, replacement_team_id)

    async def test_warranty_reuse_rejects_non_owner_email(self):
        first_redeemed_at = get_now() - timedelta(days=5)

        async with self.session_factory() as session:
            banned_team = await self._create_team(
                session,
                email="banned-owner@example.com",
                account_id="acc-banned",
                team_name="Banned Team",
                status="banned",
            )
            replacement_team = await self._create_team(
                session,
                email="active-owner@example.com",
                account_id="acc-active",
                team_name="Replacement Team",
                status="active",
            )
            session.add(
                RedemptionCode(
                    code="WARRANTY-OWNER-BLOCKED",
                    status="used",
                    has_warranty=True,
                    warranty_days=30,
                    used_by_email="owner@example.com",
                    used_team_id=banned_team.id,
                    used_at=first_redeemed_at,
                    warranty_expires_at=first_redeemed_at + timedelta(days=30),
                )
            )
            await session.flush()
            session.add(
                RedemptionRecord(
                    email="owner@example.com",
                    code="WARRANTY-OWNER-BLOCKED",
                    team_id=banned_team.id,
                    account_id=banned_team.account_id,
                    redeemed_at=first_redeemed_at,
                    is_warranty_redemption=True,
                )
            )
            await session.commit()
            replacement_team_id = replacement_team.id

        self.service.team_service.sync_team_info = AsyncMock(return_value={"success": True, "member_emails": []})
        self.service.team_service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.send_invite = AsyncMock()

        async with self.session_factory() as session:
            result = await self.service.redeem_and_join_team(
                "stranger@example.com",
                "WARRANTY-OWNER-BLOCKED",
                replacement_team_id,
                session,
            )
            code_row = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "WARRANTY-OWNER-BLOCKED")
            )
            record_count = await session.scalar(
                select(func.count(RedemptionRecord.id)).where(
                    RedemptionRecord.code == "WARRANTY-OWNER-BLOCKED"
                )
            )

        self.assertFalse(result["success"])
        self.assertIn("原兑换邮箱", result["error"])
        self.assertEqual(code_row.status, "used")
        self.assertEqual(code_row.used_by_email, "owner@example.com")
        self.assertEqual(code_row.used_team_id, banned_team.id)
        self.assertEqual(record_count, 1)
        self.service.chatgpt_service.send_invite.assert_not_awaited()


class WarrantyRouteEmailNormalizationRegressionTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(warranty.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def test_route_keeps_matching_records_when_only_local_part_case_differs(self):
        service_result = {
            "success": True,
            "has_warranty": True,
            "warranty_valid": True,
            "warranty_expires_at": "2026-05-01T00:00:00+00:00",
            "banned_teams": [{"team_name": "Demo Team", "banned_at": None}],
            "can_reuse": True,
            "original_code": "CASE-CODE",
            "records": [
                {
                    "code": "CASE-CODE",
                    "has_warranty": True,
                    "warranty_valid": True,
                    "warranty_expires_at": "2026-05-01T00:00:00+00:00",
                    "status": "warranty_active",
                    "used_at": "2026-04-01T00:00:00+00:00",
                    "team_name": "Demo Team",
                    "team_status": "banned",
                    "team_expires_at": None,
                    "email": "Owner@Example.com",
                }
            ],
            "message": "查询成功",
        }

        with patch(
            "app.routes.warranty.warranty_service.check_warranty_status",
            new=AsyncMock(return_value=service_result),
        ):
            with TestClient(self.app) as client:
                response = client.post(
                    "/warranty/check",
                    json={"email": "owner@example.com", "code": "CASE-CODE"},
                )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["success"])
        self.assertEqual(len(body["records"]), 1)
        self.assertTrue(body["can_reuse"])
        self.assertEqual(body["original_code"], "CASE-CODE")


class RedeemFrontendRegressionTests(unittest.TestCase):
    def test_one_click_replace_button_uses_backend_can_reuse_gate(self):
        script = Path("app/static/js/redeem.js").read_text(encoding="utf-8")
        self.assertIn(
            "data.can_reuse && data.original_code === record.code",
            script,
        )


class GetAllRecordsRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = RedemptionService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_get_all_records_returns_results_instead_of_failing_on_stale_record_reference(self):
        redeemed_at = datetime(2026, 3, 5, 12, 0, 0, tzinfo=timezone.utc)

        async with self.session_factory() as session:
            team = Team(
                email="owner@example.com",
                account_id="acc-1",
                access_token_encrypted=encryption_service.encrypt_token("token"),
                team_name="Records Team",
                status="active",
                max_members=TEAM_MEMBER_LIMIT,
            )
            code = RedemptionCode(code="RECORDS-CODE", status="used")
            session.add_all([team, code])
            await session.flush()

            session.add(
                RedemptionRecord(
                    email="user@example.com",
                    code=code.code,
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=redeemed_at,
                )
            )
            await session.commit()

        async with self.session_factory() as session:
            result = await self.service.get_all_records(session, email="user@example.com")

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["records"][0]["email"], "user@example.com")
        self.assertEqual(result["records"][0]["code"], "RECORDS-CODE")


class TeamDeletionHistoryRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = TeamService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _create_team(self, session, email: str, account_id: str, team_name: str) -> Team:
        team = Team(
            email=email,
            account_id=account_id,
            access_token_encrypted=encryption_service.encrypt_token("token"),
            team_name=team_name,
            status="active",
            max_members=TEAM_MEMBER_LIMIT,
        )
        session.add(team)
        await session.flush()
        return team

    async def test_delete_team_rejects_when_redemption_history_exists(self):
        redeemed_at = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)

        async with self.session_factory() as session:
            team = await self._create_team(session, "owner@example.com", "acc-1", "Protected Team")
            session.add(
                RedemptionCode(
                    code="DELETE-BLOCKED",
                    status="used",
                    has_warranty=True,
                    warranty_days=30,
                    used_by_email="user@example.com",
                    used_team_id=team.id,
                    used_at=redeemed_at,
                    warranty_expires_at=redeemed_at + timedelta(days=30),
                )
            )
            await session.flush()
            session.add(
                RedemptionRecord(
                    email="user@example.com",
                    code="DELETE-BLOCKED",
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=redeemed_at,
                    is_warranty_redemption=True,
                )
            )
            await session.commit()
            team_id = team.id

        async with self.session_factory() as session:
            result = await self.service.delete_team(team_id, session)
            team_row = await session.get(Team, team_id)
            code_row = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "DELETE-BLOCKED")
            )
            record_count = await session.scalar(
                select(func.count(RedemptionRecord.id)).where(RedemptionRecord.team_id == team_id)
            )

        self.assertFalse(result["success"])
        self.assertIsNone(result["message"])
        self.assertEqual(
            result["error"],
            "该 Team 存在历史兑换记录，无法删除，否则会影响质保查询和复兑判断。",
        )
        self.assertIsNotNone(team_row)
        self.assertEqual(record_count, 1)
        self.assertEqual(code_row.used_team_id, team_id)

    async def test_delete_team_still_succeeds_without_redemption_history(self):
        async with self.session_factory() as session:
            team = await self._create_team(session, "owner2@example.com", "acc-2", "Disposable Team")
            await session.commit()
            team_id = team.id

        async with self.session_factory() as session:
            result = await self.service.delete_team(team_id, session)
            team_row = await session.get(Team, team_id)
            record_count = await session.scalar(
                select(func.count(RedemptionRecord.id)).where(RedemptionRecord.team_id == team_id)
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["message"], "Team deleted successfully.")
        self.assertIsNone(result["error"])
        self.assertIsNone(team_row)
        self.assertEqual(record_count, 0)


class TeamStatusPersistenceRegressionTests(unittest.IsolatedAsyncioTestCase):
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
                access_token_encrypted=encryption_service.encrypt_token("expired-access-token"),
                session_token_encrypted=encryption_service.encrypt_token("session-token"),
                team_name="Persisted Status Team",
                status="active",
                max_members=TEAM_MEMBER_LIMIT,
            )
            session.add(team)
            await session.commit()
            self.team_id = team.id

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_sync_team_info_persists_status_changes_after_token_refresh_failure(self):
        self.service.jwt_parser.is_token_expired = Mock(return_value=True)
        self.service.chatgpt_service.refresh_access_token_with_session_token = AsyncMock(
            return_value={
                "success": False,
                "error": "The token has been invalidated",
                "error_code": "token_invalidated",
            }
        )

        async with self.session_factory() as session:
            result = await self.service.sync_team_info(self.team_id, session)

        async with self.session_factory() as session:
            team = await session.get(Team, self.team_id)

        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "Token 已失效")
        self.assertEqual(team.status, "banned")


class BatchActionResponseRegressionTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.mount("/static", StaticFiles(directory="app/static"), name="static")
        self.app.add_middleware(SessionMiddleware, secret_key="test-secret", https_only=False)
        self.app.include_router(admin.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency
        self.app.dependency_overrides[admin.require_admin] = fake_admin_dependency
        self.endpoint_specs = [
            ("/admin/teams/batch-refresh", "app.routes.admin.team_service.sync_team_info"),
            ("/admin/teams/batch-delete", "app.routes.admin.team_service.delete_team"),
            (
                "/admin/teams/batch-enable-device-auth",
                "app.routes.admin.team_service.enable_device_code_auth",
            ),
        ]

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def _exercise_endpoint(self, endpoint: str, patch_target: str, success_ids: set[int]):
        async def side_effect(team_id, *_args, **_kwargs):
            return {"success": team_id in success_ids}

        with patch(patch_target, new=AsyncMock(side_effect=side_effect)):
            with TestClient(self.app) as client:
                response = client.post(endpoint, json={"ids": [1, 2]})
        return response

    def test_batch_endpoints_report_partial_success(self):
        for endpoint, patch_target in self.endpoint_specs:
            with self.subTest(endpoint=endpoint):
                response = self._exercise_endpoint(endpoint, patch_target, {1})
                body = response.json()
                self.assertEqual(response.status_code, 200)
                self.assertTrue(body["success"])
                self.assertTrue(body["partial"])
                self.assertEqual(body["success_count"], 1)
                self.assertEqual(body["failed_count"], 1)

    def test_batch_endpoints_report_total_failure_without_500(self):
        for endpoint, patch_target in self.endpoint_specs:
            with self.subTest(endpoint=endpoint):
                response = self._exercise_endpoint(endpoint, patch_target, set())
                body = response.json()
                self.assertEqual(response.status_code, 200)
                self.assertFalse(body["success"])
                self.assertFalse(body["partial"])
                self.assertEqual(body["success_count"], 0)
                self.assertEqual(body["failed_count"], 2)

    def test_batch_endpoints_treat_per_item_exceptions_as_failures_without_aborting(self):
        async def side_effect(team_id, *_args, **_kwargs):
            if team_id == 2:
                raise RuntimeError("boom")
            return {"success": True}

        for endpoint, patch_target in self.endpoint_specs:
            with self.subTest(endpoint=endpoint):
                with patch(patch_target, new=AsyncMock(side_effect=side_effect)):
                    with TestClient(self.app) as client:
                        response = client.post(endpoint, json={"ids": [1, 2]})

                body = response.json()
                self.assertEqual(response.status_code, 200)
                self.assertTrue(body["success"])
                self.assertTrue(body["partial"])
                self.assertEqual(body["success_count"], 1)
                self.assertEqual(body["failed_count"], 1)

    def test_batch_endpoints_report_full_success(self):
        for endpoint, patch_target in self.endpoint_specs:
            with self.subTest(endpoint=endpoint):
                response = self._exercise_endpoint(endpoint, patch_target, {1, 2})
                body = response.json()
                self.assertEqual(response.status_code, 200)
                self.assertTrue(body["success"])
                self.assertFalse(body["partial"])
                self.assertEqual(body["success_count"], 2)
                self.assertEqual(body["failed_count"], 0)


class AdminPageRegressionTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.mount("/static", StaticFiles(directory="app/static"), name="static")
        self.app.add_middleware(SessionMiddleware, secret_key="test-secret", https_only=False)
        self.app.include_router(admin.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency
        self.app.dependency_overrides[admin.require_admin] = fake_admin_dependency

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def test_dashboard_pagination_preserves_status_filter(self):
        team_row = {
            "id": 1,
            "email": "owner@example.com",
            "account_role": None,
            "account_id": "acc-1",
            "team_name": "Alpha Team",
            "current_members": 1,
            "max_members": TEAM_MEMBER_LIMIT,
            "subscription_plan": "Plus",
            "expires_at": None,
            "device_code_auth_enabled": False,
            "status": "active",
        }

        with patch(
            "app.routes.admin.team_service.get_all_teams",
            new=AsyncMock(
                return_value={"teams": [team_row], "current_page": 2, "total_pages": 3, "total": 41}
            ),
        ), patch(
            "app.routes.admin.team_service.get_stats",
            new=AsyncMock(return_value={"total": 1, "available": 1}),
        ), patch(
            "app.routes.admin.redemption_service.get_stats",
            new=AsyncMock(return_value={"total": 0, "used": 0}),
        ):
            with TestClient(self.app) as client:
                response = client.get("/admin?status=active&page=2")

        self.assertEqual(response.status_code, 200)
        self.assertIn('href="?page=1&amp;status=active"', response.text)
        self.assertIn('href="?page=3&amp;status=active"', response.text)

    def test_export_codes_forwards_status_filter_to_service(self):
        with patch(
            "app.routes.admin.redemption_service.get_all_codes",
            new=AsyncMock(return_value={"codes": []}),
        ) as mock_get_all_codes:
            with TestClient(self.app) as client:
                response = client.get("/admin/codes/export?search=test&status_filter=expired")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            mock_get_all_codes.await_args.kwargs,
            {
                "page": 1,
                "per_page": 100000,
                "search": "test",
                "status": "expired",
            },
        )

    def test_refresh_team_failure_branch_also_reloads_page(self):
        template_source = Path("app/templates/admin/index.html").read_text(encoding="utf-8")
        pattern = re.compile(
            r"async function refreshTeam\(teamId\).*?"
            r"else \{\s*showToast\(data\.error \|\| '刷新失败', 'error'\);\s*"
            r"setTimeout\(\(\) => location\.reload\(\), 1000\);\s*\}",
            re.S,
        )

        self.assertRegex(template_source, pattern)

    def test_settings_page_keeps_supported_sections_and_removes_webhook_block(self):
        with patch(
            "app.services.settings.settings_service.get_proxy_config",
            new=AsyncMock(return_value={"enabled": True, "proxy": "http://127.0.0.1:7890"}),
        ), patch(
            "app.services.settings.settings_service.get_log_level",
            new=AsyncMock(return_value="INFO"),
        ), patch(
            "app.services.settings.settings_service.get_setting",
            new=AsyncMock(
                side_effect=lambda _db, key, default=None: {
                    "homepage_announcement": "公告内容",
                    "homepage_usage_notice": "须知内容",
                }.get(key, default)
            ),
        ):
            with TestClient(self.app) as client:
                response = client.get("/admin/settings")

        self.assertEqual(response.status_code, 200)
        self.assertIn("代理配置", response.text)
        self.assertIn("首页展示", response.text)
        self.assertIn("修改密码", response.text)
        self.assertIn("日志级别", response.text)
        self.assertNotIn("库存预警 Webhook", response.text)
        self.assertNotIn("/admin/settings/webhook", response.text)
        self.assertNotIn("Webhook URL", response.text)
        self.assertNotIn("X-API-Key", response.text)

    def test_homepage_content_settings_endpoint_saves_both_fields(self):
        with patch(
            "app.services.settings.settings_service.update_settings",
            new=AsyncMock(return_value=True),
        ) as mock_update_settings:
            with TestClient(self.app) as client:
                response = client.post(
                    "/admin/settings/homepage-content",
                    json={
                        "homepage_announcement": "  公告第一行\n公告第二行  ",
                        "homepage_usage_notice": "  须知内容  ",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {"success": True, "message": "首页展示内容已保存"},
        )
        self.assertEqual(
            mock_update_settings.await_args.args[1],
            {
                "homepage_announcement": "公告第一行\n公告第二行",
                "homepage_usage_notice": "须知内容",
            },
        )

    def test_legacy_webhook_settings_endpoint_is_gone(self):
        with TestClient(self.app) as client:
            response = client.post("/admin/settings/webhook", json={})

        self.assertEqual(response.status_code, 404)


class TokenParserRegressionTests(unittest.TestCase):
    def setUp(self):
        self.parser = TokenParser()

    def test_comma_batch_format_preserves_empty_access_token_column_and_keeps_session_token(self):
        session_token = "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJzZXNzaW9uIn0.signature"
        refresh_token = "rt_abc.def"
        client_id = "app_123456"

        parsed = self.parser.parse_team_import_text(
            f"owner@example.com,,{refresh_token},{session_token},{client_id}"
        )

        self.assertEqual(len(parsed), 1)
        self.assertIsNone(parsed[0]["token"])
        self.assertEqual(parsed[0]["email"], "owner@example.com")
        self.assertEqual(parsed[0]["refresh_token"], refresh_token)
        self.assertEqual(parsed[0]["session_token"], session_token)
        self.assertEqual(parsed[0]["client_id"], client_id)

    def test_single_jwt_without_access_token_claims_is_treated_as_session_token(self):
        session_token = "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJzZXNzaW9uIn0.signature"

        parsed = self.parser.parse_team_import_text(session_token)

        self.assertEqual(len(parsed), 1)
        self.assertIsNone(parsed[0]["token"])
        self.assertEqual(parsed[0]["session_token"], session_token)


class PreservedRouteRegressionTests(unittest.TestCase):
    def test_refresh_api_still_uses_sync_team_info(self):
        app_instance = FastAPI()
        app_instance.include_router(api.router)
        app_instance.dependency_overrides[get_db] = fake_db_dependency
        app_instance.dependency_overrides[api.get_current_user] = lambda: {"username": "admin"}

        try:
            with patch(
                "app.routes.api.team_service.sync_team_info",
                new=AsyncMock(return_value={"success": True, "message": "同步成功", "error": None}),
            ) as mock_sync:
                with TestClient(app_instance) as client:
                    response = client.get("/api/teams/12/refresh?force=true")

            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["success"])
            mock_sync.assert_awaited_once_with(12, unittest.mock.ANY, force_refresh=True)
        finally:
            app_instance.dependency_overrides.clear()

    def test_user_redeem_page_still_shows_remaining_spots(self):
        app_instance = FastAPI()
        app_instance.mount("/static", StaticFiles(directory="app/static"), name="static")
        app_instance.include_router(user.router)
        app_instance.dependency_overrides[get_db] = fake_db_dependency

        try:
            with patch(
                "app.services.team.TeamService.get_total_available_seats",
                new=AsyncMock(return_value=7),
            ), patch(
                "app.services.settings.settings_service.get_setting",
                new=AsyncMock(side_effect=lambda _db, _key, default=None: default),
            ):
                with TestClient(app_instance) as client:
                    response = client.get("/")

            self.assertEqual(response.status_code, 200)
            self.assertIn("剩余车位", response.text)
            self.assertIn("7", response.text)
        finally:
            app_instance.dependency_overrides.clear()

    def test_user_redeem_page_shows_homepage_announcement_and_usage_notice_when_configured(self):
        app_instance = FastAPI()
        app_instance.mount("/static", StaticFiles(directory="app/static"), name="static")
        app_instance.include_router(user.router)
        app_instance.dependency_overrides[get_db] = fake_db_dependency

        try:
            with patch(
                "app.services.team.TeamService.get_total_available_seats",
                new=AsyncMock(return_value=5),
            ), patch(
                "app.services.settings.settings_service.get_setting",
                new=AsyncMock(
                    side_effect=lambda _db, key, default=None: {
                        "homepage_announcement": "系统公告第一行\n系统公告第二行",
                        "homepage_usage_notice": "请确认邮箱可接收邀请\n兑换码一人一码",
                    }.get(key, default)
                ),
            ):
                with TestClient(app_instance) as client:
                    response = client.get("/")

            self.assertEqual(response.status_code, 200)
            self.assertIn("homepage-announcement", response.text)
            self.assertIn("系统公告第一行", response.text)
            self.assertIn("系统公告第二行", response.text)
            self.assertIn("usage-notice", response.text)
            self.assertIn("请确认邮箱可接收邀请", response.text)
            self.assertIn("兑换码一人一码", response.text)
            self.assertIn("剩余车位", response.text)
        finally:
            app_instance.dependency_overrides.clear()

    def test_user_redeem_page_hides_empty_homepage_content_blocks(self):
        app_instance = FastAPI()
        app_instance.mount("/static", StaticFiles(directory="app/static"), name="static")
        app_instance.include_router(user.router)
        app_instance.dependency_overrides[get_db] = fake_db_dependency

        try:
            with patch(
                "app.services.team.TeamService.get_total_available_seats",
                new=AsyncMock(return_value=9),
            ), patch(
                "app.services.settings.settings_service.get_setting",
                new=AsyncMock(
                    side_effect=lambda _db, key, default=None: {
                        "homepage_announcement": "   ",
                        "homepage_usage_notice": "\n\t",
                    }.get(key, default)
                ),
            ):
                with TestClient(app_instance) as client:
                    response = client.get("/")

            self.assertEqual(response.status_code, 200)
            self.assertNotIn("homepage-announcement", response.text)
            self.assertNotIn("usage-notice", response.text)
            self.assertIn("剩余车位", response.text)
            self.assertIn("9", response.text)
        finally:
            app_instance.dependency_overrides.clear()


class StartupAndSecurityRegressionTests(unittest.TestCase):
    def test_fastapi_metadata_uses_configured_app_name_and_version(self):
        from app.main import app as fastapi_app
        from app.config import settings

        self.assertEqual(fastapi_app.title, settings.app_name)
        self.assertEqual(fastapi_app.version, settings.app_version)

    def test_non_production_default_credentials_emit_prominent_warning(self):
        config = Settings(
            environment="development",
            secret_key=DEFAULT_SECRET_KEY,
            admin_password=DEFAULT_ADMIN_PASSWORD,
        )

        with patch("app.config.logger.warning") as mock_warning:
            config.validate_security()

        warning_messages = [call.args[0] for call in mock_warning.call_args_list]
        self.assertIn(
            "WARNING: Running with default credentials, NOT suitable for public deployment",
            warning_messages,
        )

    def test_apply_persisted_log_level_keeps_current_level_when_missing_or_invalid(self):
        from app.main import apply_persisted_log_level

        test_root_logger = logging.Logger("persisted-log-level-test", level=logging.ERROR)

        with patch("app.main.logger.warning") as mock_warning:
            apply_persisted_log_level(None, root_logger=test_root_logger)
            self.assertEqual(test_root_logger.level, logging.ERROR)

            apply_persisted_log_level("not-a-level", root_logger=test_root_logger)
            self.assertEqual(test_root_logger.level, logging.ERROR)

        self.assertEqual(mock_warning.call_count, 2)

    def test_apply_persisted_log_level_overrides_root_logger_when_valid(self):
        from app.main import apply_persisted_log_level

        test_root_logger = logging.Logger("persisted-log-level-override-test", level=logging.INFO)
        apply_persisted_log_level("warning", root_logger=test_root_logger)
        self.assertEqual(test_root_logger.level, logging.WARNING)


class InitializationBootstrapRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from app.services.settings import settings_service

        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        settings_service._cache.clear()
        self.settings_service = settings_service

    async def asyncTearDown(self):
        self.settings_service._cache.clear()
        await self.engine.dispose()

    async def test_ensure_default_settings_backfills_required_keys_and_removes_initialized_marker(self):
        from app.bootstrap import ensure_default_settings
        from app.config import settings

        async with self.session_factory() as session:
            session.add(Setting(key="initialized", value="true", description="legacy"))
            await session.commit()

            created_settings, removed_legacy_settings = await ensure_default_settings(session)

            rows = (
                await session.execute(
                    select(Setting).where(
                        Setting.key.in_([
                            "proxy",
                            "proxy_enabled",
                            "log_level",
                            "homepage_announcement",
                            "homepage_usage_notice",
                            "initialized",
                        ])
                    )
                )
            ).scalars().all()

        values_by_key = {row.key: row.value for row in rows}
        self.assertCountEqual(
            created_settings,
            [
                "proxy",
                "proxy_enabled",
                "log_level",
                "homepage_announcement",
                "homepage_usage_notice",
            ],
        )
        self.assertEqual(removed_legacy_settings, ["initialized"])
        self.assertEqual(values_by_key["proxy"], settings.proxy or "")
        self.assertEqual(values_by_key["proxy_enabled"], str(settings.proxy_enabled).lower())
        self.assertEqual(values_by_key["log_level"], settings.log_level)
        self.assertEqual(values_by_key["homepage_announcement"], "")
        self.assertEqual(values_by_key["homepage_usage_notice"], "")
        self.assertNotIn("initialized", values_by_key)

    async def test_bootstrap_application_uses_shared_sequence_and_returns_persisted_log_level(self):
        from app.bootstrap import bootstrap_application
        from app.config import settings

        with patch("app.bootstrap.AsyncSessionLocal", new=self.session_factory), patch(
            "app.bootstrap.init_db",
            new=AsyncMock(),
        ) as mock_init_db, patch(
            "app.bootstrap.run_auto_migration",
            new=Mock(),
        ) as mock_run_auto_migration, patch(
            "app.bootstrap.auth_service.initialize_admin_password",
            new=AsyncMock(return_value=True),
        ) as mock_initialize_admin_password:
            result = await bootstrap_application()

        self.assertEqual(result.persisted_log_level, settings.log_level)
        self.assertCountEqual(
            result.created_settings,
            [
                "proxy",
                "proxy_enabled",
                "log_level",
                "homepage_announcement",
                "homepage_usage_notice",
            ],
        )
        self.assertEqual(result.removed_legacy_settings, [])
        mock_init_db.assert_awaited_once()
        mock_run_auto_migration.assert_called_once_with()
        mock_initialize_admin_password.assert_awaited_once()


class RedemptionCodeGenerationRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = RedemptionService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_generate_code_single_retries_until_unique_and_persists_expected_fields(self):
        async with self.session_factory() as session:
            session.add(RedemptionCode(code="EXISTING-CODE", status="unused"))
            await session.commit()

        with patch.object(
            self.service,
            "_generate_random_code",
            side_effect=["EXISTING-CODE", "SINGLE-UNIQUE-CODE"],
        ):
            async with self.session_factory() as session:
                result = await self.service.generate_code_single(
                    session,
                    expires_days=7,
                    has_warranty=True,
                    warranty_days=21,
                )
                created_code = await session.scalar(
                    select(RedemptionCode).where(RedemptionCode.code == "SINGLE-UNIQUE-CODE")
                )

        self.assertTrue(result["success"])
        self.assertEqual(result["code"], "SINGLE-UNIQUE-CODE")
        self.assertIsNotNone(created_code)
        self.assertEqual(created_code.status, "unused")
        self.assertTrue(created_code.has_warranty)
        self.assertEqual(created_code.warranty_days, 21)
        self.assertIsNotNone(created_code.expires_at)

    async def test_generate_code_batch_avoids_db_and_in_batch_duplicates(self):
        async with self.session_factory() as session:
            session.add(RedemptionCode(code="EXISTING-CODE", status="unused"))
            await session.commit()

        with patch.object(
            self.service,
            "_generate_random_code",
            side_effect=[
                "EXISTING-CODE",
                "BATCH-CODE-1",
                "BATCH-CODE-1",
                "BATCH-CODE-2",
            ],
        ):
            async with self.session_factory() as session:
                result = await self.service.generate_code_batch(
                    session,
                    count=2,
                    expires_days=5,
                    has_warranty=True,
                    warranty_days=14,
                )
                created_rows = (
                    await session.execute(
                        select(RedemptionCode)
                        .where(RedemptionCode.code.in_(["BATCH-CODE-1", "BATCH-CODE-2"]))
                        .order_by(RedemptionCode.code.asc())
                    )
                ).scalars().all()

        self.assertTrue(result["success"])
        self.assertEqual(result["codes"], ["BATCH-CODE-1", "BATCH-CODE-2"])
        self.assertEqual(result["total"], 2)
        self.assertEqual(len(created_rows), 2)
        self.assertTrue(all(row.has_warranty for row in created_rows))
        self.assertTrue(all(row.warranty_days == 14 for row in created_rows))
        self.assertEqual(created_rows[0].expires_at, created_rows[1].expires_at)


class RedemptionCodeClaimRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = RedeemFlowService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _create_team(self, session, email: str, account_id: str, team_name: str) -> Team:
        team = Team(
            email=email,
            account_id=account_id,
            access_token_encrypted=encryption_service.encrypt_token("token"),
            team_name=team_name,
            status="active",
            max_members=TEAM_MEMBER_LIMIT,
        )
        session.add(team)
        await session.flush()
        return team

    async def test_claim_redemption_code_fails_closed_after_competing_consume(self):
        async with self.session_factory() as session:
            first_team = await self._create_team(session, "owner1@example.com", "acc-1", "First Team")
            second_team = await self._create_team(session, "owner2@example.com", "acc-2", "Second Team")
            session.add(RedemptionCode(code="RACE-CODE", status="unused"))
            await session.commit()
            first_team_id = first_team.id
            second_team_id = second_team.id

        first_redeemed_at = datetime(2026, 3, 1, 8, 0, 0, tzinfo=timezone.utc)
        second_redeemed_at = datetime(2026, 3, 1, 8, 1, 0, tzinfo=timezone.utc)

        async with self.session_factory() as first_session:
            first_claim = await self.service._claim_redemption_code(
                first_session,
                "RACE-CODE",
                "first@example.com",
                first_team_id,
                first_redeemed_at,
                False,
                None,
            )
            self.assertTrue(first_claim["success"])
            self.assertTrue(first_claim["claimed"])
            first_session.add(
                RedemptionRecord(
                    email="first@example.com",
                    code="RACE-CODE",
                    team_id=first_team_id,
                    account_id="acc-1",
                    redeemed_at=first_redeemed_at,
                )
            )
            await first_session.commit()

        async with self.session_factory() as second_session:
            second_claim = await self.service._claim_redemption_code(
                second_session,
                "RACE-CODE",
                "second@example.com",
                second_team_id,
                second_redeemed_at,
                False,
                None,
            )
            code_row = await second_session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "RACE-CODE")
            )
            record_count = await second_session.scalar(select(func.count(RedemptionRecord.id)))

        self.assertFalse(second_claim["success"])
        self.assertFalse(second_claim["claimed"])
        self.assertEqual(second_claim["error"], CODE_CONFLICT_ERROR)
        self.assertEqual(code_row.status, "used")
        self.assertEqual(code_row.used_by_email, "first@example.com")
        self.assertEqual(code_row.used_team_id, first_team_id)
        self.assertEqual(record_count, 1)


class RedeemRouteConflictRegressionTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(redeem.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def test_code_conflict_error_is_reported_as_conflict(self):
        with patch(
            "app.routes.redeem.redeem_flow_service.redeem_and_join_team",
            new=AsyncMock(return_value={"success": False, "error": CODE_CONFLICT_ERROR}),
        ):
            with TestClient(self.app) as client:
                response = client.post(
                    "/redeem/confirm",
                    json={"email": "user@example.com", "code": "ABC-123", "team_id": 1},
                )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"], CODE_CONFLICT_ERROR)


class RedeemRouteOwnerBindingRegressionTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(redeem.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def test_owner_binding_failure_is_reported_as_bad_request(self):
        with patch(
            "app.routes.redeem.redeem_flow_service.redeem_and_join_team",
            new=AsyncMock(
                return_value={
                    "success": False,
                    "error": "该质保码已绑定原兑换邮箱，仅支持原兑换邮箱复兑",
                }
            ),
        ):
            with TestClient(self.app) as client:
                response = client.post(
                    "/redeem/confirm",
                    json={"email": "user@example.com", "code": "ABC-123", "team_id": 1},
                )

        self.assertEqual(response.status_code, 400)
        self.assertIn("原兑换邮箱", response.json()["detail"])

