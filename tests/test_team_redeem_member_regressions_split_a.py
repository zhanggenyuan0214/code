import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.middleware.sessions import SessionMiddleware

from app.database import Base, get_db
from app.models import TEAM_MEMBER_LIMIT, RedemptionCode, Team
from app.routes import admin
from app.services.encryption import encryption_service
from app.services.team import TeamService


async def fake_db_dependency():
    yield object()


def fake_admin_dependency():
    return {"username": "admin", "is_admin": True}


class TeamStateRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = TeamService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _create_team(
        self,
        *,
        email="owner@example.com",
        account_id="acc-1",
        current_members=1,
        max_members=TEAM_MEMBER_LIMIT,
        status="active",
    ) -> int:
        async with self.session_factory() as session:
            team = Team(
                email=email,
                account_id=account_id,
                access_token_encrypted=encryption_service.encrypt_token("token"),
                team_name="Regression Team",
                status=status,
                current_members=current_members,
                max_members=max_members,
            )
            session.add(team)
            await session.commit()
            return team.id

    async def test_full_error_marks_team_full_without_shrinking_capacity(self):
        team_id = await self._create_team(current_members=2, max_members=TEAM_MEMBER_LIMIT, status="active")

        async with self.session_factory() as session:
            team = await session.get(Team, team_id)
            handled = await self.service._handle_api_error(
                {
                    "success": False,
                    "error": "Reached maximum number of seats for this workspace",
                    "error_code": None,
                },
                team,
                session,
            )
            await session.commit()

        async with self.session_factory() as session:
            team = await session.get(Team, team_id)

        self.assertTrue(handled)
        self.assertEqual(team.status, "full")
        self.assertEqual(team.max_members, TEAM_MEMBER_LIMIT)
        self.assertEqual(team.current_members, TEAM_MEMBER_LIMIT)

    async def test_update_team_recomputes_status_when_route_disables_manual_override(self):
        team_id = await self._create_team(current_members=2, max_members=2, status="full")

        async with self.session_factory() as session:
            result = await self.service.update_team(
                team_id,
                session,
                max_members=TEAM_MEMBER_LIMIT,
                status="full",
                manual_status_override=False,
            )
            team = await session.get(Team, team_id)

        self.assertTrue(result["success"])
        self.assertEqual(team.max_members, TEAM_MEMBER_LIMIT)
        self.assertEqual(team.status, "active")

    async def test_enable_device_code_auth_failure_updates_local_status(self):
        cases = [
            (
                {"success": False, "error": "Token revoked", "error_code": "token_invalidated"},
                "banned",
                "Token 已失效",
            ),
            (
                {
                    "success": False,
                    "error": "workspace expired",
                    "error_code": "deactivated_workspace",
                },
                "expired",
                "Team 已过期",
            ),
        ]

        for upstream_result, expected_status, expected_error in cases:
            with self.subTest(error_code=upstream_result["error_code"]):
                team_id = await self._create_team(status="active")
                self.service.ensure_access_token = AsyncMock(return_value="access-token")
                self.service.chatgpt_service.toggle_beta_feature = AsyncMock(return_value=upstream_result)

                async with self.session_factory() as session:
                    result = await self.service.enable_device_code_auth(team_id, session)

                async with self.session_factory() as session:
                    team = await session.get(Team, team_id)

                self.assertFalse(result["success"])
                self.assertEqual(result["error"], expected_error)
                self.assertEqual(team.status, expected_status)

    async def test_delete_team_blocks_pending_redemption_to_avoid_orphan_code_state(self):
        async with self.session_factory() as session:
            team = Team(
                email="owner@example.com",
                account_id="acc-1",
                access_token_encrypted=encryption_service.encrypt_token("token"),
                team_name="Pending Delete Team",
                status="active",
                max_members=TEAM_MEMBER_LIMIT,
            )
            session.add(team)
            await session.flush()
            session.add(
                RedemptionCode(
                    code="PENDING-DELETE",
                    status="invite_pending",
                    used_by_email="user@example.com",
                    used_team_id=team.id,
                )
            )
            await session.commit()
            team_id = team.id

        async with self.session_factory() as session:
            result = await self.service.delete_team(team_id, session)
            team_row = await session.get(Team, team_id)
            code_row = await session.scalar(
                select(RedemptionCode).where(RedemptionCode.code == "PENDING-DELETE")
            )

        self.assertFalse(result["success"])
        self.assertIn("待确认兑换", result["error"])
        self.assertIsNotNone(team_row)
        self.assertEqual(code_row.status, "invite_pending")
        self.assertEqual(code_row.used_team_id, team_id)


class TeamRouteRegressionTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.add_middleware(SessionMiddleware, secret_key="test-secret", https_only=False)
        self.app.include_router(admin.router)
        self.app.dependency_overrides[get_db] = fake_db_dependency
        self.app.dependency_overrides[admin.require_admin] = fake_admin_dependency

    def tearDown(self):
        self.app.dependency_overrides.clear()

    def test_team_members_list_failure_returns_http_error_instead_of_empty_state_payload(self):
        with patch(
            "app.routes.admin.team_service.get_team_members",
            new=AsyncMock(
                return_value={
                    "success": False,
                    "members": [],
                    "total": 0,
                    "error_code": "team_access_unavailable",
                    "error": "Token 无效或已过期",
                }
            ),
        ):
            with TestClient(self.app) as client:
                response = client.get("/admin/teams/1/members/list")

        self.assertEqual(response.status_code, 502)
        self.assertFalse(response.json()["success"])
        self.assertEqual(response.json()["error"], "Token 无效或已过期")

    def test_batch_delete_reports_clear_error_when_pending_redemption_blocks_deletion(self):
        async def fake_delete(team_id, *_args, **_kwargs):
            return {
                "success": False,
                "error": f"Team {team_id} 存在待确认兑换，无法删除",
            }

        with patch("app.routes.admin.team_service.delete_team", new=AsyncMock(side_effect=fake_delete)):
            with TestClient(self.app) as client:
                response = client.post("/admin/teams/batch-delete", json={"ids": [1, 2]})

        body = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(body["success"])
        self.assertEqual(body["failed_count"], 2)
        self.assertIn("待确认兑换", body["error"])


class BatchImportDedupRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = TeamService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_batch_import_keeps_same_email_different_account_ids(self):
        parsed_rows = [
            {
                "token": "token-a",
                "email": "owner@example.com",
                "account_id": "acc-1",
                "refresh_token": None,
                "session_token": None,
                "client_id": None,
            },
            {
                "token": "token-a",
                "email": "owner@example.com",
                "account_id": "acc-1",
                "refresh_token": None,
                "session_token": None,
                "client_id": None,
            },
            {
                "token": "token-b",
                "email": "owner@example.com",
                "account_id": "acc-2",
                "refresh_token": None,
                "session_token": None,
                "client_id": None,
            },
        ]

        imported_account_ids = []
        self.service.token_parser.parse_team_import_text = Mock(return_value=parsed_rows)

        async def fake_import_team_single(*, account_id=None, email=None, **_kwargs):
            imported_account_ids.append((email, account_id))
            return {
                "success": True,
                "team_id": len(imported_account_ids),
                "email": email,
                "message": "ok",
                "error": None,
            }

        self.service.import_team_single = AsyncMock(side_effect=fake_import_team_single)

        async with self.session_factory() as session:
            events = [event async for event in self.service.import_team_batch("ignored", session)]

        self.assertEqual(events[0]["type"], "start")
        self.assertEqual(events[0]["total"], 2)
        self.assertEqual(events[-1]["type"], "finish")
        self.assertEqual(events[-1]["success_count"], 2)
        self.assertEqual(
            imported_account_ids,
            [("owner@example.com", "acc-1"), ("owner@example.com", "acc-2")],
        )


class AdminUiContractRegressionTests(unittest.TestCase):
    def test_admin_edit_team_template_exposes_explicit_manual_status_override(self):
        repo_root = Path(__file__).resolve().parents[1]
        template_text = (repo_root / "app" / "templates" / "admin" / "index.html").read_text(encoding="utf-8")
        self.assertIn("edit-team-manual-status-override", template_text)
        self.assertIn("manual_status_override", template_text)

    def test_member_list_js_checks_backend_success_flag_before_rendering_empty_tables(self):
        repo_root = Path(__file__).resolve().parents[1]
        script_text = (repo_root / "app" / "static" / "js" / "main.js").read_text(encoding="utf-8")
        self.assertIn("payload.success !== false", script_text)


if __name__ == "__main__":
    unittest.main()
