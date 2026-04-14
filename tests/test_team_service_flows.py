import unittest
from unittest.mock import AsyncMock, Mock

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base
from app.models import TEAM_MEMBER_LIMIT, RedemptionCode, RedemptionRecord, Team, TeamAccount
from app.services.encryption import encryption_service
from app.services.redemption import RedemptionService
from app.services.team import TeamService


class TeamServiceFlowTests(unittest.IsolatedAsyncioTestCase):
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
                team_name="Example Team",
                status=status,
                current_members=current_members,
                max_members=max_members,
            )
            session.add(team)
            await session.commit()
            return team.id

    def _mock_sync_dependencies(self):
        self.service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.get_account_info = AsyncMock(
            return_value={
                "success": True,
                "accounts": [
                    {
                        "account_id": "acc-1",
                        "name": "Synced Team",
                        "plan_type": "team",
                        "subscription_plan": "team",
                        "expires_at": "2026-06-01T00:00:00+00:00",
                        "account_user_role": "owner",
                    }
                ],
            }
        )
        self.service.chatgpt_service.get_members = AsyncMock(
            return_value={
                "success": True,
                "total": 2,
                "members": [{"email": "member@example.com"}],
            }
        )
        self.service.chatgpt_service.get_invites = AsyncMock(
            return_value={
                "success": True,
                "total": 1,
                "items": [{"email_address": "invitee@example.com"}],
            }
        )
        self.service.chatgpt_service.get_account_settings = AsyncMock(
            return_value={"success": True, "data": {"beta_settings": {}}}
        )

    async def test_import_team_single_creates_team_and_accounts(self):
        self.service.jwt_parser.is_token_expired = Mock(return_value=False)
        self.service.jwt_parser.extract_email = Mock(return_value="owner@example.com")
        self.service.chatgpt_service.get_account_info = AsyncMock(
            return_value={
                "success": True,
                "accounts": [
                    {
                        "account_id": "acc-1",
                        "name": "Imported Team",
                        "plan_type": "team",
                        "subscription_plan": "team",
                        "expires_at": "2026-06-01T00:00:00+00:00",
                        "has_active_subscription": True,
                        "account_user_role": "owner",
                    }
                ],
            }
        )
        self.service.chatgpt_service.get_members = AsyncMock(
            return_value={"success": True, "total": 1, "members": []}
        )
        self.service.chatgpt_service.get_invites = AsyncMock(
            return_value={"success": True, "total": 0, "items": []}
        )
        self.service.chatgpt_service.get_account_settings = AsyncMock(
            return_value={
                "success": True,
                "data": {"beta_settings": {"codex_device_code_auth": True}},
            }
        )

        async with self.session_factory() as session:
            result = await self.service.import_team_single(
                access_token="access-token",
                db_session=session,
                email="owner@example.com",
            )
            team = await session.get(Team, result["team_id"])
            team_accounts = (
                await session.execute(select(TeamAccount).where(TeamAccount.team_id == result["team_id"]))
            ).scalars().all()

        self.assertTrue(result["success"])
        self.assertIsNotNone(team)
        self.assertEqual(team.email, "owner@example.com")
        self.assertEqual(team.current_members, 1)
        self.assertTrue(team.device_code_auth_enabled)
        self.assertEqual(len(team_accounts), 1)
        self.assertTrue(team_accounts[0].is_primary)

    async def test_sync_team_info_updates_members_status_and_last_sync(self):
        team_id = await self._create_team(current_members=0)
        self._mock_sync_dependencies()

        async with self.session_factory() as session:
            result = await self.service.sync_team_info(team_id, session)
            team = await session.get(Team, team_id)

        self.assertTrue(result["success"])
        self.assertEqual(team.current_members, 3)
        self.assertEqual(team.status, "active")
        self.assertIsNotNone(team.last_sync)
        self.assertCountEqual(result["member_emails"], ["member@example.com", "invitee@example.com"])

    async def test_sync_team_info_defers_commit_for_internal_callers(self):
        team_id = await self._create_team(current_members=0)
        self._mock_sync_dependencies()

        async with self.session_factory() as session:
            original_commit = session.commit
            original_flush = session.flush
            session.commit = AsyncMock(wraps=original_commit)
            session.flush = AsyncMock(wraps=original_flush)

            with self.service.defer_sync_commit():
                result = await self.service.sync_team_info(team_id, session)

            team = await session.get(Team, team_id)

        self.assertTrue(result["success"])
        self.assertEqual(team.current_members, 3)
        session.flush.assert_awaited_once()
        session.commit.assert_not_awaited()

    async def test_update_team_rejects_member_limit_above_supported_capacity(self):
        team_id = await self._create_team()

        async with self.session_factory() as session:
            result = await self.service.update_team(
                team_id,
                session,
                max_members=TEAM_MEMBER_LIMIT + 1,
            )

        self.assertFalse(result["success"])
        self.assertIn(str(TEAM_MEMBER_LIMIT), result["error"])

    async def test_revoke_team_invite_refreshes_local_state_after_success(self):
        team_id = await self._create_team()
        self.service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.delete_invite = AsyncMock(return_value={"success": True})
        self.service.sync_team_info = AsyncMock(return_value={"success": True, "member_emails": []})
        self.service._reset_error_status = AsyncMock()

        async with self.session_factory() as session:
            result = await self.service.revoke_team_invite(team_id, "invitee@example.com", session)

        self.assertTrue(result["success"])
        self.assertFalse(result["partial"])
        self.service._reset_error_status.assert_awaited_once()

    async def test_add_team_member_refreshes_local_state_after_success(self):
        team_id = await self._create_team()
        self.service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.send_invite = AsyncMock(
            return_value={"success": True, "data": {"account_invites": [{"id": "invite-1"}]}}
        )
        self.service.sync_team_info = AsyncMock(
            side_effect=[
                {"success": True, "member_emails": []},
                {"success": True, "member_emails": ["new-member@example.com"]},
            ]
        )
        self.service._reset_error_status = AsyncMock()

        async with self.session_factory() as session:
            result = await self.service.add_team_member(team_id, "new-member@example.com", session)

        self.assertTrue(result["success"])
        self.assertFalse(result["pending"])
        self.service._reset_error_status.assert_awaited_once()

    async def test_add_team_member_returns_pending_when_invite_visibility_is_delayed(self):
        team_id = await self._create_team()
        self.service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.send_invite = AsyncMock(
            return_value={"success": True, "data": {"account_invites": [{"id": "invite-1"}]}}
        )
        self.service.sync_team_info = AsyncMock(
            side_effect=[
                {"success": True, "member_emails": []},
                {"success": True, "member_emails": []},
                {"success": True, "member_emails": []},
                {"success": True, "member_emails": []},
            ]
        )
        self.service._reset_error_status = AsyncMock()

        async with self.session_factory() as session:
            with unittest.mock.patch("app.services.team.asyncio.sleep", new=AsyncMock()):
                result = await self.service.add_team_member(team_id, "new-member@example.com", session)
                team = await session.get(Team, team_id)

        self.assertTrue(result["success"])
        self.assertTrue(result["pending"])
        self.assertIn("尚未完成同步确认", result["message"])
        self.assertEqual(team.status, "active")
        self.service._reset_error_status.assert_not_awaited()

    async def test_delete_team_member_refreshes_local_state_after_success(self):
        team_id = await self._create_team()
        self.service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.delete_member = AsyncMock(return_value={"success": True})
        self.service.sync_team_info = AsyncMock(return_value={"success": True, "member_emails": []})
        self.service._reset_error_status = AsyncMock()

        async with self.session_factory() as session:
            result = await self.service.delete_team_member(team_id, "user-123", session)

        self.assertTrue(result["success"])
        self.assertFalse(result["partial"])
        self.service._reset_error_status.assert_awaited_once()

    async def test_delete_team_member_returns_partial_when_sync_fails_after_upstream_delete(self):
        team_id = await self._create_team(status="active")
        self.service.ensure_access_token = AsyncMock(return_value="access-token")
        self.service.chatgpt_service.delete_member = AsyncMock(return_value={"success": True})
        self.service.sync_team_info = AsyncMock(return_value={"success": False, "error": "sync failed"})

        async with self.session_factory() as session:
            result = await self.service.delete_team_member(team_id, "user-123", session)
            team = await session.get(Team, team_id)

        self.assertFalse(result["success"])
        self.assertTrue(result["partial"])
        self.assertTrue(result["upstream_success"])
        self.assertFalse(result["local_team_consistent"])
        self.assertEqual(team.status, "error")

    async def test_remove_invite_or_member_matches_invites_case_insensitively(self):
        team_id = await self._create_team()
        self.service.get_team_members = AsyncMock(
            return_value={
                "success": True,
                "members": [{"email": "Invitee@Example.com", "status": "invited", "user_id": None}],
            }
        )
        self.service.revoke_team_invite = AsyncMock(
            return_value={"success": True, "message": "revoked", "error": None}
        )

        async with self.session_factory() as session:
            result = await self.service.remove_invite_or_member(team_id, "invitee@example.com", session)

        self.assertTrue(result["success"])
        self.service.revoke_team_invite.assert_awaited_once_with(team_id, "Invitee@Example.com", session)


class WithdrawRecordFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = RedemptionService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _create_team(self, session, email: str, account_id: str) -> Team:
        team = Team(
            email=email,
            account_id=account_id,
            access_token_encrypted=encryption_service.encrypt_token("token"),
            team_name="Withdraw Team",
            status="active",
            max_members=TEAM_MEMBER_LIMIT,
        )
        session.add(team)
        await session.flush()
        return team

    async def test_withdraw_record_restores_code_and_deletes_record(self):
        async with self.session_factory() as session:
            team = await self._create_team(session, "owner@example.com", "acc-1")
            code = RedemptionCode(
                code="WITHDRAW-OK",
                status="used",
                used_by_email="user@example.com",
                used_team_id=team.id,
                has_warranty=False,
            )
            session.add(code)
            await session.flush()
            record = RedemptionRecord(
                email="user@example.com",
                code=code.code,
                team_id=team.id,
                account_id=team.account_id,
            )
            session.add(record)
            await session.commit()
            record_id = record.id

        async with self.session_factory() as session:
            with unittest.mock.patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(
                    return_value={
                        "success": True,
                        "local_team_consistent": True,
                        "upstream_success": True,
                    }
                ),
            ):
                result = await self.service.withdraw_record(record_id, session)
                record_row = await session.get(RedemptionRecord, record_id)
                code_row = await session.scalar(
                    select(RedemptionCode).where(RedemptionCode.code == "WITHDRAW-OK")
                )

        self.assertTrue(result["success"])
        self.assertIsNone(record_row)
        self.assertEqual(code_row.status, "unused")
        self.assertIsNone(code_row.used_by_email)
        self.assertIsNone(code_row.used_team_id)

    async def test_withdraw_record_requires_force_when_team_access_is_unavailable(self):
        async with self.session_factory() as session:
            team = await self._create_team(session, "owner@example.com", "acc-1")
            code = RedemptionCode(
                code="WITHDRAW-UPSTREAM-MISSING",
                status="used",
                used_by_email="user@example.com",
                used_team_id=team.id,
                has_warranty=False,
            )
            session.add(code)
            await session.flush()
            record = RedemptionRecord(
                email="user@example.com",
                code=code.code,
                team_id=team.id,
                account_id=team.account_id,
            )
            session.add(record)
            await session.commit()
            record_id = record.id

        async with self.session_factory() as session:
            with unittest.mock.patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(
                    return_value={
                        "success": False,
                        "error_code": "team_access_unavailable",
                        "can_force_local_repair": True,
                        "error": "Token 无效或已过期",
                    }
                ),
            ):
                result = await self.service.withdraw_record(record_id, session)
                record_row = await session.get(RedemptionRecord, record_id)
                code_row = await session.scalar(
                    select(RedemptionCode).where(RedemptionCode.code == "WITHDRAW-UPSTREAM-MISSING")
                )

        self.assertFalse(result["success"])
        self.assertTrue(result["requires_force"])
        self.assertIsNotNone(record_row)
        self.assertEqual(code_row.status, "used")
        self.assertEqual(code_row.used_by_email, "user@example.com")

    async def test_withdraw_record_force_repairs_local_state_when_team_access_is_unavailable(self):
        async with self.session_factory() as session:
            team = await self._create_team(session, "owner@example.com", "acc-1")
            code = RedemptionCode(
                code="WITHDRAW-FORCE-LOCAL",
                status="used",
                used_by_email="user@example.com",
                used_team_id=team.id,
                has_warranty=False,
            )
            session.add(code)
            await session.flush()
            record = RedemptionRecord(
                email="user@example.com",
                code=code.code,
                team_id=team.id,
                account_id=team.account_id,
            )
            session.add(record)
            await session.commit()
            record_id = record.id

        async with self.session_factory() as session:
            with unittest.mock.patch(
                "app.services.team.team_service.remove_invite_or_member",
                new=AsyncMock(
                    return_value={
                        "success": True,
                        "partial": True,
                        "local_only": True,
                        "upstream_success": False,
                        "local_team_consistent": False,
                        "message": "local only",
                    }
                ),
            ):
                result = await self.service.withdraw_record(record_id, session, force=True)
                record_row = await session.get(RedemptionRecord, record_id)
                code_row = await session.scalar(
                    select(RedemptionCode).where(RedemptionCode.code == "WITHDRAW-FORCE-LOCAL")
                )

        self.assertTrue(result["success"])
        self.assertTrue(result["partial"])
        self.assertTrue(result["local_only"])
        self.assertIsNone(record_row)
        self.assertEqual(code_row.status, "unused")
        self.assertIsNone(code_row.used_by_email)


if __name__ == "__main__":
    unittest.main()
