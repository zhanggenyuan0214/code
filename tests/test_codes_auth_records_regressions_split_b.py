import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.database import Base, get_db
from app.models import TEAM_MEMBER_LIMIT, RedemptionCode, RedemptionRecord, Team
from app.routes import admin
from app.services.encryption import encryption_service
from app.services.redemption import RedemptionService


def fake_admin_dependency():
    return {"username": "admin", "is_admin": True}


class RedemptionRecordFilteringRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.service = RedemptionService()

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _create_team(self, session, *, team_name: str, email: str, account_id: str) -> Team:
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

    async def test_get_all_records_preserves_email_code_team_filters_and_sort_order(self):
        earlier = datetime(2026, 4, 1, 8, 0, tzinfo=timezone.utc)
        later = datetime(2026, 4, 2, 9, 30, tzinfo=timezone.utc)

        async with self.session_factory() as session:
            matched_team = await self._create_team(
                session,
                team_name="Matched Team",
                email="matched-owner@example.com",
                account_id="matched-acc",
            )
            other_team = await self._create_team(
                session,
                team_name="Other Team",
                email="other-owner@example.com",
                account_id="other-acc",
            )

            session.add_all(
                [
                    RedemptionCode(code="MATCH-CODE-1", status="used"),
                    RedemptionCode(code="MATCH-CODE-2", status="used"),
                    RedemptionCode(code="OTHER-CODE", status="used"),
                ]
            )
            await session.flush()
            session.add_all(
                [
                    RedemptionRecord(
                        email="user@example.com",
                        code="MATCH-CODE-1",
                        team_id=matched_team.id,
                        account_id=matched_team.account_id,
                        redeemed_at=earlier,
                    ),
                    RedemptionRecord(
                        email="user@example.com",
                        code="MATCH-CODE-2",
                        team_id=matched_team.id,
                        account_id=matched_team.account_id,
                        redeemed_at=later,
                    ),
                    RedemptionRecord(
                        email="other@example.com",
                        code="OTHER-CODE",
                        team_id=other_team.id,
                        account_id=other_team.account_id,
                        redeemed_at=later,
                    ),
                ]
            )
            await session.commit()

        async with self.session_factory() as session:
            result = await self.service.get_all_records(
                session,
                email="user@example.com",
                code="MATCH-CODE",
                team_id=matched_team.id,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["total"], 2)
        self.assertEqual([record["code"] for record in result["records"]], ["MATCH-CODE-2", "MATCH-CODE-1"])
        self.assertTrue(all(record["team_name"] == "Matched Team" for record in result["records"]))


class AdminCodesAndRecordsRouteRegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "route-regressions.db"
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{db_path.as_posix()}")
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)
        self._run_async(self._create_schema())

        self.app = FastAPI()
        self.app.mount("/static", StaticFiles(directory="app/static"), name="static")
        self.app.include_router(admin.router)

        async def override_get_db():
            async with self.session_factory() as session:
                yield session

        self.app.dependency_overrides[get_db] = override_get_db
        self.app.dependency_overrides[admin.require_admin] = fake_admin_dependency

    def tearDown(self):
        self.app.dependency_overrides.clear()
        self._run_async(self.engine.dispose())
        self.temp_dir.cleanup()

    def _run_async(self, coro):
        return asyncio.run(coro)

    async def _create_schema(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def _create_team(self, *, email="owner@example.com", account_id="acc-1", team_name="Route Team") -> Team:
        async with self.session_factory() as session:
            team = Team(
                email=email,
                account_id=account_id,
                access_token_encrypted=encryption_service.encrypt_token("token"),
                team_name=team_name,
                status="active",
                max_members=TEAM_MEMBER_LIMIT,
            )
            session.add(team)
            await session.commit()
            await session.refresh(team)
            return team

    async def _insert_objects(self, *objects):
        async with self.session_factory() as session:
            session.add_all(list(objects))
            await session.commit()

    async def _get_code(self, code: str) -> RedemptionCode | None:
        async with self.session_factory() as session:
            return await session.scalar(select(RedemptionCode).where(RedemptionCode.code == code))

    async def _get_existing_codes(self) -> list[str]:
        async with self.session_factory() as session:
            result = await session.execute(select(RedemptionCode.code).order_by(RedemptionCode.code.asc()))
            return list(result.scalars().all())

    def test_single_edit_keeps_used_code_fields_consistent(self):
        used_at = datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc)
        team = self._run_async(self._create_team(team_name="Single Edit Team", account_id="single-edit-acc"))
        self._run_async(
            self._insert_objects(
                RedemptionCode(
                    code="SINGLE-EDIT-CODE",
                    status="used",
                    has_warranty=False,
                    warranty_days=30,
                    used_by_email="user@example.com",
                    used_team_id=team.id,
                    used_at=used_at,
                ),
                RedemptionRecord(
                    email="user@example.com",
                    code="SINGLE-EDIT-CODE",
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=used_at,
                ),
            )
        )

        with TestClient(self.app) as client:
            response = client.post(
                "/admin/codes/SINGLE-EDIT-CODE/update",
                json={"has_warranty": True, "warranty_days": 45},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["success"])

        code_row = self._run_async(self._get_code("SINGLE-EDIT-CODE"))
        self.assertIsNotNone(code_row)
        self.assertEqual(code_row.status, "warranty_active")
        self.assertTrue(code_row.has_warranty)
        self.assertEqual(code_row.warranty_days, 45)
        self.assertEqual(code_row.warranty_expires_at, used_at + timedelta(days=45))

    def test_bulk_edit_keeps_used_and_pending_code_fields_consistent(self):
        used_at = datetime(2026, 4, 4, 12, 0, tzinfo=timezone.utc)
        team = self._run_async(self._create_team(team_name="Bulk Edit Team", account_id="bulk-edit-acc"))
        self._run_async(
            self._insert_objects(
                RedemptionCode(
                    code="BULK-EDIT-USED",
                    status="warranty_active",
                    has_warranty=True,
                    warranty_days=30,
                    used_by_email="user@example.com",
                    used_team_id=team.id,
                    used_at=used_at,
                    warranty_expires_at=used_at + timedelta(days=30),
                ),
                RedemptionCode(
                    code="BULK-EDIT-PENDING",
                    status="invite_pending",
                    has_warranty=True,
                    warranty_days=14,
                    used_by_email="pending@example.com",
                    used_team_id=team.id,
                ),
                RedemptionRecord(
                    email="user@example.com",
                    code="BULK-EDIT-USED",
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=used_at,
                    is_warranty_redemption=True,
                ),
            )
        )

        with TestClient(self.app) as client:
            response = client.post(
                "/admin/codes/bulk-update",
                json={
                    "codes": ["BULK-EDIT-USED", "BULK-EDIT-PENDING"],
                    "has_warranty": False,
                    "warranty_days": None,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["success"])

        used_code = self._run_async(self._get_code("BULK-EDIT-USED"))
        pending_code = self._run_async(self._get_code("BULK-EDIT-PENDING"))
        self.assertEqual(used_code.status, "used")
        self.assertFalse(used_code.has_warranty)
        self.assertIsNone(used_code.warranty_expires_at)
        self.assertEqual(pending_code.status, "invite_pending")
        self.assertFalse(pending_code.has_warranty)
        self.assertIsNone(pending_code.warranty_expires_at)

    def test_bulk_delete_reports_all_success(self):
        self._run_async(
            self._insert_objects(
                RedemptionCode(code="DELETE-OK-1", status="unused"),
                RedemptionCode(code="DELETE-OK-2", status="unused"),
            )
        )

        with TestClient(self.app) as client:
            response = client.post(
                "/admin/codes/bulk-delete",
                json={"codes": ["DELETE-OK-1", "DELETE-OK-2"]},
            )

        body = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["success"])
        self.assertFalse(body["partial"])
        self.assertEqual(body["success_count"], 2)
        self.assertEqual(body["failed_count"], 0)
        self.assertEqual(self._run_async(self._get_existing_codes()), [])

    def test_bulk_delete_reports_partial_success_with_clear_reasons(self):
        team = self._run_async(self._create_team(team_name="Delete Partial Team", account_id="delete-partial-acc"))
        redeemed_at = datetime(2026, 4, 3, 9, 0, tzinfo=timezone.utc)
        self._run_async(
            self._insert_objects(
                RedemptionCode(code="DELETE-PARTIAL-OK", status="unused"),
                RedemptionCode(
                    code="DELETE-PARTIAL-USED",
                    status="used",
                    used_by_email="used@example.com",
                    used_team_id=team.id,
                    used_at=redeemed_at,
                ),
                RedemptionCode(code="DELETE-PARTIAL-HISTORY", status="expired"),
                RedemptionRecord(
                    email="history@example.com",
                    code="DELETE-PARTIAL-HISTORY",
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=redeemed_at,
                ),
            )
        )

        with TestClient(self.app) as client:
            response = client.post(
                "/admin/codes/bulk-delete",
                json={
                    "codes": [
                        "DELETE-PARTIAL-OK",
                        "DELETE-PARTIAL-USED",
                        "DELETE-PARTIAL-HISTORY",
                    ]
                },
            )

        body = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["success"])
        self.assertTrue(body["partial"])
        self.assertEqual(body["success_count"], 1)
        self.assertEqual(body["failed_count"], 2)
        self.assertIn("DELETE-PARTIAL-USED: 已使用或质保中的兑换码不能删除", body["errors"])
        self.assertIn("DELETE-PARTIAL-HISTORY: 存在兑换记录的兑换码不能删除", body["errors"])
        self.assertEqual(
            self._run_async(self._get_existing_codes()),
            ["DELETE-PARTIAL-HISTORY", "DELETE-PARTIAL-USED"],
        )

    def test_bulk_delete_reports_all_failure(self):
        team = self._run_async(self._create_team(team_name="Delete Failure Team", account_id="delete-failure-acc"))
        self._run_async(
            self._insert_objects(
                RedemptionCode(
                    code="DELETE-FAIL-USED",
                    status="used",
                    used_by_email="used@example.com",
                    used_team_id=team.id,
                ),
                RedemptionCode(
                    code="DELETE-FAIL-PENDING",
                    status="invite_pending",
                    used_by_email="pending@example.com",
                    used_team_id=team.id,
                ),
            )
        )

        with TestClient(self.app) as client:
            response = client.post(
                "/admin/codes/bulk-delete",
                json={"codes": ["DELETE-FAIL-USED", "DELETE-FAIL-PENDING"]},
            )

        body = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(body["success"])
        self.assertFalse(body["partial"])
        self.assertEqual(body["success_count"], 0)
        self.assertEqual(body["failed_count"], 2)
        self.assertIn("DELETE-FAIL-USED", body["error"])
        self.assertIn("DELETE-FAIL-PENDING", body["errors"][1])
        self.assertEqual(
            self._run_async(self._get_existing_codes()),
            ["DELETE-FAIL-PENDING", "DELETE-FAIL-USED"],
        )

    def test_records_page_keeps_local_date_filter_and_pagination_behavior(self):
        team = self._run_async(self._create_team(team_name="Records Filter Team", account_id="records-filter-acc"))
        in_range_older = datetime(2026, 4, 2, 16, 30, tzinfo=timezone.utc)   # 2026-04-03 00:30 Asia/Shanghai
        in_range_newer = datetime(2026, 4, 3, 15, 30, tzinfo=timezone.utc)   # 2026-04-03 23:30 Asia/Shanghai
        out_of_range = datetime(2026, 4, 3, 16, 30, tzinfo=timezone.utc)     # 2026-04-04 00:30 Asia/Shanghai
        self._run_async(
            self._insert_objects(
                RedemptionCode(code="RECORD-PAGE-OLDER", status="used"),
                RedemptionCode(code="RECORD-PAGE-NEWER", status="used"),
                RedemptionCode(code="RECORD-PAGE-OUTSIDE", status="used"),
                RedemptionCode(code="RECORD-PAGE-OTHER-EMAIL", status="used"),
                RedemptionRecord(
                    email="user@example.com",
                    code="RECORD-PAGE-OLDER",
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=in_range_older,
                ),
                RedemptionRecord(
                    email="user@example.com",
                    code="RECORD-PAGE-NEWER",
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=in_range_newer,
                ),
                RedemptionRecord(
                    email="user@example.com",
                    code="RECORD-PAGE-OUTSIDE",
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=out_of_range,
                ),
                RedemptionRecord(
                    email="other@example.com",
                    code="RECORD-PAGE-OTHER-EMAIL",
                    team_id=team.id,
                    account_id=team.account_id,
                    redeemed_at=in_range_newer,
                ),
            )
        )

        with TestClient(self.app) as client:
            response = client.get(
                f"/admin/records?email=user@example.com&team_id={team.id}&start_date=2026-04-03"
                "&end_date=2026-04-03&page=2&per_page=1"
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("2 条记录", response.text)
        self.assertIn("RECORD-PAGE-OLDER", response.text)
        self.assertNotIn("RECORD-PAGE-NEWER", response.text)
        self.assertNotIn("RECORD-PAGE-OUTSIDE", response.text)
        self.assertNotIn("other@example.com", response.text)


class AdminUiContractRegressionTests(unittest.TestCase):
    def test_codes_template_exposes_bulk_delete_ui_and_endpoint(self):
        repo_root = Path(__file__).resolve().parents[1]
        template_text = (repo_root / "app" / "templates" / "admin" / "codes" / "index.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("批量删除", template_text)
        self.assertIn("handleBulkDeleteCodes()", template_text)
        self.assertIn("/admin/codes/bulk-delete", template_text)

    def test_login_template_reads_detail_before_error(self):
        repo_root = Path(__file__).resolve().parents[1]
        template_text = (repo_root / "app" / "templates" / "auth" / "login.html").read_text(encoding="utf-8")
        self.assertIn("data.detail || data.error || '登录失败'", template_text)

    def test_settings_template_reads_detail_before_error_for_password_failures(self):
        repo_root = Path(__file__).resolve().parents[1]
        template_text = (repo_root / "app" / "templates" / "admin" / "settings" / "index.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("showToast(data.detail || data.error || '修改失败', 'error');", template_text)


if __name__ == "__main__":
    unittest.main()
