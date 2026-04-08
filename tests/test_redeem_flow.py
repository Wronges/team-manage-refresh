import asyncio
import unittest
from datetime import timedelta
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.models import RedemptionCode, RedemptionRecord, Team, TeamEmailMapping, WARRANTY_TYPE_USES
from app.services.redeem_flow import RedeemFlowService
from app.services.notification import notification_service
from app.services.redemption import RedemptionService
from app.services.settings import settings_service
from app.services.team import TeamService
from app.services.warranty import WarrantyService
from app.utils.time_utils import get_now


class StubRedemptionService:
    async def validate_code(self, code, db_session):
        return {
            "success": True,
            "valid": True,
            "redemption_code": {
                "pool_type": "normal",
                "virtual_welfare_code": False,
            },
        }


class StubTeamService:
    def __init__(self, sync_results=None, active_team_ids_by_email=None, reserve_results=None):
        self.sync_results = sync_results or {}
        self.active_team_ids_by_email = {
            str(email).strip().lower(): set(team_ids)
            for email, team_ids in (active_team_ids_by_email or {}).items()
        }
        self.mapping_updates = []
        self.reserve_results = reserve_results or {}
        self.released_team_ids = []

    async def sync_team_info(self, team_id, db_session):
        team_results = (self.sync_results or {}).get(team_id, [])
        if team_results:
            result = team_results.pop(0)
            if team_results:
                return result
            self.sync_results[team_id] = [result]
            return result

        return {"success": True, "member_emails": [], "error": None}


    async def reserve_seat_if_available(self, team_id, db_session, pool_type="normal"):
        queued = self.reserve_results.get(team_id) or []
        if queued:
            result = queued.pop(0)
            if not queued:
                self.reserve_results[team_id] = [result]
            if result.get("success"):
                team = await db_session.get(Team, team_id)
                if team:
                    team.current_members += 1
                    if team.current_members >= team.max_members:
                        team.status = "full"
                result = {**result, "team": team}
            return result

        team = await db_session.get(Team, team_id)
        if not team or team.pool_type != pool_type or team.status != "active":
            return {"success": False, "error": f"目标 Team {team_id} 不可用"}
        if team.current_members >= team.max_members:
            team.status = "full"
            return {"success": False, "error": "该 Team 已满, 请选择其他 Team 尝试"}

        team.current_members += 1
        if team.current_members >= team.max_members:
            team.status = "full"
        return {"success": True, "team": team, "error": None}

    async def release_reserved_seat(self, team_id, db_session, pool_type="normal"):
        self.released_team_ids.append(team_id)
        team = await db_session.get(Team, team_id)
        if team and team.current_members > 0:
            team.current_members -= 1
            if team.current_members >= team.max_members:
                team.status = "full"
            else:
                team.status = "active"

    async def ensure_access_token(self, team, db_session):
        return "token"

    async def get_active_team_ids_for_email(self, email, db_session, pool_type=None):
        normalized_email = str(email).strip().lower()
        return sorted(self.active_team_ids_by_email.get(normalized_email, set()))

    async def upsert_team_email_mapping(self, team_id, email, status, db_session, source="sync"):
        normalized_email = str(email).strip().lower()
        self.mapping_updates.append((team_id, normalized_email, status, source))
        active_team_ids = self.active_team_ids_by_email.setdefault(normalized_email, set())
        if status in {"joined", "invited"}:
            active_team_ids.add(team_id)
        else:
            active_team_ids.discard(team_id)
        return None


class StubChatGPTService:
    def __init__(self, invite_results):
        self.invite_results = invite_results

    async def send_invite(self, access_token, account_id, email, db_session, identifier="default"):
        team_results = self.invite_results.get(account_id, [])
        if team_results:
            result = team_results.pop(0)
            if team_results:
                return result
            self.invite_results[account_id] = [result]
            return result

        return {"success": True, "data": {"account_invites": [{"email": email}]}}


class RedeemFlowServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(__import__("sqlalchemy").text("PRAGMA foreign_keys=ON"))

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def _seed_basic_data(self):
        async with self.session_factory() as session:
            team_1 = Team(
                id=1,
                email="owner-1@example.com",
                access_token_encrypted="token-1",
                account_id="acct-1",
                team_name="Team 1",
                current_members=3,
                max_members=6,
                status="active",
                pool_type="normal",
            )
            team_2 = Team(
                id=2,
                email="owner-2@example.com",
                access_token_encrypted="token-2",
                account_id="acct-2",
                team_name="Team 2",
                current_members=1,
                max_members=6,
                status="active",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="TEST-CODE-0001",
                status="unused",
                pool_type="normal",
                reusable_by_seat=False,
            )
            session.add_all([team_1, team_2, code])
            await session.commit()

    @staticmethod
    def _close_coro(coro):
        coro.close()
        return None

    @staticmethod
    async def _noop_async(*args, **kwargs):
        return None

    async def test_auto_select_skips_team_where_user_already_exists(self):
        await self._seed_basic_data()
        service = RedeemFlowService()
        service.redemption_service = StubRedemptionService()
        service.team_service = StubTeamService(
            active_team_ids_by_email={"user@example.com": [1]}
        )
        service.chatgpt_service = StubChatGPTService(
            {
                "acct-2": [{"success": True, "data": {"account_invites": [{"email": "user@example.com"}]}}],
            }
        )

        async with self.session_factory() as session:
            with patch("app.services.redeem_flow.asyncio.create_task", side_effect=self._close_coro):
                result = await service.redeem_and_join_team(
                    email="user@example.com",
                    code="TEST-CODE-0001",
                    team_id=None,
                    db_session=session,
                )

            self.assertTrue(result["success"])
            self.assertEqual(result["team_info"]["id"], 2)

            code = await session.get(RedemptionCode, 1)
            self.assertEqual(code.status, "used")
            self.assertEqual(code.used_team_id, 2)

            records = (await session.execute(select(RedemptionRecord))).scalars().all()
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].team_id, 2)

    async def test_sync_reconcile_requires_three_misses_before_removed(self):
        await self._seed_basic_data()
        team_service = TeamService.__new__(TeamService)

        async with self.session_factory() as session:
            await team_service.upsert_team_email_mapping(
                team_id=1,
                email="user@example.com",
                status="joined",
                db_session=session,
                source="sync",
            )
            await session.commit()

            for expected_missing_count in (1, 2):
                await team_service._reconcile_team_email_mappings(1, set(), set(), session)
                await session.commit()

                mapping = (
                    await session.execute(
                        select(TeamEmailMapping).where(
                            TeamEmailMapping.team_id == 1,
                            TeamEmailMapping.email == "user@example.com",
                        )
                    )
                ).scalar_one()
                self.assertEqual(mapping.status, "joined")
                self.assertEqual(mapping.missing_sync_count, expected_missing_count)

            await team_service._reconcile_team_email_mappings(1, set(), set(), session)
            await session.commit()

            mapping = (
                await session.execute(
                    select(TeamEmailMapping).where(
                        TeamEmailMapping.team_id == 1,
                        TeamEmailMapping.email == "user@example.com",
                    )
                )
            ).scalar_one()
            self.assertEqual(mapping.status, "removed")
            self.assertEqual(mapping.missing_sync_count, 3)

    async def test_sync_reconcile_resets_missing_counter_when_email_returns(self):
        await self._seed_basic_data()
        team_service = TeamService.__new__(TeamService)

        async with self.session_factory() as session:
            await team_service.upsert_team_email_mapping(
                team_id=1,
                email="user@example.com",
                status="joined",
                db_session=session,
                source="sync",
            )
            await session.commit()

            await team_service._reconcile_team_email_mappings(1, set(), set(), session)
            await session.commit()

            await team_service._reconcile_team_email_mappings(1, {"user@example.com"}, set(), session)
            await session.commit()

            mapping = (
                await session.execute(
                    select(TeamEmailMapping).where(
                        TeamEmailMapping.team_id == 1,
                        TeamEmailMapping.email == "user@example.com",
                    )
                )
            ).scalar_one()
            self.assertEqual(mapping.status, "joined")
            self.assertEqual(mapping.missing_sync_count, 0)


    async def test_virtual_welfare_code_creates_shadow_code_for_redemption_record(self):
        async with self.session_factory() as session:
            team = Team(
                id=10,
                email="welfare-owner@example.com",
                access_token_encrypted="token-10",
                account_id="acct-welfare",
                team_name="Welfare Team",
                current_members=1,
                max_members=6,
                status="active",
                pool_type="welfare",
            )
            session.add(team)
            await session.commit()

            service = RedeemFlowService()
            shadow = await service.redemption_service.ensure_virtual_welfare_shadow_code(session, "WELF-TEST-CODE")
            await session.commit()

            self.assertIsNotNone(shadow)
            self.assertEqual(shadow.code, "WELF-TEST-CODE")
            self.assertEqual(shadow.pool_type, "welfare")
            self.assertTrue(shadow.reusable_by_seat)

            record = RedemptionRecord(
                email="user@example.com",
                code="WELF-TEST-CODE",
                team_id=10,
                account_id="acct-welfare",
            )
            session.add(record)
            await session.commit()

            stored_record = (await session.execute(select(RedemptionRecord).where(RedemptionRecord.code == "WELF-TEST-CODE"))).scalar_one()
            self.assertEqual(stored_record.team_id, 10)


    async def test_delete_used_normal_code_with_history_is_blocked(self):
        async with self.session_factory() as session:
            team = Team(
                id=20,
                email="normal-owner@example.com",
                access_token_encrypted="token-20",
                account_id="acct-normal",
                team_name="Normal Team",
                current_members=2,
                max_members=6,
                status="active",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="NORMAL-CODE-DELETE",
                status="used",
                used_by_email="user@example.com",
                used_team_id=20,
                pool_type="normal",
            )
            session.add(team)
            await session.commit()

            session.add(code)
            await session.commit()

            session.add(
                RedemptionRecord(
                    email="user@example.com",
                    code="NORMAL-CODE-DELETE",
                    team_id=20,
                    account_id="acct-normal",
                )
            )
            await session.commit()

            service = RedemptionService()
            result = await service.delete_code("NORMAL-CODE-DELETE", session)

            self.assertFalse(result["success"])
            self.assertIn("无法直接删除", result["error"])

            remaining_code = (
                await session.execute(
                    select(RedemptionCode).where(RedemptionCode.code == "NORMAL-CODE-DELETE")
                )
            ).scalar_one_or_none()
            self.assertIsNotNone(remaining_code)

    async def test_atomic_seat_reservation_prevents_over_allocation(self):
        async with self.session_factory() as session:
            team = Team(
                id=30,
                email="capacity-owner@example.com",
                access_token_encrypted="token-30",
                account_id="acct-capacity",
                team_name="Capacity Team",
                current_members=5,
                max_members=6,
                status="active",
                pool_type="normal",
            )
            session.add(team)
            await session.commit()

        async with self.session_factory() as session_one, self.session_factory() as session_two:
            team_service = TeamService()
            reserve_one, reserve_two = await asyncio.gather(
                team_service.reserve_seat_if_available(30, session_one, pool_type="normal"),
                team_service.reserve_seat_if_available(30, session_two, pool_type="normal"),
            )

            successes = [result for result in (reserve_one, reserve_two) if result["success"]]
            failures = [result for result in (reserve_one, reserve_two) if not result["success"]]

            self.assertEqual(len(successes), 1)
            self.assertEqual(len(failures), 1)
            self.assertIn("已满", failures[0]["error"])

            await session_one.commit()
            await session_two.rollback()

        async with self.session_factory() as verify_session:
            stored_team = await verify_session.get(Team, 30)
            self.assertIsNotNone(stored_team)
            self.assertEqual(stored_team.current_members, 6)
            self.assertEqual(stored_team.status, "full")

    async def test_locked_team_returns_conflict_without_consuming_code(self):
        await self._seed_basic_data()
        service = RedeemFlowService()
        service.redemption_service = StubRedemptionService()
        service.team_service = StubTeamService(
            active_team_ids_by_email={"user@example.com": [1]}
        )
        service.chatgpt_service = StubChatGPTService({})

        async with self.session_factory() as session:
            with patch("app.services.redeem_flow.asyncio.create_task", side_effect=self._close_coro):
                result = await service.redeem_and_join_team(
                    email="user@example.com",
                    code="TEST-CODE-0001",
                    team_id=1,
                    db_session=session,
                )

            self.assertFalse(result["success"])
            self.assertIn("当前兑换码不会被消耗", result["error"])

            code = await session.get(RedemptionCode, 1)
            self.assertEqual(code.status, "unused")
            self.assertIsNone(code.used_team_id)

            records = (await session.execute(select(RedemptionRecord))).scalars().all()
            self.assertEqual(records, [])

            team_1 = await session.get(Team, 1)
            self.assertEqual(team_1.current_members, 3)

    async def test_auto_retry_when_invite_api_reports_user_already_in_team(self):
        await self._seed_basic_data()
        service = RedeemFlowService()
        service.redemption_service = StubRedemptionService()
        service.team_service = StubTeamService()
        service.chatgpt_service = StubChatGPTService(
            {
                "acct-1": [{"success": False, "error": "Already in workspace"}],
                "acct-2": [{"success": True, "data": {"account_invites": [{"email": "user@example.com"}]}}],
            }
        )

        async with self.session_factory() as session:
            with patch("app.services.redeem_flow.asyncio.create_task", side_effect=self._close_coro):
                result = await service.redeem_and_join_team(
                    email="user@example.com",
                    code="TEST-CODE-0001",
                    team_id=None,
                    db_session=session,
                )

            self.assertTrue(result["success"])
            self.assertEqual(result["team_info"]["id"], 2)

            code = await session.get(RedemptionCode, 1)
            self.assertEqual(code.used_team_id, 2)

            records = (await session.execute(select(RedemptionRecord))).scalars().all()
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].team_id, 2)

    async def test_validate_code_rejects_expired_warranty_code(self):
        async with self.session_factory() as session:
            code = RedemptionCode(
                code="WARRANTY-EXPIRED-001",
                status="used",
                has_warranty=True,
                warranty_days=30,
                used_at=get_now() - timedelta(days=40),
                warranty_expires_at=get_now() - timedelta(days=10),
            )
            session.add(code)
            await session.commit()

            service = RedemptionService()
            result = await service.validate_code("WARRANTY-EXPIRED-001", session)

            self.assertTrue(result["success"])
            self.assertFalse(result["valid"])
            self.assertEqual(result["reason"], "质保已过期")

            refreshed_code = (
                await session.execute(
                    select(RedemptionCode).where(RedemptionCode.code == "WARRANTY-EXPIRED-001")
                )
            ).scalar_one()
            self.assertEqual(refreshed_code.status, "expired")

    async def test_warranty_reuse_rejects_email_handoff(self):
        async with self.session_factory() as session:
            team = Team(
                id=40,
                email="owner-40@example.com",
                access_token_encrypted="token-40",
                account_id="acct-40",
                team_name="Old Team",
                current_members=6,
                max_members=6,
                status="expired",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="WARRANTY-HANDOFF-001",
                status="used",
                has_warranty=True,
                warranty_days=30,
                used_at=get_now() - timedelta(days=3),
                warranty_expires_at=get_now() + timedelta(days=27),
            )
            record = RedemptionRecord(
                email="buyer@example.com",
                code="WARRANTY-HANDOFF-001",
                team_id=40,
                account_id="acct-40",
                is_warranty_redemption=False,
            )
            session.add_all([team, code, record])
            await session.commit()

            service = WarrantyService()
            result = await service.validate_warranty_reuse(
                session,
                "WARRANTY-HANDOFF-001",
                "attacker@example.com",
            )

            self.assertTrue(result["success"])
            self.assertFalse(result["can_reuse"])
            self.assertIn("仅限原使用邮箱", result["reason"])

    async def test_warranty_check_keeps_record_when_sync_misses_member(self):
        async with self.session_factory() as session:
            team = Team(
                id=50,
                email="owner-50@example.com",
                access_token_encrypted="token-50",
                account_id="acct-50",
                team_name="Sync Team",
                current_members=2,
                max_members=6,
                status="active",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="WARRANTY-CHECK-001",
                status="used",
                has_warranty=True,
                warranty_days=30,
                used_at=get_now() - timedelta(days=1),
                warranty_expires_at=get_now() + timedelta(days=29),
            )
            record = RedemptionRecord(
                email="buyer@example.com",
                code="WARRANTY-CHECK-001",
                team_id=50,
                account_id="acct-50",
            )
            session.add_all([team, code, record])
            await session.commit()

            service = WarrantyService()
            service.team_service = StubTeamService(sync_results={50: [{"success": True, "member_emails": []}]})

            result = await service.check_warranty_status(
                session,
                code="WARRANTY-CHECK-001",
            )

            self.assertTrue(result["success"])
            self.assertTrue(result["has_warranty"])
            self.assertEqual(len(result["records"]), 1)
            self.assertEqual(result["records"][0]["team_status"], "suspected_inconsistent")
            self.assertIn("保留原始记录", result["message"])

            stored_records = (
                await session.execute(
                    select(RedemptionRecord).where(RedemptionRecord.code == "WARRANTY-CHECK-001")
                )
            ).scalars().all()
            self.assertEqual(len(stored_records), 1)

    async def test_warranty_reuse_allows_original_email_after_orphan_cleanup(self):
        async with self.session_factory() as session:
            team = Team(
                id=51,
                email="owner-51@example.com",
                access_token_encrypted="token-51",
                account_id="acct-51",
                team_name="Ghost Team",
                current_members=2,
                max_members=6,
                status="active",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="WARRANTY-GHOST-001",
                status="used",
                has_warranty=True,
                warranty_days=30,
                used_at=get_now() - timedelta(days=2),
                warranty_expires_at=get_now() + timedelta(days=28),
            )
            record = RedemptionRecord(
                email="buyer@example.com",
                code="WARRANTY-GHOST-001",
                team_id=51,
                account_id="acct-51",
            )
            session.add_all([team, code, record])
            await session.commit()

            service = WarrantyService()
            service.team_service = StubTeamService(sync_results={51: [{"success": True, "member_emails": []}]})

            result = await service.validate_warranty_reuse(
                session,
                "WARRANTY-GHOST-001",
                "buyer@example.com",
            )

            self.assertTrue(result["success"])
            self.assertTrue(result["can_reuse"])
            self.assertIn("已自动修复", result["reason"])

            stored_records = (
                await session.execute(
                    select(RedemptionRecord).where(RedemptionRecord.code == "WARRANTY-GHOST-001")
                )
            ).scalars().all()
            self.assertEqual(stored_records, [])

    async def test_seat_rolls_back_after_full_error(self):
        await self._seed_basic_data()
        service = RedeemFlowService()
        service.redemption_service = StubRedemptionService()
        service.team_service = StubTeamService()
        service.chatgpt_service = StubChatGPTService(
            {
                "acct-1": [{"success": False, "error": "maximum number of seats reached"}],
            }
        )

        async with self.session_factory() as session:
            team_1 = await session.get(Team, 1)
            team_1.current_members = 5
            team_1.max_members = 6
            await session.commit()

            with patch("app.services.redeem_flow.asyncio.create_task", side_effect=self._close_coro):
                result = await service.redeem_and_join_team(
                    email="user@example.com",
                    code="TEST-CODE-0001",
                    team_id=1,
                    db_session=session,
                )

            self.assertFalse(result["success"])
            self.assertIn("席位已满", result["error"])

            refreshed_team = await session.get(Team, 1)
            self.assertEqual(refreshed_team.current_members, 5)
            self.assertEqual(refreshed_team.status, "active")

    async def test_virtual_welfare_code_usage_does_not_double_decrement_remaining(self):
        async with self.session_factory() as session:
            welfare_team = Team(
                id=60,
                email="welfare-owner@example.com",
                access_token_encrypted="token-60",
                account_id="acct-60",
                team_name="Welfare Pool",
                current_members=2,
                max_members=5,
                status="active",
                pool_type="welfare",
            )
            session.add(welfare_team)
            await session.commit()

            service = RedemptionService()
            await service.ensure_virtual_welfare_shadow_code(session, "WELF-CODE-001")
            settings_service.clear_cache()
            await settings_service.update_setting(session, "welfare_common_code", "WELF-CODE-001")
            session.add_all([
                RedemptionRecord(
                    email="one@example.com",
                    code="WELF-CODE-001",
                    team_id=60,
                    account_id="acct-60",
                ),
                RedemptionRecord(
                    email="two@example.com",
                    code="WELF-CODE-001",
                    team_id=60,
                    account_id="acct-60",
                ),
            ])
            await session.commit()

            usage = await service.get_virtual_welfare_code_usage(session, welfare_code="WELF-CODE-001")
            self.assertEqual(usage["used_count"], 2)
            self.assertEqual(usage["usable_capacity"], 3)
            self.assertEqual(usage["remaining_count"], 3)

            result = await service.validate_code("WELF-CODE-001", session)
            self.assertTrue(result["success"])
            self.assertTrue(result["valid"])
            self.assertEqual(result["redemption_code"]["limit"], 3)
            self.assertEqual(result["redemption_code"]["used_count"], 2)

    async def test_virtual_welfare_code_handles_concurrent_redemptions_up_to_capacity(self):
        async with self.session_factory() as session:
            welfare_team = Team(
                id=61,
                email="welfare-owner-61@example.com",
                access_token_encrypted="token-61",
                account_id="acct-61",
                team_name="Welfare Concurrent Team",
                current_members=0,
                max_members=5,
                status="active",
                pool_type="welfare",
            )
            session.add_all([
                welfare_team,
            ])
            await session.commit()
            settings_service.clear_cache()
            await settings_service.update_setting(session, "welfare_common_code", "WELF-CONCURRENT-001")

            service = RedeemFlowService()
            service.team_service = StubTeamService()
            service.chatgpt_service = StubChatGPTService(
                {
                    "acct-61": [
                        {"success": True, "data": {"account_invites": [{"email": f"user{i}@example.com"}]}}
                        for i in range(6)
                    ]
                }
            )

            async def redeem(email):
                async with self.session_factory() as inner_session:
                    with patch.object(service, "_background_verify_sync", new=self._noop_async), \
                         patch.object(notification_service, "check_and_notify_low_stock", new=self._noop_async):
                        return await service.redeem_and_join_team(
                            email=email,
                            code="WELF-CONCURRENT-001",
                            team_id=None,
                            db_session=inner_session,
                        )

            results = await asyncio.gather(*[
                redeem(f"user{i}@example.com")
                for i in range(6)
            ])

            success_count = sum(1 for result in results if result["success"])
            failure_count = sum(1 for result in results if not result["success"])
            self.assertEqual(success_count, 5)
            self.assertEqual(failure_count, 1)

            async with self.session_factory() as verify_session:
                stored_team = await verify_session.get(Team, 61)
                self.assertEqual(stored_team.current_members, 5)
                self.assertEqual(stored_team.status, "full")

                records = (
                    await verify_session.execute(
                        select(RedemptionRecord).where(RedemptionRecord.code == "WELF-CONCURRENT-001")
                    )
                ).scalars().all()
                self.assertEqual(len(records), 5)

                usage = await RedemptionService().get_virtual_welfare_code_usage(
                    verify_session,
                    welfare_code="WELF-CONCURRENT-001",
                )
                self.assertEqual(usage["remaining_count"], 0)


class WarrantyUsesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(__import__("sqlalchemy").text("PRAGMA foreign_keys=ON"))

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_usage_based_warranty_allows_one_more_redeem_when_quota_remains(self):
        async with self.session_factory() as session:
            team = Team(
                id=101,
                email="owner-101@example.com",
                access_token_encrypted="token-101",
                account_id="acct-101",
                team_name="Banned Team",
                current_members=0,
                max_members=6,
                status="banned",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="USES-WARRANTY-001",
                status="used",
                has_warranty=True,
                warranty_type=WARRANTY_TYPE_USES,
                warranty_uses=1,
                used_at=get_now() - timedelta(days=1),
                pool_type="normal",
            )
            record = RedemptionRecord(
                email="user@example.com",
                code="USES-WARRANTY-001",
                team_id=101,
                account_id="acct-101",
                redeemed_at=get_now() - timedelta(days=1),
            )
            session.add_all([team, code, record])
            await session.commit()

            service = WarrantyService()
            service.team_service = StubTeamService()

            result = await service.validate_warranty_reuse(session, "USES-WARRANTY-001", "user@example.com")

            self.assertTrue(result["success"])
            self.assertTrue(result["can_reuse"])

    async def test_usage_based_warranty_ignores_orphan_record_before_counting_remaining_uses(self):
        async with self.session_factory() as session:
            banned_team = Team(
                id=102,
                email="owner-102@example.com",
                access_token_encrypted="token-102",
                account_id="acct-102",
                team_name="Banned Team",
                current_members=0,
                max_members=6,
                status="banned",
                pool_type="normal",
            )
            orphan_team = Team(
                id=103,
                email="owner-103@example.com",
                access_token_encrypted="token-103",
                account_id="acct-103",
                team_name="Orphan Team",
                current_members=1,
                max_members=6,
                status="active",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="USES-WARRANTY-ORPHAN-001",
                status="used",
                has_warranty=True,
                warranty_type=WARRANTY_TYPE_USES,
                warranty_uses=1,
                used_at=get_now() - timedelta(days=1),
                pool_type="normal",
            )
            session.add_all([
                banned_team,
                orphan_team,
                code,
                RedemptionRecord(
                    email="user@example.com",
                    code="USES-WARRANTY-ORPHAN-001",
                    team_id=102,
                    account_id="acct-102",
                    redeemed_at=get_now() - timedelta(days=2),
                ),
                RedemptionRecord(
                    email="ghost@example.com",
                    code="USES-WARRANTY-ORPHAN-001",
                    team_id=103,
                    account_id="acct-103",
                    redeemed_at=get_now() - timedelta(days=1),
                    is_warranty_redemption=True,
                ),
            ])
            await session.commit()

            service = WarrantyService()
            service.team_service = StubTeamService(
                sync_results={103: [{"success": True, "member_emails": []}]}
            )

            result = await service.validate_warranty_reuse(
                session,
                "USES-WARRANTY-ORPHAN-001",
                "user@example.com",
            )

            self.assertTrue(result["success"])
            self.assertTrue(result["can_reuse"])

            stored_records = (
                await session.execute(
                    select(RedemptionRecord).where(RedemptionRecord.code == "USES-WARRANTY-ORPHAN-001")
                )
            ).scalars().all()
            self.assertEqual(len(stored_records), 1)
            self.assertEqual(stored_records[0].team_id, 102)

    async def test_usage_based_warranty_status_uses_real_reuse_validation(self):
        async with self.session_factory() as session:
            banned_team = Team(
                id=104,
                email="owner-104@example.com",
                access_token_encrypted="token-104",
                account_id="acct-104",
                team_name="Banned Team",
                current_members=0,
                max_members=6,
                status="banned",
                pool_type="normal",
            )
            active_team = Team(
                id=105,
                email="owner-105@example.com",
                access_token_encrypted="token-105",
                account_id="acct-105",
                team_name="Active Team",
                current_members=1,
                max_members=6,
                status="active",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="USES-WARRANTY-STATUS-001",
                status="used",
                has_warranty=True,
                warranty_type=WARRANTY_TYPE_USES,
                warranty_uses=2,
                used_at=get_now() - timedelta(hours=1),
                pool_type="normal",
            )
            session.add_all([
                banned_team,
                active_team,
                code,
                RedemptionRecord(
                    email="user@example.com",
                    code="USES-WARRANTY-STATUS-001",
                    team_id=104,
                    account_id="acct-104",
                    redeemed_at=get_now() - timedelta(days=2),
                ),
                RedemptionRecord(
                    email="user@example.com",
                    code="USES-WARRANTY-STATUS-001",
                    team_id=105,
                    account_id="acct-105",
                    redeemed_at=get_now() - timedelta(hours=1),
                    is_warranty_redemption=True,
                ),
            ])
            await session.commit()

            service = WarrantyService()
            service.team_service = StubTeamService(
                sync_results={105: [{"success": True, "member_emails": ["user@example.com"]}]}
            )

            status_result = await service.check_warranty_status(
                session,
                code="USES-WARRANTY-STATUS-001",
            )

            self.assertTrue(status_result["success"])
            self.assertTrue(status_result["warranty_valid"])
            self.assertFalse(status_result["can_reuse"])
            self.assertEqual(status_result["warranty_uses_remaining"], 1)

    async def test_usage_based_warranty_blocks_redeem_after_quota_is_exhausted(self):
        async with self.session_factory() as session:
            team_1 = Team(
                id=111,
                email="owner-111@example.com",
                access_token_encrypted="token-111",
                account_id="acct-111",
                team_name="Banned Team 1",
                current_members=0,
                max_members=6,
                status="banned",
                pool_type="normal",
            )
            team_2 = Team(
                id=112,
                email="owner-112@example.com",
                access_token_encrypted="token-112",
                account_id="acct-112",
                team_name="Banned Team 2",
                current_members=0,
                max_members=6,
                status="banned",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="USES-WARRANTY-002",
                status="used",
                has_warranty=True,
                warranty_type=WARRANTY_TYPE_USES,
                warranty_uses=1,
                used_at=get_now() - timedelta(days=2),
                pool_type="normal",
            )
            records = [
                RedemptionRecord(
                    email="user@example.com",
                    code="USES-WARRANTY-002",
                    team_id=111,
                    account_id="acct-111",
                    redeemed_at=get_now() - timedelta(days=2),
                ),
                RedemptionRecord(
                    email="user@example.com",
                    code="USES-WARRANTY-002",
                    team_id=112,
                    account_id="acct-112",
                    redeemed_at=get_now() - timedelta(days=1),
                    is_warranty_redemption=True,
                ),
            ]
            session.add_all([team_1, team_2, code, *records])
            await session.commit()

            service = WarrantyService()
            service.team_service = StubTeamService()

            result = await service.validate_warranty_reuse(session, "USES-WARRANTY-002", "user@example.com")
            status_result = await service.check_warranty_status(session, code="USES-WARRANTY-002")

            self.assertTrue(result["success"])
            self.assertFalse(result["can_reuse"])
            self.assertIn("质保次数已用完", result["reason"])
            self.assertTrue(status_result["success"])
            self.assertEqual(status_result["warranty_type"], WARRANTY_TYPE_USES)
            self.assertEqual(status_result["warranty_uses_remaining"], 0)
            self.assertFalse(status_result["warranty_valid"])
            self.assertFalse(status_result["can_reuse"])

    async def test_usage_based_warranty_exhausted_codes_are_invalid_cleanup_candidates(self):
        async with self.session_factory() as session:
            team_1 = Team(
                id=121,
                email="owner-121@example.com",
                access_token_encrypted="token-121",
                account_id="acct-121",
                team_name="Expired Banned Team 1",
                current_members=0,
                max_members=6,
                status="banned",
                pool_type="normal",
            )
            team_2 = Team(
                id=122,
                email="owner-122@example.com",
                access_token_encrypted="token-122",
                account_id="acct-122",
                team_name="Expired Banned Team 2",
                current_members=0,
                max_members=6,
                status="banned",
                pool_type="normal",
            )
            code = RedemptionCode(
                code="USES-WARRANTY-CLEANUP-001",
                status="used",
                has_warranty=True,
                warranty_type=WARRANTY_TYPE_USES,
                warranty_uses=1,
                used_at=get_now() - timedelta(days=31),
                pool_type="normal",
            )
            session.add_all([
                team_1,
                team_2,
                code,
                RedemptionRecord(
                    email="user@example.com",
                    code="USES-WARRANTY-CLEANUP-001",
                    team_id=121,
                    account_id="acct-121",
                    redeemed_at=get_now() - timedelta(days=45),
                ),
                RedemptionRecord(
                    email="user@example.com",
                    code="USES-WARRANTY-CLEANUP-001",
                    team_id=122,
                    account_id="acct-122",
                    redeemed_at=get_now() - timedelta(days=31),
                    is_warranty_redemption=True,
                ),
            ])
            await session.commit()

            result = await RedemptionService().get_invalid_code_candidates(session)

            self.assertTrue(result["success"])
            cleanup_entry = next(
                (item for item in result["codes"] if item["code"] == "USES-WARRANTY-CLEANUP-001"),
                None,
            )
            self.assertIsNotNone(cleanup_entry)
            self.assertEqual(cleanup_entry["reason"], "质保次数已用完")

    async def test_usage_based_warranty_count_prefers_explicit_warranty_redemption_flag(self):
        async with self.session_factory() as session:
            code = RedemptionCode(
                code="USES-WARRANTY-FLAG-001",
                status="used",
                has_warranty=True,
                warranty_type=WARRANTY_TYPE_USES,
                warranty_uses=1,
                used_at=get_now() - timedelta(days=1),
                pool_type="normal",
            )
            session.add(code)
            session.add_all([
                Team(
                    id=131,
                    email="owner-131@example.com",
                    access_token_encrypted="token-131",
                    account_id="acct-131",
                    team_name="Original Team",
                    current_members=0,
                    max_members=6,
                    status="banned",
                    pool_type="normal",
                ),
                Team(
                    id=132,
                    email="owner-132@example.com",
                    access_token_encrypted="token-132",
                    account_id="acct-132",
                    team_name="Warranty Team",
                    current_members=0,
                    max_members=6,
                    status="banned",
                    pool_type="normal",
                ),
            ])
            session.add_all([
                RedemptionRecord(
                    email="user@example.com",
                    code="USES-WARRANTY-FLAG-001",
                    team_id=131,
                    account_id="acct-131",
                    redeemed_at=get_now() - timedelta(days=5),
                    is_warranty_redemption=False,
                ),
                RedemptionRecord(
                    email="user@example.com",
                    code="USES-WARRANTY-FLAG-001",
                    team_id=132,
                    account_id="acct-132",
                    redeemed_at=get_now() - timedelta(days=1),
                    is_warranty_redemption=True,
                ),
            ])
            await session.commit()

            await session.execute(
                __import__("sqlalchemy").delete(RedemptionRecord).where(
                    RedemptionRecord.code == "USES-WARRANTY-FLAG-001",
                    RedemptionRecord.team_id == 131,
                )
            )
            await session.commit()

            service = RedemptionService()
            refreshed_code = (
                await session.execute(
                    select(RedemptionCode).where(RedemptionCode.code == "USES-WARRANTY-FLAG-001")
                )
            ).scalar_one()

            remaining_uses = await service.get_remaining_warranty_uses(session, refreshed_code)
            validate_result = await service.validate_code("USES-WARRANTY-FLAG-001", session)

            self.assertEqual(remaining_uses, 0)
            self.assertTrue(validate_result["success"])
            self.assertFalse(validate_result["valid"])
            self.assertEqual(validate_result["reason"], "质保次数已用完")
