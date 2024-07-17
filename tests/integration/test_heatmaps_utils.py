from datetime import datetime
from typing import Any, Coroutine
from unittest import IsolatedAsyncioTestCase

from tc_analyzer_lib.metrics.heatmaps.heatmaps_utils import HeatmapsUtils
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestHeatmapsUtils(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> Coroutine[Any, Any, None]:
        self.platform_id = "1234567890"
        self.mongo_singleton = MongoSingleton.get_instance(skip_singleton=True)
        client = self.mongo_singleton.get_async_client()
        self.database = client[self.platform_id]
        self.utils = HeatmapsUtils(self.platform_id)
        await self.database.drop_collection("rawmembers")

    async def test_get_users_empty_collection(self):
        cursor = await self.utils.get_users()
        users = []
        async for user in cursor:
            users.append(user)
        self.assertEqual(users, [])

        self.database.client.close()

    async def test_get_real_users(self):
        sample_users = [
            {
                "id": 9000,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9001,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9002,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2024, 1, 1),
                "options": {},
            },
        ]
        await self.database["rawmembers"].insert_many(sample_users)

        cursor = await self.utils.get_users()
        users = []
        async for user in cursor:
            users.append(user)
        self.assertEqual(users, [{"id": 9000}, {"id": 9001}])
        self.database.client.close()

    async def test_get_bots(self):
        sample_users = [
            {
                "id": 9000,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 6, 2),
                "options": {},
            },
            {
                "id": 9001,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9002,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2024, 1, 1),
                "options": {},
            },
        ]
        await self.database["rawmembers"].insert_many(sample_users)

        cursor = await self.utils.get_users(is_bot=True)
        users = []
        async for user in cursor:
            users.append(user)

        self.assertEqual(users, [{"id": 9001}, {"id": 9002}])

    async def test_get_users_count_empty_data(self):
        count = await self.utils.get_users_count()
        self.assertIsInstance(count, int)
        self.assertEqual(count, 0)

    async def test_get_users_count_real_users(self):
        sample_users = [
            {
                "id": 9000,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 6, 2),
                "options": {},
            },
            {
                "id": 9001,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9002,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2024, 1, 1),
                "options": {},
            },
        ]
        await self.database["rawmembers"].insert_many(sample_users)

        count = await self.utils.get_users_count()
        self.assertIsInstance(count, int)
        self.assertEqual(count, 2)

    async def test_get_users_count_bots(self):
        sample_users = [
            {
                "id": 9000,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 6, 2),
                "options": {},
            },
            {
                "id": 9001,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2023, 6, 1),
                "options": {},
            },
            {
                "id": 9002,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2024, 1, 1),
                "options": {},
            },
            {
                "id": 9003,
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2024, 2, 1),
                "options": {},
            },
            {
                "id": 9004,
                "is_bot": True,
                "left_at": None,
                "joined_at": datetime(2024, 2, 3),
                "options": {},
            },
        ]
        await self.database["rawmembers"].insert_many(sample_users)

        count = await self.utils.get_users_count(is_bot=True)
        self.assertIsInstance(count, int)
        self.assertEqual(count, 3)

    async def test_get_last_date_no_document(self):
        await self.database.drop_collection("heatmaps")

        last_date = await self.utils.get_last_date()

        self.assertIsNone(last_date)

    async def test_get_last_date_single_document(self):
        await self.database.drop_collection("heatmaps")

        document = {
            "user": 9000,
            "channel_id": "124",
            "date": datetime(2023, 1, 1),
            "hourly_analytics": [],
            "raw_analytics": [],
        }
        await self.database["heatmaps"].insert_one(document)

        last_date = await self.utils.get_last_date()
        self.assertEqual(last_date, datetime(2023, 1, 1))

    async def test_get_last_date_multiple_documents(self):
        await self.database.drop_collection("heatmaps")

        documents = [
            {
                "user": 9000,
                "channel_id": "124",
                "date": datetime(2023, 1, 1),
                "hourly_analytics": [],
                "raw_analytics": [],
            },
            {
                "user": 9000,
                "channel_id": "124",
                "date": datetime(2023, 1, 2),
                "hourly_analytics": [],
                "raw_analytics": [],
            },
            {
                "user": 9001,
                "channel_id": "126",
                "date": datetime(2023, 1, 3),
                "hourly_analytics": [],
                "raw_analytics": [],
            },
        ]
        await self.database["heatmaps"].insert_many(documents)

        last_date = await self.utils.get_last_date()
        self.assertEqual(last_date, datetime(2023, 1, 3))
