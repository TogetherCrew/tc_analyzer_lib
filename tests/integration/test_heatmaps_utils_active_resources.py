from datetime import datetime
from unittest import IsolatedAsyncioTestCase

from tc_analyzer_lib.metrics.heatmaps.heatmaps_utils import HeatmapsUtils
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestHeatmapsUtilsActiveResources(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        client = MongoSingleton.get_instance(skip_singleton=True).get_client()
        self.platform_id = "1234567890"
        self.database = client[self.platform_id]
        self.database.drop_collection("rawmemberactivities")

        self.utils = HeatmapsUtils(self.platform_id)

    async def test_get_users_empty_collection(self):
        start_day = datetime(2024, 1, 1)
        end_day = datetime(2024, 1, 2)
        users = await self.utils.get_active_resources_period(
            start_day,
            end_day,
            resource_identifier="channel_id",
        )
        self.assertEqual(list(users), [])

    async def test_get_multiple_users(self):
        start_day = datetime(2024, 1, 1)
        end_day = datetime(2024, 1, 2)
        samples = [
            {
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": "user1",
                "date": datetime(2024, 1, 1, 1),
                "interactions": [
                    {
                        "name": "reply",
                        "type": "emitter",
                        "users_engaged_id": ["user2"],
                    }
                ],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "11111",
                    "thread_id": None,
                },
                "source_id": "11188143219343360",
            },
            {
                "actions": [],
                "author_id": "user2",
                "date": datetime(2024, 1, 1, 5),
                "interactions": [
                    {
                        "name": "reply",
                        "type": "receiver",
                        "users_engaged_id": ["user4", "user5"],
                    }
                ],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "22222",
                    "thread_id": None,
                },
                "source_id": "11188143219343361",
            },
            {
                "actions": [],
                "author_id": "user2",
                "date": datetime(2024, 1, 1, 5),
                "interactions": [
                    {
                        "name": "reply",
                        "type": "receiver",
                        "users_engaged_id": ["user4", "user5"],
                    }
                ],
                "metadata": {
                    "bot_activity": True,
                    "channel_id": "44444",
                    "thread_id": None,
                },
                "source_id": "11188143219343361",
            },
            {
                "actions": [],
                "author_id": "user3",
                "date": datetime(2024, 1, 2),
                "interactions": [
                    {"name": "reply", "type": "receiver", "users_engaged_id": ["user6"]}
                ],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "33333",
                    "thread_id": None,
                },
                "source_id": "11188143219343361",
            },
        ]
        self.database["rawmemberactivities"].insert_many(samples)

        users = await self.utils.get_active_resources_period(
            start_day,
            end_day,
            resource_identifier="channel_id",
        )

        self.assertEqual(set(users), set(["11111", "22222", "44444"]))

    async def test_get_multiple_users_with_metadata_filter(self):
        start_day = datetime(2024, 1, 1)
        end_day = datetime(2024, 1, 2)
        samples = [
            {
                "actions": [{"name": "message", "type": "emitter"}],
                "author_id": "user1",
                "date": datetime(2024, 1, 1, 1),
                "interactions": [
                    {
                        "name": "reply",
                        "type": "emitter",
                        "users_engaged_id": ["user2"],
                    }
                ],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "11111",
                    "thread_id": None,
                },
                "source_id": "11188143219343360",
            },
            {
                "actions": [],
                "author_id": "user2",
                "date": datetime(2024, 1, 1, 5),
                "interactions": [
                    {
                        "name": "reply",
                        "type": "receiver",
                        "users_engaged_id": ["user4", "user5"],
                    }
                ],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "22222",
                    "thread_id": None,
                },
                "source_id": "11188143219343361",
            },
            {
                "actions": [],
                "author_id": "user2",
                "date": datetime(2024, 1, 1, 5),
                "interactions": [
                    {
                        "name": "reply",
                        "type": "receiver",
                        "users_engaged_id": ["user4", "user5"],
                    }
                ],
                "metadata": {
                    "bot_activity": True,
                    "channel_id": "44444",
                    "thread_id": None,
                },
                "source_id": "11188143219343361",
            },
            {
                "actions": [],
                "author_id": "user3",
                "date": datetime(2024, 1, 2),
                "interactions": [
                    {"name": "reply", "type": "receiver", "users_engaged_id": ["user6"]}
                ],
                "metadata": {
                    "bot_activity": False,
                    "channel_id": "33333",
                    "thread_id": None,
                },
                "source_id": "11188143219343361",
            },
        ]
        self.database["rawmemberactivities"].insert_many(samples)

        users = await self.utils.get_active_resources_period(
            start_day,
            end_day,
            resource_identifier="channel_id",
            metadata_filter={"metadata.channel_id": {"$in": ["22222"]}},
        )

        self.assertEqual(set(users), set(["22222"]))
