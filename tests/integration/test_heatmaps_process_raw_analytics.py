from typing import Any, Coroutine
from datetime import datetime
from unittest import IsolatedAsyncioTestCase

from tc_analyzer_lib.metrics.heatmaps import Heatmaps
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestHeatmapsProcessRawAnalyticsSingleDay(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> Coroutine[Any, Any, None]:
        self.mongo_client = MongoSingleton.get_instance(
            skip_singleton=True
        ).get_async_client()
        await self.mongo_client[self.platform_id].drop_collection("rawmemberactivities")

    async def asyncTearDown(self) -> Coroutine[Any, Any, None]:
        await self.mongo_client.drop_database(self.platform_id)

    def setUp(self) -> None:
        self.platform_id = "1234567890"
        period = datetime(2024, 1, 1)
        resources = list["123", "124", "125"]
        # using one of the configs we currently have
        # it could be any other platform's config
        discord_analyzer_config = DiscordAnalyzerConfig()

        self.heatmaps = Heatmaps(
            platform_id=self.platform_id,
            period=period,
            resources=resources,
            analyzer_config=discord_analyzer_config,
        )

    async def test_empty_data(self):
        day = datetime(2023, 1, 1)

        analytics = await self.heatmaps._process_raw_analytics(
            day=day,
            resource="124",
            author_id=9000,
        )
        self.assertIn("replied_per_acc", analytics.keys())
        self.assertIn("mentioner_per_acc", analytics.keys())
        self.assertIn("reacted_per_acc", analytics.keys())

        self.assertIsInstance(analytics["replied_per_acc"], list)
        self.assertEqual(len(analytics["replied_per_acc"]), 0)

        self.assertIsInstance(analytics["mentioner_per_acc"], list)
        self.assertEqual(len(analytics["mentioner_per_acc"]), 0)

        self.assertIsInstance(analytics["reacted_per_acc"], list)
        self.assertEqual(len(analytics["reacted_per_acc"]), 0)

    async def test_single_author(self):
        platform_id = self.heatmaps.platform_id
        day = datetime(2023, 1, 1)

        sample_raw_data = [
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {
                    "thread_id": "7000",
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9003,
                        ],
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": day,
                "source_id": "10001",
                "metadata": {
                    "thread_id": "7000",
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "receiver",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {
                    "thread_id": "7000",
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9003,
                        ],
                        "type": "receiver",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 4),
                "source_id": "10001",
                "metadata": {
                    "thread_id": None,
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9002],
                        "type": "emitter",
                    }
                ],
            },
        ]
        await self.mongo_client[platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics = await self.heatmaps._process_raw_analytics(
            day=day,
            resource="124",
            author_id=9001,
        )

        self.assertIsInstance(analytics["replied_per_acc"], list)
        self.assertIsInstance(analytics["mentioner_per_acc"], list),
        self.assertIsInstance(analytics["reacted_per_acc"], list),

        self.assertEqual(len(analytics["replied_per_acc"]), 1)
        self.assertEqual(analytics["replied_per_acc"][0]["account"], 9003)
        self.assertEqual(analytics["replied_per_acc"][0]["count"], 1)

        self.assertEqual(len(analytics["mentioner_per_acc"]), 2)
        self.assertIn(analytics["mentioner_per_acc"][0]["account"], [9002, 9003])
        self.assertEqual(analytics["mentioner_per_acc"][0]["count"], 1)
        self.assertIn(analytics["mentioner_per_acc"][1]["account"], [9002, 9003])
        self.assertEqual(analytics["mentioner_per_acc"][1]["count"], 1)

        self.assertEqual(analytics["reacted_per_acc"], [])

    async def test_multiple_authors(self):
        platform_id = self.heatmaps.platform_id
        day = datetime(2023, 1, 1)

        sample_raw_data = [
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {
                    "thread_id": "7000",
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9003,
                        ],
                        "type": "receiver",
                    }
                ],
            },
            {
                "author_id": 9001,
                "date": day,
                "source_id": "10001",
                "metadata": {
                    "thread_id": "7000",
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9005],
                        "type": "receiver",
                    },
                    {
                        "name": "reaction",
                        "users_engaged_id": [9003, 9008],
                        "type": "emitter",
                    },
                ],
            },
            {
                "author_id": 9002,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {
                    "thread_id": "7000",
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "reaction",
                        "users_engaged_id": [9003, 9008],
                        "type": "emitter",
                    },
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9005],
                        "type": "receiver",
                    },
                ],
            },
            {
                "author_id": 9001,
                "date": datetime(2023, 1, 1, 4),
                "source_id": "10001",
                "metadata": {
                    "thread_id": None,
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": [9003, 9005],
                        "type": "emitter",
                    }
                ],
            },
        ]
        await self.mongo_client[platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics = await self.heatmaps._process_raw_analytics(
            day=day,
            resource="124",
            author_id=9001,
        )

        self.assertIsInstance(analytics["replied_per_acc"], list)
        self.assertIsInstance(analytics["mentioner_per_acc"], list),
        self.assertIsInstance(analytics["reacted_per_acc"], list),

        self.assertEqual(analytics["replied_per_acc"], [])

        self.assertEqual(len(analytics["mentioner_per_acc"]), 2)
        self.assertIn(analytics["mentioner_per_acc"][0]["account"], [9003, 9005])
        self.assertEqual(analytics["mentioner_per_acc"][0]["count"], 1)
        self.assertIn(analytics["mentioner_per_acc"][1]["account"], [9003, 9005])
        self.assertEqual(analytics["mentioner_per_acc"][1]["count"], 1)

        self.assertEqual(len(analytics["reacted_per_acc"]), 2)
        self.assertIn(analytics["reacted_per_acc"][0]["account"], [9003, 9008])
        self.assertEqual(analytics["reacted_per_acc"][0]["count"], 1)
        self.assertIn(analytics["reacted_per_acc"][1]["account"], [9003, 9008])
        self.assertEqual(analytics["reacted_per_acc"][1]["count"], 1)
