from datetime import datetime, timedelta
from unittest import IsolatedAsyncioTestCase

from tc_analyzer_lib.metrics.heatmaps import Heatmaps
from tc_analyzer_lib.schemas.platform_configs import DiscordAnalyzerConfig
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestHeatmapsAnalytics(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        platform_id = "1234567890"
        period = (datetime.now() - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        resources = ["123", "124", "125"]
        # using one of the configs we currently have
        # it could be any other platform's config
        discord_analyzer_config = DiscordAnalyzerConfig()

        self.heatmaps = Heatmaps(
            platform_id=platform_id,
            period=period,
            resources=resources,
            analyzer_config=discord_analyzer_config,
        )
        self.mongo_client = MongoSingleton.get_instance(
            skip_singleton=True
        ).get_client()
        self.mongo_client[platform_id].drop_collection("rawmemberactivities")
        self.mongo_client[platform_id].drop_collection("rawmembers")

    def tearDown(self) -> None:
        self.mongo_client[self.heatmaps.platform_id].drop_collection(
            "rawmemberactivities"
        )
        self.mongo_client[self.heatmaps.platform_id].drop_collection("rawmembers")

    async def test_heatmaps_single_day_from_start(self):
        platform_id = self.heatmaps.platform_id
        day = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0)

        self.mongo_client[platform_id]["rawmembers"].insert_one(
            {
                "id": "9001",
                "is_bot": False,
                "left_at": None,
                "joined_at": datetime(2023, 1, 1),
                "options": {},
            },
        )

        sample_raw_data = [
            {
                "author_id": "9001",
                "date": day + timedelta(hours=1),
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
                            "9003",
                        ],
                        "type": "emitter",
                    },
                ],
            },
            {
                "author_id": "9001",
                "date": day + timedelta(hours=1),
                "source_id": "10005",
                "metadata": {
                    "thread_id": "7000",
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [],
                "interactions": [
                    {
                        "name": "reaction",
                        "users_engaged_id": [
                            "9009",
                        ],
                        "type": "emitter",
                    }
                ],
            },
            {
                "author_id": "9001",
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
                        "users_engaged_id": ["9003", "9002"],
                        "type": "receiver",
                    }
                ],
            },
            {
                "author_id": "9001",
                "date": day + timedelta(hours=2),
                "source_id": "10003",
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
                            "9003",
                        ],
                        "type": "receiver",
                    }
                ],
            },
            {
                "author_id": "9001",
                "date": day + timedelta(hours=4),
                "source_id": "10004",
                "metadata": {
                    "thread_id": None,
                    "channel_id": "124",
                    "bot_activity": False,
                },
                "actions": [{"name": "message", "type": "emitter"}],
                "interactions": [
                    {
                        "name": "mention",
                        "users_engaged_id": ["9003", "9002"],
                        "type": "emitter",
                    },
                    {
                        "name": "mention",
                        "users_engaged_id": ["9008", "9007"],
                        "type": "emitter",
                    },
                ],
            },
        ]
        self.mongo_client[platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics = []
        async for heatmaps_data in self.heatmaps.start(from_start=True):
            analytics.extend(heatmaps_data)

        self.assertIsInstance(analytics, list)

        # 5 iteration for heatmaps analytics
        # (1 resources) * (6 rawmember) * (1 day)
        self.assertEqual(len(analytics), 6)

        for i in range(5):
            if analytics[i]["user"] == "9001":
                # the second resource "124"
                if i == 4:
                    self.assertEqual(sum(analytics[i]["thr_messages"]), 3)
                    self.assertEqual(sum(analytics[i]["lone_messages"]), 1)
                    self.assertEqual(sum(analytics[i]["replier"]), 1)
                    self.assertEqual(sum(analytics[i]["replied"]), 1)
                    # 4 people
                    self.assertEqual(sum(analytics[i]["mentioner"]), 4)
                    self.assertEqual(sum(analytics[i]["mentioned"]), 2)
                    self.assertEqual(sum(analytics[i]["reacter"]), 0)
                    self.assertEqual(sum(analytics[i]["reacted"]), 1)

                    self.assertEqual(
                        analytics[i]["replied_per_acc"],
                        [
                            {
                                "account": "9003",
                                "count": 1,
                            }
                        ],
                    )
                    self.assertIn(
                        {"account": "9003", "count": 1},
                        analytics[i]["mentioner_per_acc"],
                    )
                    self.assertIn(
                        {"account": "9002", "count": 1},
                        analytics[i]["mentioner_per_acc"],
                    )

                    self.assertEqual(
                        analytics[i]["reacted_per_acc"],
                        [
                            {
                                "account": "9009",
                                "count": 1,
                            }
                        ],
                    )
            else:
                self.assertEqual(sum(analytics[i]["thr_messages"]), 0)
                self.assertEqual(sum(analytics[i]["lone_messages"]), 0)
                self.assertEqual(sum(analytics[i]["replier"]), 0)
                self.assertEqual(sum(analytics[i]["replied"]), 0)
                self.assertEqual(sum(analytics[i]["mentioner"]), 0)
                self.assertEqual(sum(analytics[i]["mentioned"]), 0)
                self.assertEqual(sum(analytics[i]["reacter"]), 0)
                self.assertEqual(sum(analytics[i]["reacted"]), 0)

                self.assertEqual(analytics[i]["mentioner_per_acc"], [])
                self.assertEqual(analytics[i]["mentioner_per_acc"], [])
                self.assertEqual(analytics[i]["reacted_per_acc"], [])
