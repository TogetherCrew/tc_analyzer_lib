from datetime import datetime, timedelta
from unittest import IsolatedAsyncioTestCase

from tc_analyzer_lib.metrics.heatmaps.analytics_raw import AnalyticsRaw
from tc_analyzer_lib.schemas import ActivityDirection, RawAnalyticsItem
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestHeatmapsRawAnalytics(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.platform_id = "3456789"
        self.analytics_raw = AnalyticsRaw(self.platform_id)
        self.mongo_client = MongoSingleton.get_instance(
            skip_singleton=True
        ).get_client()
        self.mongo_client[self.platform_id].drop_collection("rawmemberactivities")

    async def test_raw_analytics_single_user(self):
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
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
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = await self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.RECEIVER.value,
            user_ids=[9000],
        )

        self.assertIsInstance(analytics_result, dict)
        self.assertEqual(list(analytics_result.keys()), [9000])
        self.assertEqual(analytics_result[9000][0].account, 9003)
        self.assertEqual(analytics_result[9000][0].count, 1)

    async def test_raw_analytics_wrong_user(self):
        """
        asking for another user's analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
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
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = await self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.RECEIVER.value,
            user_ids=[9003],
        )

        self.assertEqual(analytics_result, {})

    async def test_raw_analytics_wrong_activity_direction(self):
        """
        asking for another activity direction analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
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
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = await self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.EMITTER.value,
            user_ids=[9000],
        )

        self.assertEqual(analytics_result, {})

    async def test_raw_analytics_wrong_day(self):
        """
        asking for another day analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
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
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = await self.analytics_raw.analyze(
            day=day + timedelta(days=1),
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.RECEIVER.value,
            user_ids=[9000],
        )
        self.assertEqual(analytics_result, {})

    async def test_raw_analytics_wrong_activity(self):
        """
        asking for another activity analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
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
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = await self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="mention",
            activity_direction=ActivityDirection.RECEIVER.value,
            user_ids=[9000],
        )

        self.assertEqual(analytics_result, {})

    async def test_raw_analytics_multiple_users(self):
        """
        asking for another activity analytics
        results should be empty
        """
        day = datetime(2023, 1, 1).date()
        sample_raw_data = [
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 2),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9005,
                        ],
                        "type": "receiver",
                    }
                ],
            },
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 1, 4),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [9006, 9005],
                        "type": "receiver",
                    }
                ],
            },
            {
                "author_id": 9000,
                "date": datetime(2023, 1, 2, 4),
                "source_id": "10000",
                "metadata": {"thread_id": 7000, "channel_id": 2000},
                "actions": [{"name": "message", "type": "receiver"}],
                "interactions": [
                    {
                        "name": "reply",
                        "users_engaged_id": [
                            9001,
                        ],
                        "type": "receiver",
                    }
                ],
            },
        ]
        self.mongo_client[self.platform_id]["rawmemberactivities"].insert_many(
            sample_raw_data
        )

        analytics_result = await self.analytics_raw.analyze(
            day=day,
            activity="interactions",
            activity_name="reply",
            activity_direction=ActivityDirection.RECEIVER.value,
            user_ids=[9000, 9005, 9006],
        )
        self.assertIsInstance(analytics_result, dict)
        self.assertEqual(len(analytics_result.keys()), 1)

        # just the user with id 9000 was interacting
        for analytics in analytics_result[9000]:
            self.assertIsInstance(analytics, RawAnalyticsItem)
            if analytics.account == 9006:
                self.assertEqual(analytics.count, 1)
            elif analytics.account == 9005:
                self.assertEqual(analytics.count, 2)
            else:
                # raising with values for debug purposes
                ValueError(
                    "Never reaches here! "
                    f"analytics.account: {analytics.account} "
                    f"| analytics.count: {analytics.count}"
                )
