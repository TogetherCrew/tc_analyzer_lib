from datetime import datetime
from unittest import TestCase

from tc_analyzer_lib.metrics.heatmaps.heatmaps_utils import HeatmapsUtils
from tc_analyzer_lib.utils.mongo import MongoSingleton


class TestHeatmapsUtilsActiveUsers(TestCase):
    def setUp(self) -> None:
        client = MongoSingleton.get_instance().get_client()
        self.platform_id = "1234567890"
        self.database = client[self.platform_id]
        self.database.drop_collection("rawmemberactivities")

        self.utils = HeatmapsUtils(self.platform_id)

    def test_get_users_empty_collection(self):
        users = self.utils.get_users()
        self.assertEqual(list(users), [])

    def test_get_multiple_users(self):
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
                    "channel_id": "1020707129214111827",
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
                    "channel_id": "1020707129214111827",
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
                    "channel_id": "1020707129214111827",
                    "thread_id": None,
                },
                "source_id": "11188143219343361",
            },
        ]
        self.database["rawmemberactivities"].insert_many(samples)

        users = self.utils.get_activity_users(start_day, end_day)

        self.assertEqual(set(users), set(["user1", "user2", "user4", "user5"]))
