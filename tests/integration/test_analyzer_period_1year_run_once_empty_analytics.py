from datetime import datetime, timedelta
from unittest import IsolatedAsyncioTestCase

import numpy as np

from .utils.analyzer_setup import launch_db_access
from .utils.setup_platform import setup_platform


class TestAnalyzerOneYearRunOnce(IsolatedAsyncioTestCase):
    async def test_analyzer_one_year_period_run_once_empty_analytics(self):
        """
        test the whole analyzer pipeline for a guild with a 6 month period
        and use run_once method with empty analytics available
        """
        # first create the collections
        platform_id = "515151515151515151515151"
        db_access = launch_db_access(platform_id, skip_singleton=True)

        acc_id = [
            "973993299281076285",
            "973993299281076286",
        ]

        analyzer = setup_platform(
            db_access, platform_id, discordId_list=acc_id, days_ago_period=360
        )

        db_access.db_mongo_client[platform_id].drop_collection("heatmaps")
        db_access.db_mongo_client[platform_id].drop_collection("memberactivities")

        # generating rawinfo samples
        rawinfo_samples = []

        # generating random rawinfo data
        # 24 hours
        # 360 days
        for i in range(24 * 360):
            author = np.random.choice(acc_id)
            replied_user = np.random.choice(acc_id)
            samples = [
                {
                    "actions": [{"name": "message", "type": "emitter"}],
                    "author_id": author,
                    "date": datetime.now() - timedelta(hours=i),
                    "interactions": [
                        {
                            "name": "reply",
                            "type": "emitter",
                            "users_engaged_id": [replied_user],
                        }
                    ],
                    "metadata": {
                        "bot_activity": False,
                        "channel_id": "1020707129214111827",
                        "thread_id": None,
                    },
                    "source_id": f"11188143219343360{i}",
                },
                {
                    "actions": [],
                    "author_id": replied_user,
                    "date": datetime.now() - timedelta(hours=i),
                    "interactions": [
                        {
                            "name": "reply",
                            "type": "receiver",
                            "users_engaged_id": [author],
                        }
                    ],
                    "metadata": {
                        "bot_activity": False,
                        "channel_id": "1020707129214111827",
                        "thread_id": None,
                    },
                    "source_id": f"11188143219343360{i}",
                },
            ]
            rawinfo_samples.extend(samples)

        db_access.db_mongo_client[platform_id]["rawmemberactivities"].insert_many(
            rawinfo_samples
        )

        await analyzer.run_once()

        memberactivities_cursor = db_access.query_db_find(
            "memberactivities", {}, sorting=("date", -1)
        )
        memberactivities_data = list(memberactivities_cursor)

        yesterday = (datetime.now() - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        # 354 days, analytics saving is the end day
        assert len(memberactivities_data) == (354)
        assert memberactivities_data[0]["date"] == yesterday
        # yesterday is `-1` day and so
        # we would use 173 days ago rather than 174
        document_start_date = yesterday - timedelta(days=353)
        assert memberactivities_data[-1]["date"] == document_start_date

        heatmaps_cursor = db_access.query_db_find("heatmaps", {}, sorting=("date", -1))
        heatmaps_data = list(heatmaps_cursor)

        # 360 days, multiplied with 2
        # (accounts are: "973993299281076285", "973993299281076286")
        assert len(heatmaps_data) == 360 * 2
        # checking first and last document
        assert heatmaps_data[0]["date"] == yesterday
        year_ago = yesterday - timedelta(359)
        assert heatmaps_data[-1]["date"] == year_ago
