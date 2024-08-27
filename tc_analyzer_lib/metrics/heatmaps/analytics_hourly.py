from datetime import date, datetime, time, timedelta
from typing import Any

import numpy as np
from tc_analyzer_lib.utils.mongo import MongoSingleton


class AnalyticsHourly:
    def __init__(self, platform_id: str, testing: bool = False) -> None:
        client = MongoSingleton.get_instance(skip_singleton=testing).get_async_client()
        # `rawmemberactivities` is the collection we would use for analytics
        self.collection = client[platform_id]["rawmemberactivities"]
        self.msg_prefix = f"PLATFORMID: {platform_id}:"

    async def analyze(
        self,
        day: date,
        activity: str,
        activity_name: str,
        activity_direction: str,
        user_ids: list[str | int],
        **kwargs,
    ) -> list[int]:
        """
        analyze the hourly the messages

        Parameters
        ------------
        day : date
            analyze for a specific day
        activity : str
            the activity to be `actions` or `interactions`
        activity_name : str
            the activity name to be used from `rawmemberactivities` data
            could be `reply`, `mention`, `message`, `commit` or any other
            thing that is available on `rawmemberactivities` data
        user_ids : list[str | int]
            the users to filter data for
        activity_direction : str
            should be always either `emitter` or `receiver`
        **kwargs :
            resource_filtering : dict[str, str]
                a filtering applied for resources on data
        """
        resource_filtering: dict[str, str] = kwargs.get("resource_filtering", {})

        if activity_direction not in ["emitter", "receiver"]:
            raise AttributeError(
                "Wrong activity_direction given, "
                "should be either `emitter` or `receiver`!"
            )

        if activity not in ["interactions", "actions"]:
            raise AttributeError(
                "Wrong `activity` given, "
                "should be either `interactions` or `actions`"
            )

        activity_vector = await self.get_hourly_analytics(
            day=day,
            activity=activity,
            user_ids=user_ids,
            filters={
                f"{activity}.name": activity_name,
                f"{activity}.type": activity_direction,
            },
            resource_filters=resource_filtering,
        )

        return activity_vector

    async def get_hourly_analytics(
        self,
        day: date,
        activity: str,
        user_ids: list[str | int],
        filters: dict[str, dict[str, Any] | str] | None = None,
        resource_filters: dict[str, str] | None = None,
    ) -> list[int]:
        """
        Gets the list of documents for the stated day

        Parameters
        ------------
        day : date
            a specific day date
        activity : str
            to be `interactions` or `actions`
        user_ids : list[str | int]
            a list of users to compute data for
        filter : dict[str, dict[str] | str] | None
            the filtering that we need to apply on actions or interactions
            for default it is an None meaning
            no filtering would be applied
        resource_filtering : dict[str, str] | None
            the filtering on resources of data
            could make the query more efficient if provided

        Returns
        ---------
        hourly_analytics : list[int]
            a vector with length of 24
            each index representing the count of activity for that day
        """
        start_day = datetime.combine(day, time(0, 0, 0))
        end_day = start_day + timedelta(days=1)

        pipeline = [
            {
                "$match": {
                    "date": {"$gte": start_day, "$lt": end_day},
                    "author_id": {"$in": user_ids},
                    **(resource_filters or {}),
                }
            },
            {"$unwind": f"${activity}"},
        ]

        if filters:
            pipeline.append({"$match": filters})

        if activity == "interactions":
            pipeline.extend(
                [
                    {"$unwind": "$interactions.users_engaged_id"},
                    # ignoring self-interactions
                    {
                        "$match": {
                            "$expr": {
                                "$ne": ["$interactions.users_engaged_id", "$author_id"]
                            }
                        }
                    },
                ]
            )

        pipeline.extend(
            [
                {"$addFields": {"hour": {"$hour": "$date"}}},
                {
                    "$group": {
                        "_id": {
                            "hour": "$hour",
                            "user": "$author_id",
                        },
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )

        results = await self.get_aggregate_results(pipeline)
        return results

    async def get_aggregate_results(self, pipeline):
        results = {}
        async for doc in self.collection.aggregate(pipeline):
            user = doc["_id"]["user"]
            hour = doc["_id"]["hour"]
            activity_count = doc["count"]
            results.setdefault(user, [0] * 24)

            doc[user][hour] = activity_count
        return results
