from datetime import date, datetime, time, timedelta

from tc_analyzer_lib.schemas import RawAnalyticsItem
from tc_analyzer_lib.utils.mongo import MongoSingleton


class AnalyticsRaw:
    def __init__(self, platform_id: str) -> None:
        client = MongoSingleton.get_instance().get_async_client()
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
    ) -> dict[str, list[RawAnalyticsItem]]:
        """
        analyze the count of messages

        Parameters
        ------------
        day : datetime.date
            analyze for a specific day
        activity : str
            the activity to be `actions` or `interactions`
        activity_name : str
            the activity name to be used from `rawmemberactivities` data
            could be `reply`, `mention`, `message`, `commit` or any other
            thing that is available on `rawmemberactivities` data
        user_ids : str
            Users to compute analytics for
        activity_direction : str
            should be always either `emitter` or `receiver`
        **kwargs :
            additional_filters : dict[str, str]
                the additional filtering for `rawmemberactivities` data of each platform
                the keys could be `metadata.channel_id` with a specific value

        Returns
        ---------
        activity_count : dict[str, list[RawAnalyticsItem]]
            raw analytics item which holds the user and
            the count of interaction in that day
        """
        if activity_direction not in ["emitter", "receiver"]:
            raise ValueError(
                "Wrong activity_direction given, "
                "should be either `emitter` or `receiver`!"
            )

        if activity not in ["interactions", "actions"]:
            raise ValueError(
                "Wrong `activity` given, "
                "should be either `interactions` or `actions`!"
                f" The provided one is {activity}"
            )

        activity_count = await self.get_analytics_count(
            day=day,
            activity=activity,
            user_ids=user_ids,
            activity_name=activity_name,
            activity_direction=activity_direction,
            filters=kwargs.get("additional_filters", {}),
        )

        return activity_count

    async def get_analytics_count(
        self,
        day: date,
        activity: str,
        activity_name: str,
        user_ids: list[str | int],
        activity_direction: str,
        **kwargs,
    ) -> dict[str, list[RawAnalyticsItem]]:
        """
        Gets the list of documents for the stated day

        Parameters
        ------------
        day : date
            a specific day date
        activity : str
            to be `interactions` or `actions`
        activity_name : str
            the activity name to do filtering
            could be `reply`, `reaction`, `mention, or ...
        user_ids : list[str | int]
            Users to compute analytics on their raw data
        activity_direction : str
            the direction of activity
            could be `emitter` or `receiver`
        **kwargs : dict
            filters : dict[str, dict[str] | str]
                the filtering that we need to apply
                for default it is an None meaning
                no filtering would be applied

        Returns
        ---------
        activity_count : dict[str, list[RawAnalyticsItem]]
            raw analytics item which holds the users as key and
            the count of interaction in that day
        """
        start_day = datetime.combine(day, time(0, 0, 0))
        end_day = start_day + timedelta(days=1)

        pipeline = [
            {
                "$match": {
                    "date": {"$gte": start_day, "$lt": end_day},
                    "author_id": {"$in": user_ids},
                    **kwargs.get("filters", {}),
                }
            },
            {"$unwind": f"${activity}"},
            {
                "$match": {
                    f"{activity}.name": activity_name,
                    f"{activity}.type": activity_direction,
                }
            },
            {"$unwind": f"${activity}.users_engaged_id"},
            {
                "$match": {
                    "$expr": {"$ne": ["$interactions.users_engaged_id", "$author_id"]}
                }
            },
            {
                "$group": {
                    "_id": {
                        "engaged_user": "$interactions.users_engaged_id",
                        "author_id": "$author_id",
                    },
                    "count": {"$sum": 1},
                }
            },
        ]

        results = await self.get_aggregate_results(pipeline)
        return results

    async def get_aggregate_results(
        self, pipeline
    ) -> dict[str, list[RawAnalyticsItem]]:
        results: dict[str, list[RawAnalyticsItem]] = {}
        async for doc in self.collection.aggregate(pipeline):
            user = doc["_id"]["author_id"]
            results.setdefault(user, [])

            engaged_user = doc["_id"]["engaged_user"]
            engagement_count = doc["count"]

            results[user].append(
                RawAnalyticsItem(account=engaged_user, count=engagement_count)
            )

        return results
