from datetime import datetime

from pymongo.cursor import Cursor
from tc_analyzer_lib.utils.mongo import MongoSingleton


class HeatmapsUtils:
    def __init__(self, platform_id: str) -> None:
        self.platform_id = platform_id
        client = MongoSingleton.get_instance().get_client()
        self.database = client[platform_id]

    def get_users(self, is_bot: bool = False) -> Cursor:
        """
        get the users of a platform

        Parameters
        -----------
        is_bot : bool
            if we want to fetch the bots
            for default is False meaning the real users will be returned

        Returns:
        ---------
        bots : pymongo.cursor.Cursor
            MongoDB cursor for users
            in case of large amount of data we should loop over this
            the cursor data format would be as `{'id': xxxx}`
        """
        cursor = self.database["rawmembers"].find(
            {"is_bot": is_bot}, {"_id": 0, "id": 1}
        )
        return cursor

    def get_active_users(
            self, start_day: datetime, end_day: datetime, metadata_filter: dict = {}
        ) -> list[str]:
        """
        get the users doing activities for a specific period

        Parameters
        -------------
        start_day : datetime
            the time to filter the data from
        end_day : datetime
            the end day for filtering data from
        metadata_filter : dict
            the additional filtering to be applied on data
            default is no filtering which an empty dictionary will be passed

        Returns
        ---------
        users : list[str]
            a list of user ids doing activity in that day
        """
        # cursor = self.database["rawmemberactivities"].aggregate(
        #     [
        #         {"$match": {"date": {"$gte": start_day, "$lt": end_day}}},
        #         {"$unwind": "$interactions"},
        #         {"$unwind": "$interactions.users_engaged_id"},
        #         {
        #             "$group": {
        #                 "_id": None,
        #                 "all_ids": {"$addToSet": "$interactions.users_engaged_id"},
        #                 "author_ids": {"$addToSet": "$author_id"},
        #             }
        #         },
        #         {
        #             "$project": {
        #                 "_id": 0,
        #                 "combined_ids": {"$setUnion": ["$all_ids", "$author_ids"]},
        #             }
        #         },
        #     ]
        # )

        cursor = self.database["rawmemberactivities"].aggregate(
            [
                {
                    "$match": {
                        "date": {"$gte": start_day, "$lt": end_day},
                        "metadata.bot_activity": False,
                        **metadata_filter,
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "all_ids": {"$addToSet": "$interactions.users_engaged_id"},
                        "author_ids": {"$addToSet": "$author_id"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "combined_engaged_ids": {"$setUnion": ["$all_ids"]},
                        "combined_author_ids": {"$setUnion": ["$author_ids"]},
                    }
                },
            ]
        )

        combined_ids = []
        for doc in cursor:
            combined_ids.extend(doc.get("combined_author_ids", []))
            nested_list = doc.get("combined_engaged_ids", [])
            combined_ids.extend(sum(sum(nested_list, []), []))

        # making the values to be unique
        return list(set(combined_ids))

    def get_users_count(self, is_bot: bool = False) -> int:
        """
        get the count of users

        Parameters
        -----------
        is_bot : bool
            if we want to fetch the bots
            for default is False meaning the real users will be returned

        Returns
        ---------
        users_count : int
            the count of users
        """
        users_count = self.database["rawmembers"].count_documents(
            {"is_bot": is_bot},
        )
        return users_count

    def get_last_date(self) -> datetime | None:
        """
        get the last document's date
        """
        cursor = (
            self.database["heatmaps"]
            .find({}, {"date": 1, "_id": 0})
            .sort("date", -1)
            .limit(1)
        )
        documents = list(cursor)
        last_date = documents[0]["date"] if documents != [] else None

        return last_date
