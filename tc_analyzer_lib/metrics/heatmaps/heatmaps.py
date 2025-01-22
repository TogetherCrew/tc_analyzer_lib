import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

from tc_analyzer_lib.metrics.heatmaps import AnalyticsHourly, AnalyticsRaw
from tc_analyzer_lib.metrics.heatmaps.heatmaps_utils import HeatmapsUtils
from tc_analyzer_lib.schemas import RawAnalyticsItem
from tc_analyzer_lib.schemas.platform_configs.config_base import PlatformConfigBase


class Heatmaps:
    def __init__(
        self,
        platform_id: str,
        period: datetime,
        resources: list[str],
        analyzer_config: PlatformConfigBase,
    ) -> None:
        """
        Heatmaps analytics wrapper

        Parameters
        ------------
        platform_id : str
            the platform that we want heatmaps analytics for
        period : datetime
            the date that analytics could be started
        resources : list[str]
            a list of resources id
            i.e. a list of `channel_id` for discord or `chat_id` for telegram
        analyzer_config : PlatformConfigBase
            the configuration for analytics job
            should be a class inheriting from `PlatformConfigBase` and with predefined values
        """
        self.platform_id = platform_id
        self.resources = resources
        self.period = period

        self.analyzer_config = analyzer_config
        self.utils = HeatmapsUtils(platform_id)

    async def start(
        self,
        from_start: bool = False,
        batch_return: int = 5,
    ):
        """
        Based on the rawdata creates and stores the heatmap data

        Parameters:
        -------------
        from_start : bool
            do the analytics from scrach or not
            if True, if wouldn't pay attention to the existing data in heatmaps
            and will do the analysis from the first date

        Returns:
        ---------
        heatmaps_results : list of dictionary
            the list of data analyzed
            also the return could be None if no database for guild
              or no raw info data was available
        """
        log_prefix = f"PLATFORMID: {self.platform_id}:"

        last_date = await self.utils.get_last_date()

        analytics_date: datetime
        if last_date is None or from_start:
            # Ensure self.period is offset-aware
            analytics_date = (
                self.period.replace(tzinfo=timezone.utc)
                if self.period.tzinfo is None
                else self.period
            )
        else:
            # Ensure last_date is offset-aware and add a day
            analytics_date = last_date.astimezone(timezone.utc) + timedelta(days=1)

        # in order to skip bots
        bot_ids = []
        bot_cursor = await self.utils.get_users(is_bot=True)
        async for bot in bot_cursor:
            bot_ids.append(bot["id"])

        # initialize the data array
        heatmaps_results = []

        index = 0
        today = datetime.now(tz=timezone.utc)
        max_index = (analytics_date - today).days
        while analytics_date.date() < today.date():
            start_day = analytics_date.replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
            )
            end_day = start_day + timedelta(days=1)
            logging.info(
                f"{log_prefix} ANALYZING HEATMAPS {start_day.date()} - {end_day.date()}!"
                f" | iteration: {index + 1} / {max_index}"
            )

            # getting the active resource_ids (activities being done there by users)
            period_resources = await self.utils.get_active_resources_period(
                start_day=start_day,
                end_day=end_day,
                resource_identifier=self.analyzer_config.resource_identifier,
                metadata_filter={
                    f"metadata.{self.analyzer_config.resource_identifier}": {
                        "$in": self.resources,
                    }
                },
            )
            if len(period_resources) == 0:
                logging.warning(
                    "No users interacting on platform for date: "
                    f"{start_day.date()} - {end_day.date()}"
                )

            for _, resource_id in enumerate(period_resources):
                user_ids = await self.utils.get_active_users(
                    start_day,
                    end_day,
                    metadata_filter={
                        "metadata."
                        + self.analyzer_config.resource_identifier: resource_id,
                    },
                )
                if len(user_ids) == 0:
                    logging.warning(
                        f"{log_prefix} No users interacting for the time window: "
                        f"{start_day.date()} - {end_day.date()} for resource: {resource_id}"
                        " Skipping the day."
                    )
                    continue

                hourly_analytics = await self._process_hourly_analytics(
                    day=analytics_date,
                    resource=resource_id,
                    user_ids=user_ids,
                )
                raw_analytics = await self._process_raw_analytics(
                    day=analytics_date,
                    resource=resource_id,
                    user_ids=user_ids,
                )
                heatmaps_doc = await self._init_heatmaps_documents(
                    hourly_analytics=hourly_analytics,
                    raw_analytics=raw_analytics,
                    resource_id=resource_id,
                    user_ids=user_ids,
                    bot_ids=bot_ids,
                    date=start_day,
                )
                heatmaps_results.extend(heatmaps_doc)

            if index % batch_return == 0:
                yield heatmaps_results
                # emptying it
                heatmaps_results = []

            index += 1

            # analyze next day
            analytics_date += timedelta(days=1)

        # returning any other values
        yield heatmaps_results

    async def _process_hourly_analytics(
        self,
        day: date,
        resource: str,
        user_ids: list[str | int],
    ) -> dict[str, list]:
        """
        start processing hourly analytics for a day based on given config

        Parameters
        ------------
        day : date
            analyze for a specific day
        resurce : str
            the resource we want to apply the filtering on
        user_ids : list[str | int]
            users we want the analytics for
        """
        analytics_hourly = AnalyticsHourly(self.platform_id)

        # first dict user analytics
        # second dict each hourly analytics item
        analytics: dict[str, dict[str, list[int]]] = {}
        for config in self.analyzer_config.hourly_analytics:
            # if it was a predefined analytics
            if config.name in [
                "replied",
                "replier",
                "mentioner",
                "mentioned",
                "reacter",
                "reacted",
            ]:
                activity_name: str
                if config.name in ["replied", "replier"]:
                    activity_name = "reply"
                elif config.name in ["mentioner", "mentioned"]:
                    activity_name = "mention"
                else:
                    activity_name = "reaction"

                analytics_vector = await analytics_hourly.analyze(
                    day=day,
                    activity=config.type.value,
                    activity_name=activity_name,
                    activity_direction=config.direction.value,
                    user_ids=user_ids,
                    resource_filtering={
                        f"metadata.{self.analyzer_config.resource_identifier}": resource,
                        "metadata.bot_activity": False,
                    },
                )
                analytics[config.name] = analytics_vector

            # if it was a custom analytics that we didn't write code
            # the mongodb condition is given in their configuration
            else:
                conditions = config.rawmemberactivities_condition

                if config.activity_name is None or conditions is None:
                    raise ValueError(
                        "For custom analytics the `activity_name` and `conditions`"
                        "in analyzer config shouldn't be None"
                    )

                activity_name = config.activity_name

                analytics_vector = await analytics_hourly.analyze(
                    day=day,
                    activity=config.type.value,
                    activity_name=activity_name,
                    activity_direction=config.direction.value,
                    user_ids=user_ids,
                    resource_filtering={
                        f"metadata.{self.analyzer_config.resource_identifier}": resource,
                        "metadata.bot_activity": False,
                        **conditions,
                    },
                )
                analytics[config.name] = analytics_vector

        return analytics

    async def _process_raw_analytics(
        self,
        day: date,
        resource: str,
        user_ids: list[str | int],
    ) -> dict[str, dict[str, list[RawAnalyticsItem]]]:
        analytics_raw = AnalyticsRaw(self.platform_id)
        analytics: dict[str, dict[str, list[RawAnalyticsItem]]] = {}

        for config in self.analyzer_config.raw_analytics:
            # default analytics that we always can have
            activity_name: str
            if config.name == "reacted_per_acc":
                activity_name = "reaction"
            elif config.name == "mentioner_per_acc":
                activity_name = "mention"
            elif config.name == "replied_per_acc":
                activity_name = "reply"
            else:
                # custom analytics
                if config.activity_name is None:
                    raise ValueError(
                        "`activity_name` for custom analytics should be provided"
                    )
                activity_name = config.activity_name

            additional_filters: dict[str, str] = {
                f"metadata.{self.analyzer_config.resource_identifier}": resource,
                "metadata.bot_activity": False,
            }
            # preparing for custom analytics (if available in config)
            if config.rawmemberactivities_condition is not None:
                additional_filters = {
                    **additional_filters,
                    **config.rawmemberactivities_condition,
                }

            analytics_items = await analytics_raw.analyze(
                day=day,
                activity=config.type.value,
                activity_name=activity_name,
                activity_direction=config.direction.value,
                user_ids=user_ids,
                additional_filters=additional_filters,
            )

            # converting to dict data
            # so we could later save easily in db
            analytics[config.name] = analytics_items

        return analytics

    def _compute_iteration_counts(
        self,
        analytics_date: datetime,
    ) -> int:
        iteration_count = (datetime.now() - analytics_date).days

        return iteration_count

    async def _init_heatmaps_documents(
        self,
        hourly_analytics: dict[str, dict[str, list[str | dict]]],
        raw_analytics: dict[str, dict[str, dict[str, list[RawAnalyticsItem]]]],
        resource_id: str,
        user_ids: list[str],
        bot_ids: list[str],
        date: datetime,
    ) -> list[dict[str, Any]]:
        """
        initialize the heatmaps documents from the given results on given analytics

        Parameters
        -----------
        hourly_analytics : dict[str, dict[str, list[str | dict]]]
            analytics data with schema as
            ```
            {
                "analytics1": {
                    "user1": [0, 0, 0, 0],
                    "user2": [0, 0, 0, 0],
                    "user3": [0, 0, 0, 0],
                },
                "analytics2": {
                    "user1": [0, 0, 0, 0],
                    "user2": [0, 0, 0, 0],
                    "user3": [0, 0, 0, 0],
                },
                "analytics3": {
                    "user1": [0, 0, 0, 0],
                    "user2": [0, 0, 0, 0],
                    "user3": [0, 0, 0, 0],
                },
                .
                .
                .
            }
            ```
        raw_analytics : dict[str, dict[str, list[RawAnalyticsItem]]]
            analytics data with schema as
            ```
            {
                "analytics1": {
                    "user1": {'account': 'user2', 'count': 2},
                    "user2": {'account': 'user7', 'count': 3},
                    "user3": {'account': 'user4', 'count': 6},
                },
                "analytics2": {
                    "user1": {'account': 'user3', 'count': 3},
                    "user2": {'account': 'user1', 'count': 4},
                    "user3": {'account': 'user9', 'count': 7},
                },
                "analytics3": {
                    "user1": [0, 0, 0, 0],
                    "user2": [0, 0, 0, 0],
                    "user3": [0, 0, 0, 0],
                },
                .
                .
                .
            }
            ```
        resource_id : str
            the resource id to put in list
        user_ids : list[str]
            a list of users to include analytics for it
        date : datetime
            the date of analytics

        Returns
        ---------
        heatmaps_docs : list[dict[str, Any]]
            a list of heatmaps data
        """
        # the dict with users in first dim and analytics second dim
        restructured_dict = {}

        for analytics_name, users_dict in hourly_analytics.items():
            for user in user_ids:
                restructured_dict.setdefault(user, {})
                restructured_dict[user][analytics_name] = users_dict.get(user, [0] * 24)

        # raw analytics data have a different format
        for analytics_name, users_dict in raw_analytics.items():
            for user in user_ids:
                restructured_dict.setdefault(user, {})
                restructured_dict[user][analytics_name] = [
                    item.to_dict() for item in users_dict.get(user, [])
                ]

        heatmaps_docs = []
        for user_id, analytics_dict in restructured_dict.items():
            if user_id in bot_ids:
                continue

            document = {
                self.analyzer_config.resource_identifier: resource_id,
                "date": datetime(date.year, date.month, date.day),
                "user": user_id,
                **analytics_dict,
            }

            heatmaps_docs.append(document)

        return heatmaps_docs
