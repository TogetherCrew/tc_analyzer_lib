import logging
from datetime import date, datetime, timedelta
from typing import Any

from tc_analyzer_lib.metrics.heatmaps import AnalyticsHourly, AnalyticsRaw
from tc_analyzer_lib.metrics.heatmaps.heatmaps_utils import HeatmapsUtils
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
            analytics_date = self.period
        else:
            analytics_date = last_date + timedelta(days=1)

        # initialize the data array
        heatmaps_results = []

        # we should not prepare the bot's data
        # cursor = await self.utils.get_users(is_bot=True)
        # bot_ids: list[str] = []
        # async for bot in cursor:
        #     bot_ids.append(bot["id"])

        index = 0
        while analytics_date.date() < datetime.now().date():
            start_day = analytics_date.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            end_day = start_day + timedelta(days=1)
            logging.info(
                f"{log_prefix} ANALYZING HEATMAPS {start_day.date()} - {end_day.date()}! | index: {index}"
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
                heatmaps_doc = self._init_heatmaps_documents(
                    analytics_dict={**hourly_analytics, **raw_analytics},
                    resource_id=resource_id,
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

    # async def _prepare_heatmaps_document(
    #     self, date: datetime, resource_id: str, author_id: str
    # ) -> dict[str, str | datetime]:
    #     """
    #     prepare the document for heatmaps analytics
    #     the hourly analytics and raw analytics data would be added after it
    #     """
    #     document = {
    #         self.analyzer_config.resource_identifier: resource_id,
    #         "date": datetime(date.year, date.month, date.day),
    #         "user": author_id,
    #     }
    #     return document

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
    ) -> dict[str, list[dict]]:
        analytics_raw = AnalyticsRaw(self.platform_id)
        analytics: dict[str, list[dict]] = {}

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

    def _init_heatmaps_documents(
        self,
        analytics_dict: dict[str, dict[str, list[str | dict]]],
        resource_id: str,
    ) -> list[dict[str, Any]]:
        """
        initialize the heatmaps documents from the given results on analytics_dict

        Parameters
        -----------
        analytics_dict : dict[str, dict[str, list[str | dict]]]
            the dictionary is like
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
            }
            ```
        resource_id : str
            the resource id to put in list

        Returns
        ---------
        heatmaps_docs : list[dict[str, Any]]
            a list of heatmaps data
        """
        # the dict with users in first dim and analytics second dim
        restructured_dict = {}

        for analytics_name, users_dict in analytics_dict.items():
            for user_id, values_list in users_dict.items():
                restructured_dict.setdefault(user_id, {})
                restructured_dict[user_id][analytics_name] = values_list

        heatmaps_docs = []
        for user_id, analytics_dict in restructured_dict:
            document = [
                {
                    "user": user_id,
                    self.analyzer_config.resource_identifier: resource_id,
                    **analytics_dict,
                }
            ]

            heatmaps_docs.append(document)

        return heatmaps_docs
