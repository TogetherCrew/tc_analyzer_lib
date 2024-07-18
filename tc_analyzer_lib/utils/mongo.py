import logging
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from tc_analyzer_lib.utils.credentials import get_mongo_credentials


class MongoSingleton:
    __instance = None

    def __init__(self):
        if MongoSingleton.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            creds = get_mongo_credentials()
            connection_uri = config_mogno_creds(creds)
            self.client = MongoClient(connection_uri)
            self.async_client = AsyncIOMotorClient(connection_uri)
            logging.info(f"MongoDB connected! server info: {self.client.server_info()}")
            MongoSingleton.__instance = self

    @staticmethod
    def get_instance(skip_singleton=False):
        """
        get mongo client class instance
        the `skip_singleton` is for the case of test cases
        """
        if skip_singleton:
            MongoSingleton.__instance = None

        if MongoSingleton.__instance is None:
            MongoSingleton()
        return MongoSingleton.__instance

    def get_client(self):
        return self.client

    def get_async_client(self):
        return self.async_client

    def close(self):
        # Attempt to close the clients gracefully
        try:
            self.async_client.close()
        except Exception as e:
            logging.error(f"Failed to close async mongo client: {e}")

        try:
            self.client.close()
        except Exception as e:
            logging.error(f"Failed to close mongo client: {e}")

        self.__instance = None


def config_mogno_creds(mongo_creds: dict[str, Any]):
    user = mongo_creds["user"]
    password = mongo_creds["password"]
    host = mongo_creds["host"]
    port = mongo_creds["port"]

    connection = f"mongodb://{user}:{password}@{host}:{port}"

    return connection
