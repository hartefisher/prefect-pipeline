import os
from typing import Any

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()


class MongoDB:
    def __init__(self, uri: str, db_name: str) -> None:
        self.db_name = db_name
        self.client: AsyncIOMotorClient[dict[str, Any]] = AsyncIOMotorClient(
            uri,
            minPoolSize=20,
            maxPoolSize=50,
            connectTimeoutMS=60000,
            maxidletimems=60000,
        )
        self.db = self.client[self.db_name]

    def close(self) -> None:
        self.client.close()

    def __getattr__(self, name: str) -> Any:
        return self.db[name]


def get_prefect() -> MongoDB:
    return MongoDB(MONGO_URI, "prefect")


MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:27017/?directConnection=true"
WORKFLOW_DB = os.getenv("WORKFLOW_DB", "workflow")
DB = MongoDB(MONGO_URI, WORKFLOW_DB)
