from clickhouse_driver import Client
from urllib.parse import urlparse
from django.conf import settings
import atexit
import logging
from typing import Any
import threading

logger = logging.getLogger(__name__)


class ClickHouseClient:
    """
    Thread-safe ClickHouse client singleton with proper resource cleanup.
    """

    _instance = None
    _lock = threading.Lock()
    _client: Client | None = None

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._initialize()
        return cls._instance

    @classmethod
    def _initialize(cls):
        db_url = urlparse(settings.CLICKHOUSE_DB_URL)

        connection_settings: dict[str, Any] = {
            "host": db_url.hostname,
            "port": db_url.port or 9000,
            "user": db_url.username or "default",
            "password": db_url.password or "",
            "database": db_url.path.lstrip("/") or "default",
            "settings": {
                "connect_timeout": 10,
                "send_receive_timeout": 30,
                "insert_block_size": 100000,
            },
        }

        if db_url.scheme == "https":
            connection_settings["secure"] = True

        cls._client = Client(**connection_settings)
        atexit.register(cls._cleanup)
        logger.info({"event_log": "ClickHouseClient", "message": "ClickHouse client initialized"})

    @classmethod
    def _cleanup(cls):
        """Cleanup resources safely"""
        if cls._client is not None:
            try:
                cls._client.disconnect()
                logger.info("ClickHouse connection closed")
            except Exception as e:
                logger.warning(
                    {
                        "event_log": "ClickHouseClient",
                        "message": "Error closing ClickHouse connection",
                        "error": e,
                    }
                )
            finally:
                cls._client = None

    @property
    def client(self) -> Client:
        if self._client is None:
            self._initialize()
        return self._client


def get_clickhouse() -> Client:
    return ClickHouseClient().client
