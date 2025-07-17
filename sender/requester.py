import logging
import httpx
import time
from typing import Any
from contextlib import asynccontextmanager

from models import TestConfig

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": %(message)s}',
)
logger = logging.getLogger(__name__)


class RequestSender:
    def __init__(self, config: TestConfig):
        self.config = config

    @asynccontextmanager
    async def get_client(self):
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(
                max_connections=self.config.max_requests_per_second or 100,
                max_keepalive_connections=20,
            ),
        ) as client:
            yield client

    async def send_request(
        self, client: httpx.AsyncClient, params: dict[str, Any]
    ) -> tuple[bool, float]:
        start = time.monotonic()
        try:
            response = await client.get(self.config.target_url, params=params)
            response.raise_for_status()
            return True, time.monotonic() - start
        except Exception as e:
            logger.error(
                {
                    "event_log": "send_request_failed",
                    "error": str(e),
                    "url": self.config.target_url,
                    "params": params,
                }
            )
            return False, time.monotonic() - start
