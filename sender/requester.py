import logging
import httpx
import time
from typing import Any
from contextlib import asynccontextmanager

from models import TestConfig
from rate_limiter import PreciseRateLimiter

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": %(message)s}',
)
logger = logging.getLogger(__name__)


class RequestSender:
    def __init__(self, config: TestConfig):
        self.config = config
        self.client = None
        self.rate_limiter = PreciseRateLimiter(config.max_requests_per_second)

    @asynccontextmanager
    async def get_client(self):
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(
                max_connections=self.config.max_requests_per_second or 1000,
                max_keepalive_connections=500,
            ),
            http2=True,
        ) as client:
            yield client

    async def send_request(
        self, client: httpx.AsyncClient, params: dict[str, Any]
    ) -> tuple[bool, float]:
        await self.rate_limiter.wait()
        start = time.monotonic()
        try:
            response = await client.get(self.config.target_url, params=params)
            response.raise_for_status()
            return True, time.monotonic() - start
        except Exception as e:
            logger.error(
                {
                    "event_log": "send_request_failed",
                    "error": e,
                    "url": self.config.target_url,
                    "params": params,
                }
            )
            return False, time.monotonic() - start

    async def __aenter__(self):
        limits = httpx.Limits(
            max_connections=self.config.parallel_workers * 2,
            max_keepalive_connections=self.config.parallel_workers,
        )
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=limits,
            http2=True,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()
