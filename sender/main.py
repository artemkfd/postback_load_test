import asyncio
import logging
import random
import sqlite3
import sys
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))
logger = logging.getLogger(__name__)


class PostbackTester:
    def __init__(self, config):
        self.config = config
        self.db_path = (
            Path(__file__).resolve().parent.parent.parent / f"postback_load_test/{config.db_name}"
        )
        self._init_db()
        self._setup_logging()

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sending_requests (
                    request_id TEXT PRIMARY KEY,
                    test_id TEXT,
                    postback_type TEXT,
                    event_name TEXT,
                    source_id TEXT,
                    campaign_id TEXT,
                    placement_id TEXT,
                    adset_id TEXT,
                    ad_id TEXT,
                    advertising_id TEXT,
                    country TEXT,
                    click_id TEXT,
                    mmp TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS received_requests (
                    request_id TEXT PRIMARY KEY,
                    test_id TEXT,
                    postback_type TEXT,
                    event_name TEXT,
                    source_id TEXT,
                    campaign_id TEXT,
                    placement_id TEXT,
                    adset_id TEXT,
                    ad_id TEXT,
                    advertising_id TEXT,
                    country TEXT,
                    click_id TEXT,
                    mmp TEXT,
                    gaid TEXT,
                    idfa TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    );

                CREATE TABLE IF NOT EXISTS metrics (
                    test_id TEXT PRIMARY KEY,
                    test_datetime TEXT,
                    duration INTEGER,
                    sending_count INTEGER,
                    success_count INTEGER,
                    success_rate REAL,
                    avg_latency REAL,
                    min_latency REAL,
                    max_latency REAL
                );

                CREATE INDEX IF NOT EXISTS idx_sending_test ON sending_requests(test_id, request_id);
                CREATE INDEX IF NOT EXISTS idx_received_test ON received_requests(test_id, request_id);
                PRAGMA cache_size = -10000;
            """
            )

    def generate_postback(self, test_id: str) -> dict:
        return {
            "request_id": str(uuid.uuid4()),
            "test_id": test_id,
            "postback_type": random.choice(self.config.postback_types),
            "event_name": random.choice(self.config.event_names),
            "source_id": random.choice(self.config.source_ids),
            "campaign_id": str(uuid.uuid4()),
            "placement_id": str(uuid.uuid4()),
            "adset_id": random.choice(["123456", "654321"]),
            "ad_id": random.choice(["0123456", "0654321"]),
            "advertising_id": test_id,
            "country": "ru",
            "click_id": str(uuid.uuid4()),
            "mmp": random.choice(self.config.mmp),
        }

    async def save_request(self, params: dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO sending_requests
                (request_id, test_id, postback_type, event_name, source_id,
                 campaign_id, placement_id, adset_id, ad_id, advertising_id,
                 country, click_id, mmp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(params.values()),
            )

    @asynccontextmanager
    async def _get_client(self) -> httpx.AsyncClient:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_connections=self.config.max_requests_per_second or 100),
        ) as client:
            yield client

    async def _send_request(self, client: httpx.AsyncClient, params: dict) -> tuple[bool, float]:
        start = time.monotonic()
        try:
            response = await client.get(self.config.target_url, params=params)
            response.raise_for_status()
            return True, time.monotonic() - start
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return False, time.monotonic() - start

    async def run_test(self):
        test_id = str(uuid.uuid4())
        logger.info(f"Starting test {test_id} with {self.config.request_count} requests")

        stats = {"success": 0, "failure": 0, "latencies": []}

        start_time = time.monotonic()

        async with self._get_client() as client:
            tasks = []
            for _ in range(self.config.request_count):
                params = self.generate_postback(test_id)
                task = asyncio.create_task(self._process_request(client, params, stats))
                tasks.append(task)

            await asyncio.gather(*tasks)

        duration = time.monotonic() - start_time
        self._save_results(test_id, duration, stats)
        self._log_results(test_id, duration, stats)

    async def _process_request(self, client: httpx.AsyncClient, params: dict, stats: dict):
        await self.save_request(params)
        success, latency = await self._send_request(client, params)

        if success:
            stats["success"] += 1
        else:
            stats["failure"] += 1
        stats["latencies"].append(latency)

    def _save_results(self, test_id: str, duration: float, stats: dict):
        success_rate = (stats["success"] / self.config.request_count) * 100
        latencies = stats["latencies"]

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO metrics
                (test_id, test_datetime, duration, sending_count,
                 success_count, success_rate, avg_latency, min_latency, max_latency)
                VALUES (?, datetime('now'), ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    test_id,
                    duration,
                    self.config.request_count,
                    stats["success"],
                    success_rate,
                    sum(latencies) / len(latencies) if latencies else 0,
                    min(latencies) if latencies else 0,
                    max(latencies) if latencies else 0,
                ),
            )

    def _log_results(self, test_id: str, duration: float, stats: dict):
        rps = self.config.request_count / duration
        logger.info(
            f"Test {test_id} completed in {duration:.2f}s ({rps:.1f} requests/sec)\n"
            f"Success: {stats['success']} | Failed: {stats['failure']}\n"
            f"Success rate: {(stats['success']/self.config.request_count):.2%}\n"
            f"Latency: Avg={sum(stats['latencies'])/len(stats['latencies']):.3f}s, "
            f"Min={min(stats['latencies']):.3f}s, Max={max(stats['latencies']):.3f}s"
        )


async def main():
    from postback_load_test.config import TEST_CONFIG

    tester = PostbackTester(TEST_CONFIG)
    await tester.run_test()


if __name__ == "__main__":
    asyncio.run(main())
