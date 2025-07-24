import asyncio
import random
import uuid
import time
import logging
from dataclasses import dataclass
import httpx

from database import DatabaseManager
from models import TestConfig, TestMetrics, TestStats
from reporter import TestReporter
from requester import RequestSender

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": %(message)s}',
)
logger = logging.getLogger(__name__)


@dataclass
class TestRunner:
    config: TestConfig
    db_manager: DatabaseManager
    request_sender: RequestSender
    reporter: TestReporter
    start_time: float = 0.0

    def _generate_postback(self, test_id: str) -> dict:
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

    async def run_test(self):
        test_id = self.config.test_id
        self.start_time = time.perf_counter()
        stats = TestStats(
            verified_success=0, unverified_success=0, failed=0, latencies=[], sent_count=0
        )
        all_requests = [self._generate_postback(test_id) for _ in range(self.config.request_count)]

        try:
            async with self.request_sender.get_client() as client:
                try:
                    await asyncio.wait_for(
                        self._execute_test(client, all_requests, stats),
                        timeout=self.config.max_duration_minutes * 60,
                    )
                except asyncio.TimeoutError:
                    logger.info("Test stopped due to duration limit")

            duration = time.perf_counter() - self.start_time

            metrics = self._calculate_metrics(stats, duration)
            await self._save_and_report_results(test_id, stats, metrics)

        except asyncio.CancelledError:
            await self._handle_interruption(test_id, stats)

    async def _execute_test(self, client: httpx.AsyncClient, all_requests: list, stats: TestStats):
        queue = asyncio.Queue(maxsize=self.config.max_requests_per_second or 5000)
        semaphore = asyncio.Semaphore(self.config.max_requests_per_second or 500)
        test_end_time = self.start_time + self.config.max_duration_minutes * 60
        worker_count=100

        workers = [
            asyncio.create_task(self._worker(client, queue, stats, semaphore))
            for _ in range(worker_count)
        ]

        try:
            for postback in all_requests:
                if time.perf_counter() > test_end_time:
                    logger.info("Duration limit reached, stopping test")
                    break

                await semaphore.acquire()
                await queue.put(postback)
                stats.sent_count += 1

            await queue.join()
        except asyncio.CancelledError:
            logger.info("Test execution cancelled, cleaning up...")
        finally:
            for worker in workers:
                if not worker.done():
                    worker.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break

    async def _worker(
        self,
        client: httpx.AsyncClient,
        queue: asyncio.Queue,
        stats: TestStats,
        semaphore: asyncio.Semaphore,
    ):
        last_request_time = 0
        min_interval = 1.0 / (self.config.max_requests_per_second or 100)
        test_end_time = self.start_time + self.config.max_duration_minutes * 60
        current_task = None

        while time.perf_counter() < test_end_time:
            try:
                current_time = time.perf_counter()
                elapsed = current_time - last_request_time
                if elapsed < min_interval:
                    await asyncio.sleep(min_interval - elapsed)

                try:
                    current_task = asyncio.create_task(queue.get())
                    params = await asyncio.wait_for(current_task, timeout=0.5)
                    current_task = None

                    last_request_time = time.perf_counter()
                    await self._process_request(client=client, params=params, stats=stats)

                except asyncio.TimeoutError:
                    if time.perf_counter() >= test_end_time:
                        break
                    continue
                except Exception as e:
                    logger.error({"event_log": "_process_request", "error": str(e)})
                    stats.failed += 1
                finally:
                    if current_task is None:
                        queue.task_done()
                    semaphore.release()

            except asyncio.CancelledError:
                if current_task and not current_task.done():
                    current_task.cancel()
                break
            except Exception as e:
                logger.error({"event_log": "_worker", "message": "Worker error", "error": str(e)})
                stats.failed += 1
                if current_task and not current_task.done():
                    current_task.cancel()

    async def _process_request(self, client: httpx.AsyncClient, params: dict, stats: TestStats):
        try:
            await self.db_manager.save_requests_batch([params])
            success, latency = await self.request_sender.send_request(client, params)
            stats.latencies.append(latency)
            if not success:
                stats.failed += 1
        except Exception as e:
            logger.error({"event_log": "_process_request", "error": str(e)})
            stats.failed += 1

    def _calculate_metrics(self, stats: TestStats, duration: float) -> TestMetrics:
        latencies = stats.latencies
        if not latencies:
            return TestMetrics(
                avg_latency=0,
                min_latency=0,
                max_latency=0,
                p90=0,
                p95=0,
                p99=0,
                verified_rate=0,
                rps=0,
            )

        latencies_sorted = sorted(latencies)
        n = len(latencies_sorted)
        verified_rate = (
            (stats.verified_success / stats.sent_count * 100) if stats.sent_count > 0 else 0
        )

        return TestMetrics(
            avg_latency=sum(latencies) / n,
            min_latency=min(latencies),
            max_latency=max(latencies),
            p90=latencies_sorted[int(n * 0.9)],
            p95=latencies_sorted[int(n * 0.95)],
            p99=latencies_sorted[int(n * 0.99)],
            verified_rate=verified_rate,
            rps=stats.sent_count / duration if duration > 0 else 0,
        )

    async def _save_and_report_results(self, test_id: str, stats: TestStats, metrics: TestMetrics):
        duration = time.perf_counter() - self.start_time

        await asyncio.sleep(5)
        max_retries = 20
        retry_delay = 1

        for attempt in range(max_retries):
            verified, unverified = await self.db_manager.verify_requests(test_id)
            if verified + unverified == stats.sent_count or attempt == max_retries - 1:
                break
            await asyncio.sleep(retry_delay)

        stats.verified_success = verified
        stats.unverified_success = unverified

        metrics.verified_rate = (verified / stats.sent_count * 100) if stats.sent_count > 0 else 0
        metrics.rps = stats.sent_count / duration if duration > 0 else 0

        await self.db_manager.save_test_results(test_id, duration, stats, metrics)
        history = await self.db_manager.get_test_history()

        self.reporter.print_console_report(
            test_id,
            duration,
            {
                "avg_latency": metrics.avg_latency,
                "min_latency": metrics.min_latency,
                "max_latency": metrics.max_latency,
                "p90": metrics.p90,
                "p95": metrics.p95,
                "p99": metrics.p99,
                "verified_rate": metrics.verified_rate,
                "rps": metrics.rps,
            },
            {
                "verified_success": stats.verified_success,
                "unverified_success": stats.unverified_success,
                "failed": stats.failed,
                "sent_count": stats.sent_count,
            },
        )
        self.reporter.print_history_comparison(history)

    async def _handle_interruption(self, test_id: str, stats: TestStats):
        duration = time.perf_counter() - self.start_time
        metrics = self._calculate_metrics(stats, duration)

        try:
            await self.db_manager.save_test_results(test_id, duration, stats, metrics)
        except Exception as e:
            logger.error({"event_log": "save_results_failed", "error": str(e)})

        logger.warning(
            {
                "event_log": "test_interrupted",
                "test_id": test_id,
                "status": "interrupted",
                "partial_results": {
                    "duration": round(duration, 2),
                    "sent": stats.sent_count,
                    "failed": stats.failed,
                },
            }
        )
