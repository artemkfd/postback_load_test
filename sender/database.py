import asyncio
import sqlite3
from pathlib import Path
from typing import Any

from models import TestMetrics, TestStats


class DatabaseManager:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()
        self.sending_requests_buffer = []
        self.buffer_size = 100

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA cache_size=-10000")
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
                    duration REAL,
                    sending_count INTEGER,
                    verified_success INTEGER,
                    unverified_success INTEGER,
                    failed INTEGER,
                    verified_rate REAL,
                    avg_latency REAL,
                    min_latency REAL,
                    max_latency REAL,
                    p90 REAL,
                    p95 REAL,
                    p99 REAL,
                    rps REAL
                );

                CREATE INDEX IF NOT EXISTS idx_sending_test ON sending_requests(test_id, request_id);
                CREATE INDEX IF NOT EXISTS idx_received_test ON received_requests(test_id, request_id);
                """
            )

    async def save_requests_batch(self, requests: list[tuple]):
        values = [
            (
                req["request_id"],
                req["test_id"],
                req["postback_type"],
                req["event_name"],
                req["source_id"],
                req["campaign_id"],
                req["placement_id"],
                req["adset_id"],
                req["ad_id"],
                req["advertising_id"],
                req["country"],
                req["click_id"],
                req["mmp"],
            )
            for req in requests
        ]
        self.sending_requests_buffer.extend(values)
        if len(self.sending_requests_buffer) >= self.buffer_size:
            await self._flush_buffer()

    async def _flush_buffer(self):
        if not self.sending_requests_buffer:
            return
        values = self.sending_requests_buffer.copy()
        self.sending_requests_buffer.clear()
        await asyncio.get_event_loop().run_in_executor(None, self._sync_write, values)

    def _sync_write(self, values: list[tuple]):
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO sending_requests
                (request_id, test_id, postback_type, event_name, source_id,
                 campaign_id, placement_id, adset_id, ad_id, advertising_id,
                 country, click_id, mmp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )

    async def verify_requests(self, test_id: str) -> tuple[int, int]:
        def _sync_verify():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM sending_requests WHERE test_id = ?", (test_id,)
                )
                sent_count = cursor.fetchone()[0]

                # cursor = conn.execute(
                #     "SELECT COUNT(*) FROM received_requests WHERE test_id = ?", (test_id,)
                # )
                # received_count = cursor.fetchone()[0]
                received_count = conn.execute(
                    """SELECT COUNT(*) FROM sending_requests s
                    JOIN received_requests r ON s.request_id = r.request_id
                    WHERE s.test_id = ?""",
                    (test_id,)
                ).fetchone()[0]

                return received_count, sent_count - received_count

        return await asyncio.get_event_loop().run_in_executor(None, _sync_verify)

    async def verify_data_integrity(self, test_id: str) -> dict[str, Any]:
        def _sync_verify():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT * FROM sending_requests WHERE test_id = ?", (test_id,)
                )
                sent_requests = {row[0]: row for row in cursor.fetchall()}

                cursor = conn.execute(
                    "SELECT * FROM received_requests WHERE test_id = ?", (test_id,)
                )
                received_requests = {row[0]: row for row in cursor.fetchall()}

                missing = set(sent_requests.keys()) - set(received_requests.keys())
                mismatches = {}

                for req_id, sent_row in sent_requests.items():
                    if req_id in received_requests:
                        recv_row = received_requests[req_id]
                        for i, field in enumerate(
                            [
                                "test_id",
                                "postback_type",
                                "event_name",
                                "source_id",
                                "campaign_id",
                                "placement_id",
                                "adset_id",
                                "ad_id",
                                "advertising_id",
                                "country",
                                "click_id",
                                "mmp",
                            ]
                        ):
                            if sent_row[i + 1] != recv_row[i + 1]:
                                if field not in mismatches:
                                    mismatches[field] = 0
                                mismatches[field] += 1

                return {
                    "total_sent": len(sent_requests),
                    "total_received": len(received_requests),
                    "missing_count": len(missing),
                    "field_mismatches": mismatches,
                }

        return await asyncio.get_event_loop().run_in_executor(None, _sync_verify)

    async def save_test_results(
        self, test_id: str, duration: float, stats: TestStats, metrics: TestMetrics
    ):
        def _sync_save():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO metrics
                    (test_id, test_datetime, duration, sending_count,
                     verified_success, unverified_success, failed,
                     verified_rate, avg_latency, min_latency, max_latency, p90, p95, p99, rps)
                    VALUES (?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        test_id,
                        duration,
                        stats.sent_count,
                        stats.verified_success,
                        stats.unverified_success,
                        stats.failed,
                        metrics.verified_rate,
                        metrics.avg_latency,
                        metrics.min_latency,
                        metrics.max_latency,
                        metrics.p90,
                        metrics.p95,
                        metrics.p99,
                        metrics.rps,
                    ),
                )

        await asyncio.get_event_loop().run_in_executor(None, _sync_save)

    async def get_test_history(self, limit: int = 5) -> list[dict[str, Any]]:
        def _sync_get():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    """
                    SELECT
                        test_id,
                        strftime('%Y-%m-%d %H:%M:%S', test_datetime) as test_datetime,
                        rps,
                        avg_latency,
                        verified_rate,
                        sending_count,
                        duration
                    FROM metrics
                    ORDER BY test_datetime DESC
                    LIMIT ?
                    """,
                    (limit,),
                )
                return [dict(row) for row in cursor.fetchall()]

        return await asyncio.get_event_loop().run_in_executor(None, _sync_get)
