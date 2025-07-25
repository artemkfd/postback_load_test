import argparse
import asyncio
from datetime import datetime
import logging
import os
from pathlib import Path
import sqlite3
import time
import uuid
import random
import uvloop
import aiohttp
from dotenv import load_dotenv

uvloop.install()
BASE_DIR = Path(__file__).resolve().parent
print("BASE_DIR",BASE_DIR)
ENV_FILE = BASE_DIR / ".env"
load_dotenv(ENV_FILE)

TARGET_URL = os.environ.get("TARGET_URL", "http://127.0.0.1:8001")
# TARGET_URL = os.environ.get("TARGET_URL", "http://127.0.0.1:8001/verify")

logger = logging.getLogger(__name__)
TEST_CONFIG ={"test_id":"test_id","request_count":1,"target_url":TARGET_URL,"source_id":"100","connection_limit":200,"butch":400,"timeout":None}
METRICS_DB_PATH = "metrics.db"

def init_metrics_db():
    """Инициализация базы данных для метрик"""
    with sqlite3.connect(METRICS_DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                test_id TEXT PRIMARY KEY,
                test_datetime TEXT NOT NULL,
                duration REAL NOT NULL,
                total_requests INTEGER NOT NULL,
                success_count INTEGER NOT NULL,
                error_count INTEGER NOT NULL,
                connection_limit INTEGER NOT NULL,
                batch_size INTEGER NOT NULL,
                rps REAL NOT NULL,
                received_count INTEGER,
                verified_count INTEGER
            )
        """)
        conn.commit()

def save_metrics(
    test_id: str,
    total_time: float,
    rps: float,
    total_requests: int,
    success: int,
    errors: int,
    connection_limit: int,
    batch_size: int,
    received_count: int|None = None,
    verified_count: int|None = None
):
    with sqlite3.connect(METRICS_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO metrics (
                test_id, test_datetime, duration, total_requests,
                success_count, error_count, connection_limit, batch_size,
                rps, received_count, verified_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                test_id,
                datetime.now().isoformat(),
                total_time,
                total_requests,
                success,
                errors,
                connection_limit,
                batch_size,
                rps,
                received_count,
                verified_count
            )
        )
        conn.commit()

def get_last_metrics(limit: int = 10) -> list[dict]:
    with sqlite3.connect(METRICS_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                test_id, test_datetime, duration, total_requests,
                success_count, error_count, connection_limit, batch_size,
                rps, received_count, verified_count
            FROM metrics
            ORDER BY test_datetime DESC
            LIMIT ?
        """, (limit,))

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

def print_metrics_history(metrics: list[dict]):
    if not metrics:
        print("No metrics history available")
        return

    print("\nLast 10 test metrics:")
    print("-" * 120)
    print(f"{'Test ID':<36} | {'Date/Time':<19} | {'Duration':<8} | {'Req':<6} | {'Succ':<6} | "
          f"{'Err':<6} | {'Conn':<4} | {'Batch':<5} | {'RPS':<8} | {'Recv':<6} | {'Verif':<6}")
    print("-" * 120)

    for m in metrics:
        print(f"{m['test_id']} | {m['test_datetime'][:19]} | {m['duration']:7.2f}s | {m['total_requests']:6} | "
              f"{m['success_count']:6} | {m['error_count']:6} | {m['connection_limit']:4} | "
              f"{m['batch_size']:5} | {m['rps']:7.1f} | {m['received_count'] or 0:6} | "
              f"{m['verified_count'] or 0:6}")

def generate_postback(test_id: str) -> dict:
    return {
        "request_id": str(uuid.uuid4()),
        "test_id": test_id,
        "postback_type": "install",
        "event_name": "registration",
        "source_id": TEST_CONFIG.get("source_id"),
        "campaign_id": str(uuid.uuid4()),
        "placement_id": str(uuid.uuid4()),
        "adset_id": random.choice(["123456", "654321"]),
        "ad_id": random.choice(["0123456", "0654321"]),
        "advertising_id": test_id,
        "country": "ru",
        "click_id": str(uuid.uuid4()),
        "mmp": "appsflyer",
    }

def generate_all_postbacks(test_id: str, count: int = 100000) -> list[dict]:
    return [generate_postback(test_id) for _ in range(count)]


async def send_postback(postback: dict, session: aiohttp.ClientSession) -> bool:
    """Асинхронно отправляет один postback через aiohttp."""
    timeout = TEST_CONFIG.get("timeout","5.0")
    try:
        async with session.get(
            TEST_CONFIG.get("target_url"),
            params=postback,
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as response:
            response.raise_for_status()
            return True
    except Exception as e:
        logger.error(f"Postback failed: {e}")
        return False

async def process_batch(batch: list[dict], session: aiohttp.ClientSession) -> tuple[int, int]:
    """Обрабатывает один батч postbacks."""
    success = errors = 0
    tasks = [send_postback(pb, session) for pb in batch]

    for task in asyncio.as_completed(tasks):
        try:
            if await task:
                success += 1
            else:
                errors += 1
        except Exception as e:
            errors += 1
            logger.error(f"Task failed: {e}")

    return success, errors

async def send_postbacks_batched(
    postbacks: list[dict],
    session: aiohttp.ClientSession,
    batch_size: int = 1000
) -> tuple[int, int]:
    """Отправляет postbacks батчами с контролем памяти и нагрузки."""
    success_total = errors_total = 0
    total_batches = (len(postbacks) // batch_size) + 1

    for i in range(total_batches):
        batch = postbacks[i*batch_size : (i+1)*batch_size]
        if not batch:
            continue

        logger.info(f"Processing batch {i+1}/{total_batches} ({len(batch)} requests)")
        success, errors = await process_batch(batch, session)
        success_total += success
        errors_total += errors

    return success_total, errors_total


async def async_start(test_id:str,postbacks: list[dict],connection_limit:int,batch_size:int):
    """Основная асинхронная функция с замером времени."""
    start_time = time.perf_counter()

    connector = aiohttp.TCPConnector(limit=connection_limit, force_close=False,enable_cleanup_closed=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        success, errors = await send_postbacks_batched(
            postbacks,
            session=session,
            batch_size=batch_size,
        )

    total_time = time.perf_counter() - start_time
    rps = len(postbacks) / total_time
    total_requests=len(postbacks)
    print(f"\nResults:")
    print(f"Total requests: {total_requests:,}")
    print(f"Success: {success:,}, Errors: {errors:,}")
    print(f"Time: {total_time:.2f} sec")
    print(f"Requests per second (RPS): {rps:,.2f}")
    save_metrics(test_id=test_id,total_time=total_time,rps=rps,total_requests=total_requests,success=success,errors=errors,connection_limit=connection_limit,batch_size=batch_size)


def postback_preparation(postback_count:int, test_id:str):
    start_time = time.perf_counter()

    postbacks = generate_all_postbacks(test_id, count=postback_count)  # 1M запросов
    print(f"Generated {len(postbacks):,} postbacks")
    result_time = time.perf_counter() - start_time
    print(f"Generation time: {result_time:.2f} sec")
    return postbacks

def check_received_postbacks(test_id:str,db_path:str="requests.db"):
    with sqlite3.connect(db_path) as conn:
        received_count = conn.execute(
                    """SELECT COUNT(*) FROM received_requests r
                    WHERE r.test_id = ?""",
                    (test_id,)
                ).fetchone()[0]
        return received_count

def check_data_consistent(postbacks:list[dict],test_id:str,db_path:str="requests.db"):
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
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
                );""")

        for postback in postbacks:
            values = [value for value in postback.values()]
            conn.execute(
                """
                INSERT OR IGNORE INTO sending_requests
                (request_id, test_id, postback_type, event_name, source_id,
                 campaign_id, placement_id, adset_id, ad_id, advertising_id,
                 country, click_id, mmp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
        verified_received_count = conn.execute(
                """SELECT COUNT(*) FROM sending_requests s
                JOIN received_requests r ON s.request_id = r.request_id
                WHERE s.test_id = ?""",
                (test_id,)
            ).fetchone()[0]
        return verified_received_count

def update_metric(received:int,verified_count:int,test_id:str):
    with sqlite3.connect(METRICS_DB_PATH) as conn:
            conn.execute(
                """
                UPDATE metrics
                SET received_count = ?, verified_count = ?
                WHERE test_id = ?
                """,
                (received, verified_count, test_id)
            )
            conn.commit()

def parse_args():
    parser = argparse.ArgumentParser(description="Postback Load Tester")
    parser.add_argument(
        "--test_id",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--source_id",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--connection_limit",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=5.0,
    )
    return parser.parse_args()


def main():
    args = parse_args()
    TEST_CONFIG["test_id"] = args.test_id or str(uuid.uuid4())
    TEST_CONFIG["requests"] = args.requests or 10
    TEST_CONFIG["source_id"] = args.source_id or "100"
    TEST_CONFIG["connection_limit"] = args.connection_limit or 200
    TEST_CONFIG["batch_size"] = args.batch_size or 400
    TEST_CONFIG["timeout"] = args.timeout or 0.5
    init_metrics_db()

    postback_count=TEST_CONFIG.get("requests")
    test_id = TEST_CONFIG["test_id"]
    postbacks = postback_preparation(postback_count=postback_count,test_id=test_id)

    asyncio.run(async_start(test_id=test_id,postbacks=postbacks,connection_limit=TEST_CONFIG["connection_limit"],batch_size=TEST_CONFIG["batch_size"]))

    received = check_received_postbacks(test_id)
    print("received",received)
    if postback_count == received:
        verified_count = check_data_consistent(postbacks=postbacks,test_id=test_id)
        print("verified_count",verified_count)

        update_metric(received=received,verified_count=verified_count,test_id=test_id)

    metrics_history = get_last_metrics(10)
    print_metrics_history(metrics_history)


if __name__ == "__main__":
    main()
