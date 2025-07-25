import asyncio
import httpx
import logging
import time
import uuid
import random
from typing import List, Dict, Tuple
import uvloop
import aiohttp
uvloop.install()


logger = logging.getLogger(__name__)

async def send_postback(postback: dict, client: httpx.AsyncClient) -> bool:
    """Асинхронно отправляет один postback."""
    try:
        response = await client.get(
            "http://127.0.0.1:8001/verify",
            params=postback,
            timeout=5.0
        )
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Postback failed: {e}")
        return False

async def process_batch(batch: List[Dict], client: httpx.AsyncClient) -> Tuple[int, int]:
    """Обрабатывает один батч postbacks."""
    success = errors = 0
    tasks = [send_postback(pb, client) for pb in batch]

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
    batch_size: int = 1000,
    max_concurrent: int = 100

) -> Tuple[int, int]:
    """Отправляет postbacks батчами с контролем памяти и нагрузки."""
    success_total = errors_total = 0
    limits = httpx.Limits(max_connections=max_concurrent)

    async with httpx.AsyncClient(
        limits=limits,
        timeout=httpx.Timeout(6.0)
    ) as client:
        # Делим postbacks на равные батчи
        total_batches = (len(postbacks) // batch_size) + 1

        for i in range(total_batches):
            start_idx = i * batch_size
            end_idx = start_idx + batch_size
            batch = postbacks[start_idx:end_idx]

            if not batch:
                continue

            logger.info(f"Processing batch {i+1}/{total_batches} ({len(batch)} requests)")
            success, errors = await process_batch(batch, client)
            success_total += success
            errors_total += errors

    return success_total, errors_total

async def async_main(postbacks: List[Dict]):
    """Основная асинхронная функция с замером времени."""
    start_time = time.perf_counter()

    # Настройки батчинга:
    # - batch_size: сколько запросов в одном батче (оптимально 10k-50k)
    # - max_concurrent: сколько одновременных соединений (100-500)
    success, errors = await send_postbacks_batched(
        postbacks,
        batch_size=500,
        max_concurrent=300
    )

    total_time = time.perf_counter() - start_time
    rps = len(postbacks) / total_time if total_time > 0 else 0

    print(f"\nResults:")
    print(f"Total requests: {len(postbacks):,}")
    print(f"Success: {success:,}, Errors: {errors:,}")
    print(f"Time: {total_time:.2f} sec")
    print(f"Requests per second (RPS): {rps:,.2f}")

def generate_postback(test_id: str) -> dict:
    return {
        "request_id": str(uuid.uuid4()),
        "test_id": test_id,
        "postback_type": "click",
        "event_name": "purchase",
        "source_id": "facebook",
        "campaign_id": str(uuid.uuid4()),
        "placement_id": str(uuid.uuid4()),
        "adset_id": random.choice(["123456", "654321"]),
        "ad_id": random.choice(["0123456", "0654321"]),
        "advertising_id": test_id,
        "country": "ru",
        "click_id": str(uuid.uuid4()),
        "mmp": "appsflyer",
    }

def generate_all_postbacks(test_id: str, count: int = 100000) -> List[Dict]:
    """Генерирует тестовые postbacks."""
    return [generate_postback(test_id) for _ in range(count)]

def main():
    """Генерирует данные и запускает тест."""
    start_time = time.perf_counter()
    test_id = str(uuid.uuid4())
    postbacks = generate_all_postbacks(test_id, count=10_000)  # 1M запросов
    print(f"Generated {len(postbacks):,} postbacks")
    result_time = time.perf_counter() - start_time
    print(f"Generation time: {result_time:.2f} sec")
    return postbacks

async def async_main(postbacks: List[Dict]):
    start_time = time.perf_counter()

    connector = aiohttp.TCPConnector(limit=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        success, errors = await send_postbacks_batched(
            postbacks,
            batch_size=500,
            max_concurrent=300,
            session=session
        )

    total_time = time.perf_counter() - start_time
    rps = len(postbacks) / total_time
    print(f"RPS: {rps:,.2f}")

if __name__ == "__main__":
    postbacks = main()
    asyncio.run(async_main(postbacks))
