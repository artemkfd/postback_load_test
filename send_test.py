import asyncio
import httpx
import random
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import logging
logger = logging.getLogger(__name__)


def generate_postback(test_id: str) -> dict:
        return {
            "request_id": str(uuid.uuid4()),
            "test_id": test_id,
            "postback_type": "random.choice(self.config.postback_types)",
            "event_name": "random.choice(self.config.event_names)",
            "source_id": "random.choice(self.config.source_ids)",
            "campaign_id": str(uuid.uuid4()),
            "placement_id": str(uuid.uuid4()),
            "adset_id": random.choice(["123456", "654321"]),
            "ad_id": random.choice(["0123456", "0654321"]),
            "advertising_id": test_id,
            "country": "ru",
            "click_id": str(uuid.uuid4()),
            "mmp": "random.choice(self.config.mmp)",
        }
# async def async_generate_postback(test_id: str) -> dict:
#         return {
#             "request_id": str(uuid.uuid4()),
#             "test_id": test_id,
#             "postback_type": "random.choice(self.config.postback_types)",
#             "event_name": "random.choice(self.config.event_names)",
#             "source_id": "random.choice(self.config.source_ids)",
#             "campaign_id": str(uuid.uuid4()),
#             "placement_id": str(uuid.uuid4()),
#             "adset_id": random.choice(["123456", "654321"]),
#             "ad_id": random.choice(["0123456", "0654321"]),
#             "advertising_id": test_id,
#             "country": "ru",
#             "click_id": str(uuid.uuid4()),
#             "mmp": "random.choice(self.config.mmp)",
#         }

def generate_all_postbacks(test_id:str):
    postbacks = []
    for _ in range(100_000):
        postbacks.append(generate_postback(test_id))
    return postbacks

# async def async_generate_all_postbacks(test_id:str):
#     postbacks = []
#     for _ in range(1000_000):
#         postback = await async_generate_postback(test_id)
#         postbacks.append(postback)
#     return postbacks



async def send_postback(postback: dict, client: httpx.AsyncClient) -> bool:
    """Асинхронно отправляет один postback-запрос."""
    try:
        response = await client.get(
            "http://127.0.0.1:8001/verify",
            params=postback,
            timeout=10.0  # Таймаут на случай зависания
        )
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Postback failed: {e}")
        return False

async def send_postbacks_concurrently(postbacks: list[dict], max_concurrent: int = 100) -> tuple[int, int]:
    """Отправляет множество postbacks асинхронно с ограничением на параллелизм."""
    success = errors = 0

    # Ограничиваем количество одновременных соединений
    limits = httpx.Limits(max_connections=max_concurrent)

    async with httpx.AsyncClient(
        http2=True,  # Используем HTTP/2 для эффективности
        limits=limits,
        timeout=httpx.Timeout(10.0)
    ) as client:
        # Создаем задачи для всех postbacks
        tasks = [send_postback(pb, client) for pb in postbacks]

        # Обрабатываем результаты по мере завершения
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

async def async_main(postbacks: list[dict]):
    """Основная асинхронная функция с замером времени."""
    start_time = time.perf_counter()

    # Отправляем с ограничением в 100 одновременных запросов
    success, errors = await send_postbacks_concurrently(postbacks, max_concurrent=100)

    total_time = time.perf_counter() - start_time
    rps = len(postbacks) / total_time if total_time > 0 else 0

    print(f"Total requests: {len(postbacks)}")
    print(f"Success: {success}, Errors: {errors}")
    print(f"Time: {total_time:.2f} sec")
    print(f"Requests per second (RPS): {rps:.2f}")

def send_request_with_client(postback: dict):
    with httpx.Client() as client:
        try:
            response = client.get("http://127.0.0.1:8001/verify", params=postback)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error({
                "event_log": "send_request_failed",
                "error": str(e),
            })
            return False

def thread_pool_main(postbacks):
    start_time = time.perf_counter()
    success, errors = 0,0
    with httpx.Client() as client:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(send_request, postback, client) for postback in postbacks]

            # Ожидаем завершения всех задач и считаем результаты
            for future in futures:
                result = future.result()  # Блокирует, пока запрос не завершится
                if result:
                    success += 1
                else:
                    errors += 1
    result_time = time.perf_counter() - start_time
    print("thread_pool_main result_time",result_time)
    print("thread_pool_main success",success)
    print("thread_pool_main errors",errors)

# def process_pool_main(postbacks):
#     start_time = time.perf_counter()
#     success, errors = 0,0
#     with ProcessPoolExecutor(max_workers=2) as executor:
#         for postback in postbacks:
#             future = executor.submit(send_request_with_client, postback)
#             # print(future.result())
#             # result = future.result()
#             # if result:
#             #     success += 1
#             # else:
#             #     errors += 1
#     result_time = time.perf_counter() - start_time
#     print("process_pool_main result_time",result_time)
#     # print("process_pool_main success",success)
#     # print("process_pool_main errors",errors)

# async def async_main(postbacks):
#     start_time = time.perf_counter()
#     success, errors = 0,0
#     async with httpx.AsyncClient(http2=True) as client:
#         for postback in postbacks:
#             result = await async_send_request(postback=postback,client=client)
#             # print(result)
#             if result:
#                 success += 1
#             else:
#                 errors += 1
#     result_time = time.perf_counter() - start_time
#     print("async_main result_time",result_time)
#     print("async_main success",success)
#     print("async_main errors",errors)

def main():
    start_time = time.perf_counter()
    test_id = str(uuid.uuid4())
    result = generate_all_postbacks(test_id)
    print(len(result))
    result_time = time.perf_counter() - start_time
    print("main result_time",result_time)
    return result



async def send_postbacks_concurrently(postbacks: list[dict], max_concurrent: int = 100) -> tuple[int, int]:
    """Отправляет множество postbacks асинхронно с ограничением на параллелизм."""
    success = errors = 0

    # Ограничиваем количество одновременных соединений
    limits = httpx.Limits(max_connections=max_concurrent)

    async with httpx.AsyncClient(
        http2=True,  # Используем HTTP/2 для эффективности
        limits=limits,
        timeout=httpx.Timeout(10.0)
    ) as client:
        # Создаем задачи для всех postbacks
        tasks = [send_postback(pb, client) for pb in postbacks]

        # Обрабатываем результаты по мере завершения
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

async def async_main(postbacks: list[dict]):
    """Основная асинхронная функция с замером времени."""
    start_time = time.perf_counter()

    # Отправляем с ограничением в 100 одновременных запросов
    success, errors = await send_postbacks_concurrently(postbacks, max_concurrent=100)

    total_time = time.perf_counter() - start_time
    rps = len(postbacks) / total_time if total_time > 0 else 0

    print(f"Total requests: {len(postbacks)}")
    print(f"Success: {success}, Errors: {errors}")
    print(f"Time: {total_time:.2f} sec")
    print(f"Requests per second (RPS): {rps:.2f}")

if __name__ == "__main__":
    postbacks = main()
    asyncio.run(async_main(postbacks))
    # thread_pool_main(postbacks)
    # process_pool_main(postbacks)
