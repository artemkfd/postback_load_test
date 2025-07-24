import asyncio
import multiprocessing
from pathlib import Path
import uuid
from config import parse_args, TEST_CONFIG
from database import DatabaseManager
from requester import RequestSender
from reporter import TestReporter
from runner import TestRunner

BASE_DIR = Path(__file__).resolve().parent.parent.parent


async def main():
    args = parse_args()
    config = TEST_CONFIG.model_copy(
        update={
            "test_id": args.test_id or str(uuid.uuid4()),
            "request_count": args.requests or 1,
            "parallel_threads_count": args.threads or 1,
            "max_requests_per_second": args.rps or 1_000_000,
            "max_duration_minutes": args.duration,
            "target_url": args.target,
        }
    )

    db_manager = DatabaseManager(BASE_DIR / f"postback_load_test/{config.db_name}")
    request_sender = RequestSender(config)
    reporter = TestReporter(config)

    runner = TestRunner(config, db_manager, request_sender, reporter)
    await runner.run_test()

    print("Test_id",config.test_id)


# async def run_test_instance():
#     await main()
if __name__ == "__main__":
    asyncio.run(main())
    # processes = []
    # for i in range(4):  # 4 параллельных процесса
    #     p = multiprocessing.Process(target=asyncio.run, args=(run_test_instance(),))
    #     p.start()
    #     processes.append(p)

    # for p in processes:
    #     p.join()
