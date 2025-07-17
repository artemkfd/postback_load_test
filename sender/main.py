import asyncio
from pathlib import Path
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
            "request_count": args.requests,
            "parallel_threads_count": args.threads,
            "max_requests_per_second": args.rps,
            "max_duration_minutes": args.duration,
            "target_url": args.target,
        }
    )

    db_manager = DatabaseManager(BASE_DIR / f"postback_load_test/{config.db_name}")
    request_sender = RequestSender(config)
    reporter = TestReporter(config)

    runner = TestRunner(config, db_manager, request_sender, reporter)
    await runner.run_test()


if __name__ == "__main__":
    asyncio.run(main())
