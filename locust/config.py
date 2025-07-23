import logging
import os
import dotenv
import argparse
from pathlib import Path
from models import TestConfig as Config

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env"
dotenv.load_dotenv(ENV_FILE)
print("BASE_DIR", BASE_DIR),
print("env_file", ENV_FILE),
TARGET_URL = os.environ.get("TARGET_URL", "http://127.0.0.1:8001/verify")


TEST_CONFIG = Config(
    test_id=None,
    target_url=TARGET_URL,
    db_name="requests.db",
    request_count=10000,
    parallel_threads_count=1,
    # It's for generate random postbacks
    source_ids=["100"],
    postback_types=["install", "event"],
    event_names=["registration", "level", "purchase"],
    mmp=["appmetrica", "appsflyer"],
    # It's optional for limitation
    max_duration_minutes=60,
    max_requests_per_second=50,
)
logger.info({"event_log": "config", "massage": "Main config load success"})


def parse_args():
    parser = argparse.ArgumentParser(description="Postback Load Tester")
    parser.add_argument(
        "--test_id",
        type=str,
        default=TEST_CONFIG.test_id,
        help="Total number of requests to send",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=TEST_CONFIG.request_count,
        help="Total number of requests to send",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=TEST_CONFIG.parallel_threads_count,
        help="Number of parallel threads",
    )
    parser.add_argument(
        "--rps",
        type=int,
        default=TEST_CONFIG.max_requests_per_second,
        help="Maximum requests per second",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=TEST_CONFIG.max_duration_minutes,
        help="Maximum test duration in minutes",
    )
    parser.add_argument(
        "--target", type=str, default=TEST_CONFIG.target_url, help="Target URL to test"
    )
    return parser.parse_args()
