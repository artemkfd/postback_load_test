import logging
import os
import dotenv
from pathlib import Path
from postback_load_test.pydantic_models import Config

logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env"
dotenv.load_dotenv(ENV_FILE)

TARGET_URL = os.environ.get("TARGET_URL", "http://127.0.0.1:8001/verify")

TEST_CONFIG = Config(
    target_url=TARGET_URL,
    db_name="requests.db",
    request_count=10,
    parallel_threads_count=1,
    # It's for generate random postbacks
    source_ids=["100", "101", "102"],
    postback_types=["install", "event"],
    event_names=["registration", "level", "purchase"],
    mmp=["appmetrica", "appsflyer"],
    # It's optional for limitation
    max_duration_minutes=60,
    max_requests_per_second=50,
    delay=0,
)
logger.info({"event_log": "config", "massage": "Main config load success"})
