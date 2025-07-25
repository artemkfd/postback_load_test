import asyncio
from collections import defaultdict
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging
import uvicorn
import aiosqlite
from contextlib import asynccontextmanager
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "requests.db"

logger = logging.getLogger("receiver")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

class Database:
    _instance: Optional['Database'] = None

    def __init__(self):
        self._pool = None
        self._batch = []
        self._batch_size = 500
        self._lock = asyncio.Lock()

    @classmethod
    async def get_instance(cls):
        if cls._instance is None:
            cls._instance = Database()
            await cls._instance._initialize()
        return cls._instance

    async def _initialize(self):
        self._pool = await aiosqlite.connect(DB_PATH)
        await self._pool.execute("PRAGMA journal_mode=WAL")
        await self._pool.execute("PRAGMA synchronous=NORMAL")
        await self._pool.execute("PRAGMA cache_size=-10000")
        await self._pool.execute("PRAGMA temp_store=MEMORY")
        await self._pool.execute("PRAGMA mmap_size=268435456")
        await self._pool.execute("""CREATE TABLE IF NOT EXISTS received_requests (
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
                );""")
        await self._pool.commit()

    async def save_request(self, data: dict):
        async with self._lock:
            self._batch.append(data)
            if len(self._batch) >= self._batch_size:
                await self._flush_batch()

    async def _flush_batch(self):
        if not self._batch:
            return

        try:
            async with self._pool.cursor() as cursor:
                await cursor.executemany(
                    """INSERT OR IGNORE INTO received_requests
                    (request_id, test_id, postback_type, event_name,
                     source_id, campaign_id, placement_id, adset_id,
                     ad_id, advertising_id, country, click_id, mmp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(d.get("request_id"), d.get("test_id"), d.get("postback_type"),
                      d.get("event_name"), d.get("source_id"), d.get("campaign_id"),
                      d.get("placement_id"), d.get("adset_id"), d.get("ad_id"),
                      d.get("advertising_id"), d.get("country"), d.get("click_id"),
                      d.get("mmp")) for d in self._batch]
                )
                await self._pool.commit()
                self._batch.clear()
        except Exception as e:
            logger.error(f"Failed to save batch: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    db = await Database.get_instance()
    yield

    await db._flush_batch()
    if db._pool:
        await db._pool.close()

app = FastAPI(lifespan=lifespan)
stats = defaultdict(int)
stats.update({"success_count": 0, "error_count": 0, "all_count": 0})


@app.get("/flush")
async def flush(request: Request):
    db = await Database.get_instance()
    if db._batch:
        await db._flush_batch()
        if db._pool:
            await db._pool.close()
    return {"status": "flushed"}



@app.get("/verify")
async def verify(request: Request):
    try:
        params = dict(request.query_params)
        stats["all_count"] += 1

        db = await Database.get_instance()
        await db.save_request(params)

        stats["success_count"] += 1
        return {"status": "ok"}

    except Exception as e:
        stats["error_count"] += 1
        logger.error(f"Request failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "internal server error"}
        )

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        workers=1,
        # loop="uvloop",
        # http="h11",
        # limit_concurrency=5000
    )
