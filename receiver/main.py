from collections import defaultdict
from pathlib import Path
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging
import uvicorn


app = FastAPI()
logger = logging.getLogger("receiver")
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "requests.db"


stats = defaultdict(int)
stats.update({"success_count": 0, "error_count": 0, "all_count": 0})


def save_data_to_db(data: dict):
    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    columns = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    sql = f"INSERT INTO received_requests ({columns}) VALUES ({placeholders})"

    try:
        cursor.execute(sql, tuple(data.values()))
        connection.commit()
    except sqlite3.IntegrityError as e:
        logger.warning(
            {"event_log": "save_request", "message": "duplicate request_id", "error": str(e)}
        )
    finally:
        connection.close()


@app.get("/verify")
async def verify(request: Request):
    logger.info("postback start")
    try:
        params = dict(request.query_params)

        logger.info({"event_log": "verify", "message": "receive message", "data": params})
        stats["all_count"] += 1

        save_data_to_db(params)

        return {"status": "ok"}

    except Exception as e:
        stats["error_count"] += 1
        logger.error({"event_log": "verify", "message": "exception", "error": e})
        return JSONResponse(status_code=500, content={"error": "internal server error"})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
