import os
from locust import HttpUser, task, between
import uuid
import random
import sqlite3
from datetime import datetime

def init_db():
    conn = sqlite3.connect('requests.db')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS requests_send (
            request_id TEXT PRIMARY KEY,
            test_id TEXT,
            status_code INTEGER,
            response_time REAL,
            timestamp DATETIME
        )
    ''')
    conn.close()

init_db()


class PostbackUser(HttpUser):
    host = "http://0.0.0.0:8001"
    wait_time = between(float(os.getenv("LOCUST_MIN_WAIT", "0.01")),
                       float(os.getenv("LOCUST_MAX_WAIT", "0.05")))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_id = os.getenv("TEST_ID", str(uuid.uuid4()))
        self.postback_types = os.getenv("POSTBACK_TYPES", "install,event").split(",")
        self.event_names = os.getenv("EVENT_NAMES", "registration,level,purchase").split(",")
        self.source_ids = os.getenv("SOURCE_IDS", "100").split(",")
        self.mmp = os.getenv("MMP", "appmetrica,appsflyer").split(",")

    @task
    def send_postback(self):
        params = {
            "request_id": str(uuid.uuid4()),
            "test_id": self.test_id,
            "postback_type": random.choice(self.postback_types),
            "event_name": random.choice(self.event_names),
            "source_id": random.choice(self.source_ids),
            "campaign_id": str(uuid.uuid4()),
            "placement_id": str(uuid.uuid4()),
            "adset_id": random.choice(["123456", "654321"]),
            "ad_id": random.choice(["0123456", "0654321"]),
            "advertising_id": self.test_id,
            "country": "ru",
            "click_id": str(uuid.uuid4()),
            "mmp": random.choice(self.mmp),
        }

        start_time = datetime.now()
        with self.client.get("/verify", params=params, catch_response=True) as response:
            response_time = (datetime.now() - start_time).total_seconds()
            self.save_to_db(params, response.status_code, response_time)

    def save_to_db(self, params, status_code, response_time):
        conn = sqlite3.connect('requests.db')
        try:
            conn.execute('''
                INSERT INTO requests_send
                (request_id, test_id, status_code, response_time, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                params['request_id'],
                params['test_id'],
                status_code,
                response_time,
                datetime.now().isoformat()
            ))
            conn.commit()
        except Exception as e:
            print(f"Error saving to DB: {e}")
        finally:
            conn.close()
