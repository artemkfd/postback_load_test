import csv
from dataclasses import dataclass
import sqlite3
import time
import requests
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

success =  0
errors = []
@dataclass
class Postback:
    postback_url:str
    response_text:str
    status_code:int


def init_metrics_db():
    with sqlite3.connect("postback.db") as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS postbacks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                postback_url TEXT,
                response_text TEXT,
                status_code INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

def save_to_db(data:Postback):
    with sqlite3.connect("postback.db") as conn:
        conn.execute("""
            INSERT INTO postbacks (postback_url, response_text, status_code)
            VALUES (?, ?, ?)
        """, (data.postback_url, data.response_text, data.status_code))
        conn.commit()

def send_request(url):
    response = requests.get(url, timeout=10, verify=False)
    response_status_code = response.status_code
    response_text = response.text

    return response_status_code, response_text



def main():
    global success
    global errors
    filename = "ru_dublgis_dgismobile_postbacks_2025_07_20_2025_07_27_UTC_1.csv"

    init_metrics_db()

    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        postback_urls = [row['Postback Url'] for row in reader]

        print(postback_urls)

    total = len(postback_urls)
    print(f"Found {total} URLs to process")

    start_time = time.time()

    for index,url in enumerate(postback_urls,1):

        remaining = total - index
        if remaining % 500 == 0 or remaining < 100:
            print(f"Осталось {remaining} запросов")
        try:
            status_code, response_text = send_request(url)
            data = Postback(
                postback_url=url,
                status_code=status_code,
                response_text=response_text
            )
            save_to_db(data)

            if status_code is None:
                errors.append({url: response_text})
                continue

            if status_code == 200:
                success += 1
            else:
                errors.append({url: response_text})

        except Exception as e:
            errors.append({url: str(e)})
            continue
        time.sleep(0.1)

    elapsed_time = time.time() - start_time
    print(f"\nWork done in {elapsed_time:.2f} seconds")
    print(f"Success: {success} ({success/total*100:.1f}%)")
    print(f"Errors: {len(errors)} ({len(errors)/total*100:.1f}%)")


    print("success", success)
    print("errors", len(errors))
    if errors:
        print("\nError details:")
        for error in errors[:10]:
            print(error)

if __name__ == "__main__":
    main()
