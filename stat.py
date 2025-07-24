
import argparse
import sqlite3
from rich.console import Console
from rich.table import Table
from rich import box

DB_PATH = 'requests.db'

def get_last_test_id()->str:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute("""SELECT test_id FROM sending_requests
        ORDER BY created_at DESC
        LIMIT 1;""")
        test_id = cursor.fetchone()[0]
        return test_id or ""
def parse_args():
    parser = argparse.ArgumentParser(description="Postback Load Tester")
    parser.add_argument(
        "--test_id",
        type=str,
        default=get_last_test_id() or 'test_1',
        help="Test ID for get data from DB",
    )
    return parser.parse_args()

def verify_requests(test_id:str)->tuple[int,int]:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            "SELECT COUNT(*) FROM sending_requests WHERE test_id = ?", (test_id,)
        )
        sent_count = cursor.fetchone()[0]

        received_count = conn.execute(
                """SELECT COUNT(*) FROM sending_requests s
                JOIN received_requests r ON s.request_id = r.request_id
                WHERE s.test_id = ?""",
                (test_id,)
            ).fetchone()[0]

        return received_count, sent_count

def print_basic_stat(data: dict):
        if not data:
            return
        test_id =data.get("test_id")

        table = Table(
            title=f"Результат теста {test_id}",
            box=box.ROUNDED,
            title_style="bold cyan",
            header_style="bold magenta",
        )

        table.add_column("Отправлено", style="cyan", justify="center")
        table.add_column("Получено", style="blue", justify="center")
        table.add_column("Потеряно", style="red", justify="center")
        table.add_column("Успешно", style="green", justify="center")

        table.add_row(
            str(data.get("sent", 0)),
            str(data.get("received", 0)),
            str(data.get("errors", 0))+"%",
            str(data.get("success", 0))+"%",
        )



        Console().print(table)
def main():
    args = parse_args()
    received_count,sent_count = verify_requests(args.test_id)
    if sent_count > 0:
        success_rate = round((received_count / sent_count) * 100)
        error_rate = 100 - success_rate
    else:
        success_rate = 0
        error_rate = 100
    data = {"test_id":args.test_id,"sent":sent_count,"received":received_count,"errors":error_rate,"success":success_rate}
    print_basic_stat(data)



if __name__ == "__main__":
    main()
