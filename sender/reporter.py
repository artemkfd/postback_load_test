import logging
from rich.console import Console
from rich.table import Table
from rich import box

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "message": %(message)s}',
)
logger = logging.getLogger(__name__)


class TestReporter:
    def __init__(self, config):
        self.console = Console()
        self.config = config

    def print_console_report(self, test_id: str, duration: float, metrics: dict, stats: dict):
        main_table = Table(
            title=f"Результаты теста {test_id[:8]}...",
            box=box.ROUNDED,
            title_style="bold cyan",
            header_style="bold magenta",
        )
        main_table.add_column("Метрика", style="cyan", justify="right")
        main_table.add_column("Значение", style="green", justify="left")

        main_table.add_row("═" * 20, "═" * 20)
        main_table.add_row("Общие показатели", "")
        main_table.add_row("Длительность теста", f"{duration:.2f} сек")
        main_table.add_row("Скорость запросов", f"{metrics['rps']:.1f} RPS")
        main_table.add_row("Всего запросов", str(self.config.request_count))

        main_table.add_row("═" * 20, "═" * 20)
        main_table.add_row("Статусы запросов", "")

        total = stats["sent_count"]
        verified = stats["verified_success"]
        unverified = stats["unverified_success"]
        failed = stats["failed"]

        if verified + unverified + failed != total:
            logger.warning(
                f"Data inconsistency: verified({verified}) + unverified({unverified}) + failed({failed}) != total({total})"
            )
            unverified = max(0, total - verified - failed)

        main_table.add_row(
            "Успешно (HTTP 200 + проверка БД)", f"{verified} ({(verified/total*100):.1f}%)"
        )
        main_table.add_row(
            "Доставлено (HTTP 200, ожидает проверки)",
            f"{unverified} ({(unverified/total*100):.1f}%)",
        )
        main_table.add_row(
            "Ошибки соединения (не доставлено)", f"{failed} ({(failed/total*100):.1f}%)"
        )
        main_table.add_row(
            "Всего доставлено на сервер",
            f"{verified + unverified} ({(verified + unverified)/total*100:.1f}%)",
        )

        main_table.add_row("═" * 20, "═" * 20)
        main_table.add_row("Задержки (секунды)", "")
        main_table.add_row("Средняя", f"{metrics['avg_latency']:.4f}")
        main_table.add_row("Минимальная", f"{metrics['min_latency']:.4f}")
        main_table.add_row("Максимальная", f"{metrics['max_latency']:.4f}")
        main_table.add_row("90-й перцентиль", f"{metrics['p90']:.4f}")
        main_table.add_row("95-й перцентиль", f"{metrics['p95']:.4f}")
        main_table.add_row("99-й перцентиль", f"{metrics['p99']:.4f}")

        self.console.print(main_table)

    def print_history_comparison(self, history: list[dict]):
        if not history:
            return

        table = Table(
            title="История последних тестов",
            box=box.ROUNDED,
            title_style="bold cyan",
            header_style="bold magenta",
        )

        table.add_column("Тест", style="cyan", justify="center")
        table.add_column("Дата", style="blue", justify="center")
        table.add_column("Длительность", style="green", justify="right")
        table.add_column("Запросы", style="green", justify="right")
        table.add_column("RPS", style="green", justify="right")
        table.add_column("Задержка", style="yellow", justify="right")
        table.add_column("Успешных", style="green", justify="right")

        for row in history:
            test_id = (
                row.get("test_id", "")[:6] + ".."
                if len(row.get("test_id", "")) > 6
                else row.get("test_id", "")
            )
            date = (
                row.get("test_datetime", "")[11:19]
                if len(row.get("test_datetime", "")) > 10
                else row.get("test_datetime", "")
            )

            table.add_row(
                test_id,
                date,
                f"{row.get('duration', 0):.3f}",
                str(row.get("sending_count", 0)),
                f"{row.get('rps', 0):.1f}",
                f"{row.get('avg_latency', 0):.3f}",
                f"{row.get('verified_rate', 0):.1f}%",
            )

        self.console.print(table)
