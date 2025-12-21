import asyncio
import datetime
import calendar

from app_v3.browser.manager import BrowserManager
from app_v3.utils.config import app_config
from app_v3.utils.logger import app_logger


MAIN_CONFIG = app_config.main


class Orchestrator:
    """Класс, объединяющий все необходимые для загрузки и обработки файлов менеджеры, сервисы и службы."""

    def __init__(self):
        self.browser_manager = BrowserManager()

        # Даты
        self.dates_map = {
            'yesterday': datetime.datetime.today() - datetime.timedelta(days=1),
            'three_weeks_before': datetime.datetime.today() - datetime.timedelta(days=31),
            'today': datetime.datetime.today(),
        }
        self._fill_from_scratches_dates()

    async def run(self):
        """Alga!"""

        app_logger.info("=" * 60)
        app_logger.info("[Orch] Старт")
        app_logger.info("=" * 60)

        await self.browser_manager.setup_browser()

        url = MAIN_CONFIG["site"]["url"]
        await self.browser_manager.page.goto(url)

        await self._log_in()


        await self.browser_manager.shutdown()

    async def _log_in(self):
        """Вход в систему."""

        app_logger("[Orch] Вход в систему")

        for action in MAIN_CONFIG["log_in_actions"]:
            await self.browser_manager.click(action)

    def _fill_from_scratches_dates(self):
        """Подготовка словаря дат для определения периода перезаписи аналитик."""

        current_year = datetime.datetime.today().year
        year_first_day = datetime.datetime(current_year, 1, 1)

        quarters_first_days = [
            datetime.datetime(current_year, 4, 1),
            datetime.datetime(current_year, 7, 1),
            datetime.datetime(current_year, 10, 1),
        ]
        self.quarters_first_days = {
            datetime.datetime(current_year, 4, 1): "quarter_one",
            datetime.datetime(current_year, 7, 1): "quarter_two",
            datetime.datetime(current_year, 10, 1): "quarter_three",
        }

        months_first_week_days = [
            datetime.datetime(current_year, month, day)
            for month in range(1, 13)
            for day in range(1, 8)
            if datetime.datetime(current_year, month, day) not in quarters_first_days
        ]

        mondays = []

        for month in range(1, 13):
            _, last_day = calendar.monthrange(current_year, month)

            for day in range(1, last_day):
                _date = datetime.datetime(current_year, month, day)
                if (
                    _date not in quarters_first_days
                    and _date not in months_first_week_days
                    and _date.weekday() == 0
                ):
                    mondays.append(_date)

        self.from_scratch_dates = {
            "year_first_day": year_first_day, # Первый день в году перегружаем за предыдущий год
            # В начале каждого квартала перегружаем за предыдущий квартал:
            # в начале второго квартала за первый, третьего за второй и четвертого за третий
            "quarters_first_days": quarters_first_days,
            # Первые 7 дней каждого месяца перегружаем за предыдущий месяц
            "months_first_week_days": months_first_week_days,
            # Каждый понедельник грузим за предыдущую неделю
            "mondays": mondays,
        }
