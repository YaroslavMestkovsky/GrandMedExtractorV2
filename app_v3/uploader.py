import asyncio
import datetime
import calendar
from uuid import uuid4

from app_v3.browser.manager import BrowserManager
from app_v3.utils.config import app_config
from app_v3.utils.logger import app_logger


MAIN_CONFIG = app_config.main


class Orchestrator:
    """Класс, объединяющий все необходимые для загрузки и обработки файлов менеджеры, сервисы и службы."""

    def __init__(self):
        self.browser_manager = BrowserManager()

        # Флаги
        self.analytics_uploaded = False
        self.cosmetology_uploaded = False
        self.specialists_uploaded = False
        self.users_uploaded = False

        # Загрузки
        self.users = 'users'
        self.analytics_today = 'analytics_today'
        self.analytics_period = 'analytics_period'
        self.specialists = 'specialists'

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
        # Даем время WebSocket'ам установиться после логина
        await asyncio.sleep(3)
        await self.browser_manager.connect_to_socket()
        print()

        await self._upload_analytics()
        await asyncio.sleep(3)
        print()

        await asyncio.sleep(10)
        await self.browser_manager.shutdown()

    async def _log_in(self):
        """Вход в систему."""

        app_logger.info("[Orch] Вход в систему")

        for action in MAIN_CONFIG["log_in_actions"]:
            if 'value' in action:
                await self.browser_manager.input(action)
            else:
                await self.browser_manager.click(action)

    async def _upload_analytics(self):
        """Загрузка файла аналитик. Каждый день грузим за предыдущий - чтобы выгрузить Косметологию в битрикс.

        Если это выгрузка за период, грузим сразу два файла.
        """

        choice = None

        app_logger.info("[Orch] Начало загрузки аналитик за вчерашний день")
        await self.browser_manager.setup_upload(self.analytics_today)

        # Сначала загружаем аналитики за вчерашний день, чтобы сразу обработать косметологию.
        for action in MAIN_CONFIG["analytics_actions"]:
            if action.get("calculate_date"):
                today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
                choices = action["choices"]

                if today == self.from_scratch_dates["year_first_day"]:
                    choice = choices["last_year"]
                elif today in self.from_scratch_dates["quarters_first_days"]:
                    choice = choices[self.quarters_first_days[today]]
                elif today in self.from_scratch_dates["months_first_week_days"]:
                    choice = choices["last_month"]
                elif today in self.from_scratch_dates["mondays"]:
                    choice = choices["last_week"]

            await self.browser_manager.click(action)

        await self._await_for_download(self.analytics_uploaded)

        # Если сегодня нужен период, грузим ещё один файл.
        if choice:
            app_logger.info("[Orch] Начало загрузки аналитик за период")
            await self.browser_manager.setup_upload(self.analytics_period)

            for action in MAIN_CONFIG["analytics_actions"]:
                if action.get("calculate_date"):
                    action["text_to_search"] = choice

                await self.browser_manager.click(action)

            await self._await_for_download(self.analytics_uploaded)

    async def _await_for_download(self, flag):
        """Ожидание загрузки файлов."""

        seconds = 0
        max_wait_time = 3600 * 4  # Максимальное время ожидания: 4 часа

        while not flag and seconds < max_wait_time:
            seconds += 1
            print(f"\r[Orch] Ожидание загрузки аналитик: {seconds} сек...", end="", flush=True)
            await asyncio.sleep(1)

        print()

        if not flag:
            error_msg = f"Таймаут загрузки ({max_wait_time} сек)"
            app_logger.error(f"[Orch] {error_msg}")
            # self._add_error(error_msg) # todo бот
        else:
            # self.report_messages['statistics']['analytics']['uploaded'] = True # todo бот
            app_logger.info("[Orch] Файл успешно загружен")

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
