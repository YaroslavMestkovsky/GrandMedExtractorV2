import asyncio
import datetime
import calendar
from uuid import uuid4

from app_v3.browser.manager import BrowserManager
from app_v3.services.files import FileProcessor
from app_v3.utils.config import app_config
from app_v3.utils.logger import app_logger


MAIN_CONFIG = app_config.main


class Orchestrator:
    """Класс, объединяющий все необходимые для загрузки и обработки файлов менеджеры, сервисы и службы."""

    def __init__(self):
        self.browser_manager = BrowserManager()
        self.file_processor = FileProcessor(self.browser_manager.redirect_dir)

        # Файлы
        self.today_analytics_file = None
        self.period_analytics_file = None
        self.users_file = None
        self.specialists_file = None

        # Загрузки
        self.today_analytics = 'today_analytics'
        self.period_analytics = 'period_analytics'
        self.specialists = 'specialists'
        self.users = 'users'

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
        app_logger.info("[Orch] Начало загрузки данных")
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

        await self._upload_specialists()
        await asyncio.sleep(3)
        print()

        await self._upload_users()
        await asyncio.sleep(3)
        print()

        app_logger.info("=" * 60)
        app_logger.info("[Orch] Начало обработки загруженных данных.")
        app_logger.info("=" * 60)

        self.file_processor.process_today_analytics(self.today_analytics_file)
        await asyncio.sleep(10)

        if self.period_analytics_file:
            self.file_processor.process_period_analytics(self.period_analytics_file)

        self.file_processor.process_users(self.users_file)
        self.file_processor.process_specialists(self.specialists_file)

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
        file_name = await self.browser_manager.setup_upload(self.today_analytics)
        self.today_analytics_file = file_name

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

        await self.browser_manager.await_for_download()

        # Если сегодня нужен период, грузим ещё один файл.
        if choice:
            app_logger.info("[Orch] Начало загрузки аналитик за период")
            file_name = await self.browser_manager.setup_upload(self.period_analytics)
            self.period_analytics_file = file_name

            for action in MAIN_CONFIG["analytics_actions"]:
                if action.get("calculate_date"):
                    action["text_to_search"] = choice

                await self.browser_manager.click(action)

            await self.browser_manager.await_for_download()

    async def _upload_users(self):
        """Запуск формирования отчета по пользователям."""

        app_logger.info("[Orch] Начало загрузки пациентов")
        file_name = await self.browser_manager.setup_upload(self.users)
        self.users_file = file_name

        for action in MAIN_CONFIG["users_actions"]:
            await self.browser_manager.click(action)

        await self.browser_manager.await_for_download()

        for action in MAIN_CONFIG["users_after_upload_actions"]:
            await self.browser_manager.click(action)

    async def _upload_specialists(self):
        """Загрузка файла специалистов."""

        app_logger.info("[Orch] Начало загрузки специалистов")
        file_name = await self.browser_manager.setup_upload(self.specialists)
        self.specialists_file = file_name

        for action in MAIN_CONFIG["specialists_actions"]:
            if action.get("is_date", False):
                _start = self.dates_map[action["start"]]
                _end = self.dates_map[action["end"]]

                await self.browser_manager.fill_dates(action, _start, _end)
            else:
                await self.browser_manager.click(action)

        await self.browser_manager.await_for_download()

        for action in MAIN_CONFIG["specialists_after_upload_actions"]:
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
