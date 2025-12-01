import asyncio
import logging
import os

import pandas as pd
import urllib3
import yaml
import datetime
import calendar

from uuid import uuid4
from pathlib import Path

from typing import (
    Dict,
    Any,
    Optional,
)
from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
)

from manager import (
    SQLManager,
    BitrixManager,
    TelegramManager,
)
from service import SocketService


# Отключение предупреждения о небезопасных HTTPS-запросах
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Uploader:
    """Выгрузка отчётов."""

    def __init__(self, config_path: str = "app/config.yaml"):
        self.config = self._load_config(config_path)
        self._setup_logging()

        # Отчет в телеграмм
        self.report_messages = {
            'messages': [],
            'errors': [],
            'statistics': {
                'analytics': {'uploaded': False, 'processed': False, 'records': 0},
                'specialists': {'uploaded': False, 'processed': False, 'records': 0},
                'users': {'uploaded': False, 'processed': False, 'records': 0},
            },
            'start_time': None,
            'end_time': None,
        }

        # Сервисы
        self.service: Optional[SocketService] = None
        self.sql_manager: Optional[SQLManager] = SQLManager(logger=self.logger, messages=self.report_messages)
        self.bitrix_manager: Optional[BitrixManager] = BitrixManager(logger=self.logger, messages=self.report_messages)
        self.telegram_manager: Optional[TelegramManager] = TelegramManager(logger=self.logger)

        # Браузер
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.base_timeout = 30000

        # Сокет
        self.websockets_list: list = []
        self.ws_block_patterns: list[str] = []
        self.cancel_download_patterns: list[str] = []

        # Файлы
        self.redirect_dir: Path = Path(self.config["download"]["output_dir"]).absolute()
        self.filename: str = 'dummy'
        self.files_to_process: list = []
        self.download_params: Optional[Dict[str, Any]] = None
        self.cookies: Dict[str, str] = {}

        # Флаги
        self.analytics_uploaded = False
        self.specialists_uploaded = False
        self.users_uploaded = False
        self.force_upload_today = False
        self.from_scratch = True

        # Текущее активное скачивание
        self.active_download: Optional[str] = None
        self.users = 'users'
        self.analytics = 'analytics'
        self.specialists = 'specialists'

        # Пути к локальным браузерам
        self.browser_paths = {
            "chromium": str(Path("browsers/chromium/chrome-win").absolute()),
            "firefox": str(Path("browsers/firefox").absolute()),
            "webkit": str(Path("browsers/webkit").absolute()),
        }

        # Даты
        self.dates_map = {
            'yesterday': datetime.datetime.today() - datetime.timedelta(days=1),
            'three_weeks_before': datetime.datetime.today() - datetime.timedelta(days=31),
            'today': datetime.datetime.today(),
        }
        self.from_scratch_dates = {}
        self._fill_from_scratches_dates()

    async def run(self):
        """Основной метод запуска процесса выгрузки отчётов."""
        
        self.report_messages['start_time'] = datetime.datetime.now()
        self.logger.info("=" * 60)
        self.logger.info("[Uploader] Запуск процесса выгрузки отчётов")
        self.logger.info("=" * 60)

        try:
            await self.setup_browser()

            # Автоматически включаем перенаправление загрузок в локальную папку и инжектим перехватчик
            try:
                self.redirect_dir.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"[Uploader] Создана директория для загрузок: {self.redirect_dir}")
                await self._inject_web_socket()
            except Exception as e:
                error_msg = f"Не удалось настроить перенаправление загрузок: {str(e)}"
                self.logger.warning(f"[Uploader] {error_msg}")
                self._add_error(error_msg)

            url = self.config["site"]["url"]
            self.logger.info(f"[Uploader] Переход на страницу: {url}")
            await self.page.goto(url)
            self.logger.debug("[Uploader] Страница загружена")

            await self._log_in()

            # Даем время WebSocket'ам установиться после логина
            await asyncio.sleep(2)

            await self._connect_to_socket()
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

            if all((
                self.analytics_uploaded,
                self.specialists_uploaded,
                self.users_uploaded,
            )):
                self.logger.info("[Uploader] Все файлы успешно загружены, начало обработки")
                print()
            else:
                error_details = (
                    f"Не все файлы загружены:\n"
                    f"  - Аналитики: {'✓' if self.analytics_uploaded else '✗'}\n"
                    f"  - Специалисты: {'✓' if self.specialists_uploaded else '✗'}\n"
                    f"  - Пациенты: {'✓' if self.users_uploaded else '✗'}"
                )
                self.logger.error(f"[Uploader] {error_details}")
                self._add_error(error_details)

            self._process_files()
            
            self.report_messages['end_time'] = datetime.datetime.now()
            duration = (self.report_messages['end_time'] - self.report_messages['start_time']).total_seconds()
            self.logger.info(f"[Uploader] Процесс завершён успешно. Время выполнения: {duration:.1f} сек")
            
        except Exception as e:
            error_msg = str(e) if e.args else "Неизвестная ошибка"
            self.logger.error(f"[Uploader] Критическая ошибка: {error_msg}", exc_info=True)
            self._add_error(f"Критическая ошибка: {error_msg}")
            self.report_messages['end_time'] = datetime.datetime.now()
        finally:
            self._send_messages()
            await self._shutdown()

    async def _upload_analytics(self):
        """Запуск формирования отчета по аналитикам."""

        self.logger.info("[Uploader] Начало загрузки аналитик")
        await self._setup_upload(self.analytics)

        for action in self.config["analytics_actions"]:
            if action.get("calculate_date"):
                today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
                choices = action["choices"]

                if today == self.from_scratch_dates["year_first_day"]:
                    period_msg = "Аналитики за предыдущий год"
                    self._add_message(period_msg)
                    self.logger.info(f"[Uploader] Период выгрузки: {period_msg}")
                    action["text_to_search"] = choices["last_year"]

                elif today in self.from_scratch_dates["quarters_first_days"]:
                    period_msg = "Аналитики за предыдущий квартал"
                    self._add_message(period_msg)
                    self.logger.info(f"[Uploader] Период выгрузки: {period_msg}")
                    action["text_to_search"] = choices[self.quarters_first_days[today]]

                elif today in self.from_scratch_dates["months_first_week_days"]:
                    period_msg = "Аналитики за предыдущий месяц"
                    self._add_message(period_msg)
                    self.logger.info(f"[Uploader] Период выгрузки: {period_msg}")
                    action["text_to_search"] = choices["last_month"]

                    # Тут выгружается за предыдущий месяц - не в смысле текущая дата -30, а с 1 по 30 пред. месяца.
                    # А значит, сегодняшний день надо загрузить отдельно.
                    self.force_upload_today = True
                    self.logger.debug("[Uploader] Будет выполнена дополнительная загрузка за сегодняшний день")

                elif today in self.from_scratch_dates["mondays"]:
                    period_msg = "Аналитики за предыдущую неделю"
                    self._add_message(period_msg)
                    self.logger.info(f"[Uploader] Период выгрузки: {period_msg}")
                    action["text_to_search"] = choices["last_week"]

                else:
                    period_msg = "Аналитики за предыдущий день"
                    self._add_message(period_msg)
                    self.logger.info(f"[Uploader] Период выгрузки: {period_msg}")
                    action["text_to_search"] = choices["yesterday"]
                    self.from_scratch = False

            await self.click(action)

        seconds = 0
        max_wait_time = 3600 * 4  # Максимальное время ожидания: 4 часа

        while not self.analytics_uploaded and seconds < max_wait_time:
            seconds += 1
            print(f"\r[Uploader] Ожидание загрузки аналитик: {seconds} сек...", end="", flush=True)
            await asyncio.sleep(1)

        print()

        if not self.analytics_uploaded:
            error_msg = f"Таймаут загрузки аналитик ({max_wait_time} сек)"
            self.logger.error(f"[Uploader] {error_msg}")
            self._add_error(error_msg)
        else:
            self.report_messages['statistics']['analytics']['uploaded'] = True
            self.logger.info("[Uploader] Аналитики успешно загружены")

        # Дополнительная загрузка за сегодняшний день при необходимости
        if self.force_upload_today:
            self.logger.info("[Uploader] Начало дополнительной загрузки аналитик за сегодняшний день")
            await self._setup_upload(self.analytics)
            self.analytics_uploaded = False
            self.from_scratch = False

            for action in self.config["analytics_actions"]:
                await self.click(action)

            seconds = 0

            while not self.analytics_uploaded and seconds < max_wait_time:
                seconds += 1
                print(f"\r[Uploader] Ожидание дополнительной загрузки: {seconds} сек...", end="", flush=True)
                await asyncio.sleep(1)

            print()
            
            if self.analytics_uploaded:
                self.logger.info("[Uploader] Дополнительная загрузка аналитик завершена")
            else:
                error_msg = f"Таймаут дополнительной загрузки аналитик ({max_wait_time} сек)"
                self.logger.error(f"[Uploader] {error_msg}")
                self._add_error(error_msg)

    async def _upload_users(self):
        """Запуск формирования отчета по пользователям."""

        self.logger.info("[Uploader] Начало загрузки пациентов")
        await self._setup_upload(self.users)

        for action in self.config["users_actions"]:
            await self.click(action)

        seconds = 0
        max_wait_time = 3600

        while not self.users_uploaded and seconds < max_wait_time:
            seconds += 1
            print(f"\r[Uploader] Ожидание загрузки пациентов: {seconds} сек...", end="", flush=True)
            await asyncio.sleep(1)

        print()

        if not self.users_uploaded:
            error_msg = f"Таймаут загрузки пациентов ({max_wait_time} сек)"
            self.logger.error(f"[Uploader] {error_msg}")
            self._add_error(error_msg)
        else:
            self.report_messages['statistics']['users']['uploaded'] = True
            self.logger.info("[Uploader] Пациенты успешно загружены")

        for action in self.config["users_after_upload_actions"]:
            await self.click(action)

    async def _upload_specialists(self):
        """Запуск формирования отчета по специалистам."""

        self.logger.info("[Uploader] Начало загрузки специалистов")
        await self._setup_upload(self.specialists)

        for action in self.config["specialists_actions"]:
            if action.get("is_date", False):
                _start = self.dates_map[action["start"]]
                _end = self.dates_map[action["end"]]

                await self.fill_dates(action, _start, _end)
            else:
                await self.click(action)

        seconds = 0
        max_wait_time = 3600

        while not self.specialists_uploaded and seconds < max_wait_time:
            seconds += 1
            print(f"\r[Uploader] Ожидание загрузки специалистов: {seconds} сек...", end="", flush=True)
            await asyncio.sleep(1)

        print()

        if not self.specialists_uploaded:
            error_msg = f"Таймаут загрузки специалистов ({max_wait_time} сек)"
            self.logger.error(f"[Uploader] {error_msg}")
            self._add_error(error_msg)
        else:
            self.report_messages['statistics']['specialists']['uploaded'] = True
            self.logger.info("[Uploader] Специалисты успешно загружены")

        for action in self.config["specialists_after_upload_actions"]:
            await self.click(action)

    def _process_files(self):
        """Обработка и загрузка в БД скачанных файлов."""

        self.logger.info(f"[Uploader] Начало обработки {len(self.files_to_process)} файлов")
        
        kwargs = {}
        funcs = {
            'a': self._process_analytics,
            's': self._process_specialists,
            'u': self._process_users,
        }

        for file in self.files_to_process:
            path = self.redirect_dir.joinpath(file)
            
            if not path.exists():
                error_msg = f"Файл не найден: {file}"
                self.logger.error(f"[Uploader] {error_msg}")
                self._add_error(error_msg)
                continue
            
            self.logger.info(f"[Uploader] Обработка файла: {file}")

            if self.analytics in file:
                skip_rows = 3
                bottom_drops = [-1]
                func = 'a'

                if self.from_scratch:
                    kwargs['from_scratch'] = True

            elif self.specialists in file:
                skip_rows = 2
                bottom_drops = []
                func = 's'
            elif self.users in file:
                skip_rows = 2
                bottom_drops = [-1]
                func = 'u'
            else:
                error_msg = f"Неизвестный тип файла: {file}"
                self.logger.error(f"[Uploader] {error_msg}")
                self._add_error(error_msg)
                continue

            try:
                df = pd.read_csv(
                    path,
                    skiprows=skip_rows,
                    encoding='cp1251',
                    delimiter=';',
                    low_memory=False,
                )
                self.logger.debug(f"[Uploader] Файл прочитан: {len(df)} строк")

                indices_to_drop = [df.index[i] for i in bottom_drops]
                if indices_to_drop:
                    df = df.drop(indices_to_drop)
                    self.logger.debug(f"[Uploader] Удалено {len(indices_to_drop)} строк снизу")

                funcs[func](df, **kwargs)
                print()
            except Exception as e:
                error_msg = f"Ошибка при обработке файла {file}: {str(e)}"
                self.logger.error(f"[Uploader] {error_msg}", exc_info=True)
                self._add_error(error_msg)

    def _send_messages(self):
        """Отправка отчета в телеграм."""

        messages = self.report_messages.get('messages', [])
        errors = self.report_messages.get('errors', [])

        self.logger.info("[Uploader] Подготовка отчёта для Telegram")
        self.telegram_manager.send_messages(messages, errors, self.report_messages.get('statistics', {}))

    def _process_analytics(self, df, **kwargs):
        """Обработка файла аналитик."""

        self.logger.info(f"[Uploader] Начало обработки аналитик: {len(df)} записей")
        df = self.sql_manager.process_analytics(df, **kwargs)

        self.logger.info(f"[Uploader] Обработка аналитик для выгрузки Косметологии в битрикс.")
        self.bitrix_manager.process_analytics(df)

        self.report_messages['statistics']['analytics']['processed'] = True
        self.logger.info("[Uploader] Аналитики успешно обработаны")

    def _process_specialists(self, df, **kwargs):
        """Обработка файла специалистов."""

        self.logger.info(f"[Uploader] Начало обработки специалистов: {len(df)} записей")
        self.sql_manager.process_specialists(df)
        self.report_messages['statistics']['specialists']['processed'] = True
        self.logger.info("[Uploader] Специалисты успешно обработаны")

    def _process_users(self, df, **kwargs):
        """Обработка файла пользователей."""

        self.logger.info(f"[Uploader] Начало обработки пациентов: {len(df)} записей")
        self.bitrix_manager.process(df)
        self.report_messages['statistics']['users']['processed'] = True
        self.logger.info("[Uploader] Пациенты успешно обработаны")

    async def click(self, action):
        action_desc = action.get('elem', action.get('description', 'Неизвестное действие'))
        self.logger.debug(f"[Uploader] Выполнение действия: {action_desc}")

        try:
            if text_to_search := action.get("text_to_search"):
                inner_text = await self.page.locator(action["id"]).inner_text()

                if text_to_search not in inner_text:
                    error_msg = f"Элемент '{text_to_search}' не найден на странице"
                    self.logger.error(f"[Uploader] {error_msg}")
                    raise Exception(error_msg)

                locator = self.page.locator(f"{action['root_node']} >> text={text_to_search}").first
                await locator.click(timeout=action.get("timeout", self.base_timeout))
            elif key := action.get("key"):
                await self.page.keyboard.press(key)
            else:
                await self.page.click(action["id"], timeout=action.get("timeout", self.base_timeout))

            if sleep := action.get("sleep"):
                await asyncio.sleep(sleep)

            self.logger.debug(f"[Uploader] Действие '{action_desc}' выполнено успешно")
        except Exception as e:
            error_msg = f"Ошибка при выполнении действия '{action_desc}': {str(e)}"
            self.logger.error(f"[Uploader] {error_msg}")
            raise

    async def fill_dates(self, action, _start, _end):
        action_desc = action.get('elem', 'Ввод дат')
        self.logger.debug(f"[Uploader] {action_desc}: {_start.strftime('%d.%m.%Y')} - {_end.strftime('%d.%m.%Y')}")

        try:
            container = self.page.locator(action["id"])
            row = container.locator(action["row_text"])

            await row.click()
            await self.page.keyboard.type(_start.strftime('%d.%m.%Y'))
            await asyncio.sleep(1)
            await self.page.keyboard.press("Tab")
            await asyncio.sleep(0.5)
            await self.page.keyboard.type(_end.strftime('%d.%m.%Y'))
            await asyncio.sleep(1)
            await self.page.keyboard.press("Tab")
            self.logger.debug(f"[Uploader] Даты введены успешно")
        except Exception as e:
            error_msg = f"Ошибка при вводе дат: {str(e)}"
            self.logger.error(f"[Uploader] {error_msg}")
            raise

    async def _setup_upload(self, active_download):
        now = datetime.datetime.now().strftime('d%d_m%m_y%Y')

        self.active_download = active_download
        self.filename = f'{active_download}__{now}__{uuid4().hex[:4]}.csv'
        self.files_to_process.append(self.filename)

        # Сброс параметров загрузки перед новым формированием отчёта
        self.download_params = None
        self.service.download_params = None

        try:
            await self.page.evaluate('() => { window.__DOWNLOAD_PARAMS = undefined; }')
            self.logger.debug('[Uploader] Сброшены параметры скачивания в окне')
        except Exception:
            pass

        # Обновить параметры для перехватчика
        await self._update_download_params()

    def _setup_logging(self) -> None:
        """Настройка логирования."""

        log_config = self.config["logging"]
        log_params = {
            "encoding": "utf-8",
            "level": getattr(logging, log_config["level"]),
            "format": "%(asctime)s - %(levelname)s - %(message)s",
        }

        if log_config["log_in_file"]:
            log_params["filename"] = log_config["file"]

        logging.basicConfig(**log_params)
        self.logger = logging.getLogger(__name__)

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

    async def setup_browser(self) -> None:
        """Инициализация браузера и создание нового контекста."""

        self.playwright = await async_playwright().start()

        # Используем локальный путь к браузеру
        executable_path = os.path.join(self.browser_paths["chromium"], "chrome.exe")

        if not os.path.exists(executable_path):
            self.logger.warning(f"[Uploader] Локальный браузер не найден по пути {executable_path}")
            self.logger.info("[Uploader] Используем браузер из системной установки")
            self.browser = await self.playwright.chromium.launch(
                headless=False,
                args=["--ignore-certificate-errors"],
            )
        else:
            self.logger.info(f"[Uploader] Используем локальный браузер из {executable_path}")
            self.browser = await self.playwright.chromium.launch(
                headless=False,
                executable_path=executable_path,
                args=["--start-maximized", "--ignore-certificate-errors"],
            )

        # Настраиваем контекст с отключенным автоматического открытия файлов
        self.context = await self.browser.new_context(
            no_viewport=True,
            accept_downloads=True,
            ignore_https_errors=True,
        )
        # Устанавливаем обработчики событий
        self.page = await self.context.new_page()

        # Инициализируем сервис скачивания
        self.service = SocketService(self.context, self.page, self.config, logger=self.logger)

        # Перехватываем все WebSocket'ы
        def on_websocket_created(ws):
            self.websockets_list.append(ws)

        self.page.on("websocket", on_websocket_created)
        self.logger.info("[Uploader] Браузер успешно инициализирован")

    async def _log_in(self):
        self.logger.info("[Uploader] Начало процесса аутентификации")

        for action in self.config["log_in_actions"]:
            action_type = action["type"]
            selector = action.get("selector", None)
            description = action.get("description", "")
            reset_wss = action.get("reset_wss", False)

            self.logger.debug(f"[Uploader] Шаг аутентификации: {description}")

            if reset_wss:
                # Скидываем все веб-сокеты без авторизации
                self.websockets_list = []
                self.logger.debug("[Uploader] Список WebSocket соединений сброшен")

            try:
                if action_type == "click" and selector:
                    await self.page.click(selector)
                    self.logger.debug(f"[Uploader] Нажатие выполнено: {description}")
                elif action_type == "input" and selector:
                    value = action["value"]

                    # Подстановка значений из конфигурации
                    if isinstance(value, str) and value.startswith("${"):
                        config_path = value[2:-1].split(".")
                        value = self.config

                        for key in config_path:
                            value = value[key]

                    await self.page.click(selector)
                    await self.page.type(selector, value)
                    self.logger.debug(f"[Uploader] Текст введён: {description}")
            except Exception as e:
                error_msg = f"Ошибка при выполнении шага '{description}': {str(e)}"
                self.logger.error(f"[Uploader] {error_msg}")
                raise

        self.logger.info("[Uploader] Аутентификация завершена успешно")

    async def _connect_to_socket(self):
        """Подключение обработчиков к целевому WebSocket (через сервис)."""

        websocket_url = self.config["site"]["web-socket"]
        
        self.logger.info(f"[Uploader] Подключение к WebSocket: {websocket_url}")

        if not self.websockets_list:
            error_msg = "Список WebSocket соединений пуст. Возможно, страница ещё не загрузилась полностью."
            self.logger.error(f"[Uploader] {error_msg}")
            self._add_error(error_msg)
            raise RuntimeError(error_msg)
        
        self.logger.debug(f"[Uploader] Найдено {len(self.websockets_list)} WebSocket соединений")

        def on_writefileend(_payload: str) -> None:
            if self.active_download == self.users:
                self.users_uploaded = True
            elif self.active_download == self.analytics:
                self.analytics_uploaded = True
            elif self.active_download == self.specialists:
                self.specialists_uploaded = True

            asyncio.create_task(self._process_download_via_http())

        try:
            await self.service.connect_to_socket(
                websocket_url=websocket_url,
                websockets_list=self.websockets_list,
                ws_block_patterns=self.ws_block_patterns,
                on_writefileend=on_writefileend,
            )
            self.logger.info("[Uploader] WebSocket успешно подключен и настроен")
        except Exception as e:
            error_msg = f"Ошибка при подключении к WebSocket: {str(e)}"
            self.logger.error(f"[Uploader] {error_msg}")
            self._add_error(error_msg)
            raise

    async def _interrupt_ws(self) -> None:
        """Грубое прерывание действий страницы для остановки скачивания и открытия файла.

        Стратегия: попытаться уйти на about:blank (быстро рвёт WS).
        """

        try:
            await self.page.goto("about:blank")
            self.logger.info("[Uploader] Успешно прервано автоматическое скачивание и открытие файла.")
        except Exception:
            self.logger.warning("[Uploader] Не удалось прервать скачивание файла браузером.")

    async def _inject_web_socket(self) -> None:
        """Инжект перехватчика WS; читает конфиг из window-глобалов."""

        try:
            await self.service.update_download_targets(self.redirect_dir, self.filename)
            await self.service.inject_interceptor()
            self.logger.info("[Uploader] JS перехватчик инжектирован в веб-сокет.")
        except Exception as e:
            self.logger.warning(f"[Uploader] Не удалось инжектировать JS перехватчик WS: {e}")

    async def _update_download_params(self) -> None:
        """Обновить параметры скачивания (директория и имя) в окне."""

        try:
            await self.service.update_download_targets(self.redirect_dir, self.filename)
        except Exception as e:
            self.logger.warning(f"[Uploader] Не удалось обновить параметры скачивания: {e}")

    async def _extract_download_params_async(self) -> None:
        """Асинхронно извлечь параметры скачивания при их появлении (через сервис)."""

        try:
            await self.service.ensure_params()
            if self.service.download_params and not self.download_params:
                self.download_params = self.service.download_params
                self.logger.info("[Uploader] Параметры скачивания синхронизированы из сервиса")
            if self.service.cookies and not self.cookies:
                self.cookies = self.service.cookies
        except Exception as e:
            self.logger.warning(f"[Uploader] Не удалось извлечь параметры заранее: {e}")

    async def _get_download_params(self) -> None:
        """Получить параметры скачивания (через сервис)."""

        try:
            await self.service.ensure_params()

            if self.service.download_params:
                self.download_params = self.service.download_params
        except Exception as e:
            self.logger.warning(f"[Uploader] Не удалось получить параметры скачивания: {e}")

    async def _get_cookies(self) -> None:
        """Получить cookies (через сервис)."""

        try:
            await self.service._get_cookies()
            if self.service.cookies:
                self.cookies = self.service.cookies
        except Exception as e:
            self.logger.warning(f"[Uploader] Не удалось получить cookies: {e}")

    async def _download_file_via_http(self) -> bool:
        """Скачать файл через HTTP-запрос используя параметры из WebSocket."""

        if not self.service:
            self.logger.error("[Uploader] Сервис скачивания не инициализирован")
            return False

        self.logger.debug(f"[Uploader] Начало HTTP-скачивания файла: {self.filename}")
        
        self.service.redirect_dir = self.redirect_dir
        self.service.filename = self.filename
        self.service.download_params = self.download_params
        self.service.cookies = self.cookies
        
        ok = await self.service.download_via_http()

        if ok:
            self.logger.debug(f"[Uploader] HTTP-скачивание завершено успешно: {self.filename}")
        else:
            self.logger.warning(f"[Uploader] HTTP-скачивание не удалось: {self.filename}")

        return ok

    async def _process_download_via_http(self) -> None:
        """Обработка скачивания через HTTP после получения параметров из WebSocket."""

        try:
            self.logger.debug(f"[Uploader] Обработка HTTP-скачивания для файла: {self.filename}")
            
            # Небольшая задержка, чтобы параметры успели извлечься
            await asyncio.sleep(0.5)

            # Принудительно получить свежие параметры для текущего отчёта
            self.download_params = None
            self.service.download_params = None
            await self._get_download_params()

            if not self.download_params:
                self.logger.warning(f"[Uploader] Параметры скачивания не получены для файла: {self.filename}")

            # Если cookies еще не получены, пытаемся их получить
            if not self.cookies:
                await self._get_cookies()

            # Скачиваем файл через HTTP
            success = await self._download_file_via_http()

            if success:
                self.logger.info(f"[Uploader] Файл '{self.filename}' успешно скачан через HTTP")
            else:
                error_msg = f"Не удалось скачать файл '{self.filename}' через HTTP"
                self.logger.error(f"[Uploader] {error_msg}")
                self._add_error(error_msg)

        except Exception as e:
            error_msg = f"Ошибка при обработке HTTP-скачивания файла '{self.filename}': {str(e)}"
            self.logger.error(f"[Uploader] {error_msg}", exc_info=True)
            self._add_error(error_msg)

    async def _shutdown(self) -> None:
        """Корректное завершение Playwright и браузера."""
        
        self.logger.info("[Uploader] Начало завершения работы")
        
        try:
            if self.page is not None:
                await self.page.close()
                self.logger.debug("[Uploader] Страница закрыта")
        except Exception as e:
            self.logger.warning(f"[Uploader] Ошибка при закрытии страницы: {str(e)}")
        
        try:
            if self.context is not None:
                await self.context.close()
                self.logger.debug("[Uploader] Контекст браузера закрыт")
        except Exception as e:
            self.logger.warning(f"[Uploader] Ошибка при закрытии контекста: {str(e)}")
        
        try:
            if self.browser is not None:
                await self.browser.close()
                self.logger.debug("[Uploader] Браузер закрыт")
        except Exception as e:
            self.logger.warning(f"[Uploader] Ошибка при закрытии браузера: {str(e)}")
        
        try:
            if self.playwright is not None:
                await self.playwright.stop()
                self.logger.debug("[Uploader] Playwright остановлен")
        except Exception as e:
            self.logger.warning(f"[Uploader] Ошибка при остановке Playwright: {str(e)}")
        
        self.logger.info("[Uploader] Завершение работы выполнено")

    def _add_message(self, message: str) -> None:
        """Добавить сообщение в отчёт."""
        self.report_messages['messages'].append(message)

    def _add_error(self, error: str) -> None:
        """Добавить ошибку в отчёт."""
        if isinstance(self.report_messages.get('errors'), list):
            self.report_messages['errors'].append(error)
        else:
            self.report_messages['errors'] = [error]

    @staticmethod
    def _load_config(config_path: str) -> Dict[str, Any]:
        """Загрузка конфигурации из YAML файла.

        Args:
            config_path: Путь к файлу конфигурации

        Returns:
            Dict[str, Any]: Загруженная конфигурация
        """

        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    @staticmethod
    def _extract_payload(frame):
        """Извлечение полезной нагрузки из сообщения веб-сокета."""

        for attr in ("text", "payload", "data"):
            try:
                value = getattr(frame, attr)

                if callable(value):
                    value = value()

                if value is not None:
                    return value
            except Exception:
                pass

        return frame
