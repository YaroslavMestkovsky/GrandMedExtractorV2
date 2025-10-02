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
            'errors': '',
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
        """Агла!"""

        try:
            await self.setup_browser()

            # Автоматически включаем перенаправление загрузок в локальную папку и инжектим перехватчик
            try:
                self.redirect_dir.mkdir(parents=True, exist_ok=True)
                await self._inject_web_socket()
            except Exception as e:
                self.logger.warning(f"[Uploader] Не удалось включить перенаправление загрузок: {e}")

            url = self.config["site"]["url"]

            await self.page.goto(url)
            self.logger.info(f"[Uploader] Переход на страницу {url}")

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
                self.logger.info("[Uploader] Загрузки завершены, начало обработки файлов.")
                print()

            else:
                self.logger.error(
                    f"[Uploader] Проблемы с загрузкой файлов:"
                    f"\nАналитики: {self.analytics_uploaded}"
                    f"\nСпециалисты {self.specialists_uploaded}"
                    f"\nПациенты {self.users_uploaded}"
                )

            self._process_files()
        except RuntimeError:
            self._send_messages()
            await self._shutdown()
            pass
        except Exception as e:
            self.logger.error(e)
            self.report_messages['errors'] = e.args[0]
            self._send_messages()
            await self._shutdown()
        finally:
            self._send_messages()
            await self._shutdown()

    async def _upload_analytics(self):
        """Запуск формирования отчета по аналитикам."""

        self.logger.info("[Uploader] Начало загрузки Аналитик")
        await self._setup_upload(self.analytics)

        for action in self.config["analytics_actions"]:
            if action.get("calculate_date"):
                today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
                choices = action["choices"]

                if today == self.from_scratch_dates["year_first_day"]:
                    self.report_messages['messages'].append('Аналитики за предыдущий год.')
                    self.logger.info("[Uploader] Выгрузка за предыдущий год.")
                    action["text_to_search"] = choices["last_year"]

                elif today in self.from_scratch_dates["quarters_first_days"]:
                    self.report_messages['messages'].append('Аналитики за предыдущий квартал.')
                    self.logger.info("[Uploader] Выгрузка за предыдущий квартал")
                    action["text_to_search"] = choices[self.quarters_first_days[today]]

                elif today in self.from_scratch_dates["months_first_week_days"]:
                    self.report_messages['messages'].append('Аналитики за предыдущий месяц.')
                    self.logger.info("[Uploader] Выгрузка за предыдущий месяц.")
                    action["text_to_search"] = choices["last_month"]

                elif today in self.from_scratch_dates["mondays"]:
                    self.report_messages['messages'].append('Аналитики за предыдущую неделю.')
                    self.logger.info("[Uploader] Выгрузка за предыдущую неделю.")
                    action["text_to_search"] = choices["last_week"]

                else:
                    self.report_messages['messages'].append('Аналитики за предыдущий день.')
                    self.logger.info("[Uploader] Выгрузка за предыдущий день.")
                    action["text_to_search"] = choices["yesterday"]
                    self.from_scratch = False

            await self.click(action)

        seconds = 0

        while not self.analytics_uploaded:
            seconds += 1
            print(f"\r[Uploader] Ожидание загрузки: {seconds}...", end="", flush=True)

            await asyncio.sleep(1)

        print()
        self.logger.info("[Uploader] Аналитики загружены.")

    async def _upload_users(self):
        """Запуск формирования отчета по юзерам."""

        self.logger.info("[Uploader] Начало загрузки Пациентов")
        await self._setup_upload(self.users)

        for action in self.config["users_actions"]:
            await self.click(action)

        seconds = 0

        while not self.users_uploaded:
            seconds += 1
            print(f"\r[Uploader] Ожидание загрузки: {seconds}...", end="", flush=True)

            await asyncio.sleep(1)

        for action in self.config["users_after_upload_actions"]:
            await self.click(action)

        print()
        self.logger.info("[Uploader] Пациенты за предыдущий день загружены.")

    async def _upload_specialists(self):
        """Запуск формирования отчета по специалистам."""

        self.logger.info("[Uploader] Начало загрузки Специалистов")
        await self._setup_upload(self.specialists)

        for action in self.config["specialists_actions"]:
            if action.get("is_date", False):
                _start = self.dates_map[action["start"]]
                _end = self.dates_map[action["end"]]

                await self.fill_dates(action, _start, _end)
            else:
                await self.click(action)

        seconds = 0

        while not self.specialists_uploaded:
            seconds += 1
            print(f"\r[Uploader] Ожидание загрузки: {seconds}...", end="", flush=True)

            await asyncio.sleep(1)

        print()

        for action in self.config["specialists_after_upload_actions"]:
            await self.click(action)

        self.logger.info(f"[Uploader] Специалисты загружены.")

    def _process_files(self):
        """Обработка и загрузка в БД скачанных файлов."""

        kwargs = {}
        funcs = {
            'a': self._process_analytics,
            's': self._process_specialists,
            'u': self._process_users,
        }

        for file in self.files_to_process:
            path = self.redirect_dir.joinpath(file)

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
                self.logger.error(f"[Uploader] Ошибка при обработке файла: {file}")
                raise Exception

            df = pd.read_csv(
                path,
                skiprows=skip_rows,
                encoding='cp1251',
                delimiter=';',
                low_memory=False,
            )

            indices_to_drop = [df.index[i] for i in bottom_drops]
            df = df.drop(indices_to_drop)

            funcs[func](df, **kwargs)
            print()

    def _send_messages(self):
        """Отправка отчета в телеграм."""

        messages = self.report_messages.get('messages', [])
        errors = self.report_messages.get('errors', '')

        self.telegram_manager.send_messages(messages, errors)

    def _process_analytics(self, df, **kwargs):
        """Обработка файла аналитик."""

        self.sql_manager.process_analytics(df, **kwargs)
        self.logger.info("[Uploader] Аналитики обработаны.")

    def _process_specialists(self, df, **kwargs):
        """Обработка файла спецов."""

        self.sql_manager.process_specialists(df)
        self.logger.info("[Uploader] Специалисты обработаны.")

    def _process_users(self, df, **kwargs):
        """Обработка файла юзеров."""

        self.bitrix_manager.process(df)
        self.logger.info("[Uploader] Пользователи обработаны.")

    async def click(self, action):
        self.logger.info(f"[Uploader] Действие: {action['elem']}")

        if text_to_search := action.get("text_to_search"):
            inner_text = await self.page.locator(action["id"]).inner_text()

            if not text_to_search in inner_text:
                self.logger.error(f"[Uploader] Не найден элемент {text_to_search}")
                raise Exception

            locator = self.page.locator(f"{action['root_node']} >> text={text_to_search}").first
            await locator.click(timeout=action.get("timeout", self.base_timeout))
        elif key := action.get("key"):
            await self.page.keyboard.press(key)
        else:
            await self.page.click(action["id"], timeout=action.get("timeout", self.base_timeout))

        if sleep := action.get("sleep"):
            await asyncio.sleep(sleep)

        self.logger.info('[Uploader]\t- готово.')

    async def fill_dates(self, action, _start, _end):
        self.logger.info(f"[Uploader] Ввод: {action['elem']}")

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
        self.logger.info("[Uploader] Аутентификация...")

        for action in self.config["log_in_actions"]:
            action_type = action["type"]
            selector = action.get("selector", None)
            description = action.get("description", "")
            reset_wss = action.get("reset_wss", "")

            self.logger.debug(f"[Uploader] Выполнение действия: {description}")

            if reset_wss:
                # Скидываем все веб-сокеты без авторизации
                self.websockets_list = []

            if action_type == "click" and selector:
                await self.page.click(selector)
                self.logger.debug(f"[Uploader] \tВыполнено нажатие на элемент")
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

                self.logger.debug(f"[Uploader] \tВведен текст {value} в элемент")

        self.logger.info("\tГотово.")

    async def _connect_to_socket(self):
        """Подключение обработчиков к целевому WebSocket (через сервис)."""

        websocket_url = self.config["site"]["web-socket"]
        
        self.logger.info(f"[Uploader] Попытка подключения к WebSocket: {websocket_url}")

        if not self.websockets_list:
            msg = "[Uploader] Список WebSocket'ов пуст! Возможно, страница еще не загрузилась полностью."
            self.report_messages["errors"] = msg
            self.logger.error(msg)

            raise RuntimeError("Нет доступных WebSocket соединений")

        def on_writefileend(_payload: str) -> None:
            if self.active_download == self.users:
                self.users_uploaded = True
            elif self.active_download == self.analytics:
                self.analytics_uploaded = True
            elif self.active_download == self.specialists:
                self.specialists_uploaded = True

            asyncio.create_task(self._process_download_via_http())

        await self.service.connect_to_socket(
            websocket_url=websocket_url,
            websockets_list=self.websockets_list,
            ws_block_patterns=self.ws_block_patterns,
            on_writefileend=on_writefileend,
        )
        self.logger.info("[Uploader] WebSocket успешно подключен")

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
            return False

        self.service.redirect_dir = self.redirect_dir
        self.service.filename = self.filename
        self.service.download_params = self.download_params
        self.service.cookies = self.cookies
        ok = await self.service.download_via_http()

        return ok

    async def _process_download_via_http(self) -> None:
        """Обработка скачивания через HTTP после получения параметров из WebSocket."""

        try:
            # Небольшая задержка, чтобы параметры успели извлечься
            await asyncio.sleep(0.5)

            # Принудительно получить свежие параметры для текущего отчёта
            self.download_params = None
            self.service.download_params = None
            await self._get_download_params()

            # Если cookies еще не получены, пытаемся их получить
            if not self.cookies:
                await self._get_cookies()

            # Скачиваем файл через HTTP
            success = await self._download_file_via_http()

            if success:
                self.logger.info(f"[Uploader] Файл '{self.filename}' успешно скачан через HTTP.")
            else:
                self.logger.error(f"[Uploader] Не удалось скачать файл '{self.filename}' через HTTP.")

        except Exception as e:
            self.logger.error(f"[Uploader] Ошибка при обработке HTTP-скачивания: {e}")

    async def _shutdown(self) -> None:
        """Корректное завершение Playwright и браузера."""
        try:
            if self.page is not None:
                await self.page.close()
        except Exception:
            pass
        try:
            if self.context is not None:
                await self.context.close()
        except Exception:
            pass
        try:
            if self.browser is not None:
                await self.browser.close()
        except Exception:
            pass
        try:
            if self.playwright is not None:
                await self.playwright.stop()
        except Exception:
            pass

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
