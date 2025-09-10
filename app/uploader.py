import asyncio
import logging
import os
from turtledemo.penrose import start

import urllib3
import yaml
import datetime

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

from service import SocketService


# Отключение предупреждения о небезопасных HTTPS-запросах
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Uploader:
    """Выгрузка отчётов."""

    def __init__(self, config_path: str = "app/config.yaml"):
        self.config = self._load_config(config_path)
        self._setup_logging()

        # Сервис загрузки
        self.service: Optional[SocketService] = None

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
        self.download_params: Optional[Dict[str, Any]] = None
        self.cookies: Dict[str, str] = {}

        # Флаги
        self.analytics_uploaded = False
        self.specialists_uploaded = False
        self.users_uploaded = False

        # Текущее активное скачивание
        self.active_download: Optional[str] = None
        self.USERS = 'users'
        self.ANALYTICS = 'analytics'
        self.SPECIALISTS = 'specialists'

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

    async def run(self):
        """Агла!"""

        try:
            await self.setup_browser()

            # Автоматически включаем перенаправление загрузок в локальную папку и инжектим перехватчик
            try:
                self.redirect_dir.mkdir(parents=True, exist_ok=True)
                await self._inject_web_socket()
            except Exception as e:
                self.logger.warning(f"[Uploader] Не удалось включить перенаправление загрузок: {e.args[0]}")

            await self.page.goto(self.config["site"]["url"])

            url = self.config["site"]["url"]
            self.logger.info(f"[Uploader] Переход на страницу {url}")

            await self._log_in()
            await self._connect_to_socket()

            await self._upload_analytics()
            await asyncio.sleep(3)
            await self._upload_specialists()
            await asyncio.sleep(3)
            await self._upload_users()

            if all((
                self.analytics_uploaded,
                self.specialists_uploaded,
                self.users_uploaded,
            )):
                self.logger.info("Загрузки завершены, начало обработки файлов.")
            else:
                self.logger.error(
                    f"Проблемы с загрузкой файлов:"
                    f"\nАналитики: {self.analytics_uploaded}"
                    f"\nСпециалисты {self.specialists_uploaded}"
                    f"\nПациенты {self.users_uploaded}"
                )

        except Exception as e:
            self.logger.error(e.args[0])
            await self.shutdown()
        finally:
            await self.shutdown()

    async def _upload_users(self):
        """Запуск формирования отчета по юзерам."""

        self.logger.info("[Uploader] Начало загрузки Пациентов")
        await self._setup_upload(self.USERS)

        for action in self.config["users_actions"]:
            await self.click(action)

        seconds = 0

        while not self.users_uploaded:
            seconds += 1
            print(f"\rОжидание загрузки: {seconds}...", end="", flush=True)

            await asyncio.sleep(1)

        for action in self.config["users_after_upload_actions"]:
            await self.click(action)

        print()
        self.logger.info("[Uploader] Пациенты за предыдущий день загружены.")

    async def _upload_specialists(self):
        """Запуск формирования отчета по специалистам."""

        self.logger.info("[Uploader] Начало загрузки Специалистов")
        await self._setup_upload(self.SPECIALISTS)

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
            print(f"\rОжидание загрузки: {seconds}...", end="", flush=True)

            await asyncio.sleep(1)

        print()

        for action in self.config["specialists_after_upload_actions"]:
            await self.click(action)

        self.logger.info(f"[Uploader] Специалисты загружены.")

    async def _upload_analytics(self):
        """Запуск формирования отчета по аналитикам."""

        self.logger.info("[Uploader] Начало загрузки Аналитик")
        await self._setup_upload(self.ANALYTICS)

        for action in self.config["analytics_actions"]:
            await self.click(action)

        seconds = 0

        while not self.analytics_uploaded:
            seconds += 1
            print(f"\rОжидание загрузки: {seconds}...", end="", flush=True)

            await asyncio.sleep(1)

        print()
        self.logger.info("[Uploader] Аналитики загружены.")

    async def click(self, action):
        self.logger.info(f"[Uploader] Действие: {action['elem']}")

        if text_to_search := action.get("text_to_search"):
            inner_text = await self.page.locator(action["id"]).inner_text()

            if not text_to_search in inner_text:
                self.logger.error(f"[Uploader] Не найден элемент {text_to_search}")
                raise Exception

            locator = self.page.locator(f"{action['root_node']} >> text={text_to_search}")
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
        await asyncio.sleep(0.5)
        await self.page.keyboard.press("Tab")
        await asyncio.sleep(0.5)
        await self.page.keyboard.type(_end.strftime('%d.%m.%Y'))
        await asyncio.sleep(0.5)
        await self.page.keyboard.press("Tab")

    async def _setup_upload(self, active_download):
        self.active_download = active_download
        now = datetime.datetime.now().strftime('d%d_m%m_y%Y')
        self.filename = f'{active_download}__{now}__{uuid4().hex[:4]}.csv'

        # Обновить параметры для перехватчика
        await self._update_download_params()

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Загрузка конфигурации из YAML файла.

        Args:
            config_path: Путь к файлу конфигурации

        Returns:
            Dict[str, Any]: Загруженная конфигурация
        """

        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

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
        self.logger.info("Аутентификация...")

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

        def on_writefileend(_payload: str) -> None:
            if self.active_download == self.USERS:
                self.users_uploaded = True
            elif self.active_download == self.ANALYTICS:
                self.analytics_uploaded = True
            elif self.active_download == self.SPECIALISTS:
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
            self.logger.warning(f"[Uploader] Не удалось инжектировать JS перехватчик WS: {e.args[0]}")

    async def _update_download_params(self) -> None:
        """Обновить параметры скачивания (директория и имя) в окне."""

        try:
            await self.service.update_download_targets(self.redirect_dir, self.filename)
        except Exception as e:
            self.logger.warning(f"[Uploader] Не удалось обновить параметры скачивания: {e.args[0]}")

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
            self.logger.warning(f"[Uploader] Не удалось извлечь параметры заранее: {e.args[0]}")

    async def _get_download_params(self) -> None:
        """Получить параметры скачивания (через сервис)."""

        try:
            await self.service.ensure_params()
            if self.service.download_params:
                self.download_params = self.service.download_params
        except Exception as e:
            self.logger.warning(f"[Uploader] Не удалось получить параметры скачивания: {e.args[0]}")

    async def _get_cookies(self) -> None:
        """Получить cookies (через сервис)."""

        try:
            await self.service._get_cookies()
            if self.service.cookies:
                self.cookies = self.service.cookies
        except Exception as e:
            self.logger.warning(f"[Uploader] Не удалось получить cookies: {e.args[0]}")

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

            # Если параметры еще не получены, пытаемся их получить
            if not self.download_params:
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
            self.logger.error(f"[Uploader] Ошибка при обработке HTTP-скачивания: {e.args[0]}")

    async def shutdown(self) -> None:
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
