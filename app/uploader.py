import asyncio
import logging
import os
import time

import yaml
import websockets

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


class SocketUploader:
    """Загрузка отчетов через веб-сокет."""

    def __init__(self, config_path: str = "app/config.yaml"):
        self.config = self._load_config(config_path)
        self._setup_logging()

        self.web_socket = None
        self.playwright = None
        self.websockets_list: list = []
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

        # Пути к локальным браузерам
        self.browser_paths = {
            "chromium": str(Path("browsers/chromium/chrome-win").absolute()),
            "firefox": str(Path("browsers/firefox").absolute()),
            "webkit": str(Path("browsers/webkit").absolute()),
        }

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Загрузка конфигурации из YAML файла.

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

    async def run(self):
        """Агла!"""

        await self.setup_browser()
        await self.page.goto(self.config["site"]["url"])

        url = self.config["site"]["url"]
        self.logger.info(f"Переход на страницу {url}")

        await self._log_in()
        await self._connect_to_socket()

        # self._upload_analytics()
        # self._upload_specialists()
        await self._upload_users()

    async def setup_browser(self) -> None:
        """Инициализация браузера и создание нового контекста."""

        self.playwright = await async_playwright().start()

        # Используем локальный путь к браузеру
        executable_path = os.path.join(self.browser_paths["chromium"], "chrome.exe")

        if not os.path.exists(executable_path):
            self.logger.warning(f"Локальный браузер не найден по пути {executable_path}")
            self.logger.info("Используем браузер из системной установки")
            self.browser = await self.playwright.chromium.launch(
                headless=False,
                args=["--ignore-certificate-errors"],
            )
        else:
            self.logger.info(f"Используем локальный браузер из {executable_path}")
            self.browser = await self.playwright.chromium.launch(
                headless=False,
                executable_path=executable_path,
                args=["--ignore-certificate-errors"],
            )

        # Настраиваем контекст с отключенным автоматическим открытием файлов
        self.context = await self.browser.new_context(
            no_viewport=True,
            accept_downloads=True,
            ignore_https_errors=True,
        )
        # Устанавливаем обработчики событий
        self.page = await self.context.new_page()

        # Перехватываем все WebSocket'ы
        def on_websocket_created(ws):
            self.websockets_list.append(ws)

        self.page.on("websocket", on_websocket_created)
        self.logger.info("Браузер успешно инициализирован")

    async def _log_in(self):
        for action in self.config["actions"]:
            action_type = action["type"]
            selector = action.get("selector", None)
            description = action.get("description", "")
            reset_wss = action.get("reset_wss", "")

            self.logger.info(f"Выполнение действия: {description}")

            if reset_wss:
                # Скидываем все веб-сокеты без авторизации
                self.websockets_list = []

            if action_type == "click" and selector:
                await self.page.click(selector)
                self.logger.info(f"\tВыполнено нажатие на элемент")
            elif action_type == "input" and selector:
                value = action["value"]

                # Подстановка значений из конфигурации
                if isinstance(value, str) and value.startswith("${"):
                    config_path = value[2:-1].split(".")
                    value = self.config

                    for key in config_path:
                        value = value[key]

                await self.page.click(selector)
                await asyncio.sleep(1)
                await self.page.type(selector, value)

                self.logger.info(f"\tВведен текст {value} в элемент")

    async def _connect_to_socket(self):
        """Подключение к веб-сокету."""

        websocket_url = self.config["site"]["web-socket"]
        self.web_socket = (ws for ws in self.websockets_list if ws.url == websocket_url).__next__()

        if not self.web_socket:
            self.logger.error("Не найден веб-сокет.")
            raise Exception

        self.logger.info("WebSocket успешно подключен")

    async def _upload_users(self):
        """
        Автоматически:
        1. Открывает меню "Отчёты"
        2. Выбирает пункт "Сформировать отчёт"
        3. Нажимает "Запустить" в модальном окне
        4. Скачивает файл
        """
        # 1. Нажимаем на кнопку с Id='Z26' (панель Q6.TBar0)
        await self.page.click("css=[path='Q6.TBar0']")

        # 2. Ждём и кликаем по пункту меню (по тексту — уточни точное название)
        await self.page.wait_for_selector("text=Сформировать отчёт", timeout=5000)
        await self.page.click("text=Сформировать отчёт")

        # 3. Ждём появления модального окна (любое с кнопкой Z2)
        #    Можно ждать по кнопке "Запустить" или по любому элементу окна
        await self.page.wait_for_selector("css=[path$='.TBarFind']", timeout=10000)  # Q13.T2.TBarFind

        # 4. Ожидаем скачивание
        async with self.page.expect_download() as download_info:
            # Нажимаем "Запустить"
            await self.page.click("css=[path$='.TBarFind']")

            # Ждём, пока начнётся скачивание
            download = await download_info.value

            # Сохраняем файл
            filepath = f"./reports/{download.suggested_filename}"
            await download.save_as(filepath)
            print(f"Файл отчёта успешно сохранён: {filepath}")

        return filepath
