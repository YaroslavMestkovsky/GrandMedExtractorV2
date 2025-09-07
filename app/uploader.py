import asyncio
import logging
import os
import time
from datetime import datetime

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
    Download,
)


class SocketUploader:
    """Загрузка отчетов через веб-сокет."""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self._setup_logging()

        self.web_socket = None
        self.playwright = None
        self.websockets_list: list = []
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        # Отслеживания скачивания
        self.download_path: Optional[Path] = None
        self.downloaded_file: Optional[Path] = None
        self.download_complete = asyncio.Event()

        # Пути к локальным браузерам
        self.browser_paths = {
            "chromium": str(Path("../browsers/chromium/chrome-win").absolute()),
            "firefox": str(Path("../browsers/firefox").absolute()),
            "webkit": str(Path("../browsers/webkit").absolute()),
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

        # Папка для скачивания
        download_dir = Path(self.config["download"]["output_dir"])
        download_dir.mkdir(exist_ok=True)
        self.download_path = download_dir.absolute()

        # Контекст с автоматическим скачиванием файлов
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

        # Обработчик скачивания файлов
        def on_download(download: Download):
            self.logger.info(f"Начато скачивание файла: {download.suggested_filename}")
            # В Playwright файл скачивается во временную папку, 
            # мы получим его путь после завершения скачивания
            self.downloaded_file = download.path()
            self.download_complete.set()

        self.page.on("websocket", on_websocket_created)
        self.page.on("download", on_download)
        self.logger.info("Браузер успешно инициализирован")

    async def _log_in(self):
        for action in self.config["log_in_actions"]:
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

    async def _process_downloaded_file(self) -> Optional[Path]:
        """
        Обработка скачанного файла: переименование и перемещение.
        
        Returns:
            Optional[Path]: Путь к обработанному файлу или None, если файл не найден
        """
        if not self.downloaded_file or not Path(self.downloaded_file).exists():
            self.logger.warning("Скачанный файл не найден")
            return None
            
        # Генерируем новое имя файла на основе шаблона
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_template = self.config["download"]["filename_template"]
        new_filename = filename_template.format(timestamp=timestamp)
        
        # Создаем новый путь для файла
        new_file_path = self.download_path / new_filename
        
        try:
            # Копируем файл из временной папки в нашу папку с новым именем
            import shutil
            shutil.copy2(self.downloaded_file, new_file_path)
            self.logger.info(f"Файл успешно скопирован: {new_file_path}")
            return new_file_path
        except Exception as e:
            self.logger.error(f"Ошибка при копировании файла: {e}")
            return None

    async def _upload_users(self):
        """Запуск формирования отчета по юзерам."""

        # Сбрасываем флаг скачивания
        self.download_complete.clear()
        self.downloaded_file = None

        for action in self.config["users_actions"]:
            await self.click(action)

        # Ждем завершения скачивания файла
        self.logger.info("Ожидание завершения скачивания файла...")
        try:
            # Ждем максимум 60 секунд
            await asyncio.wait_for(self.download_complete.wait(), timeout=60.0)
            
            # Обрабатываем скачанный файл
            processed_file = await self._process_downloaded_file()
            if processed_file:
                self.logger.info(f"Отчет успешно сохранен: {processed_file}")
            else:
                self.logger.error("Не удалось обработать скачанный файл")
                
        except asyncio.TimeoutError:
            self.logger.error("Таймаут ожидания скачивания файла")
        except Exception as e:
            self.logger.error(f"Ошибка при ожидании скачивания: {e}")

        # Дополнительное ожидание для завершения всех операций
        await asyncio.sleep(5)

    async def click(self, action):
        self.logger.info(f"Клик: {action['elem']}")

        if text_to_search := action.get("text_to_search"):
            inner_text = await self.page.locator(action["id"]).inner_text()

            if not text_to_search in inner_text:
                self.logger.error(f"Не найден элемент {text_to_search}")
                raise Exception

            locator = self.page.locator(f"{action['root_node']} >> text={text_to_search}")
            await locator.click()
        else:
            await self.page.click(action["id"])

        self.logger.info('\t- готово.')
