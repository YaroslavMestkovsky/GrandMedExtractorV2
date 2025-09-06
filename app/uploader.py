import asyncio
import logging
import os
import time
import yaml
import websockets
import json

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

        self.playwright = None
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
        
        # Сначала тестируем подключение к публичному серверу
        await self._test_websocket_connection()
        
        # Затем подключаемся к целевому серверу
        await self._connect_to_socket()

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
        self.logger.info("Браузер успешно инициализирован")

    async def _log_in(self):
        for action in self.config["actions"]:
            action_type = action["type"]
            selector = action.get("selector", None)
            description = action.get("description", "")

            self.logger.info(f"Выполнение действия: {description}")

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
        self.logger.info(f"Подключение к WebSocket: {websocket_url}")
        
        try:
            # Подключаемся к WebSocket серверу с дополнительными параметрами
            async with websockets.connect(
                websocket_url,
                ping_interval=20,  # Отправляем ping каждые 20 секунд
                ping_timeout=10,   # Ждем pong 10 секунд
                close_timeout=10,   # Ждем закрытия 10 секунд
            ) as websocket:
                self.logger.info("WebSocket успешно подключен")
                
                # Отправляем тестовое сообщение
                ping_message = json.dumps({"type": "ping", "timestamp": time.time()})
                await websocket.send(ping_message)
                self.logger.info(f"Отправлено ping сообщение: {ping_message}")
                
                # Ждем ответ в течение 5 секунд
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    self.logger.info(f"Получен ответ от сервера: {response}")
                except asyncio.TimeoutError:
                    self.logger.warning("Таймаут ожидания ответа от сервера (5 секунд)")
                
                # Слушаем сообщения от сервера с таймаутом
                self.logger.info("Начинаем прослушивание сообщений от сервера...")
                message_count = 0
                
                while True:
                    try:
                        # Ждем сообщение с таймаутом 30 секунд
                        message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                        message_count += 1
                        self.logger.info(f"Сообщение #{message_count} от WebSocket: {message}")
                        
                        # Если получили 5 сообщений, выходим (для демонстрации)
                        if message_count >= 5:
                            self.logger.info("Получено достаточно сообщений, завершаем прослушивание")
                            break
                            
                    except asyncio.TimeoutError:
                        self.logger.info("Таймаут ожидания сообщений (30 секунд), завершаем прослушивание")
                        break

        except websockets.exceptions.ConnectionClosed as e:
            self.logger.warning(f"WebSocket соединение закрыто: код={e.code}, причина='{e.reason}'")
        except websockets.exceptions.InvalidURI as e:
            self.logger.error(f"Неверный URI WebSocket: {e}")
        except websockets.exceptions.WebSocketException as e:
            self.logger.error(f"Ошибка WebSocket: {e}")
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при подключении к WebSocket: {e}")
    
    async def _test_websocket_connection(self):
        """Тестирование подключения к публичному WebSocket серверу."""
        
        test_url = "wss://echo.websocket.org"
        self.logger.info(f"Тестирование подключения к публичному WebSocket: {test_url}")
        
        try:
            async with websockets.connect(test_url) as websocket:
                self.logger.info("Тестовое WebSocket подключение успешно")
                
                # Отправляем тестовое сообщение
                test_message = "Hello WebSocket!"
                await websocket.send(test_message)
                self.logger.info(f"Отправлено тестовое сообщение: {test_message}")
                
                # Ждем ответ
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                self.logger.info(f"Получен ответ от тестового сервера: {response}")
                
        except Exception as e:
            self.logger.error(f"Ошибка тестового подключения: {e}")
