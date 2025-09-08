import asyncio
import json
import logging
import os
import yaml

from jinja2 import Template
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

        # Браузер
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        # Сокет
        self.web_socket = None
        self.websockets_list: list = []
        self.ws_block_patterns: list[str] = []
        self.cancel_download_patterns: list[str] = []
        
        # Целевая папка загрузок
        self.redirect_dir: Path = Path(self.config["download"]["output_dir"]).absolute()

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
        
        # Автоматически включаем перенаправление загрузок в локальную папку
        try:
            self.redirect_dir.mkdir(parents=True, exist_ok=True)

            await self.enable_download_redirect(str(self.redirect_dir))
            await self.inject_web_socket(str(self.redirect_dir))
        except Exception as e:
            self.logger.warning(f"Не удалось включить перенаправление загрузок: {e}")

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

        def _on_frame_sent(frame: Any):
            payload = self._extract_payload(frame)

            # Ищем триггеры во фрейме и прерываем выполнение
            try:
                text = str(payload)

                for pattern in self.ws_block_patterns:
                    if pattern in text:
                        asyncio.create_task(self._interrupt_ws())
                        break
            except Exception as _e:
                self.logger.warning(f"Ошибка при попытке перехвата в веб-сокете: {_e.args[0]}")

        self.web_socket.on("framesent", _on_frame_sent)
        self.logger.info("WebSocket успешно подключен")

    async def _interrupt_ws(self) -> None:
        """Грубое прерывание действий страницы для остановки скачивания и открытия файла.

        Стратегия: попытаться уйти на about:blank (быстро рвёт WS),
        """

        try:
            await self.page.goto("about:blank")
        except Exception:
            self.logger.warning("Не удалось прервать скачивание файла браузером.")

    async def inject_web_socket(self, download_path: str) -> None:
        """Манки-патчинг веб-сокета.
        
        Удаляет SuccessAction в _Writefileend (чтобы не открывать файл);
        Переписывает путь FileFastSave на указанный каталог, сохраняя имя файла.
        """

        download_dir_json = json.dumps(download_path)

        with open("app/websocket_interceptor.js") as f:
            template = Template(f.read())

        script = template.render(DOWNLOAD_DIR=json.dumps(download_dir_json))

        try:
            await self.context.add_init_script(script)
            self.logger.info("JS WS interceptor enabled")
        except Exception as e:
            self.logger.warning(f"Не удалось инжектировать JS перехватчик WS: {e}")

    async def enable_download_redirect(self, download_path: str) -> None:
        """Включить безопасное подавление авто-открытия и перенаправление скачиваний.

        Без JS: используем Playwright/Chromium для переноса загрузок в нужную папку.
        """

        self.redirect_dir = download_path
        # Перенаправим встроенные скачивания Playwright в нужную директорию
        try:
            # В Chromium можно задать поведение загрузок через CDP
            client = await self.context.new_cdp_session(self.page)
            await client.send('Browser.setDownloadBehavior', {
                'behavior': 'allow',
                'downloadPath': download_path,
                'eventsEnabled': True
            })
        except Exception as e:
            # fallback: полагаемся на accept_downloads=True и хук на событие download
            self.logger.info(f"CDP download redirect не применён: {e}")

        try:
            self.page.off('download')
        except Exception:
            pass

        async def _on_download(download):
            try:
                suggested = download.suggested_filename
                # Отмена по шаблонам (если задано): просто не сохраняем файл
                if any(pat in suggested for pat in self.cancel_download_patterns):
                    self.logger.info(f"Download отменён по шаблону: {suggested}")
                    return

                target = os.path.join(download_path, suggested)
                await download.save_as(target)
                self.logger.info(f"Download сохранён в {target}")
            except Exception as e:
                self.logger.error(f"Ошибка сохранения загрузки: {e}")

        self.page.on('download', _on_download)

    async def _upload_users(self):
        """Запуск формирования отчета по юзерам."""

        for action in self.config["users_actions"]:
            await self.click(action)

        await asyncio.sleep(100)

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
