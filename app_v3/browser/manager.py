import datetime
import os
import asyncio

from pathlib import Path
from typing import Dict, Optional, Any
from uuid import uuid4

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
)

from app_v3.services.socket import SocketService
from app_v3.utils.logger import app_logger
from app_v3.utils.config import app_config


MAIN_CONFIG = app_config.main


class BrowserManager:
    """Менеджер для управления браузером."""

    def __init__(self):
        # Браузер
        self.browser = None
        self.playwright = None
        self.context = None
        self.page = None
        self.service = None
        self.base_timeout = 30000

        # Сокет
        self.websockets_list: list = []
        self.ws_block_patterns: list[str] = []
        self.active_download = None

        # Загрузки
        self.yesterday_analytics = 'yesterday_analytics'
        self.period_analytics = 'period_analytics'
        self.specialists = 'specialists'
        self.users = 'users'

        # Флаги
        self.current_file_uploaded = False
        self.yesterday_analytics_uploaded = False
        self.period_analytics_uploaded = False
        self.specialists_uploaded = False
        self.users_uploaded = False

        # Файлы
        self.filename = 'dummy'
        self.cookies: Dict[str, str] = {}
        self.download_params: Optional[Dict[str, Any]] = None
        self.redirect_dir: Path = Path(MAIN_CONFIG["download"]["output_dir"]).absolute()

        # Пути к локальным браузерам
        self.browser_paths = {
            "chromium": str(Path("browsers/chromium/chrome-win").absolute()),
            # не используются
            # "firefox": str(Path("browsers/firefox").absolute()),
            # "webkit": str(Path("browsers/webkit").absolute()),
        }

    async def setup_browser(self) -> None:
        """Инициализация браузера и создание нового контекста."""

        def on_websocket_created(ws):
            """Перехват WebSocket'ов"""
            self.websockets_list.append(ws)

        self.playwright = await async_playwright().start()

        # Используем локальный путь к браузеру
        executable_path = os.path.join(self.browser_paths["chromium"], "chrome.exe")

        if not os.path.exists(executable_path):
            app_logger.warning(f"[BrM] Локальный браузер не найден по пути {executable_path}")
            app_logger.info("[BrM] Используем браузер из системной установки")
            self.browser = await self.playwright.chromium.launch(
                headless=False,
                args=["--ignore-certificate-errors"],
            )
        else:
            app_logger.info(f"[BrM] Используем локальный браузер из {executable_path}")
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
        self.service = SocketService(self.context, self.page)

        self.page.on("websocket", on_websocket_created)

        # Автоматически включаем перенаправление загрузок в локальную папку и инжектим перехватчик
        try:
            self.redirect_dir.mkdir(parents=True, exist_ok=True)
            app_logger.debug(f"[BrM] Создана директория для загрузок: {self.redirect_dir}")
            await self._inject_web_socket()
        except Exception as e:
            error_msg = f"Не удалось настроить перенаправление загрузок: {str(e)}"
            app_logger.warning(f"[BrM] {error_msg}")

        app_logger.info("[BrM] Браузер успешно инициализирован")

    async def _inject_web_socket(self) -> None:
        """Инжект перехватчика WS; читает конфиг из window-глобалов."""

        try:
            await self.service.update_download_targets(self.redirect_dir, self.filename)
            await self.service.inject_interceptor()
            app_logger.info("[BrM] JS перехватчик инжектирован в веб-сокет.")
        except Exception as e:
            app_logger.warning(f"[BrM] Не удалось инжектировать JS перехватчик WS: {e}")

    async def connect_to_socket(self):
        """Подключение обработчиков к целевому WebSocket (через сервис)."""

        def on_write_file_end(_payload: str) -> None:
            if self.active_download == self.yesterday_analytics:
                self.analytics_uploaded = self.current_file_uploaded = True
            elif self.active_download == self.period_analytics:
                self.analytics_uploaded = self.current_file_uploaded = True
            elif self.active_download == self.specialists:
                self.specialists_uploaded = self.current_file_uploaded = True
            elif self.active_download == self.users:
                self.users_uploaded = self.current_file_uploaded = True

            asyncio.create_task(self._process_download_via_http())

        websocket_url = MAIN_CONFIG["site"]["web-socket"]
        app_logger.info(f"[BrM] Подключение к WebSocket: {websocket_url}")

        if not self.websockets_list:
            error_msg = "Список WebSocket соединений пуст. Возможно, страница ещё не загрузилась полностью."
            app_logger.error(f"[BrM] {error_msg}")

            raise RuntimeError(error_msg)

        app_logger.debug(f"[BrM] Найдено {len(self.websockets_list)} WebSocket соединений")

        try:
            await self.service.connect_to_socket(
                websocket_url=websocket_url,
                websockets_list=self.websockets_list,
                ws_block_patterns=self.ws_block_patterns,
                on_write_file_end=on_write_file_end,
            )
            app_logger.info("[BrM] WebSocket успешно подключен и настроен")
        except Exception as e:
            error_msg = f"Ошибка при подключении к WebSocket: {str(e)}"
            app_logger.error(f"[BrM] {error_msg}")
            raise

    async def await_for_download(self):
        """Ожидание загрузки файлов."""

        seconds = 0
        max_wait_time = 3600 * 4  # Максимальное время ожидания: 4 часа

        while not self.current_file_uploaded and seconds < max_wait_time:
            seconds += 1
            print(f"\r[BrM] Ожидание загрузки аналитик: {seconds} сек...", end="", flush=True)
            await asyncio.sleep(1)

        print()

        if not self.current_file_uploaded:
            error_msg = f"Таймаут загрузки ({max_wait_time} сек)"
            app_logger.error(f"[BrM] {error_msg}")
        else:
            app_logger.info("[BrM] Файл успешно загружен")

    async def _process_download_via_http(self) -> None:
        """Обработка скачивания через HTTP после получения параметров из WebSocket."""

        try:
            app_logger.debug(f"[BrM] Обработка HTTP-скачивания для файла: {self.filename}")

            # Небольшая задержка, чтобы параметры успели извлечься
            await asyncio.sleep(0.5)

            # Принудительно получить свежие параметры для текущего отчёта
            self.download_params = None
            self.service.download_params = None
            await self._get_download_params()

            if not self.download_params:
                app_logger.warning(f"[BrM] Параметры скачивания не получены для файла: {self.filename}")

            # Если cookies еще не получены, пытаемся их получить
            if not self.cookies:
                await self._get_cookies()

            # Скачиваем файл через HTTP
            success = await self._download_file_via_http()

            if success:
                app_logger.info(f"[BrM] Файл '{self.filename}' успешно скачан через HTTP")
            else:
                error_msg = f"Не удалось скачать файл '{self.filename}' через HTTP"
                app_logger.error(f"[BrM] {error_msg}")

        except Exception as e:
            error_msg = f"Ошибка при обработке HTTP-скачивания файла '{self.filename}': {str(e)}"
            app_logger.error(f"[BrM] {error_msg}", exc_info=True)

    async def _download_file_via_http(self) -> bool:
        """Скачать файл через HTTP-запрос используя параметры из WebSocket."""

        if not self.service:
            app_logger.error("[BrM] Сервис скачивания не инициализирован")
            return False

        app_logger.debug(f"[BrM] Начало HTTP-скачивания файла: {self.filename}")

        self.service.redirect_dir = self.redirect_dir
        self.service.filename = self.filename
        self.service.download_params = self.download_params
        self.service.cookies = self.cookies

        ok = await self.service.download_via_http()

        if ok:
            app_logger.debug(f"[BrM] HTTP-скачивание завершено успешно: {self.filename}")
        else:
            app_logger.warning(f"[BrM] HTTP-скачивание не удалось: {self.filename}")

        return ok

    async def _get_download_params(self) -> None:
        """Получить параметры скачивания (через сервис)."""

        try:
            await self.service.ensure_params()

            if self.service.download_params:
                self.download_params = self.service.download_params
        except Exception as e:
            app_logger.warning(f"[BrM] Не удалось получить параметры скачивания: {e}")

    async def _get_cookies(self) -> None:
        """Получить cookies (через сервис)."""

        try:
            await self.service.get_cookies()

            if self.service.cookies:
                self.cookies = self.service.cookies
        except Exception as e:
            app_logger.warning(f"[BrM] Не удалось получить cookies: {e}")

    async def click(self, action):
        """Клик на определенный элемент согласно переданным параметрам."""

        action_desc = action.get('elem', action.get('description', 'Неизвестное действие'))
        app_logger.debug(f"[BrM] Выполнение действия: {action_desc}")

        if action.get("reset_wss"):
            # Скидываем все веб-сокеты без авторизации
            self.websockets_list = []
            app_logger.debug("[BrM] Список WebSocket соединений сброшен")

        try:
            if text_to_search := action.get("text_to_search"):
                inner_text = await self.page.locator(action["id"]).inner_text()

                if text_to_search not in inner_text:
                    error_msg = f"Элемент '{text_to_search}' не найден на странице"
                    app_logger.error(f"[BrM] {error_msg}")
                    raise Exception(error_msg)

                locator = self.page.locator(f"{action['root_node']} >> text={text_to_search}").first
                await locator.click(timeout=action.get("timeout", self.base_timeout))
            elif key := action.get("key"):
                await self.page.keyboard.press(key)
            else:
                await self.page.click(action["id"], timeout=action.get("timeout", self.base_timeout))

            if sleep := action.get("sleep"):
                await asyncio.sleep(sleep)

            app_logger.debug(f"[BrM] Действие '{action_desc}' выполнено успешно")
        except Exception as e:
            error_msg = f"Ошибка при выполнении действия '{action_desc}': {str(e)}"
            app_logger.error(f"[BrM] {error_msg}")
            raise

    async def input(self, action):
        """Ввод текста в определенный элемент."""

        # Сначала кликаем в элемент.
        await self.click(action)
        value = action["value"]

        # Подстановка значений из конфигурации
        if isinstance(value, str) and value.startswith("${"):
            config_path = value[2:-1].split(".")
            value = MAIN_CONFIG

            for key in config_path:
                value = value[key]

        await self.page.keyboard.type(value)
        app_logger.debug(f"[BrM] Текст введён: {value}")

    async def fill_dates(self, action, _start, _end):
        """Ввод дат в элемент 'С - По'. Переход между двумя полями через нажатие кнопки Tab.

        Почему не используется метод input? Потому что при выделении поля даты что-то странное происходит с дивами,
        и их невозможно заранее обозначить.
        """

        action_desc = action.get('elem', 'Ввод дат')
        app_logger.debug(f"[BrM] {action_desc}: {_start.strftime('%d.%m.%Y')} - {_end.strftime('%d.%m.%Y')}")

        try:
            container = self.page.locator(action["id"])
            row = container.locator(action["row_text"])

            await row.click()
            await asyncio.sleep(1)
            await self.page.keyboard.type(_start.strftime('%d.%m.%Y'))
            await asyncio.sleep(1)
            await self.page.keyboard.press("Tab")
            await asyncio.sleep(1)
            await self.page.keyboard.type(_end.strftime('%d.%m.%Y'))
            await asyncio.sleep(1)
            await self.page.keyboard.press("Tab")
            app_logger.debug(f"[BrM] Даты введены успешно")
        except Exception as e:
            error_msg = f"Ошибка при вводе дат: {str(e)}"
            app_logger.error(f"[BrM] {error_msg}")
            raise

    async def setup_upload(self, active_download) -> str:
        """Подготовка к загрузке файлов."""

        now = datetime.datetime.now().strftime('d%d_m%m_y%Y')

        self.active_download = active_download
        self.filename = f'{active_download}__{now}__{uuid4().hex[:4]}.csv'

        # Сброс параметров загрузки перед новым формированием отчёта
        self.current_file_uploaded = False
        self.download_params = None
        self.service.download_params = None

        try:
            await self.page.evaluate('() => { window.__DOWNLOAD_PARAMS = undefined; }')
            app_logger.debug('[BrM] Сброшены параметры скачивания в окне')
        except Exception:
            pass

        # Обновить параметры для перехватчика
        await self._update_download_params()

        return self.filename

    async def _update_download_params(self) -> None:
        """Обновление параметров скачивания (директория и имя) в окне."""

        try:
            await self.service.update_download_targets(self.redirect_dir, self.filename)
        except Exception as e:
            app_logger.warning(f"[BrM] Не удалось обновить параметры скачивания: {e}")

    async def shutdown(self) -> None:
        """Корректное завершение Playwright и браузера."""

        app_logger.info("[BrM] Начало завершения работы")

        try:
            if self.page is not None:
                await self.page.close()
                app_logger.debug("[BrM] Страница закрыта")
        except Exception as e:
            app_logger.warning(f"[BrM] Ошибка при закрытии страницы: {str(e)}")

        try:
            if self.context is not None:
                await self.context.close()
                app_logger.debug("[BrM] Контекст браузера закрыт")
        except Exception as e:
            app_logger.warning(f"[BrM] Ошибка при закрытии контекста: {str(e)}")

        try:
            if self.browser is not None:
                await self.browser.close()
                app_logger.debug("[BrM] Браузер закрыт")
        except Exception as e:
            app_logger.warning(f"[BrM] Ошибка при закрытии браузера: {str(e)}")

        try:
            if self.playwright is not None:
                await self.playwright.stop()
                app_logger.debug("[BrM] Playwright остановлен")
        except Exception as e:
            app_logger.warning(f"[BrM] Ошибка при остановке Playwright: {str(e)}")

        app_logger.info("[BrM] Завершение работы выполнено")

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
