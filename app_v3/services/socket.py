import asyncio
import json
import tempfile
import requests

from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import quote

from app_v3.utils.config import app_config
from app_v3.utils.logger import app_logger


MAIN_CONFIG = app_config.main


class SocketService:
    """Сервис для работы с сокетом загрузки файлов."""

    def __init__(self, context, page):
        self.context = context
        self.page = page

        self.download_params: Optional[Dict[str, Any]] = None
        self.cookies: Dict[str, str] = {}

        self.redirect_dir: Optional[Path] = None
        self.filename: Optional[str] = None

    async def inject_interceptor(self) -> None:
        """Инжектирование перехватчика WebSocket и инициализирование глобалов."""

        with open("app_v3/services/websocket_interceptor.js", "r", encoding="utf-8") as t:
            script = t.read()

        await self.context.add_init_script(script)

        blackhole_path = str(Path(tempfile.gettempdir()) / "qms_discard.tmp")
        await self.page.add_init_script(
            f"window.__DOWNLOAD_DIR = {json.dumps(str(self.redirect_dir or ''))}; "
            f"window.__FILENAME = {json.dumps(str(self.filename or ''))}; "
            f"window.__BLACKHOLE_PATH = {json.dumps(blackhole_path)};"
        )

        app_logger.info("[SSv] Перехватчик WS инжектирован")

    async def update_download_targets(self, redirect_dir: Path, filename: str) -> None:
        """Обновляет window‑глобалы (директория и имя файла)."""

        self.redirect_dir = redirect_dir
        self.filename = filename

        try:
            await self.page.evaluate(
                "({dir, name}) => { window.__DOWNLOAD_DIR = dir; window.__FILENAME = name; }",
                {"dir": str(self.redirect_dir), "name": self.filename},
            )
            app_logger.debug(
                f"[SSv] Обновлены цели скачивания: dir='{self.redirect_dir}', name='{self.filename}'")
        except Exception:
            # Не критично
            pass

    async def extract_params_soon(self) -> None:
        """Раннее извлечение параметров после появления FileFastSave в WS."""

        try:
            await asyncio.sleep(0.1)
            params = await self.page.evaluate("() => window.__DOWNLOAD_PARAMS")

            if params and not self.download_params:
                self.download_params = params
                await self.get_cookies()

                app_logger.debug(f"[SSv] Ранние параметры скачивания извлечены")
        except Exception:
            # Не критично
            pass

    async def ensure_params(self) -> None:
        """Обеспечение наличия параметров скачивания (повторная проверка)."""

        if self.download_params:
            return
        try:
            params = await self.page.evaluate("() => window.__DOWNLOAD_PARAMS")

            if params:
                self.download_params = params

                app_logger.info(f"[SSv] Параметры скачивания получены")
        except Exception:
            # Не критично
            pass

    async def get_cookies(self) -> None:
        """Считывание cookies активного контекста браузера."""

        try:
            cookies = await self.context.cookies()
            cookie_dict: Dict[str, str] = {}

            for cookie in cookies:
                cookie_dict[cookie["name"]] = cookie["value"]

            self.cookies = cookie_dict

            app_logger.debug(f"[SSv] Cookies считаны: {list(self.cookies.keys())}")
        except Exception:
            self.cookies = {}

    async def download_via_http(self) -> bool:
        """Выполнение прямого HTTP‑скачивание, используя параметры и cookies."""

        if not self.download_params or not self.cookies or not self.redirect_dir or not self.filename:
            return False

        function_call = (
            f'^mtempPrt({self.download_params["report_id"]},'
            f'"{self.download_params["report_type"]}",'
            f'{self.download_params["mode"]},'
            f'"{self.download_params["body"]}",'
            f'"{self.download_params["fmt"]}",'
            f'"{self.download_params["layout"]}")'
        )
        encoded_function = quote(function_call, safe="")

        base_url = MAIN_CONFIG["site"]["url"].rstrip("/")
        url = f"{base_url}/download/{encoded_function}"

        params = {"enc": "0", "addCRLF": "No"}
        headers = {
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": base_url,
        }

        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                cookies=self.cookies,
                verify=False,
            )

            if response.status_code == 200:
                file_path = Path(self.redirect_dir) / str(self.filename)

                with open(file_path, "wb") as f:
                    f.write(response.content)

                app_logger.debug(f"[SSv] Файл скачан: {file_path}")
                return True

            app_logger.error(f"[SSv] Ошибка скачивания: {response.status_code} - {response.text}")
            return False
        except Exception:
            app_logger.error("[SSv] Исключение при HTTP-скачивании")
            return False

    async def connect_to_socket(
            self,
            websocket_url: str,
            websockets_list: list,
            ws_block_patterns: list[str],
            on_write_file_end: Callable[[str], None],
    ) -> None:
        """Подключение обработчика к нужному WebSocket и отслеживание события."""

        def _download_completed(payload_text: str) -> None:
            try:
                data = json.loads(payload_text)

                if isinstance(data, dict) and data.get("Action") == "useraction" and data.get(
                        "path") == "_Writefileend":
                    on_write_file_end(payload_text)
            except Exception:
                pass

        def _on_frame_sent(frame: Any):
            payload = self._extract_payload(frame)

            try:
                text = str(payload)

                for pattern in ws_block_patterns:
                    if pattern in text:
                        asyncio.create_task(self._interrupt_ws())
                        break
                _download_completed(text)
            except Exception:
                pass

        def _on_frame_received(frame: Any):
            try:
                text = str(self._extract_payload(frame))

                if "FileFastSave" in text and "mtempPrt" in text:
                    asyncio.create_task(self.extract_params_soon())

                _download_completed(text)
            except Exception:
                pass

        # Ищем WebSocket с нужным URL
        web_socket = None

        for ws in websockets_list:
            if ws.url == websocket_url:
                web_socket = ws
                break

        if web_socket is None:
            app_logger.error(
                f"[SSv] WebSocket с URL {websocket_url} не найден в списке: {[ws.url for ws in websockets_list]}")

            raise RuntimeError(f"WebSocket с URL {websocket_url} не найден")

        app_logger.info(f"[SSv] Подключение к WS: {websocket_url}")

        web_socket.on("framesent", _on_frame_sent)
        web_socket.on("framereceived", _on_frame_received)

        app_logger.info("[SSv] Обработчики WS установлены")

    async def _interrupt_ws(self) -> None:
        """Попытка оборвать активность страницы (сброс WS)."""

        try:
            await self.page.goto("about:blank")
        except Exception:
            pass

    @staticmethod
    def _extract_payload(frame: Any):
        """Извлечение полезной нагрузки из сообщения WS."""

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
