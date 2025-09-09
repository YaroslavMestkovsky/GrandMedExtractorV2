import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import requests
from urllib.parse import quote


class SocketService:
    """Сервис работы с веб‑сокетом и прямой загрузкой файлов.

    Обязанности:
    - Инжектировать JS‑перехватчик и инициализировать window‑глобалы
    - Раннее извлекать параметры скачивания и cookies
    - Подключать обработчики к WebSocket для обнаружения событий
    - Выполнять прямое HTTP‑скачивание файла по параметрам
    """

    def __init__(self, context, page, config: Dict[str, Any], logger):
        self.context = context
        self.page = page
        self.config = config
        self.logger = logger

        self.download_params: Optional[Dict[str, Any]] = None
        self.cookies: Dict[str, str] = {}

        # Targets provided by caller
        self.redirect_dir: Optional[Path] = None
        self.filename: Optional[str] = None

    async def inject_interceptor(self) -> None:
        """Инжектировать перехватчик WebSocket и инициализировать глобалы."""
        from pathlib import Path as _Path
        with open("app/websocket_interceptor.js", "r", encoding="utf-8") as t:
            script = t.read()

        await self.context.add_init_script(script)

        blackhole_path = str(_Path(tempfile.gettempdir()) / "qms_discard.tmp")
        await self.page.add_init_script(
            f"window.__DOWNLOAD_DIR = {json.dumps(str(self.redirect_dir or ''))}; "
            f"window.__FILENAME = {json.dumps(str(self.filename or ''))}; "
            f"window.__BLACKHOLE_PATH = {json.dumps(blackhole_path)};"
        )

        self.logger.info("[SocketService] Перехватчик WS инжектирован")

    async def update_download_targets(self, redirect_dir: Path, filename: str) -> None:
        """Обновить window‑глобалы (директория и имя файла)."""

        self.redirect_dir = redirect_dir
        self.filename = filename

        try:
            await self.page.evaluate(
                "({dir, name}) => { window.__DOWNLOAD_DIR = dir; window.__FILENAME = name; }",
                {"dir": str(self.redirect_dir), "name": self.filename},
            )
            self.logger.debug(f"[SocketService] Обновлены цели скачивания: dir='{self.redirect_dir}', name='{self.filename}'")
        except Exception:
            # Non-fatal
            pass

    async def extract_params_soon(self) -> None:
        """Раннее извлечение параметров после появления FileFastSave в WS."""
        try:
            await asyncio.sleep(0.1)
            params = await self.page.evaluate("() => window.__DOWNLOAD_PARAMS")
            if params and not self.download_params:
                self.download_params = params
                # Also get cookies while context is alive
                await self._get_cookies()

                self.logger.debug(f"[SocketService] Ранние параметры скачивания извлечены")
        except Exception:
            pass

    async def ensure_params(self) -> None:
        """Гарантировать наличие параметров скачивания (повторная проверка)."""
        if self.download_params:
            return
        try:
            params = await self.page.evaluate("() => window.__DOWNLOAD_PARAMS")
            if params:
                self.download_params = params

                self.logger.info(f"[SocketService] Параметры скачивания получены при ensure_params")
        except Exception:
            pass

    async def _get_cookies(self) -> None:
        """Считать cookies активного контекста браузера."""
        try:
            cookies = await self.context.cookies()
            cookie_dict: Dict[str, str] = {}
            for cookie in cookies:
                cookie_dict[cookie["name"]] = cookie["value"]
            self.cookies = cookie_dict

            self.logger.debug(f"[SocketService] Cookies считаны: {list(self.cookies.keys())}")
        except Exception:
            self.cookies = {}

    async def download_via_http(self) -> bool:
        """Выполнить прямое HTTP‑скачивание, используя параметры и cookies."""
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

        base_url = self.config["site"]["url"].rstrip("/")
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
                self.logger.debug(f"[SocketService] Файл скачан: {file_path}")
                return True
            self.logger.error(f"[SocketService] Ошибка скачивания: {response.status_code} - {response.text}")
            return False
        except Exception:
            self.logger.error("[SocketService] Исключение при HTTP-скачивании")
            return False

    async def connect_to_socket(
        self,
        websocket_url: str,
        websockets_list: list,
        ws_block_patterns: list[str],
        on_writefileend: Callable[[str], None],
    ) -> None:
        """Подключить обработчики к нужному WebSocket и отслеживать события.

        - Раннее извлекать параметры при появлении FileFastSave
        - На _Writefileend вызывать on_writefileend(payload)
        """

        web_socket = (ws for ws in websockets_list if ws.url == websocket_url).__next__()
        self.logger.info(f"[SocketService] Подключение к WS: {websocket_url}")

        def _download_completed(payload_text: str) -> None:
            try:
                data = json.loads(payload_text)
                if isinstance(data, dict) and data.get("Action") == "useraction" and data.get("path") == "_Writefileend":
                    on_writefileend(payload_text)
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

        web_socket.on("framesent", _on_frame_sent)
        web_socket.on("framereceived", _on_frame_received)
        self.logger.info("[SocketService] Обработчики WS установлены")

    async def _interrupt_ws(self) -> None:
        """Пробовать оборвать активность страницы (сбросить WS)."""
        try:
            await self.page.goto("about:blank")
        except Exception:
            pass

    @staticmethod
    def _extract_payload(frame: Any):
        """Извлечь полезную нагрузку из сообщения WS."""
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


