import asyncio
import os

class DownloadManager:
    def __init__(self, page, logger=None):
        self.page = page
        self.logger = logger
        self.active_download = None
        self._download_complete = asyncio.Event()
        self.on_writefileend = None  # callback(user-settable)
        self._ws_handlers = []

    async def inject_interceptor(self, js_path=None):
        if js_path is None:
            js_path = os.path.join(os.path.dirname(__file__), 'websocket_interceptor.js')
        with open(js_path, 'r', encoding='utf-8') as f:
            js_code = f.read()
        await self.page.evaluate(js_code)
        if self.logger:
            self.logger.info('Интерцептор для WebSocket инъецирован.')

    def subscribe_websocket(self):
        async def websocket_handler(ws):
            if self.logger:
                self.logger.debug(f'WS открыт: {ws.url}')
            ws.on('framereceived', self.ws_frame_received)
        self.page.on('websocket', websocket_handler)

    def ws_frame_received(self, frame):
        payload = self._extract_payload(frame)
        if self.logger:
            self.logger.debug(f'WS payload: {payload}')
        if '_Writefileend' in str(payload) or 'writefileend' in str(payload):
            self._download_complete.set()
            if self.on_writefileend:
                self.on_writefileend(payload)

    async def wait_for_download(self, timeout=300):
        try:
            await asyncio.wait_for(self._download_complete.wait(), timeout=timeout)
        finally:
            self._download_complete.clear()

    @staticmethod
    def _extract_payload(frame):
        for attr in ('text', 'payload', 'data'):
            try:
                value = getattr(frame, attr, None)
                if callable(value):
                    value = value()
                if value is not None:
                    return value
            except Exception:
                pass
        return frame
