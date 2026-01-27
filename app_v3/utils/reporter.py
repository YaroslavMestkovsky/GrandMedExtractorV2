import requests

from datetime import date
from app_v3.utils.config import app_config


TELEGRAM_CONFIG = app_config.telegram


class TelegramService:
    """Сервис для отправки отчета в телеграм."""

    INFO = []
    EXCEPTIONS = []

    def add_info(self, info):
        self.INFO.append(info)

    def add_exception(self, ex):
        self.EXCEPTIONS.append(str(ex))

    def send_message(self):
        yesterday_str = date.today().strftime("%d.%m.%Y")
        info = '\n✅'.join(self.INFO)
        exceptions = '\n⚠️'.join(self.EXCEPTIONS)

        message = f"""
        "📊 *Отчёт о выгрузке данных за {yesterday_str}*"
        
        Информация по выгрузке:
        ✅{info}
        
        Ошибки:
        {exceptions}
        """

        url = f"https://api.telegram.org/bot{TELEGRAM_CONFIG['token']}/sendMessage"
        payload = {
            "chat_id": str(TELEGRAM_CONFIG['user_id']),
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }

        requests.post(url, json=payload, timeout=15)

reporter = TelegramService()
