import asyncio
import urllib3

from app_v3.utils.logger import app_logger
from uploader import Uploader


# Отключение предупреждения о небезопасных HTTPS-запросах
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


async def main():
    uploader = Uploader()

    try:
        await uploader.run()
    except Exception as e:
        app_logger.error(f"Произошла ошибка: {str(e.args[0])}")


if __name__ == "__main__":
    asyncio.run(main())
