import asyncio

from database.db_manager import check_db
from uploader import Uploader


async def main():
    uploader = Uploader()

    try:
        await uploader.run()
    except Exception as e:
        uploader.logger.error(f"Произошла ошибка: {str(e.args[0])}")


if __name__ == "__main__":
    check_db()
    asyncio.run(main())
