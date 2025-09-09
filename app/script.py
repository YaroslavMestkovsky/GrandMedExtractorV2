import asyncio

from uploader import Uploader


async def main():
    uploader = Uploader()

    try:
        await uploader.run()
    except Exception as e:
        uploader.logger.error(f"Произошла ошибка: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
