import os
from playwright.async_api import async_playwright

class BrowserController:
    def __init__(self, config=None, logger=None):
        self.config = config or {}
        self.logger = logger
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    async def start(self):
        self.playwright = await async_playwright().start()
        browser_path = self.config.get("chromium_path")
        headless = self.config.get("headless", False)
        if browser_path and os.path.exists(browser_path):
            self.browser = await self.playwright.chromium.launch(
                headless=headless,
                executable_path=browser_path,
                args=["--ignore-certificate-errors"],
            )
            if self.logger:
                self.logger.info(f"Browser запущен локально: {browser_path}")
        else:
            self.browser = await self.playwright.chromium.launch(
                headless=headless,
                args=["--ignore-certificate-errors"],
            )
            if self.logger:
                self.logger.info("Browser запущен из system path")
        self.context = await self.browser.new_context(
            no_viewport=True,
            accept_downloads=True,
            ignore_https_errors=True,
        )
        self.page = await self.context.new_page()

    async def stop(self):
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
