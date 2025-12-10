import asyncio
from app_v2.uploader.browser import BrowserController
from app_v2.uploader.actions import Actions
from app_v2.uploader.reporter import Reporter
from app_v2.uploader.workflow import UploadWorkflow
from app_v2.uploader.analytics_job import AnalyticsJob
from app_v2.uploader.specialists_job import SpecialistsJob
from app_v2.uploader.users_job import UsersJob

class Uploader:
    def __init__(self, config, logger=None):
        self.config = config
        self.logger = logger
        self.browser_controller = BrowserController(config={
            "chromium_path": config["browser"].get("chromium_path"),
            "headless": config["browser"].get("headless", False),
        }, logger=logger)
        self.reporter = Reporter()
        self.jobs = {
            'analytics': AnalyticsJob(None, self.reporter), # репо пока None
            'specialists': SpecialistsJob(None, self.reporter),
            'users': UsersJob(None, self.reporter),
        }
        self.workflow = UploadWorkflow(
            browser=self.browser_controller,
            config=config,
            logger=logger,
            reporter=self.reporter,
            jobs=self.jobs,
        )

    def run(self):
        asyncio.run(self.workflow.run_full_pipeline())
