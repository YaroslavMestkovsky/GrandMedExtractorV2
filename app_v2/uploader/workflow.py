from app_v2.uploader.browser import BrowserController
from app_v2.uploader.actions import Actions
from app_v2.uploader.download import DownloadManager

class UploadWorkflow:
    def __init__(self, browser: BrowserController, config: dict, logger, reporter, jobs=None):
        self.browser = browser
        self.config = config
        self.logger = logger
        self.reporter = reporter
        self.page = None
        self.actions = None
        self.download_manager = None
        self.jobs = jobs or {}
        self.analytics_job = self.jobs.get('analytics')
        self.specialists_job = self.jobs.get('specialists')
        self.users_job = self.jobs.get('users')

    async def run_step_login_and_prepare_download(self):
        await self.browser.start()
        self.page = self.browser.page
        await self.page.goto(self.config["site"]["url"])
        self.logger.info(f"Открыта страница логина: {self.config['site']['url']}")
        self.actions = Actions(self.browser)
        await self.actions.login(self.config["log_in_actions"], self.config)

        self.download_manager = DownloadManager(self.page, logger=self.logger)
        await self.download_manager.inject_interceptor()
        self.download_manager.subscribe_websocket()

    async def run_step_download(self, actions_key: str, download_label: str, timeout=300):
        for action in self.config[actions_key]:
            await self.actions.do_action(action, self.config)
        await self.download_manager.wait_for_download(timeout=timeout)
        self.logger.info(f"Загрузка {download_label} завершена")

    async def run_full_pipeline(self):
        await self.run_step_login_and_prepare_download()
        result_stats = {}
        jobs_seq = [
            ("analytics_actions", "Аналитики", self.analytics_job),
            ("specialists_actions", "Специалисты", self.specialists_job),
            ("users_actions", "Пациенты", self.users_job),
        ]
        for key, label, job in jobs_seq:
            await self.run_step_download(actions_key=key, download_label=label)
            job.process(None)  # stub
            result_stats[label] = "ok"
        self.reporter.add_stat('summary', result_stats)
        self.reporter.report()
