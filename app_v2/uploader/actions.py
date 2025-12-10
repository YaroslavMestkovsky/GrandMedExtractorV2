class Actions:
    def __init__(self, browser_controller):
        self.browser = browser_controller
        self.page = browser_controller.page

    async def click(self, selector, timeout=30000):
        await self.page.click(selector, timeout=timeout)

    async def input_text(self, selector, text, timeout=30000):
        await self.page.fill(selector, str(text), timeout=timeout)

    async def login(self, login_actions: list, config: dict):
        for action in login_actions:
            await self.do_action(action, config)

    async def do_action(self, action: dict, config: dict):
        action_type = action.get("type", "click")
        selector = action.get("selector") or action.get("id")
        if action_type == "click" and selector:
            await self.click(selector)
        elif action_type == "input" and selector:
            value = action["value"]
            if isinstance(value, str) and value.startswith("${"):
                config_path = value[2:-1].split(".")
                val = config
                for key in config_path:
                    val = val[key]
                value = val
            await self.click(selector)
            await self.input_text(selector, value)
        if (sleep := action.get("sleep")):
            import asyncio
            await asyncio.sleep(sleep)
