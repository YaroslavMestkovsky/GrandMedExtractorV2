class Reporter:
    def __init__(self):
        self.messages = []
        self.errors = []
        self.statistics = {}

    def add_message(self, msg):
        self.messages.append(msg)

    def add_error(self, err):
        self.errors.append(err)

    def add_stat(self, key, value):
        self.statistics[key] = value

    def report(self):
        # Генерация текстового финального отчета (заглушка)
        return '\n'.join(self.messages + self.errors)
