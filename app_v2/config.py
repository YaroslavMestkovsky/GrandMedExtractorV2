import yaml
from pathlib import Path

class AppConfig:
    CONFIG_DIR = Path(__file__).parent / 'configs'
    _cache = {}

    @classmethod
    def load_config(cls, name: str):
        if name in cls._cache:
            return cls._cache[name]
        path = cls.CONFIG_DIR / f'{name}.yaml'
        with open(path, encoding='utf-8') as f:
            data = yaml.safe_load(f)
        cls._cache[name] = data
        return data

    @property
    def postgres(self):
        return self.load_config('postgres')

    @property
    def bitrix(self):
        return self.load_config('bitrix')

    @property
    def actions(self):
        return self.load_config('actions')

    @property
    def telegram(self):
        return self.load_config('telegram')

config = AppConfig()
