import os
from typing import Dict, Any

import yaml


class MainConfig:
    def __init__(self, **kwargs):
        self.bitrix: Dict[str, Any]
        self.main: Dict[str, Any]
        self.database: Dict[str, Any]
        self.telegram: Dict[str, Any]

        for config in os.listdir('configs'):
            config_path = os.path.join('configs', config)
            setattr(self, config.rsplit('.', 1)[0], self._load_config(config_path))

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Загрузка конфигурации из YAML файла.

        Args:
            config_path: Путь к файлу конфигурации

        Returns:
            Dict[str, Any]: Загруженная конфигурация
        """

        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)


config = MainConfig()
