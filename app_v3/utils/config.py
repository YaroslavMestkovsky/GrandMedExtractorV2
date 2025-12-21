import os
import yaml

from typing import Dict, Any
from app_v3.utils.logger import app_logger


class MainConfig:
    def __init__(self, **kwargs):
        self.bitrix: Dict[str, Any]
        self.main: Dict[str, Any]
        self.database: Dict[str, Any]
        self.telegram: Dict[str, Any]

        for _config in os.listdir('app_v3/configs'):
            config_path = os.path.join('app_v3/configs', _config)
            setattr(self, _config.rsplit('.', 1)[0], self._load_config(config_path))

        app_logger.info('Config loaded.')

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Загрузка конфигурации из YAML файла.

        Args:
            config_path: Путь к файлу конфигурации

        Returns:
            Dict[str, Any]: Загруженная конфигурация
        """

        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)


app_config = MainConfig()
