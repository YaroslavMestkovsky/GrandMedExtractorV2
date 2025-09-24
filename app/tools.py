import os

import logging
import pandas as pd

from manager import SQLManager, BitrixManager
from database.db_manager import check_db


def upload():
    log_params = {
        "encoding": "utf-8",
        "level": "INFO",
        "format": "%(asctime)s - %(levelname)s - %(message)s",
    }

    logging.basicConfig(**log_params)
    logger = logging.getLogger(__name__)

    files_to_process = os.listdir('app/files')
    sql_manager = SQLManager(logger)
    bitrix_manager = BitrixManager(logger)

    funcs = {
        'a': sql_manager.process_analytics,
        's': sql_manager.process_specialists,
        'u': bitrix_manager.process,
    }

    for file in files_to_process:
        logger.info(f'Обработка файла {file}...')

        if 'analytics' in file:
            skip_rows = 2
            bottom_drops = [-1]
            func = 'a'
        elif 'specialists' in file:
            skip_rows = 2
            bottom_drops = []
            func = 's'
        elif 'users' in file:
            skip_rows = 2
            bottom_drops = [-1]
            func = 'u'
        else:
            logger.error(f"Ошибка при обработке файла: {file}")
            continue

        df = pd.read_csv(
            f'app/files/{file}',
            skiprows=skip_rows,
            encoding='cp1251',
            delimiter=';',
            low_memory=False,
        )

        indices_to_drop = [df.index[i] for i in bottom_drops]
        df = df.drop(indices_to_drop)

        funcs[func](df)


if __name__ == '__main__':
    check_db()
    upload()
