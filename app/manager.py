import configparser
import json
import re
import urllib3

import pandas as pd
import requests

from typing import Any
from pandas import NaT
from sqlalchemy import select

from database.db_manager import get_session
from database.models import Analytics, Specialists
from enums import ANALYTICS, SPECIALISTS, BitrixDealsEnum


# Отключаем все предупреждения urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class SQLManager:
    """Менеджер по загрузке информации из CSV в PostgreSQL."""

    def __init__(self, logger, messages):
        self.logger = logger
        self.session = get_session()
        self.messages = messages

    def process_analytics(self, df, from_scratch=False):
        """Загрузка аналитик. Грузим без проверки уникальности, т.к. нет возможности её проверить."""

        len_df = df.shape[0]

        df = df[df["Категория пациента"] != "Тестовый пациент"]
        skipped_rows = len_df - df.shape[0]

        msg = f"[Manager] Пропущено {skipped_rows} тестовых пациентов"
        self.messages['messages'].append(msg)
        self.logger.info(msg)

        columns_to_keep = [col for col in [col.strip() for col in df.columns] if col in ANALYTICS]
        df.columns = df.columns.str.strip()
        df = df[columns_to_keep]
        df = df.rename(columns=ANALYTICS)
        df = df.where(pd.notna(df), None)

        len_df = df.shape[0]
        df = df[~df['okmu_code'].str.startswith('Q', na=False)]
        skipped_rows = len_df - df.shape[0]

        msg = f"[Manager] Пропущено {skipped_rows} служебных услуг"
        self.messages['messages'].append(msg)
        self.logger.info(msg)

        # Обработка поля age - извлекаем только цифры
        if "age" in df.columns:
            df["age"] = df["age"].apply(
                lambda x: int(re.search(r"\d+", str(x)).group())
                if pd.notna(x) and re.search(r"\d+", str(x))
                else None
            )

        # Обработка поля total_amount - зануляем прочерки
        if "total_amount" in df.columns:
            df["total_amount"] = df["total_amount"].apply(
                lambda x: x if x != "-" else None
            )

        # Обработка полей даты
        date_columns = ["date", "birth_date"]

        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_date)

        df = df.replace({pd.NaT: ""})
        df = df.map(lambda x: "" if x is NaT else x)

        if from_scratch:
            # Убиваем перезаписываемые записи
            instance_codes = list(df['instance_code'])
            _filter = Analytics.instance_code.in_(instance_codes)
            deleted_count = self.session.query(Analytics).filter(_filter).delete(synchronize_session=False)

            msg = f"[Manager] Удалено {deleted_count} старых записей аналитик."
            self.messages['messages'].append(msg)
            self.logger.info(msg)

        records_to_insert = df.to_dict("records")
        self._bulk_upload(Analytics, records_to_insert, "аналитикам")

    def process_specialists(self, df):
        """Загрузка специалистов."""

        columns_to_keep = [col for col in df.columns if col in SPECIALISTS]
        df = df[columns_to_keep]
        df = df.rename(columns=SPECIALISTS)

        # Обработка поля patient_age - извлекаем только цифры
        if "patient_age" in df.columns:
            df["patient_age"] = df["patient_age"].apply(
                lambda x: int(re.search(r"\d+", str(x)).group())
                if pd.notna(x) and re.search(r"\d+", str(x))
                else None
            )

        # Получаем список существующих записей
        existing_numbers = set(
            number[0] for number in
            self.session.execute(select(Specialists.material_number)).all()
        )

        # Обработка полей даты
        date_columns = ["date_d0"]

        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_date)

        # Фильтруем только новые записи
        df = df.dropna(subset=["material_number"])
        new_records = df[~df["material_number"].isin(existing_numbers)]

        if new_records.empty:
            msg = "[Manager] Нет новых записей по специалистам для загрузки"
            self.messages['messages'].append(msg)
            self.logger.info(msg)
        else:
            # Конвертируем записи в список словарей
            new_records = new_records.replace({pd.NaT: ""})
            new_records = new_records.map(lambda x: "" if x is NaT else x)
            records_to_insert = new_records.to_dict("records")

            self._bulk_upload(Specialists, records_to_insert, "специалистам")

    def _bulk_upload(self, model, records, entity):
        try:
            total_rows = len(records)
            chunk_size = 50000

            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]

                self.session.bulk_insert_mappings(model, chunk)
                self.session.commit()

                print(f"\r[Manager] Загрузка: {i}/{total_rows} записей...", end="", flush=True)

            print()
            msg = f"[Manager] Загружено {total_rows} новых записей по {entity}."
            self.messages['messages'].append(msg)
            self.logger.info(msg)
        except Exception as e:
            self.session.rollback()

            err = f"[Manager] Ошибка при загрузке данных: {str(e)}"
            self.messages['error'] = err
            self.logger.error(err)
            raise

    @staticmethod
    def _parse_date(date_str):
        if date_str is not None:
            return str(date_str)

        else:
            return None


class BitrixManager:
    HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
    SELECT: list = []
    FILTER: dict[str: Any] = {}
    ORDER = {"DATE_CREATE": "ASC"}

    DATA = {
        "SELECT": SELECT,
        "FILTER": FILTER,
        "ORDER": ORDER,
        "start": 0,
    }

    def __init__(self, logger, messages):
        self.logger = logger
        self.messages = messages
        self.reg_num_field = BitrixDealsEnum.VAR_TO_FIELD[BitrixDealsEnum.REG_NUM]

        self._init_config()

    def process(self, df):
        columns_to_keep = [col for col in [col.strip() for col in df.columns] if col in BitrixDealsEnum.NAME_TO_FIELD]
        df.columns = df.columns.str.strip()
        df = df[columns_to_keep]
        df = df.rename(columns=BitrixDealsEnum.NAME_TO_FIELD)
        df = df.where(pd.notna(df), None)

        for col in df.select_dtypes(include=["datetime64"]).columns:
            df[col] = df[col].astype(str)

        records = df.to_dict("records")

        reg_nums = [rec[self.reg_num_field] for rec in records]
        reg_nums = [reg_num for reg_num in reg_nums if reg_num]
        uploaded_by_reg_num = self._get_records_by_reg_nums(reg_nums)

        records_to_upload = [rec for rec in records if rec[self.reg_num_field] not in uploaded_by_reg_num]

        for record in records_to_upload:
            record["CATEGORY_ID"] = self.CATEGORY_ID
            record[BitrixDealsEnum.CREATION] = record[BitrixDealsEnum.CREATION]
            record[BitrixDealsEnum.VAR_TO_FIELD[BitrixDealsEnum.BIRTHDAY]] = record[BitrixDealsEnum.VAR_TO_FIELD[BitrixDealsEnum.BIRTHDAY]]

        amount = len(records_to_upload)

        for num, record in enumerate(records_to_upload, 1):
            self._upload_to_bitrix(record)
            print(f"\rВыгрузка в Bitrix: {num}/{amount}", end="", flush=True)

        print()
        msg = f"[Manager] Успешно загружено {amount} новых записей по юзерам."
        self.messages['messages'].append(msg)
        self.logger.info(msg)

    def _get_records_by_reg_nums(self, reg_nums):
        """Получаем рег. номера из битрикса что бы понять, что уже загружено."""

        _filter = {
            f"@{self.reg_num_field}": reg_nums,
            "CATEGORY_ID": self.CATEGORY_ID,
        }
        _select = [self.reg_num_field]

        self.DATA.update({
            "FILTER": _filter,
            "SELECT": _select,
        })

        records_by_reg_nums = self._get_response(self.LIST_METHOD)
        reg_nums = set([rec[self.reg_num_field] for rec in records_by_reg_nums])

        self.logger.info(f"[Manager] Уже загружено рег. номеров из этого файла: {len(reg_nums)}")

        return reg_nums

    def _get_response(self, method):
        result = []

        def get_records():
            response = requests.post(
                f"{self.WEBHOOK_URL}{method}",
                headers=self.HEADERS,
                data=json.dumps(self.DATA),
                verify=False,
            )

            response.raise_for_status()
            recs = response.json()

            return recs

        try:
            _next = 0

            while _next is not None:
                self.DATA["start"] = _next
                records = get_records()
                result.extend(records["result"])

                _next = records.get("next")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"[Manager] Произошла ошибка при выполнении запроса: {e}")
        except json.JSONDecodeError:
            self.logger.error("[Manager] Ошибка декодирования JSON из ответа сервера.")
        except Exception as e:
            self.logger.error(f"[Manager] Произошла неизвестная ошибка: {e}")
        except Exception as e:
            raise self.logger.error(e)

        return result

    def _upload_to_bitrix(self, record):
        """Выгрузка сделки в битрикс."""

        try:
            response = requests.post(f"{self.WEBHOOK_URL}{self.ADD_METHOD}", json={"fields": record}, verify=False)
            response.raise_for_status()

            if response.status_code == 200:
                result = response.json()

                if "error" in result:
                    self.logger.warning("[Manager] Ошибка при создании сделки:", result.get("error", "Неизвестная ошибка"))
            else:
                self.logger.error("[Manager] Ошибка при отправке запроса:", response.status_code, response.text)

        except requests.exceptions.RequestException as e:
            self.logger.error(f"[Manager] Произошла ошибка при выполнении запроса: {e}")
        except json.JSONDecodeError:
            self.logger.error("[Manager] Ошибка декодирования JSON из ответа сервера.")
        except Exception as e:
            self.logger.error(f"[Manager] Произошла неизвестная ошибка: {e}")
        except Exception as e:
            raise self.logger.error(e)

    def _init_config(self):
        conf_path = "app/bitrix.conf"
        config = configparser.ConfigParser()
        config.read(conf_path)

        self.WEBHOOK_URL = config.get("base", "webhook_url")
        self.CATEGORY_ID = config.get("base", "category_id")
        self.GET_METHOD = config.get("deals", "get_method")
        self.ADD_METHOD = config.get("deals", "add_method")
        self.LIST_METHOD = config.get("deals", "list_method")


class TelegramManager:
    def __init__(self, logger):
        self.logger = logger
        self.token = None
        self.user_id = None
        self._init_config()

    def _init_config(self):
        try:
            conf_path = "app/tg.conf"
            config = configparser.ConfigParser()

            if not config.read(conf_path, encoding="utf-8"):
                self.logger.warning(f"[TelegramManager] Файл конфигурации не найден: {conf_path}")
                return

            self.token = config.get("telegram", "token", fallback=None)
            self.user_id = config.get("telegram", "user_id", fallback=None)

            if not self.token or not self.user_id:
                self.logger.warning("[TelegramManager] В tg.conf отсутствуют token или user_id в секции [telegram]")

        except Exception as e:
            self.logger.error(f"[TelegramManager] Ошибка чтения конфигурации: {e}")

    def send_messages(self, messages, errors: str = "") -> bool:
        try:
            if not messages and not errors:
                self.logger.info("[TelegramManager] Нет данных для отправки")
                return False

            if not self.token or not self.user_id:
                self.logger.warning("[TelegramManager] Не настроен token или user_id")

                return False

            parts = []
            if messages:
                parts.append("\n".join(str(m) for m in messages))
            if errors:
                parts.append(f"Ошибки:\n{errors}")

            text = "\n\n".join(parts)
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": str(self.user_id),
                "text": text,
                "disable_web_page_preview": True,
            }

            resp = requests.post(url, json=payload, timeout=15)

            if resp.status_code == 200 and resp.json().get("ok"):
                self.logger.info("[TelegramManager] Сообщение отправлено")
                return True

            self.logger.error(f"[TelegramManager] Ошибка отправки: {resp.status_code} {resp.text}")
            return False

        except Exception as e:
            self.logger.error(f"[TelegramManager] Исключение при отправке сообщения: {e}")
            return False