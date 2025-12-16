import configparser
import datetime
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
from enums import ANALYTICS, ANALYTICS_TO_BITRIX, SPECIALISTS, BitrixDealsEnum


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

        initial_count = df.shape[0]
        self.logger.info(f"[SQLManager] Начало обработки аналитик: {initial_count} записей")

        # Фильтрация тестовых пациентов
        len_df = df.shape[0]
        df = df[df["Категория пациента"] != "Тестовый пациент"]
        skipped_rows = len_df - df.shape[0]

        if skipped_rows > 0:
            msg = f"Пропущено тестовых пациентов: {skipped_rows}"
            self._add_message(msg)
            self.logger.info(f"[SQLManager] {msg}")

        # Выбор и переименование колонок
        columns_to_keep = [col for col in [col.strip() for col in df.columns] if col in ANALYTICS]
        df.columns = df.columns.str.strip()
        df = df[columns_to_keep]
        df = df.rename(columns=ANALYTICS)
        df = df.where(pd.notna(df), None)

        # Фильтрация служебных услуг
        len_df = df.shape[0]
        df = df[~df['okmu_code'].str.startswith('Q', na=False)]
        skipped_rows = len_df - df.shape[0]

        if skipped_rows > 0:
            msg = f"Пропущено служебных услуг: {skipped_rows}"
            self._add_message(msg)
            self.logger.info(f"[SQLManager] {msg}")

        # Фильтрация по статусу
        len_df = df.shape[0]
        df = df[df["status"].isin(["выполнено", "авторизован"])]
        skipped_rows = len_df - df.shape[0]

        if skipped_rows > 0:
            msg = f"Пропущено записей с неактуальными статусами: {skipped_rows}"
            self._add_message(msg)
            self.logger.info(f"[SQLManager] {msg}")

        # Обработка поля age - извлекаем только цифры
        if "age" in df.columns:
            df["age"] = df["age"].apply(
                lambda x: int(re.search(r"\d+", str(x)).group())
                if pd.notna(x) and re.search(r"\d+", str(x))
                else None
            )
            self.logger.debug("[SQLManager] Поле 'age' обработано")

        # Обработка поля total_amount - зануляем прочерки
        if "total_amount" in df.columns:
            df["total_amount"] = df["total_amount"].apply(
                lambda x: x if x != "-" else None
            )
            self.logger.debug("[SQLManager] Поле 'total_amount' обработано")

        # Обработка полей даты
        date_columns = ["date", "birth_date"]

        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_date)
                self.logger.debug(f"[SQLManager] Поле даты '{col}' обработано")

        df = df.replace({pd.NaT: ""})
        df = df.map(lambda x: "" if x is NaT else x)

        final_count = df.shape[0]
        self.logger.info(f"[SQLManager] После фильтрации осталось {final_count} записей из {initial_count}")

        if from_scratch:
            # Удаляем перезаписываемые записи
            instance_codes = list(df['instance_code'])
            _filter = Analytics.instance_code.in_(instance_codes)
            deleted_count = self.session.query(Analytics).filter(_filter).delete(synchronize_session=False)

            if deleted_count > 0:
                msg = f"Удалено старых записей аналитик: {deleted_count}"
                self._add_message(msg)
                self.logger.info(f"[SQLManager] {msg}")

        records_to_insert = df.to_dict("records")
        self.messages['statistics']['analytics']['records'] = len(records_to_insert)
        self._bulk_upload(Analytics, records_to_insert, "аналитикам")

        return df

    def process_specialists(self, df):
        """Загрузка специалистов."""

        initial_count = df.shape[0]
        self.logger.info(f"[SQLManager] Начало обработки специалистов: {initial_count} записей")

        columns_to_keep = [col for col in df.columns if col in SPECIALISTS]
        df = df[columns_to_keep]
        df = df.rename(columns=SPECIALISTS)
        self.logger.debug(f"[SQLManager] Выбрано {len(columns_to_keep)} колонок для обработки")

        # Обработка поля patient_age - извлекаем только цифры
        if "patient_age" in df.columns:
            df["patient_age"] = df["patient_age"].apply(
                lambda x: int(re.search(r"\d+", str(x)).group())
                if pd.notna(x) and re.search(r"\d+", str(x))
                else None
            )
            self.logger.debug("[SQLManager] Поле 'patient_age' обработано")

        # Получаем список существующих записей
        self.logger.debug("[SQLManager] Получение списка существующих записей специалистов")
        existing_numbers = set(
            number[0] for number in
            self.session.execute(select(Specialists.material_number)).all()
        )
        self.logger.debug(f"[SQLManager] Найдено {len(existing_numbers)} существующих записей")

        # Обработка полей даты
        date_columns = ["date_d0"]

        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_date)
                self.logger.debug(f"[SQLManager] Поле даты '{col}' обработано")

        # Фильтруем только новые записи
        df = df.dropna(subset=["material_number"])
        new_records = df[~df["material_number"].isin(existing_numbers)]

        if new_records.empty:
            msg = "Нет новых записей по специалистам для загрузки"
            self._add_message(msg)
            self.logger.info(f"[SQLManager] {msg}")
        else:
            # Конвертируем записи в список словарей
            new_records = new_records.replace({pd.NaT: ""})
            new_records = new_records.map(lambda x: "" if x is NaT else x)
            records_to_insert = new_records.to_dict("records")

            self.messages['statistics']['specialists']['records'] = len(records_to_insert)
            self._bulk_upload(Specialists, records_to_insert, "специалистам")

    def _bulk_upload(self, model, records, entity):
        """Массовая загрузка записей в БД."""
        
        try:
            total_rows = len(records)
            chunk_size = 50000
            self.logger.info(f"[SQLManager] Начало массовой загрузки {total_rows} записей по {entity} (чанки по {chunk_size})")

            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]

                self.session.bulk_insert_mappings(model, chunk)
                self.session.commit()

                print(f"\r[SQLManager] Загрузка: {min(i + chunk_size, total_rows)}/{total_rows} записей...", end="", flush=True)

            print()
            msg = f"Загружено записей по {entity}: {total_rows}"
            self._add_message(msg)
            self.logger.info(f"[SQLManager] {msg}")
        except Exception as e:
            self.session.rollback()

            err = f"Ошибка при загрузке данных по {entity}: {str(e)}"
            self._add_error(err)
            self.logger.error(f"[SQLManager] {err}", exc_info=True)
            raise

    def _add_message(self, message: str) -> None:
        """Добавить сообщение в отчёт."""
        self.messages['messages'].append(message)

    def _add_error(self, error: str) -> None:
        """Добавить ошибку в отчёт."""
        if isinstance(self.messages.get('errors'), list):
            self.messages['errors'].append(error)
        else:
            self.messages['errors'] = [error]

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
        self.specialist_execution = BitrixDealsEnum.VAR_TO_FIELD[BitrixDealsEnum.SPECIALIST_EXECUTION]

        self._init_config()

    def process(self, df):
        """Обработка и загрузка пациентов в Bitrix."""
        
        initial_count = df.shape[0]
        self.logger.info(f"[BitrixManager] Начало обработки пациентов: {initial_count} записей")

        columns_to_keep = [col for col in [col.strip() for col in df.columns] if col in BitrixDealsEnum.NAME_TO_FIELD]
        df.columns = df.columns.str.strip()
        df = df[columns_to_keep]
        df = df.rename(columns=BitrixDealsEnum.NAME_TO_FIELD)
        df = df.where(pd.notna(df), None)
        self.logger.debug(f"[BitrixManager] Выбрано {len(columns_to_keep)} колонок для обработки")

        for col in df.select_dtypes(include=["datetime64"]).columns:
            df[col] = df[col].astype(str)
            self.logger.debug(f"[BitrixManager] Поле даты '{col}' преобразовано в строку")

        records = df.to_dict("records")

        reg_nums = [rec[self.reg_num_field] for rec in records]
        reg_nums = [reg_num for reg_num in reg_nums if reg_num]
        self.logger.debug(f"[BitrixManager] Найдено {len(reg_nums)} регистрационных номеров")
        
        uploaded_by_reg_num = self._get_records_by_reg_nums(reg_nums)

        records_to_upload = [rec for rec in records if rec[self.reg_num_field] not in uploaded_by_reg_num]
        skipped_count = len(records) - len(records_to_upload)

        if skipped_count > 0:
            msg = f"Пропущено уже загруженных пациентов: {skipped_count}"
            self._add_message(msg)
            self.logger.info(f"[BitrixManager] {msg}")

        for record in records_to_upload:
            record["PATIENTS_CATEGORY_ID"] = self.PATIENTS_CATEGORY_ID
            record[BitrixDealsEnum.CREATION] = record[BitrixDealsEnum.CREATION]
            record[BitrixDealsEnum.VAR_TO_FIELD[BitrixDealsEnum.BIRTHDAY]] = record[BitrixDealsEnum.VAR_TO_FIELD[BitrixDealsEnum.BIRTHDAY]]

        amount = len(records_to_upload)

        if amount > 0:
            self.logger.info(f"[BitrixManager] Начало загрузки {amount} новых записей в Bitrix")

            for num, record in enumerate(records_to_upload, 1):
                self._upload_to_bitrix(record, self.WEBHOOK_URL_PROD)
                print(f"\r[BitrixManager] Выгрузка в Bitrix: {num}/{amount}", end="", flush=True)

            print()
            msg = f"Загружено новых записей по пациентам: {amount}"
            self._add_message(msg)
            self.messages['statistics']['users']['records'] = amount
            self.logger.info(f"[BitrixManager] {msg}")
        else:
            msg = "Нет новых записей по пациентам для загрузки"
            self._add_message(msg)
            self.logger.info(f"[BitrixManager] {msg}")

    def _add_message(self, message: str) -> None:
        """Добавить сообщение в отчёт."""
        self.messages['messages'].append(message)

    def _add_error(self, error: str) -> None:
        """Добавить ошибку в отчёт."""
        if isinstance(self.messages.get('errors'), list):
            self.messages['errors'].append(error)
        else:
            self.messages['errors'] = [error]

    def process_analytics(self, df):
        df = (
            df
            [df["admission_type"] == "КОСМЕТОЛОГИЯ"]
            [df["department_execution"] == "ХГМ КОСМ АМБ"]
            [ANALYTICS_TO_BITRIX.values()]
        )
        df["total_amount"] = df["total_amount"].astype(float)
        df = df.groupby(
            [
                'registration_number',
                'full_name',
                'appointment_date',
                'department_execution',
                'specialist_execution',
            ],
            as_index=False,
        )["total_amount"].sum() #todo

        records = df.to_dict('records')

        for record in records:
            # Находим контакт юзера по его рег. номеру.
            contact = self._get_contact_by_reg_number(record['registration_number'])

            if contact:
                ad = record['appointment_date']

                if ad:
                    ad = datetime.datetime.strptime(ad, '%d.%m.%Y')
                    ad = datetime.datetime.strftime(ad, '%d.%m.%Y %H:%M:%S')

                # Создаем сделку
                deal = requests.post(
                    url='https://crm.grandmed.ru/rest/27036/pnkrzq23s3h1r71c/crm.deal.add',
                    headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
                    data=json.dumps({
                        'fields': {
                            'CATEGORY_ID': '71',
                            'UF_CRM_673DEA05D361C': ad,
                            'UF_CRM_1641810471884': record['specialist_execution'],
                            'STAGE_ID': 'C71:WON',
                            'ASSIGNED_BY_ID': '19240',
                            'TYPE_ID': 'UC_GTR0J0',
                            'OPPORTUNITY': record['total_amount'],
                        },
                    }),
                    verify=False,
                ).json()

                # Добавляем контакт в сделку
                requests.post(
                    url='https://crm.grandmed.ru/rest/27036/pnkrzq23s3h1r71c/crm.deal.contact.add',
                    headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
                    data=json.dumps({
                        'id': deal['result'],
                        'fields': {'CONTACT_ID': contact},
                    }),
                    verify=False,
                ).json()

            else:
                print(record['registration_number'])

    def _get_contact_by_reg_number(self, reg_num):
        """Здесь все очень плохо. Устал уже все выносить по энамам и проч.
        В идеале, на этом этапе уже надо полностью структуру проекта переписать."""

        response = requests.post(
            url='https://crm.grandmed.ru/rest/27036/pnkrzq23s3h1r71c/crm.contact.list',
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
            data=json.dumps({
                'SELECT': ['ID'],
                'FILTER': {
                    'UF_CRM_1744899027': reg_num,
                },
                'ORDER': {'DATE_CREATE': 'ASC'},
                'start': 0,
            }),
            verify=False,
        ).json()

        if 'result' in response and response['result']:
            return response['result'][0]['ID']

        else:
            return None

    def _get_records_by_reg_nums(self, reg_nums):
        """Получаем рег. номера из битрикса чтобы понять, что уже загружено."""

        self.logger.debug(f"[BitrixManager] Запрос существующих записей для {len(reg_nums)} регистрационных номеров")

        _filter = {
            f"@{self.reg_num_field}": reg_nums,
            "CATEGORY_ID": self.PATIENTS_CATEGORY_ID,
        }
        _select = ['*']

        self.DATA.update({
            "FILTER": _filter,
            "SELECT": _select,
        })

        records_by_reg_nums = self._get_response(self.LIST_METHOD, self.WEBHOOK_URL_PROD)
        reg_nums = set([rec[self.reg_num_field] for rec in records_by_reg_nums])

        self.logger.info(f"[BitrixManager] Найдено уже загруженных регистрационных номеров: {len(reg_nums)}")

        return reg_nums

    def _get_response(self, method, url):
        """Получение ответа от Bitrix API с пагинацией."""
        
        result = []

        def get_records():
            response = requests.post(
                f"{url}{method}",
                headers=self.HEADERS,
                data=json.dumps(self.DATA),
                verify=False,
            )

            response.raise_for_status()
            recs = response.json()

            return recs

        try:
            _next = 0
            page_count = 0

            while _next is not None:
                self.DATA["start"] = _next
                records = get_records()
                result.extend(records["result"])
                page_count += 1

                _next = records.get("next")
                self.logger.debug(f"[BitrixManager] Получена страница {page_count}, записей: {len(records['result'])}, next: {_next}")

            self.logger.debug(f"[BitrixManager] Всего получено {len(result)} записей за {page_count} страниц")

        except requests.exceptions.RequestException as e:
            error_msg = f"Ошибка HTTP-запроса к Bitrix API: {str(e)}"
            self.logger.error(f"[BitrixManager] {error_msg}")
            self._add_error(error_msg)
            raise
        except json.JSONDecodeError as e:
            error_msg = "Ошибка декодирования JSON из ответа сервера"
            self.logger.error(f"[BitrixManager] {error_msg}: {str(e)}")
            self._add_error(error_msg)
            raise
        except Exception as e:
            error_msg = f"Неизвестная ошибка при запросе к Bitrix API: {str(e)}"
            self.logger.error(f"[BitrixManager] {error_msg}", exc_info=True)
            self._add_error(error_msg)
            raise

        return result

    def _upload_to_bitrix(self, record, url):
        """Выгрузка сделки в Bitrix."""

        deal_id = None

        try:
            response = requests.post(f"{url}{self.ADD_METHOD}", json={"fields": record}, verify=False)
            response.raise_for_status()

            if response.status_code == 200:
                result = response.json()

                if "error" in result:
                    error_msg = f"Ошибка Bitrix при создании сделки: {result.get('error', 'Неизвестная ошибка')}"
                    self.logger.warning(f"[BitrixManager] {error_msg}")
                    # Не добавляем в общий список ошибок, т.к. это может быть массовая операция
                else:
                    deal_id = result['result']
            else:
                error_msg = f"Ошибка HTTP при отправке запроса: {response.status_code}"
                self.logger.error(f"[BitrixManager] {error_msg}: {response.text}")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"[BitrixManager] Ошибка HTTP-запроса при загрузке в Bitrix: {str(e)}")
        except json.JSONDecodeError as e:
            self.logger.error(f"[BitrixManager] Ошибка декодирования JSON из ответа сервера: {str(e)}")
        except Exception as e:
            self.logger.error(f"[BitrixManager] Неизвестная ошибка при загрузке в Bitrix: {str(e)}", exc_info=True)

        return deal_id

    def _init_config(self):
        conf_path = "app/bitrix.conf"
        config = configparser.ConfigParser()
        config.read(conf_path)

        self.WEBHOOK_URL_PROD = config.get("base", "webhook_url_prod")
        self.WEBHOOK_URL_TEST = config.get("base", "webhook_url_test")
        self.PATIENTS_CATEGORY_ID = config.get("deals", "patients_category_id")
        self.ANALYTICS_CATEGORY_ID = config.get("deals", "analytics_category_id")
        self.GET_METHOD = config.get("deals", "get_method")
        self.ADD_METHOD = config.get("deals", "add_method")
        self.LIST_METHOD = config.get("deals", "list_method")
        self.PRODUCT_ID_PROD = config.get("deals", "product_id_prod")
        self.PRODUCT_ID_TEST = config.get("deals", "product_id_test")


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

    def send_messages(self, messages, errors=None, statistics=None) -> bool:
        """Отправка отчёта в Telegram."""
        
        try:
            errors = errors or []
            statistics = statistics or {}
            
            if not messages and not errors:
                self.logger.info("[TelegramManager] Нет данных для отправки")
                return False

            if not self.token or not self.user_id:
                self.logger.warning("[TelegramManager] Не настроен token или user_id")
                return False

            # Формирование сообщения
            parts = []
            
            # Заголовок
            parts.append("📊 *Отчёт о выгрузке данных*")
            parts.append("")
            
            # Статистика
            if statistics:
                stats_parts = []
                for key, value in statistics.items():
                    if isinstance(value, dict):
                        uploaded = "✓" if value.get('uploaded') else "✗"
                        processed = "✓" if value.get('processed') else "✗"
                        records = value.get('records', 0)
                        
                        name_map = {
                            'analytics': 'Аналитики',
                            'specialists': 'Специалисты',
                            'users': 'Пациенты'
                        }
                        name = name_map.get(key, key)
                        
                        stats_parts.append(
                            f"  {name}:\n"
                            f"    Загрузка: {uploaded} | Обработка: {processed}\n"
                            f"    Записей: {records}"
                        )
                
                if stats_parts:
                    parts.append("*Статистика:*")
                    parts.extend(stats_parts)
                    parts.append("")
            
            # Сообщения
            if messages:
                parts.append("*Детали:*")
                for msg in messages:
                    parts.append(f"  • {msg}")
                parts.append("")
            
            # Ошибки
            if errors:
                error_list = errors if isinstance(errors, list) else [errors]
                parts.append("⚠️ *Ошибки:*")
                for error in error_list:
                    parts.append(f"  • {error}")
                parts.append("")
            
            text = "\n".join(parts)
            
            # Отправка
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": str(self.user_id),
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }

            resp = requests.post(url, json=payload, timeout=15)

            if resp.status_code == 200 and resp.json().get("ok"):
                self.logger.info("[TelegramManager] Сообщение успешно отправлено в Telegram")
                return True

            error_msg = f"Ошибка отправки в Telegram: {resp.status_code} {resp.text}"
            self.logger.error(f"[TelegramManager] {error_msg}")
            return False

        except Exception as e:
            self.logger.error(f"[TelegramManager] Исключение при отправке сообщения: {str(e)}", exc_info=True)
            return False