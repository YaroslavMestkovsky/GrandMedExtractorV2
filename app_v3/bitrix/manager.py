import json
from typing import Any

import requests

from app_v3.database.enums import BitrixEnum
from app_v3.utils.config import app_config
from app_v3.utils.logger import app_logger


BITRIX_CONFIG = app_config.bitrix


class BitrixManager:
    """Менеджер для общения с битриксом."""

    HEADERS = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    SELECT: list = []
    FILTER: dict[str: Any] = {}
    ORDER = {"DATE_CREATE": "ASC"}

    DATA = {
        "SELECT": SELECT,
        "FILTER": FILTER,
        "ORDER": ORDER,
        "start": 0,
    }

    def upload_to_bitrix(self, record):
        """Выгрузка сделки по юзерам в Bitrix."""

        deal_id = None

        try:
            response = requests.post(
                f"{BITRIX_CONFIG['base']['webhook_url_prod']}{BITRIX_CONFIG['deals']['add_method']}",
                json={"fields": record},
                verify=False,
            )
            response.raise_for_status()

            if response.status_code == 200:
                result = response.json()

                if "error" in result:
                    error_msg = f"Ошибка Bitrix при создании сделки: {result.get('error', 'Неизвестная ошибка')}"
                    app_logger.warning(f"[BMn] {error_msg}")
                    # Не добавляем в общий список ошибок, т.к. это может быть массовая операция
                else:
                    deal_id = result['result']
            else:
                error_msg = f"Ошибка HTTP при отправке запроса: {response.status_code}"
                app_logger.error(f"[BMn] {error_msg}: {response.text}")

        except requests.exceptions.RequestException as e:
            app_logger.error(f"[BMn] Ошибка HTTP-запроса при загрузке в Bitrix: {str(e)}")
        except json.JSONDecodeError as e:
            app_logger.error(f"[BMn] Ошибка декодирования JSON из ответа сервера: {str(e)}")
        except Exception as e:
            app_logger.error(f"[BMn] Неизвестная ошибка при загрузке в Bitrix: {str(e)}", exc_info=True)

        return deal_id

    def upload_cosmetology_to_bitrix(self, record):
        """Выгрузка сделки по косметологии в Bitrix."""

        not_found_contacts = []
        reg_num = record['registration_number']
        contact = self._get_contact_by_reg_number(reg_num)

        if contact:
            record = {
                'CATEGORY_ID': '65',
                'UF_CRM_673DEA05D361C': record['appointment_date'],
                'UF_CRM_1641810471884': record['specialist_execution'],
                'STAGE_ID': 'C44:WON',
                'ASSIGNED_BY_ID': '19240',
                'TYPE_ID': 'Интеграция с qMS',
            }
            deal_id = self.upload_to_bitrix(record)
        else:
            not_found_contacts.append(reg_num)


    def _get_contact_by_reg_number(self, reg_num):
        """Находим контакт юзера по его рег. номеру."""

        contact_id = None

        try:
            response = requests.post(
                f"{BITRIX_CONFIG['base']['webhook_url_prod']}{BITRIX_CONFIG['deals']['contact_list_method']}",
                headers=self.HEADERS,
                data=json.dumps({
                    'SELECT': ['ID'],
                    'FILTER': {BitrixEnum.CONTACT_REG_NUM: reg_num},
                    'ORDER': {'DATE_CREATE': 'ASC'},
                    'start': 0,
                }),
                verify=False,
            )
            response.raise_for_status()

            if response.status_code == 200:
                result = response.json()

                if "error" in result:
                    error_msg = f"Ошибка Bitrix при создании сделки: {result.get('error', 'Неизвестная ошибка')}"
                    app_logger.warning(f"[BMn] {error_msg}")
                    # Не добавляем в общий список ошибок, т.к. это может быть массовая операция
                else:
                    contact_id = result['result'][0]['ID']
            else:
                error_msg = f"Ошибка HTTP при отправке запроса: {response.status_code}"
                app_logger.error(f"[BMn] {error_msg}: {response.text}")

        except requests.exceptions.RequestException as e:
            app_logger.error(f"[BMn] Ошибка HTTP-запроса при загрузке в Bitrix: {str(e)}")
        except json.JSONDecodeError as e:
            app_logger.error(f"[BMn] Ошибка декодирования JSON из ответа сервера: {str(e)}")
        except IndexError as e:
            app_logger.warning(f"[BMn] Контакт с рег. номером {reg_num} не найден")
        except Exception as e:
            app_logger.error(f"[BMn] Неизвестная ошибка при загрузке в Bitrix: {str(e)}", exc_info=True)

        return contact_id

    def get_records_by_reg_nums(self, reg_nums):
        """Получение всех записей по переданному списку рег. номеров."""

        _filter = {
            f"@{BitrixEnum.REG_NUM}": reg_nums,
            "CATEGORY_ID": BITRIX_CONFIG['deals']['patients_category_id'],
        }
        _select = ['*']

        self.DATA.update({
            "FILTER": _filter,
            "SELECT": _select,
        })

        records_by_reg_nums = self._get_response(
            BITRIX_CONFIG['deals']['list_method'],
            BITRIX_CONFIG['base']['webhook_url_prod'],
        )
        reg_nums = set([rec[BitrixEnum.REG_NUM] for rec in records_by_reg_nums])

        app_logger.info(f"[BMn] Найдено уже загруженных регистрационных номеров: {len(reg_nums)}")

    def modify_patients_record(self, record):
        record["PATIENTS_CATEGORY_ID"] = BITRIX_CONFIG['deals']['patients_category_id']

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

        except requests.exceptions.RequestException as e:
            error_msg = f"Ошибка HTTP-запроса к Bitrix API: {str(e)}"
            app_logger.error(f"[BMn] {error_msg}")
            # self._add_error(error_msg) todo бот
            raise
        except json.JSONDecodeError as e:
            error_msg = "Ошибка декодирования JSON из ответа сервера"
            app_logger.error(f"[BMn] {error_msg}: {str(e)}")
            # self._add_error(error_msg) todo бот
            raise
        except Exception as e:
            error_msg = f"Неизвестная ошибка при запросе к Bitrix API: {str(e)}"
            app_logger.error(f"[BMn] {error_msg}", exc_info=True)
            # self._add_error(error_msg) todo бот
            raise

        return result
