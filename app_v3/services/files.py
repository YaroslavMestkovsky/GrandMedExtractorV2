import datetime
import re
from collections import defaultdict

import pandas as pd

from pathlib import Path

from app_v3.bitrix.manager import BitrixManager
from app_v3.database.enums import ANALYTICS_FIELDS, SPECIALISTS_FIELDS, BitrixEnum, ANALYTICS_TO_BITRIX
from app_v3.database.models import Analytics
from app_v3.database.repositories import AnalyticsRepository, SpecialistsRepository
from app_v3.utils.logger import app_logger
from app_v3.utils.reporter import reporter


class FileProcessor:
    """Класс-процессор для обработки файлов."""

    def __init__(self, redirect_dir: Path):
        self.bitrix_manager = BitrixManager()
        self.analytics_repository = AnalyticsRepository()
        self.specialists_repository = SpecialistsRepository()
        self.redirect_dir = redirect_dir

    def process_yesterday_analytics(self, file):
        """Загрузка за вчерашний день и выгрузка Косметологии."""

        app_logger.info("[FPr] Выгрузка Косметологии.")

        df = self.get_df(file, [-1])
        df = self.prepare_analytics_df(df)

        records = self._aggregate_cosmetology_analytics(df)
        amount = len(records)

        for num, record in enumerate(records, 1):
            print(f"\r[FPr] Выгрузка Косметологии: {num}/{amount}", end="", flush=True)
            self.bitrix_manager.upload_cosmetology_to_bitrix(record)

        reporter.add_info(
            f'Выгружено {amount - len(self.bitrix_manager.not_found_contacts)}/{amount} записей по Косметологии')

        if self.bitrix_manager.not_found_contacts:
            reporter.add_info(f'Не найденные контакты: \n``` {self.bitrix_manager.not_found_contacts} ```')

        print()

    def process_period_analytics(self, file, from_scratch):
        """Загрузка с перезаписью за период."""

        app_logger.info("[FPr] Загрузка аналитик за период .")

        df = self.get_df(file, [-1])
        df = self.prepare_analytics_df(df)

        # Удаляем перезаписываемые записи
        if from_scratch:
            instance_codes = list(df['instance_code'])
            _filter = Analytics.instance_code.in_(instance_codes)
            self.analytics_repository.delete_records(_filter)

        records_to_insert = df.to_dict("records")
        self.analytics_repository.bulk_upload(records_to_insert)

        app_logger.info("[FPr] Аналитики за период загружены.")

    def process_specialists(self, file):
        app_logger.info("[FPr] Загрузка специалистов.")

        df = self.get_df(file, [], skip_rows=2)
        initial_count = df.shape[0]

        columns_to_keep = [col for col in df.columns if col in SPECIALISTS_FIELDS]
        df = df[columns_to_keep]
        df = df.rename(columns=SPECIALISTS_FIELDS)

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
            self.specialists_repository.all_material_numbers()
        )
        # Обработка полей даты
        date_columns = ["date_d0"]

        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_date)

        # Фильтруем только новые записи
        df = df.dropna(subset=["material_number"])
        new_records = df[~df["material_number"].isin(existing_numbers)]

        final_count = len(new_records)
        msg = f"[FPr] Отобрано {final_count}/{initial_count} записей специалистов"
        app_logger.info(msg)
        reporter.add_info(msg)

        if new_records.empty:
            msg = "Нет новых записей по специалистам для загрузки"
            app_logger.info(f"[FPr] {msg}")
        else:
            # Конвертируем записи в список словарей
            new_records = new_records.replace({pd.NaT: ""})
            new_records = new_records.map(lambda x: "" if x is pd.NaT else x)
            records_to_insert = new_records.to_dict("records")

            self.specialists_repository.bulk_upload(records_to_insert)

        app_logger.info("[FPr] Специалисты загружены.")

    def process_users(self, file):
        app_logger.info("[FPr] Загрузка пациентов.")

        df = self.get_df(file, [], skip_rows=2)
        initial_count = df.shape[0]

        columns_to_keep = [col for col in [col.strip() for col in df.columns] if col in BitrixEnum.NAME_TO_FIELD]
        df.columns = df.columns.str.strip()
        df = df[columns_to_keep]
        df = df.rename(columns=BitrixEnum.NAME_TO_FIELD)
        df = df.where(pd.notna(df), None)

        for col in df.select_dtypes(include=["datetime64"]).columns:
            df[col] = df[col].astype(str)

        final_count = df.shape[0]
        app_logger.info(f"[FPr] Отобрано {final_count}/{initial_count} записей по пациентам")

        records = df.to_dict("records")
        reg_nums = [rec[BitrixEnum.REG_NUM] for rec in records]
        reg_nums = [reg_num for reg_num in reg_nums if reg_num]

        uploaded_by_reg_num = self.bitrix_manager.get_records_by_reg_nums(reg_nums)
        records_to_upload = [rec for rec in records if rec[BitrixEnum.REG_NUM] not in uploaded_by_reg_num]

        skipped_count = len(records) - len(records_to_upload)

        if skipped_count > 0:
            app_logger.info(f"[BitrixManager] Пропущено уже загруженных пациентов: {skipped_count}")

        amount = len(records_to_upload)

        if amount > 0:
            app_logger.info(f"[FPr] Начало загрузки {amount} новых записей в Bitrix")

            for num, record in enumerate(records_to_upload, 1):
                self.bitrix_manager.modify_patients_record(record)
                self.bitrix_manager.upload_to_bitrix(record)
                print(f"\r[FPr] Выгрузка в Bitrix: {num}/{amount}", end="", flush=True)

            print()
            msg = f"Загружено новых записей по пациентам: {amount}"
            app_logger.info(f"[FPr] {msg}")
            reporter.add_info(msg)
        else:
            msg = "Нет новых записей по пациентам для загрузки"
            app_logger.info(f"[FPr] {msg}")
            reporter.add_info(msg)

    def prepare_analytics_df(self, df):
        """Обработка дата-фрейма аналитик. Фильтры, группировки, исключения."""

        initial_count = df.shape[0]

        df = df[df["Категория пациента"] != "Тестовый пациент"]
        # Выбор и переименование колонок
        columns_to_keep = [col for col in [col.strip() for col in df.columns] if col in ANALYTICS_FIELDS]

        df.columns = df.columns.str.strip()
        df = df[columns_to_keep]
        df = df.rename(columns=ANALYTICS_FIELDS)
        df = df.where(pd.notna(df), None)

        # Фильтрация служебных услуг
        df = df[~df['okmu_code'].str.startswith('Q', na=False)]
        # Фильтрация по статусу
        df = df[df["status"].isin(["выполнено", "авторизован"])]

        # Обработка поля age - извлекаем только цифры
        if "age" in df.columns:
            df["age"] = df["age"].apply(
                lambda x: int(re.search(r"\d+", str(x)).group())
                if pd.notna(x) and re.search(r"\d+", str(x))
                else None
            )

        # Обработка поля total_amount - зануляем прочерки
        if "total_amount" in df.columns:
            df["total_amount"] = df["total_amount"].apply(lambda x: x if x != "-" else None)

        # Обработка полей даты
        date_columns = ["date", "birth_date"]

        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._parse_date)

        df = df.replace({pd.NaT: ""})
        df = df.map(lambda x: "" if x is pd.NaT else x)

        final_count = df.shape[0]
        msg = f"[FPr] Отобрано {final_count}/{initial_count} записей аналитик"
        app_logger.info(msg)
        reporter.add_info(msg)

        return df
    
    def _aggregate_cosmetology_analytics(self, df):
        """Агрегация суммы по аналитикам и подготовка для выгрузки в битрикс."""

        records = []
        records_map = defaultdict(list)
        initial_count = df.shape[0]

        df = (
            df
            [df["admission_type"] == "КОСМЕТОЛОГИЯ"]
            [df["department_execution"] == "ХГМ КОСМ АМБ"]
            [ANALYTICS_TO_BITRIX.values()]
        )
        df["total_amount"] = df["total_amount"].astype(float)
        
        raw_records = df.to_dict("records")
        
        for record in raw_records:
            records_map[f"'{record['registration_number']}{record['specialist_execution']}'"].append(record)
            
        for records_list in records_map.values():
            records.append(self._merge_cosmetology_records(records_list))

        msg = f"[FPr] Отобрано {len(records)}/{initial_count} записей аналитик Косметологии для выгрузки в Bitrix"
        app_logger.info(msg)
        reporter.add_info(msg)

        return records
    
    def _merge_cosmetology_records(self, records):
        """Объединение данных для выгрузки в косметологию."""

        result = {}

        for record in records:
            appointment_date = record['appointment_date']

            result.update({
                'registration_number': record['registration_number'],
                BitrixEnum.SPEC_EXECUTION: record['specialist_execution'],
                BitrixEnum.PHYS_DEPARTMENT: record['physician_department'],
                BitrixEnum.APPOINTMENT_DATE: self._modify_date_format(appointment_date) if appointment_date else None,
                BitrixEnum.TOTAL_AMOUNT: result.get(BitrixEnum.TOTAL_AMOUNT, 0) + record['total_amount'],
            })

        return result

    def get_df(self, file, bottom_drops, skip_rows=3):
        path = self.redirect_dir.joinpath(file)
        df = pd.read_csv(
            path,
            skiprows=skip_rows,
            encoding='cp1251',
            delimiter=';',
            low_memory=False,
        )
        indices_to_drop = [df.index[i] for i in bottom_drops]

        if indices_to_drop:
            df = df.drop(indices_to_drop)

        return df

    @staticmethod
    def _parse_date(date_str):
        if date_str is not None:
            return str(date_str)

        else:
            return None

    @staticmethod
    def _modify_date_format(_date):
        _date = datetime.datetime.strptime(_date, '%d.%m.%y')
        _date = datetime.datetime.strftime(_date, '%d.%m.%Y %H:%M:%S')

        return _date
