from sqlalchemy.orm import Session
from app_v2.database.models import Analytics
from sqlalchemy import func
from sqlalchemy import Integer

class AnalyticsRepository:
    def __init__(self, db: Session):
        self.db = db

    def insert_many(self, records: list[dict]):
        self.db.bulk_insert_mappings(Analytics, records)
        self.db.commit()

    def delete_by_instance_codes(self, instance_codes: list[str]) -> int:
        res = self.db.query(Analytics).filter(Analytics.instance_code.in_(instance_codes)).delete(synchronize_session=False)
        self.db.commit()
        return res

    def filter_by_status(self, statuses: list[str]):
        return self.db.query(Analytics).filter(Analytics.status.in_(statuses)).all()

    def filter_not_test_patients(self):
        return self.db.query(Analytics).filter(Analytics.category != "Тестовый пациент").all()

    def filter_not_service_codes(self):
        return self.db.query(Analytics).filter(~Analytics.okmu_code.startswith("Q")).all()

    def get_all(self):
        return self.db.query(Analytics).all()

    def select_for_bitrix(self):
        q = self.db.query(
            Analytics.registration_number,
            Analytics.full_name,
            Analytics.appointment_date,
            Analytics.department_execution,
            Analytics.specialist_execution,
            func.sum(func.cast(Analytics.total_amount, Integer)).label('total_amount_sum'),
        ).filter(
            Analytics.admission_type == "КОСМЕТОЛОГИЯ",
            Analytics.department_execution == "ХГМ КОСМ АМБ",
        ).group_by(
            Analytics.registration_number,
            Analytics.full_name,
            Analytics.appointment_date,
            Analytics.department_execution,
            Analytics.specialist_execution,
        )
        return q.all()
