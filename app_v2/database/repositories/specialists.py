from sqlalchemy.orm import Session
from app_v2.database.models import Specialists

class SpecialistsRepository:
    def __init__(self, db: Session):
        self.db = db

    def insert_many(self, records: list[dict]):
        self.db.bulk_insert_mappings(Specialists, records)
        self.db.commit()

    def select_existing_material_numbers(self) -> set:
        res = self.db.query(Specialists.material_number).all()
        return set(r[0] for r in res)

    def filter_new_materials(self, material_numbers: list[str]):
        return self.db.query(Specialists).filter(~Specialists.material_number.in_(material_numbers)).all()

    def get_all(self):
        return self.db.query(Specialists).all()
