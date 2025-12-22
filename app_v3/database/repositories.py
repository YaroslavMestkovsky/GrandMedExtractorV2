from sqlalchemy import select

from app_v3.database.models import Analytics, Specialists
from app_v3.database.session import get_session
from app_v3.utils.logger import app_logger


class BaseRepository:
    def __init__(self):
        self.session = get_session()

    def bulk_upload(self, records):
        """Массовая загрузка записей в БД"""

        try:
            total_rows = len(records)
            chunk_size = 50000
            app_logger.info(
                f"[ARep] Начало массовой загрузки {total_rows} записей (чанки по {chunk_size})",
            )

            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]

                self.session.bulk_insert_mappings(self.model, chunk)
                self.session.commit()

                print(
                    f"\r[ARep] Загрузка: {min(i + chunk_size, total_rows)}/{total_rows} записей...",
                    end="",
                    flush=True,
                )

            print()
            msg = f"Загружено записей: {total_rows}"
            # self._add_message(msg) # todo бот
            app_logger.info(f"[ARep] {msg}")
        except Exception as e:
            self.session.rollback()

            err = f"Ошибка при загрузке данных: {str(e)}"
            # self._add_error(err) # todo бот
            app_logger.error(f"[SQLManager] {err}", exc_info=True)
            raise

class AnalyticsRepository(BaseRepository):
    """Репозиторий для работы с моделью аналитик."""

    def __init__(self):
        super().__init__()

        self.model = Analytics

    def delete_records(self, _filter):
        self.session.query(Analytics).filter(_filter).delete(synchronize_session=False)


class SpecialistsRepository(BaseRepository):
    """Репозиторий для работы с моделью специалистов."""

    def __init__(self):
        super().__init__()

        self.model = Specialists

    def all_material_numbers(self):
        return self.session.execute(select(Specialists.material_number)).all()
