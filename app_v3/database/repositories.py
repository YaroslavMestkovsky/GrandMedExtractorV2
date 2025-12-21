from app_v3.database.models import Analytics, Specialists
from app_v3.database.session import get_session


class BaseRepository:
    def __init__(self):
        self.session = get_session()


class AnalyticsRepository(BaseRepository):
    """Репозиторий для работы с моделью аналитик."""

    def __init__(self):
        super().__init__()

        self.model = Analytics


class SpecialistsRepository(BaseRepository):
    """Репозиторий для работы с моделью специалистов."""

    def __init__(self):
        super().__init__()

        self.model = Specialists
