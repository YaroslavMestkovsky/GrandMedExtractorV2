import configparser

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import (
    create_engine,
    inspect,
)

from database.models import ( #todo починить, заменив все на абсолютные импорты из app
    Analytics,
    Specialists,
)


db_confing = 'app/database.conf'
config = configparser.ConfigParser()
config.read(db_confing)


Base = declarative_base()
engine = create_engine(
    f"postgresql+psycopg2://"
    f"{config.get('postgresql', 'user')}:{config.get('postgresql', 'password')}@"
    f"{config.get('postgresql', 'host')}:{config.get('postgresql', 'port')}/"
    f"{config.get('postgresql', 'dbname')}"
)


def check_db():
    """Проверка наличия в БД таблиц."""

    inspector = inspect(engine)
    analytics_exists = inspector.has_table(Analytics.__tablename__)
    specialists_exists = inspector.has_table(Specialists.__tablename__)

    if not analytics_exists:
        Base.metadata.create_all(engine, tables=[Analytics.__table__])

    if not specialists_exists:
        Base.metadata.create_all(engine, tables=[Specialists.__table__])


def get_session():
    """Создаем сессию для работы с базой данных."""
    return sessionmaker(bind=engine)()
