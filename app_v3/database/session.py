from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app_v3.utils.config import app_config
from app_v3.database.models import Base


def get_engine():
    conf = app_config.database['postgresql']

    url = (
        f"postgresql+psycopg2://{conf['user']}:{conf['password']}@"
        f"{conf['host']}:{conf['port']}/{conf['dbname']}"
    )

    engine = create_engine(
        url,
        pool_size=conf.get('pool_size', 5),
        max_overflow=conf.get('max_overflow', 10),
        pool_timeout=conf.get('pool_timeout', 30),
        pool_recycle=conf.get('pool_recycle', 1800),
        echo=bool(conf.get('echo', False)),
        future=True,
    )

    return engine

_engine = get_engine()
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, expire_on_commit=False)

def get_session():
    return SessionLocal()

def init_db():
    Base.metadata.create_all(_engine)
