from app.database.db_manager import check_db
from app.tools.tools import upload


if __name__ == '__main__':
    check_db()
    upload()
