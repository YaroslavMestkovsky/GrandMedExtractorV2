from app_v3.database.models import Analytics, Specialists


SPECIALISTS_FIELDS = {
    column.comment: column.name
    for column in Specialists.__table__.columns
    if column.comment is not None
}

ANALYTICS_FIELDS = {
    column.comment: column.name
    for column in Analytics.__table__.columns
    if column.comment is not None
}


class BitrixEnum:
    REG_NUM = "Рег.номер"
    SNAME = "Фамилия"
    FNAME = "Имя"
    LNAME = "Отчество"
    BIRTHDAY = "ДР"
    PHONE = "Телефон"
    MAIL = "Электронная почта"
    CREATION = "Дата создания"

    USERS_COLUMNS = [
        "Рег.номер",
        "Фамилия",
        "Имя",
        "Отчество",
        "ДР",
        "Телефон",
        "Электронная почта",
        "Дата создания",
    ]
