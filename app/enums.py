from database.models import Specialists, Analytics


SPECIALISTS = {
    column.comment: column.name
    for column in Specialists.__table__.columns
    if column.comment is not None
}

ANALYTICS = {
    column.comment: column.name
    for column in Analytics.__table__.columns
    if column.comment is not None
}


class BitrixDealsEnum:
    REG_NUM = "REG_NUM"
    SNAME = "SNAME"
    FNAME = "FNAME"
    LNAME = "LNAME"
    BIRTHDAY = "BIRTHDAY"
    PHONE = "PHONE"
    MAIL = "MAIL"
    CREATION = "DATE_CREATE"

    VAR_TO_NAME = {
        REG_NUM: "Рег.номер",
        SNAME: "Фамилия",
        FNAME: "Имя",
        LNAME: "Отчество",
        BIRTHDAY: "ДР",
        PHONE: "Телефон",
        MAIL: "Электронная почта",
        CREATION: "Дата создания",
    }

    NAME_TO_FIELD = {
        VAR_TO_NAME[REG_NUM]: "UF_CRM_1744898975",
        VAR_TO_NAME[SNAME]: "UF_CRM_64B7A1FD1D5BB",
        VAR_TO_NAME[FNAME]: "UF_CRM_NAME",
        VAR_TO_NAME[LNAME]: "UF_CRM_64B7A1FEF374B",
        VAR_TO_NAME[BIRTHDAY]: "UF_CRM_1671203152488",
        VAR_TO_NAME[PHONE]: "UF_CRM_66582450DADD8",
        VAR_TO_NAME[MAIL]: "UF_CRM_1744898823",
        VAR_TO_NAME[CREATION]: "DATE_CREATE",
    }

    VAR_TO_FIELD = {
        REG_NUM: NAME_TO_FIELD[VAR_TO_NAME[REG_NUM]],
        SNAME: NAME_TO_FIELD[VAR_TO_NAME[SNAME]],
        FNAME: NAME_TO_FIELD[VAR_TO_NAME[FNAME]],
        LNAME: NAME_TO_FIELD[VAR_TO_NAME[LNAME]],
        BIRTHDAY: NAME_TO_FIELD[VAR_TO_NAME[BIRTHDAY]],
        PHONE: NAME_TO_FIELD[VAR_TO_NAME[PHONE]],
        MAIL: NAME_TO_FIELD[VAR_TO_NAME[MAIL]],
        CREATION: NAME_TO_FIELD[VAR_TO_NAME[CREATION]],
    }
