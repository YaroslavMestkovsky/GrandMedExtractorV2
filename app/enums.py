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

ANALYTICS_TO_BITRIX = {
    column.comment: column.name
    for column in [
        Analytics.__table__.columns.registration_number,
        Analytics.__table__.columns.full_name,
        Analytics.__table__.columns.appointment_date,
        Analytics.__table__.columns.department_execution,
        Analytics.__table__.columns.specialist_execution,
        Analytics.__table__.columns.total_amount,
        Analytics.__table__.columns.physician_department,
    ]
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

    SPECIALIST_EXECUTION = "Специалист/Ресурс.Выполнение"

    VAR_TO_NAME = {
        REG_NUM: "Рег.номер",
        SNAME: "Фамилия",
        FNAME: "Имя",
        LNAME: "Отчество",
        BIRTHDAY: "ДР",
        PHONE: "Телефон",
        MAIL: "Электронная почта",
        CREATION: "Дата создания",

        SPECIALIST_EXECUTION: "Специалист/Ресурс.Выполнение",
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

        VAR_TO_NAME[SPECIALIST_EXECUTION]: "UF_CRM_1641810471884",
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

        SPECIALIST_EXECUTION: NAME_TO_FIELD[VAR_TO_NAME[SPECIALIST_EXECUTION]],
    }

    FIELD_TO_FIELD = {
        Analytics.__table__.columns.specialist_execution.name: "UF_CRM_1641810471884",
        Analytics.__table__.columns.registration_number.name: "UF_CRM_1744898975",
        Analytics.__table__.columns.appointment_date.name: "UF_CRM_673DEA05D361C",
        Analytics.__table__.columns.total_amount.name: "OPPORTUNITY_WITH_CURRENCY",
    }
