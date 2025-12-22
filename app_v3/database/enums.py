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

ANALYTICS_TO_BITRIX = {
    column.comment: column.name
    for column in [
        Analytics.__table__.columns.registration_number,
        Analytics.__table__.columns.full_name,
        Analytics.__table__.columns.appointment_date,
        Analytics.__table__.columns.department_execution,
        Analytics.__table__.columns.specialist_execution,
        Analytics.__table__.columns.total_amount,
    ]
}


class BitrixEnum:
    REG_NUM = "UF_CRM_1744898975"
    SNAME = "UF_CRM_64B7A1FD1D5BB"
    FNAME = "UF_CRM_NAME"
    LNAME = "UF_CRM_64B7A1FEF374B"
    BIRTHDAY = "UF_CRM_1671203152488"
    PHONE = "UF_CRM_66582450DADD8"
    MAIL = "UF_CRM_1744898823"
    CREATION = "DATE_CREATE"

    CONTACT_REG_NUM = "UF_CRM_1744899027"
    
    NAME_TO_FIELD = {
        "Рег.номер": "UF_CRM_1744898975",
        "Фамилия": "UF_CRM_64B7A1FD1D5BB",
        "Имя": "UF_CRM_NAME",
        "Отчество": "UF_CRM_64B7A1FEF374B",
        "ДР": "UF_CRM_1671203152488",
        "Телефон": "UF_CRM_66582450DADD8",
        "Электронная почта": "UF_CRM_1744898823",
        "Дата создания": "DATE_CREATE",
    }
