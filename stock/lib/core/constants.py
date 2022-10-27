from enum import Enum


class PERIOD_TYPE(Enum):
    PERIOD = "PERIOD"
    YTD = "YTD"
    QTD = "QTD"
    MTD = "MTD"
    TODAY = "TODAY"
    YESTERDAY = "YESTERDAY"
    Y = "Y"
    Q = "Q"
    M = "M"
    T3DTD = "T3DTD"
    T1WTD = "T1WTD"
    T2WTD = "T2WTD"
    T4WTD = "T4WTD"
    T8WTD = "T8WTD"


class DATA_DESTINATION_TYPE(Enum):
    LOCAL_STORAGE = "LOCAL_STORAGE"
    SHAREPOINT = "SHAREPOINT"
    SQL_SERVER = "SQL_SERVER"
