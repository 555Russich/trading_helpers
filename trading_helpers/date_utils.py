from datetime import datetime

from holidays import country_holidays

RU_HOLIDAYS = country_holidays('RU', years=([x for x in range(1970, datetime.now().year + 5)]))
RU_HOLIDAYS = [
    dt for dt in RU_HOLIDAYS
    if not (dt.month == 1 and dt.day in [3, 4, 5, 6, 7, 8]) and
    dt not in [datetime(2023, 5, 8).date()]
]
