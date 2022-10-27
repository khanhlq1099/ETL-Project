from datetime import date, datetime, timedelta, timezone
from lib.core.constants import PERIOD_TYPE


def get_utc_timestamp(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).timestamp()


def calc_period_range(business_date: date, period_type: PERIOD_TYPE):
    from_date: date = None
    to_date: date = None

    if(period_type == PERIOD_TYPE.TODAY):
        from_date = business_date
        to_date = business_date
    elif(period_type == PERIOD_TYPE.YESTERDAY):
        from_date = business_date + timedelta(days=-1)
        to_date = from_date
    elif(period_type == PERIOD_TYPE.MTD):
        from_date = business_date.replace(day=1)
        to_date = business_date
    elif(period_type == PERIOD_TYPE.QTD):
        quarter = round((business_date.month - 1) / 3 + 1)
        from_date = datetime(business_date.year, 3 * quarter - 2, 1)
        to_date = business_date
    elif(period_type == PERIOD_TYPE.YTD):
        from_date = business_date.replace(month=1, day=1)
        to_date = business_date
    elif(period_type == PERIOD_TYPE.M):
        from_date = business_date.replace(day=1)
        # print(from_date)
        next_month_date = from_date.replace(day=28) + timedelta(days=4)
        to_date = next_month_date - timedelta(days=next_month_date.day)
    elif(period_type == PERIOD_TYPE.Q):
        quarter = round((business_date.month - 1) / 3 + 1)
        from_date = date(business_date.year, 3 * quarter - 2, 1)
        to_date = date(business_date.year, 3 *
                       quarter + 1, 1) + timedelta(days=-1)
    elif(period_type == PERIOD_TYPE.Y):
        from_date = business_date.replace(month=1, day=1)
        to_date = business_date.replace(month=12, day=31)
    elif(period_type == PERIOD_TYPE.T3DTD):
        from_date = business_date + timedelta(days=-3)
        to_date = business_date
    elif(period_type == PERIOD_TYPE.T1WTD):
        from_date = business_date + timedelta(weeks=-1)
        to_date = business_date
    elif(period_type == PERIOD_TYPE.T2WTD):
        from_date = business_date + timedelta(weeks=-2)
        to_date = business_date
    elif(period_type == PERIOD_TYPE.T4WTD):
        from_date = business_date + timedelta(weeks=-4)
        to_date = business_date
    

    return from_date, to_date


def get_first_date_of_month(business_date: date):
    return business_date.replace(day=1)


def get_last_date_of_month(business_date: date):
    next_month_date = business_date.replace(day=28) + timedelta(days=4)
    return next_month_date - timedelta(days=next_month_date.day)


def get_first_date_of_next_month(business_date: date):
    return get_last_date_of_month(business_date) + timedelta(days=1)


def get_last_date_of_next_month(business_date: date):
    next_month_date = business_date.replace(day=28) + timedelta(days=4)
    return get_last_date_of_month(next_month_date)


def get_first_date_of_previous_month(business_date: date):
    previous_month_date = get_first_date_of_month(
        business_date) + timedelta(days=-1)
    return get_first_date_of_month(previous_month_date)


def get_last_date_of_previous_month(business_date: date):
    return get_first_date_of_month(business_date) + timedelta(days=-1)


def get_n_days_timespan_periods(from_date: date, to_date: date, timespan_days: int = 7):
    periods = []

    if from_date > to_date:
        return None

    # delta = to_date - from_date
    print(f"{from_date} - {to_date}")

    start_date_of_period_i: date = from_date
    end_date_of_period_i: date = from_date

    while True:
        if start_date_of_period_i + timedelta(days=6) < to_date:
            end_date_of_period_i = start_date_of_period_i + \
                timedelta(days=timespan_days - 1)
            periods.append({
                "period_type": PERIOD_TYPE.PERIOD,
                "from_date": start_date_of_period_i,
                "to_date": end_date_of_period_i

            })
            start_date_of_period_i = end_date_of_period_i + timedelta(days=1)
        else:
            periods.append({
                "period_type": PERIOD_TYPE.PERIOD,
                "from_date": start_date_of_period_i,
                "to_date": to_date
            })
            break

    # for p in periods:
    #     print(f"From Date: {p['from_date']} - To Date: {p['to_date']}")
    return periods


def get_1_month_timespan_periods(from_date: date, to_date: date):
    periods = []

    if from_date > to_date:
        return None

    start_date_of_period_i: date = from_date
    end_date_of_period_i: date = from_date

    while True:
        if get_last_date_of_month(start_date_of_period_i) < to_date:
            end_date_of_period_i = get_last_date_of_month(
                start_date_of_period_i)

            periods.append({
                "period_type": PERIOD_TYPE.PERIOD,
                "from_date": start_date_of_period_i,
                "to_date": end_date_of_period_i
            })
            start_date_of_period_i = get_first_date_of_next_month(
                start_date_of_period_i)
        else:
            periods.append({
                "period_type": PERIOD_TYPE.PERIOD,
                "from_date": start_date_of_period_i,
                "to_date": to_date
            })
            break

    # for p in periods:
    #     print(f"From Date: {p['from_date']} - To Date: {p['to_date']}")
    return periods


def get_haft_month_timespan_periods(from_date: date, to_date: date):
    periods = []

    if from_date > to_date:
        return None

    start_date_of_period_i: date = from_date
    end_date_of_period_i: date = from_date

    while True:
        if get_last_date_of_month(start_date_of_period_i) < to_date:
            end_date_of_period_i = get_last_date_of_month(
                start_date_of_period_i)
            periods.append({
                "period_type": PERIOD_TYPE.PERIOD,
                "from_date": start_date_of_period_i,
                "to_date": start_date_of_period_i.replace(day=14)
            })
            periods.append({
                "period_type": PERIOD_TYPE.PERIOD,
                "from_date": start_date_of_period_i.replace(day=15),
                "to_date": end_date_of_period_i
            })
            start_date_of_period_i = get_first_date_of_next_month(
                start_date_of_period_i)
        else:
            if to_date.day >= 15:
                periods.append({
                    "period_type": PERIOD_TYPE.PERIOD,
                    "from_date": start_date_of_period_i,
                    "to_date": start_date_of_period_i.replace(day=14)
                })
                periods.append({
                    "period_type": PERIOD_TYPE.PERIOD,
                    "from_date": start_date_of_period_i.replace(day=15),
                    "to_date": to_date
                })
            else:
                periods.append({
                    "period_type": PERIOD_TYPE.PERIOD,
                    "from_date": start_date_of_period_i,
                    "to_date": to_date
                })

            break

    # for p in periods:
    #     print(f"From Date: {p['from_date']} - To Date: {p['to_date']}")
    return periods
