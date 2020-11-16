import datetime


def get_valid_date_dict():
    month = range(1, 12)
    days = range(1, 32)

    valid_date_dict = {}

    for m in month:
        month_days = []
        for d in days:
            try:
                start_date = datetime.datetime(2020, m, d, 0, 0)
                end_of_date = datetime.datetime(2020, m, d, 23, 59)
                # print(start_date, end_of_date)
                month_days.append([start_date, end_of_date])
            except Exception as e:
                pass
        valid_date_dict[m] = {
            "m": m,
            "month_days": month_days,
            "start_of_month": month_days[0][0],
            "end_of_month": month_days[-1][1]
        }

    return valid_date_dict
