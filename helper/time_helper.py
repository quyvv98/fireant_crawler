import datetime
import constant


def next_time(current_time_str, time_format, /, hours=0, minutes=0, days=0, res=None, distance=0):
    current_time = datetime.datetime.strptime(current_time_str, time_format)
    hours_added = datetime.timedelta(hours=hours)
    minutes_added = datetime.timedelta(minutes=minutes)
    days_added = datetime.timedelta(days=days)
    if res is not None:
        if res == "1D":
            days_added = datetime.timedelta(days=distance)
        elif res == "1":
            minutes_added = datetime.timedelta(minutes=distance)
    return datetime.datetime.strftime(current_time + hours_added + minutes_added + days_added,
                                      time_format)


def convert_utc_time(utc_time_str):
    try:
        current_time = datetime.datetime.strptime(utc_time_str, constant.DATE_TIME_FORMAT_UTC)
    except:
        current_time = datetime.datetime.strptime(utc_time_str, constant.DATE_TIME_FORMAT_UTC2)
    time_added = datetime.timedelta(hours=7)
    return current_time + time_added, datetime.datetime.strftime(current_time + time_added, constant.DATE_TIME_FORMAT)


def format_utc_time(utc_time_str):
    try:
        current_time = datetime.datetime.strptime(utc_time_str, constant.DATE_TIME_FORMAT_UTC)
    except:
        current_time = datetime.datetime.strptime(utc_time_str, constant.DATE_TIME_FORMAT_UTC2)
    return current_time, datetime.datetime.strftime(current_time, constant.DATE_TIME_FORMAT)


def get_current_time_str(time_format):
    return datetime.datetime.strftime(datetime.datetime.now(), time_format)


def get_prev_year_time_str():
    now = datetime.datetime.now()
    prev_year = now.year - 1
    prev_year_time = now.replace(year=prev_year)
    return datetime.datetime.strftime(prev_year_time, constant.DATE_FORMAT)


def is_off_day(time_str):
    time = datetime.datetime.strptime(time_str, constant.DATE_TIME_FORMAT)
    day = datetime.datetime.strftime(time, "%w")
    if day == '6' or day == '0':
        return True
    return False


get_prev_year_time_str()


def is_over_close_time(date_time_str):
    date_time = datetime.datetime.strptime(date_time_str, constant.DATE_TIME_FORMAT)
    time_str = datetime.datetime.strftime(date_time, constant.TIME_FORMAT)
    return time_str >= constant.TIME_CLOSE


def next_exchange_day(date_time_str):
    date_time = datetime.datetime.strptime(date_time_str, constant.DATE_TIME_FORMAT)
    time_added = datetime.timedelta(days=1)
    date_time += time_added

    time_open = datetime.datetime.strptime(constant.TIME_OPEN, constant.TIME_FORMAT)
    # TIME_CLOSE
    date_time = date_time.replace(hour=time_open.hour, minute=time_open.minute, second=0)
    next_day = datetime.datetime.strftime(date_time, constant.DATE_TIME_FORMAT)
    if is_off_day(next_day):
        return next_exchange_day(next_day)

    return next_day


def get_date_of_date_time(date_time_str):
    date = datetime.datetime.strptime(date_time_str, constant.DATE_TIME_FORMAT)
    date = date.replace(hour=0)
    return datetime.datetime.strftime(date, constant.DATE_TIME_FORMAT)

# is_off_day("2020-09-25 15:09:09")
# is_off_day("2020-09-26 15:09:09")
# is_off_day("2020-09-27 15:09:09")
