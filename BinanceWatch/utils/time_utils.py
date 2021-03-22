import datetime


def millistamp_to_round_min(millitstamp: int):
    return millitstamp - millitstamp % 60000


def millistamp_to_upper_min(millitstamp: int):
    over_min = millitstamp % 60000
    if over_min:
        return millitstamp - over_min + 60000
    return millitstamp


def datetime_to_round_min(date_time: datetime.datetime):
    return date_time - datetime.timedelta(seconds=date_time.second, microseconds=date_time.microsecond)


def millistamp_to_datetime(millitstamp: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(millitstamp / 1000, tz=datetime.timezone.utc)


def datetime_to_millistamp(date_time: datetime.datetime) -> int:
    return int(1000 * date_time.timestamp())
