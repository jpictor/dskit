import time
import pytz
import datetime
import isodate


UNIX_EPOCH = datetime.datetime(1970, 1, 1, 0, 0)
UNIX_EPOCH_UTC = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc)
BENCHMARK_ISO_TIME = datetime.datetime(2013, 1, 1, 1, 1)


def datetime_from_isodate(iso_date_str, time_zone=None):
    """
    Given an ISO date string return a Python datetime instance.
    """
    if time_zone:
        tzinfo = pytz.timezone(time_zone)
    else:
        tzinfo = pytz.utc
    dt = isodate.parse_datetime(iso_date_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.utc)
    final_dt = dt.astimezone(tzinfo)
    return final_dt


def timestamp_from_isodate(iso_date_str):
    """
    Converts an ISO date string in UTC format to a unix time stamp (number of seconds since UTC Jan 1, 1970).
    2015-02-20T23:47:40.310Z
    """
    if not iso_date_str:
        return None
    dt = isodate.parse_datetime(iso_date_str)
    return utctimestamp_from_datetime(dt)


def datetime_from_timestamp(timestamp):
    """
    Converts an integer timesamp (seconds since UTC Jan 1, 1970) to a Python datetime instance.
    """
    return datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.utc)
