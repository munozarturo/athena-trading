from dateutil.parser._parser import parse as parse_timestr
from datetime import date, datetime, time, timedelta
import datetime as dt
from time import strftime
from numpy import isin
import pytz
import pandas_market_calendars as mcal
from typing import Iterable, Iterator, Type
import re
from pandas.core.tools.times import to_time
import dateutil.tz as tz
from dateutil.zoneinfo import tzfile

from _utils.val import val_instance

def is_dst(dt: datetime = None, timezone: str = "UTC") -> bool:
    """
    Returns true if it is daylight savings time, else it returns false.

    Args:
        dt (datetime, optional): _description_. Defaults to None.
        timezone (str, optional): _description_. Defaults to "UTC".

    Returns:
        bool: True of it is daylight savings time, false if it is not.
    """

    if dt is None:
        dt = datetime.utcnow()

    timezone = pytz.timezone(timezone)
    timezone_aware_date = timezone.localize(dt, is_dst=None)

    return timezone_aware_date.tzinfo._dst.seconds != 0


def get_city_time(city: str) -> datetime:
    """
    Returns a datetime object for the time in a given city. Daylight savings time is accounted for.

    Args:
        city (str): City name.

    Returns:
        datetime: time and date in city.
    """

    _tz = get_city_timezone(city)
    time_now = datetime.now(_tz)

    return time_now


def market_open(_time: datetime | date, market: str) -> bool:
    """
    Returns true of the market is open on the date and time provided.

    Args:
        _time (datetime | date): A datetime object containing the specific time.
        market (str): The abbreviation of the market. eg. "NYSE", "BMV", "MCX".

    Returns:
        bool: True if the market is open on that day, false otherwise.
    """

    if isinstance(_time, datetime):
        _date = datetime.combine(_time.date(), dt.time(
            hour=0, minute=0, second=0), tzinfo=_time.tzinfo)
    elif isinstance(_time, date):
        _date = datetime.combine(_time, dt.time(
            hour=0, minute=0, second=0), tzinfo=_time.tzinfo)
    else:
        raise TypeError(
            f"Expected datetime or date object for _time. Received {type(_time)}.")

    market = mcal.get_calendar(market)
    valid_days = market.valid_days(start_date=_date - timedelta(days=1),
                                   end_date=_date + timedelta(days=1),
                                   tz=_time.tzinfo)

    if _date in valid_days:
        return True
    else:
        return False


class InvalidQueryException(Exception):
    """Base class for invalid city queries."""
    pass

def get_timezone(query: str) -> datetime.tzinfo:
    """
    Returns a timezone object from the query.

    Args:
        query (str): Query. eg. Locale, city name, area. eg. "America/New_York", "UTC", "UTC-4", "Pacific"...

    Returns:
        datetime.tzinfo: Time zone information of the query.
    """
    val_instance(query, str)
    
    _tz = tz.gettz(query)
    
    if _tz is None:
        raise InvalidQueryException(
            f"Couldn't find a timezone for '{query}'.")
    
    return _tz
    
timezones = ', '.join([item for sublist in [cities for country_code, cities in pytz.country_timezones.items()] for item in sublist])

def get_city_timezone(query: str) -> datetime.tzinfo:
    """
    Returns timezone object for timezone in query city.

    Args:
        query (str): City name, locale, area. eg. "America/New_York", "Zurich", "Bogota"...

    Returns:
        datetime.tzinfo: Time zone info of the given city.
    """
    val_instance(query, str)

    for country, cities in pytz.country_timezones.items():
        for city in cities:
            if query in city:
                return pytz.timezone(city)
    
    raise InvalidQueryException(
        f"Couldn't find a timezone for '{query}'. A list of valid timezones can be found in 'pytz.country_timezones' or at https://www.timeanddate.com/time/map/.")


def parse_time(_time: str, format: str = None, infer_time_format: bool = False, errors: str = "raise") -> time:
    """
    Parse time strings to time objects using fixed strptime formats ("%H:%M",
    "%H%M", "%I:%M%p", "%I%M%p", "%H:%M:%S", "%H%M%S", "%I:%M:%S%p",
    "%I%M%S%p")

    Use infer_time_format if all the strings are in the same format to speed
    up conversion.

    Parameters
    ----------
    arg : string in time format, datetime.time, list, tuple, 1-d array,  Series
    format : str, default None
        Format used to convert arg into a time object.  If None, fixed formats
        are used.
    infer_time_format: bool, default False
        Infer the time format based on the first non-NaN element.  If all
        strings are in the same format, this will speed up conversion.
    errors : {'ignore', 'raise', 'coerce'}, default 'raise'
        - If 'raise', then invalid parsing will raise an exception
        - If 'coerce', then invalid parsing will be set as None
        - If 'ignore', then invalid parsing will return the input

    Returns
    -------
    datetime.time
    """
    
    return to_time(_time, format, infer_time_format, errors)


def time_in_range(_time: datetime | time | float | str, _from: time | str = None, _to: time | str = None) -> bool:
    """
    Returns True if the time is between _from and _to.

    Args:
        _time (datetime | time | float | str): The time to be checked.
        _from (time | str, optional): The from time. If not passed it is only checked if _time is before _to.
        _to (time | str, optional): The to time. If not passed it is only checked if _time is after _from.

        _time, _from, and _to: can be in str format: 'HH:MM:SS'.

    Returns:
        bool: True if time is between passed parameters, False otherwise.
    """

    if isinstance(_time, datetime):
        __time = _time.time()
    elif isinstance(_time, time):
        __time = _time
    elif isinstance(_time, float):
        __time = datetime.fromtimestamp(_time).time()
    elif isinstance(_time, str):
        __time = parse_time(_time)
    else:
        raise TypeError(
            f"Expect datetime, time, or float object for '_time', got {type(_time)}.")

    if not _from is None:
        if isinstance(_from, time):
            _from = _from
        elif isinstance(_from, str):
            _from = parse_time(_from)
        else:
            raise TypeError(
                f"Expect time or str object for '_from', got {type(_from)}.")

    if not _to is None:
        if isinstance(_to, time):
            _to = _to
        elif isinstance(_to, str):
            _to = parse_time(_to)
        else:
            raise TypeError(
                f"Expect time or str object for '_to', got {type(_to)}.")

    if _from is None and _to is None:
        raise ValueError("'_from' and '_to' can't both be None. If '_from' is None method checks if the time is after '_from'. If '_to' is None method checks if time is before '_to'. But only one or the other can be None.")

    if _from is None:
        if __time <= _to:
            in_range = True
        else:
            in_range = False
    elif _to is None:
        if __time >= _from:
            in_range = True
        else:
            in_range = False
    else:
        if _to < _from:
            in_range = __time >= _to and __time <= _from
        else:
            in_range = __time <= _to and __time >= _from

    return in_range


def date_in_range(_date: datetime | float, _from: datetime | float = None, _to: datetime | float = None) -> bool:
    """
    Returns True if the date and time are between _from and _to.

    Args:
        _date (datetime | float): The date to be checked.
        _from (datetime | float, optional): The from date and time. If not passed it is only checked if date and time are before _to.
        _to (datetime | float, optional): The to date and time. If not passed it is only checked if date and time are after _from.

    Returns:
        bool: True if the datetime is between passed parameters, False otherwise.
    """

    if isinstance(_date, datetime):
        _date = _date
    elif isinstance(_date, float):
        _date = datetime.fromtimestamp(_date).time()
    else:
        raise TypeError(
            f"Expect datetime, or float object for '_time', got {type(_date)}.")

    if isinstance(_from, datetime):
        _from = _from
    elif isinstance(_from, float):
        _from = datetime.fromtimestamp(_from).time()
    elif not _from is None:
        raise TypeError(
            f"Expect datetime object for '_from', got {type(_from)}.")

    if isinstance(_to, datetime):
        _to = _to
    elif isinstance(_to, float):
        _to = datetime.fromtimestamp(_to).time()
    elif not _from is None:
        raise TypeError(f"Expect datetime object for '_to', got {type(_to)}.")

    if _from is None and _to is None:
        raise ValueError("'_from' and '_to' can't both be None. If '_from' is None method checks if the date is after '_from'. If _to is None method checks if date is before '_to'. But only one or the other can be None.")

    if _from is None:
        if _date <= _to:
            in_range = True
        else:
            in_range = False
    elif _to is None:
        if _date >= _from:
            in_range = True
        else:
            in_range = False
    else:
        if _to < _from:
            in_range = _date >= _to and _date <= _from
        else:
            in_range = _date <= _to and _date >= _from

    return in_range


class TimeRange:
    def __init__(self, start: dt.datetime | dt.time | str = None, end: dt.datetime | dt.time | str = None) -> None:
        self.start = None
        self.end = None

        self.adjust(start=start, end=end)

    def adjust(self, start: dt.datetime | dt.time | str = None, end: dt.datetime | dt.time | str = None) -> None:
        val_instance(start, (dt.datetime, dt.time, str))
        val_instance(end, (dt.datetime, dt.time, str))
        
        if not start is None:
            if isinstance(start, dt.datetime):
                start = start.time()
            elif isinstance(start, dt.time):
                start = start
            elif isinstance(start, str):
                start = parse_time(start)

            self.start = start
        else:
            self.start = dt.time(0, 0, 0)

        if not end is None:
            if isinstance(end, dt.datetime):
                end = end.time()
            elif isinstance(end, dt.time):
                end = end
            elif isinstance(end, str):
                end = parse_time(end) 

            self.end = end
        else:
            self.end = dt.time(23, 59, 59)

    def __contains__(self, _time: time) -> bool:
        if self.start is None and self.end is None:
            raise ValueError(f"Both start and end can't be None.")

        return time_in_range(_time, self.start, self.end)
    
    def __str__(self) -> str:
        return f"{self.start.strftime('%H:%M:%S')} - {self.end.strftime('%H:%M:%S')}"