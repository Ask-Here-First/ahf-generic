import re, math
from datetime import timezone, timedelta
from typing import Literal, Mapping, overload

from frid.typing import FridNameArgs

from .typing import FridMixin, dateonly, timeonly, datetime, DateTypes
from .number import Quantity

date_only_re_str = r"(\d\d\d\d)-([01]\d)-([0-3]\d)"
time_zone_re_str = r"[+-](\d\d)(?::?(\d\d))|Z"
time_only_re_str = r"([012]\d):([0-5]\d)(?::([0-6]\d)(?:.(\d+))?)?(" + time_zone_re_str + ")?"
time_curt_re_str = r"([012]\d):?([0-5]\d)(?::?([0-6]\d)(?:.(\d+))?)?(" + time_zone_re_str + ")?"
date_time_regexp = re.compile(date_only_re_str + r"\s*[Tt_ ]\s*" + time_curt_re_str)
date_only_regexp = re.compile(date_only_re_str)
time_only_regexp = re.compile(time_only_re_str)
time_curt_regexp = re.compile(time_curt_re_str)

def parse_timeonly(s: str, m: re.Match|None=None) -> timeonly|None:
    """Parse ISO time string, where the colon between hour and second are time is optional.
    - Returns the Python time object or None if it fails to parse.
    Since we support Python 3.10, the new feature in 3.11 may not be available.
    """
    if m is None:
        m = time_curt_regexp.fullmatch(s)
        if m is None:
            return None
    fs_str = m.group(4)   # Fractional second
    if fs_str is not None:
        if len(fs_str) > 6:
            fs_str = fs_str[:6]
        micros = int(fs_str)
        if len(fs_str) < 6:
            micros *= 10 ** (6 - len(fs_str))
    else:
        micros = 0
    tz_str = m.group(5)  # Whole timezone string
    if not tz_str:
        tzinfo = None
    elif tz_str == 'Z':
        tzinfo = timezone.utc
    else:
        tdelta = timedelta(hours=int(m.group(6)), minutes=int(m.group(7) or 0))
        tzinfo = timezone(-tdelta if tz_str[0] == '-' else tdelta)
    return timeonly(int(m.group(1)), int(m.group(2)), int(m.group(3) or 0),
                    micros, tzinfo=tzinfo)

def parse_datetime(s: str) -> DateTypes|None:
    """Parses a date or time or date with time in extended ISO format.
    - Returns the Python datetime/date/time object, or None if it fails to parse.
    """
    if s.startswith('0T') or s.startswith('0t'):
        s = s[2:]
        if m := time_curt_regexp.match(s):
            return parse_timeonly(s, m)
        return None
    if date_time_regexp.fullmatch(s):
        (d_str, _, t_str) = s.partition('T')
        t_val = parse_timeonly(t_str)
        assert t_val is not None
        return datetime.combine(dateonly.fromisoformat(d_str), t_val)
    if date_only_regexp.fullmatch(s):
        return dateonly.fromisoformat(s)
    if m := time_only_regexp.fullmatch(s):
        return parse_timeonly(s, m)
    return None

def strfr_timeonly(time: timeonly, /, precision: int=3,
                   *, prefix: str="0T", colon: bool=False) -> str:
    """Convert to the ISO format just without the colons.
    - `prec` is the number of digits for subseconds; `< 0` if only up to minutes.
    - If the timezone is utc, use `Z` instead of `+0000`.
    """
    if precision < 0:
        if precision == -2:
            out = time.strftime("%H")
        elif precision == -1:
            out = time.strftime("%H:%M" if colon else "%H%M")
        else:
            raise ValueError("Invalid precision: " + str(precision)
                             + "; it must be either >=0 or -1 (to minutes) -2 (to hours)")
    else:
        out = time.strftime("%H:%M:%S" if colon else "%H%M%S")
        if precision > 0:
            micro = str(time.microsecond)
            if len(micro) < 6:
                micro.zfill(6 - len(micro))
            if precision < len(micro):
                micro = micro[0:precision]
            elif precision > len(micro):
                micro = micro.ljust(precision, '0')
            out += '.' + micro
    if prefix:
        out = prefix + out
    tz = time.tzinfo
    if tz is None:
        return out
    if tz is timezone.utc:
        return out + 'Z'
    return out + time.strftime("%z")

def strfr_datetime(data: DateTypes|float, /, precision: int=3, colon: bool=False) -> str:
    """Show date/time/datetime format only without colons."""
    if isinstance(data, int|float):
        data = datetime.fromtimestamp(data, timezone.utc)
    if isinstance(data, datetime):
        return data.date().isoformat() + strfr_timeonly(
            data.timetz(), precision, prefix='T', colon=colon
        )
    if isinstance(data, dateonly):
        return data.isoformat()
    if isinstance(data, timeonly):
        return strfr_timeonly(data, precision, colon=colon)
    raise ValueError(f"INvalid date/time type {type(data)}")

def timeonly_to_seconds(time: timeonly) -> float:
    """Converts a time object to the number of seconds since the midnight.
    - The midnight is the local time of the same timezone; hence timezone
      is irrelant.
    """
    out = (time.hour * 60 + time.minute) * 60 + time.second
    if time.microsecond == 0:
        return out
    return out + (time.microsecond / 1E6)

def seconds_to_timeonly(sec: float, tzinfo: timezone|None=None) -> tuple[timeonly,int]:
    """Converts the number of seconds since the midnight to a time object.
    - `tzinfo` is the timezone to construct the time object.
    - Returns a pair of the time object and the offset in terms the number of days.
    """
    (sec, f) = math.modf(sec)
    msec = int(f * 1E6)
    (minute, second) = divmod(int(sec), 60)
    (hour, minute) = divmod(minute, 60)
    (days, hour) = divmod(hour, 24)
    return (timeonly(hour, minute, second, msec, tzinfo=tzinfo), days)

class DateTimeDiff(Quantity):
    """A quatituy that represents the datetime difference."""

    DAYS_PER_YEAR = 365.2425
    DAYS_PER_MONTH = DAYS_PER_YEAR / 12.0
    SECONDS_PER_DAY = 3600 * 24
    TIME_DELTA_NAMES = {
        'w': 'weeks',
        'd': 'days',
        'h': 'hours',
        'm': 'minutes',
        's': 'seconds',
    }

    def __init__(self, s: str|float, /):
        super().__init__(s, {
            'yr': ['year', 'years', 'yrs'],
            'mo': ['month', 'months', 'mos'],
            'w': ['week', 'weeks', 'wk', 'wks'],
            'd': ['day', 'days'],
            'h': ['hour', 'hours', 'hr', 'hrs'],
            'm': ['minute', 'minutes', 'min', 'mins'],
            's': ['second', 'seconds', 'sec', 'secs'],
        })
    @classmethod
    def to_timedelta(cls, data: Mapping[str,float]) -> tuple[timedelta,int]:
        """Converts the datetime difference to timedelta.
        - Returns a pair:
            + A timedelta includes the fractional parts of year and month,
              and everything else, and
            + The number of month offset using the integral parts of year and month.
        Note: timedelta does not support year and month because they are
        not evenly sized. The purpose here is if user specify an integers
        values of year and/or month if will change year and/or month but
        keep the same day of the month.
        """
        args = {}
        for u, n in cls.TIME_DELTA_NAMES.items():
            if u == 's' or 'u' not in data:
                continue
            args[n] = data[u]
        days = data.get('d', 0)
        year = data.get('yr', 0)
        if isinstance(year, float):
            i = int(year)  # Round towards zero
            days += (year - i) * cls.DAYS_PER_YEAR
            year = i
        month = data.get('month', 0)
        if isinstance(month, float):
            i = int(month) # Round towards zero
            days += (month - i) * cls.DAYS_PER_MONTH
            month = i
        return (timedelta(days=days, **args), year * 12 + month)
    @overload
    def _add_months(self, base: datetime, months: int) -> datetime: ...
    @overload
    def _add_months(self, base: dateonly, months: int) -> dateonly: ...
    def _add_months(self, base: dateonly|datetime, months: int):
        """Adds the given number of years and months to the given date"""
        if not months:
            return base
        month = base.month + months
        (y, month) = divmod(month, 12)
        year = base.year + y
        # Doing the following to avoid the problem that the day is not allowed for the month
        return dateonly(year, month, 1) + timedelta(days=(base.day - 1))

    def add_to_timeonly(self, base_time: timeonly) -> tuple[timeonly,int]:
        """Add the date time diff to the time given by Python time.
        - Returns a tuple: the new time and the offset in terms number of days.
        """
        (delta, months) = self.to_timedelta(self.value())
        (time, days) = seconds_to_timeonly(
            timeonly_to_seconds(base_time) + delta.total_seconds()
        )
        # The offset of months will be truncated into days to avoid changing the time
        return (time, days + int(months * self.DAYS_PER_MONTH))
    def add_to_dateonly(self, base_date: dateonly) -> dateonly:
        """Add the date time diff to the date given by Python date."""
        (delta, months) = self.to_timedelta(self.value())
        oridinal = self._add_months(base_date, months).toordinal()
        oridinal += int(delta.total_seconds() / self.SECONDS_PER_DAY)  # Round towards zero
        return dateonly.fromordinal(oridinal)
    def add_to_datetime(self, date_time: datetime) -> datetime:
        """Add the date time diff to a Python datetime."""
        (delta, months) = self.to_timedelta(self.value())
        return self._add_months(date_time, months) + delta

class DateTimeSpec(FridMixin):
    def __init__(
            self, delta: str|float|None=None,
            /, *, year: int|None=None, month: int|None=None, day: int|None=None,
            hour: int|None=None, minute: int|None=None, second: int|None=None,
            microsecond: int|None=None, weekday: int|None=None, wd_dir: int=0,
    ):
        self.delta = DateTimeDiff(delta) if delta is not None else None
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second
        self.microsecond = microsecond
        self.weekday = weekday
        self.wd_dir = wd_dir
    def frid_repr(self) -> FridNameArgs:
        return FridNameArgs(self.__class__.__name__, [self.delta], {
            'year': self.year, 'month': self.month, 'day': self.day,
            'hour': self.hour, 'minute': self.minute, 'second': self.second,
            'microsecond': self.microsecond, 'weekday': self.weekday, 'wd_dir': self.wd_dir,
        })
    def _get_carry_by_dir(self, value: int, base: int, dir: Literal[0,1,-1]) -> Literal[0,1,-1]:
        if dir > 0:
            if value < base:
                return 1
        elif dir < 0:
            if value > base:
                return -1
        return 0
    def _replace_timeonly(self, base_time: timeonly,
                          time_dir: Literal[0,1,-1]=0) -> tuple[timeonly,Literal[0,1,-1]]:
        hour = base_time.hour if self.hour is None else self.hour
        minute = base_time.minute if self.minute is None else self.minute
        second = base_time.second if self.second is None else self.second
        msec = base_time.microsecond if self.microsecond is None else self.microsecond
        carry = 0
        if time_dir:
            if self.hour is not None:
                carry = self._get_carry_by_dir(hour, base_time.hour, time_dir)
            elif self.minute is not None:
                hour += self._get_carry_by_dir(minute, base_time.minute, time_dir)
            elif self.second is not None:
                minute += self._get_carry_by_dir(second, base_time.second, time_dir)
            elif self.microsecond is not None:
                second += self._get_carry_by_dir(msec, base_time.microsecond, time_dir)
        return (timeonly(hour, minute, second, tzinfo=base_time.tzinfo), carry)
    def _replace_dateonly(self, base_date: dateonly, date_dir: Literal[0,1,-1]=0) -> dateonly:
        year = base_date.year if self.year is None else self.year
        month = base_date.month if self.month is None else self.month
        day = base_date.day if self.day is None else self.day
        if date_dir and self.year is None:
            if self.month is not None:
                year += self._get_carry_by_dir(month, base_date.month, date_dir)
            elif self.day is not None:
                month += self._get_carry_by_dir(day, base_date.day, date_dir)
        return dateonly(year, month, day)
    def _replace_datetime(self, date_time: datetime, dt_dir: Literal[0,1,-1]) -> datetime:
        tm_dir = dt_dir if self.year is None and self.month is None and self.day is None else 0
        (time, carry) = self._replace_timeonly(date_time.time(), tm_dir)
        date = self._replace_dateonly(date_time.date(), dt_dir)
        if carry != 0:
            date += timedelta(days=carry)
        return datetime.combine(date, time)
    def _find_rel_weekday(self, base_date: dateonly):
        if self.weekday is None:
            return base_date
        days = self.weekday - base_date.weekday()  # 0 is Monday and 6 is Sunday
        if self.wd_dir > 0:
            days = 7 * (self.wd_dir - int(days >= 0))
        elif self.wd_dir < 0:
            days = 7 * (self.wd_dir + int(days <= 0))
        return base_date + timedelta(days=days)
    def add_to_timeonly(self, time: timeonly, dt_dir: Literal[0,1,-1]) -> tuple[timeonly,int]:
        (time, carry1) = self._replace_timeonly(time)
        if self.delta is None:
            return (time, carry1)
        (time, carry2) = self.delta.add_to_timeonly(time)
        return (time, carry1 + carry2)
    def add_to_dateonly(self, base_date: dateonly, dt_dir: Literal[0,1,-1]) -> dateonly:
        date = self._replace_dateonly(base_date, dt_dir)
        if self.delta is None:
            return date
        date = self.delta.add_to_dateonly(date)
        return self._find_rel_weekday(date)
    def add_to_datetime(self, date_time: datetime, dt_dir: Literal[0,1,-1]) -> datetime:
        out = self._replace_datetime(date_time, dt_dir)
        if self.delta is None:
            return out
        out = self.delta.add_to_datetime(out)
        date = out.date()
        new_date = self._find_rel_weekday(date)
        if new_date == date:
            return out
        return datetime.combine(date, out.time())
