import re, math
from datetime import timezone, timedelta, tzinfo
from typing import Literal, Mapping, overload

from .typing import FridNameArgs, FridMixin, dateonly, timeonly, datetime, DateTypes
from .lib import Quantity, str_find_any, int_to_str, str_to_int

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
        index = str_find_any(s, "Tt_ ")
        assert index >= 0
        d_str = s[:index].rstrip()
        t_str = s[(index+1):].lstrip()
        t_val = parse_timeonly(t_str)
        assert t_val is not None, f"{t_str=}"
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

def seconds_to_timeonly(sec: float, tzinfo: tzinfo|None=None) -> tuple[timeonly,int]:
    """Converts the number of seconds since the midnight to a time object.
    - `tzinfo` is the timezone to construct the time object.
    - Returns a pair of the time object and the offset in terms the number of days.
    """
    (frec, sec) = math.modf(sec)
    msec = int(frec * 1E6)
    (minute, second) = divmod(int(sec), 60)
    (hour, minute) = divmod(minute, 60)
    (days, hour) = divmod(hour, 24)
    return (timeonly(hour, minute, second, msec, tzinfo=tzinfo), days)

def datetime_to_murky36(t: datetime, /, upper: bool=False) -> str:
    """Convert the datetime to an encoding format using 0-9, a-z (or A-Z) called "murky36".
    - First digit is the year digit in base-36, starting at 2020 as 0 (so up to 2055).
    - We divide a year into 36 periods of 10 or 11 days (8 or 9 for the last period of Feb).
      Each period starts at the 1st, 11th, and 21th day of each month.
      The period number (0-35) is the second digit.
    - Then we divide up to 11 day period into 8 hour shifts; the shift number is the third
      digit: 0-29 (10 day period) or 0-32 (11 day period), 0-23 or 0-26 (last period of Feb).
    - Then we divide a shift into 32 quarters (in the unit of 15 minutes).
      The quater number is the fourth digit.
    - Then we dividde a quarter into 30 half-minutes (in the unit of 30 second).
      The half-minute number if the fifth digit: (0-29)
    - The sixth digit the second number in each half-minute (0-29).
      Now we can represent a date between years 2020-2055 in 6 digits of base-36.
    - Then milliseconds can be represented in 2 digits of base-36.
      Now we can represent milliseconds in 8 digits of base-36.
    - The microsecond offset to the whole milliseconds can be represented
      in two more digits of base-36. We use 10 digits to represent exact microseconds.
    The final result is a 10 digit base-36 number with the following places and
    the quotients between the units:
    ```
        year, period, shift, quarter, halfmin, second, (milli1, milli0), (micro1, micro0)
            36    24-33    32      30        30      1000             1000
    ```
    """
    d0 = int_to_str((t.year - 2020) % 36, 36, upper)
    period = (t.month - 1) * 3
    if t.day >= 21:
        period += 2
        shift = (t.day - 21) * 3
    elif t.day >= 11:
        period += 1
        shift = (t.day - 11) * 3
    else:
        shift = (t.day - 1) * 3
    d1 = int_to_str(period, 36, upper)
    (q, quarter) = divmod(t.hour, 8)
    shift += q
    quarter *= 4
    d2 = int_to_str(shift, 36, upper)
    (q, halfmin) = divmod(t.minute, 15)
    quarter += q
    halfmin *= 2
    d3 = int_to_str(quarter, 36, upper)
    (q, second) = divmod(t.second, 30)
    halfmin += q
    d4 = int_to_str(halfmin, 30, upper)
    d5 = int_to_str(second, 36, upper)
    (millis, micros) = divmod(t.microsecond, 1000)
    d67 = int_to_str(millis, 36, upper).zfill(2)
    d89 = int_to_str(micros, 36, upper).zfill(2)
    return d0 + d1 + d2 + d3 + d4 + d5 + d67 + d89

def murky36_to_datetime(s: str, /) -> datetime:
    n = len(s)
    assert n >= 3  # At least 3 bytes are needed
    year = str_to_int(s[0], 36) + 2020
    (month, day) = divmod(str_to_int(s[1], 36), 3)
    month += 1
    day = day * 10 + 1
    (q, hour) = divmod(str_to_int(s[2], 36), 3)
    day += q
    hour *= 8
    minute = second = micro = 0
    if n > 3:
        (q, minute) = divmod(str_to_int(s[3], 36), 4)
        hour += q
        minute *= 15
        if n > 4:
            (q, second) = divmod(str_to_int(s[4], 36), 2)
            minute += q
            second *= 30
            if n > 5:
                second += str_to_int(s[5], 36)
                if n > 7:
                    micro = str_to_int(s[6:8], 36) * 1000
                    if n > 9:
                        micro += str_to_int(s[8:10], 36)
                        assert n == 10
    return datetime(year, month, day, hour, minute, second, micro)

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
            'mo': ['month', 'months', 'mon', 'mons', 'mos'],
            'w': ['week', 'weeks', 'wk', 'wks'],
            'd': ['day', 'days'],
            'h': ['hour', 'hours', 'hr', 'hrs'],
            'm': ['minute', 'minutes', 'min', 'mins'],
            's': ['second', 'seconds', 'sec', 'secs'],
        })
    def __radd__(self, other):
        if isinstance(other, DateTimeDiff):
            return self.add_to_timediff(other)
        if isinstance(other, datetime):
            return self.add_to_datetime(other)
        if isinstance(other, dateonly):
            return self.add_to_dateonly(other)
        if isinstance(other, timeonly):
            return self.add_to_timeonly(other)[0]
        return NotImplemented
    def __rsub__(self, other):
        return self.__neg__().__radd__(other)
    def strfr(self, *, sign: bool=True) -> str:
        return super().strfr(sign=sign)  # Make sure start with a sign
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
            if u in data:
                args[n] = data[u]
        days = args.pop('days', 0)
        year = data.get('yr', 0)
        if isinstance(year, float):
            i = int(year)  # Round towards zero
            days += (year - i) * cls.DAYS_PER_YEAR
            year = i
        month = data.get('mo', 0)
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
        (y, month) = divmod(month - 1, 12)
        year = base.year + y
        month += 1
        # Doing the following to avoid the problem that the day is not allowed for the month
        return base.replace(year=year, month=month, day=1) + timedelta(days=(base.day - 1))

    def add_to_timediff(self, time_diff: 'DateTimeDiff') -> 'DateTimeDiff':
        """Add the date time diff to other date time diff."""
        return super().__add__(time_diff)
    def add_to_timeonly(self, base_time: timeonly) -> tuple[timeonly,int]:
        """Add the date time diff to the time given by Python time.
        - Returns a tuple: the new time and the offset in terms number of days.
        """
        (delta, months) = self.to_timedelta(self.value())
        (time, days) = seconds_to_timeonly(
            timeonly_to_seconds(base_time) + delta.total_seconds(), base_time.tzinfo
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
    """The relative datetime specification.

    There are three types of relative times that this class supports
    1. Partial absolute time: one or more of (year, month, day, hour, minute, second, msec)
       is set.
    2. Relative time offset: using DateTimeDiff above.
    3. Weekday-relative time: specify the relative weekend (Monday to Friday).
    When applied to an absolute date/time/datetime, the three types of the relative times
    are applied in the order as above.

    If `week` is set to a positive week number, then the date is determined once
    the year is determined. It is calculated this way:
    - If `month` is not given, then the week is the ISO week of the year;
      the week 1 is the week that Jan 4 falls in.
    - If `month` is given, the the week is the week number of the month; the
      week 1 is starting on the first Monday of the month.
    - For either cases, if the day is not set, then Monday is assumed;
      otherwise `day` value must be between 1-7 for Monday to Sunday.

    Note that this class do not handle timezone.
    """
    WEEKDAYS = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']

    def __init__(
            self, s1: str|float|DateTimeDiff|None=None, s2: str|float|None=None,
            /, *, year: int|None=None, month: int|None=None, day: int|None=None,
            hour: int|None=None, minute: int|None=None, second: int|None=None,
            microsecond: int|None=None,
            week: int|None=None, weekday: int|None=None, wd_dir: int=0,
            date: str|dateonly|None=None, time: str|timeonly|None=None,
    ):
        self.delta = None
        self.weekday = None
        self.wd_dir = 0
        for s in (s1, s2):
            if not s:
                continue
            if isinstance(s, DateTimeDiff):
                assert self.delta is None
                self.delta = s
            elif isinstance(s, str) and s[0].isalpha():
                assert self.weekday is None
                (self.weekday, self.wd_dir) = self.parse_weekday_str(s.strip())
            elif isinstance(s, str|float):
                self.delta = DateTimeDiff(s)
            else:
                raise ValueError(f"Invalid input type: {type(s)}")
        if date is not None:
            if isinstance(date, str):
                date = dateonly.fromisoformat(date)
            if year is None:
                year = date.year
            if month is None:
                month = date.month
            if day is None:
                day = date.day
        if time is not None:
            if isinstance(time, str):
                time = time.lower()
                if time.startswith('0t'):
                    time = time[2:]
                elif time and time[0] == 't':
                    time = time[1:]
                time = parse_timeonly(time)
            assert isinstance(time, timeonly)
            if hour is None:
                hour = time.hour
            if minute is None:
                minute = time.minute
            if second is None:
                second = time.second
            if microsecond is None:
                microsecond = time.microsecond
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second
        self.microsecond = microsecond
        assert week is None or week > 0
        self.week = week
        if weekday is not None:
            assert 0 <= weekday <= 6
            self.weekday = weekday
            self.wd_dir = wd_dir
    def __bool__(self):
        return (self.delta is not None and bool(self.delta)) or not all(x is None for x in (
            self.year, self.month, self.day, self.weekday,
            self.hour, self.minute, self.second, self.microsecond,
        ))
    def __radd__(self, other):
        if isinstance(other, datetime):
            return self.add_to_datetime(other)
        if isinstance(other, dateonly):
            return self.add_to_dateonly(other)
        if isinstance(other, timeonly):
            return self.add_to_timeonly(other)[0]
        return NotImplemented
    def frid_repr(self) -> FridNameArgs:
        args = []
        if self.delta is not None:
            args.append(self.delta)
        if self.weekday is not None:
            wd_str = self.WEEKDAYS[self.weekday]
            if self.wd_dir > 0:
                wd_str += "+"
                if self.wd_dir > 1:
                    wd_str += str(self.wd_dir)
            elif self.wd_dir < 0:
                wd_str += '-'
                if self.wd_dir < -1:
                    wd_str += str(-self.wd_dir)
            args.append(wd_str)
        return FridNameArgs(self.__class__.__name__, args, {k: v for k, v in dict(
            year=self.year, month=self.month, day=self.day, week=self.week,
            hour=self.hour, minute=self.minute, second=self.second,
            microsecond=self.microsecond,
        ).items() if v is not None})
    @classmethod
    def parse_weekday_str(cls, s: str) -> tuple[int,int]:
        """Parse the weekday string in the format like `FRI`, 'TUE-`, and `SUN+2`."""
        assert len(s) >= 3, "The weekday spec is in the format of DDD[+-[N]]"
        weekday = cls.WEEKDAYS.index(s[:3].upper())
        i = 3
        # Skip all remaining letters
        while i < len(s) and s[i].isalpha():
            i += 1
        if i >= len(s):
            return (weekday, 0)
        match s[i]:
            case '+':
                wd_dir = 1 if i + 1 == len(s) else int(s[(i+1):])
            case '-':
                wd_dir = -1 if i + 1 == len(s) else -int(s[(i+1):])
            case _:
                raise ValueError(f"Invalid weekday format: {s}")
        return (weekday, wd_dir)
    def _get_carry_by_dir(self, value: int, base: int, dir: Literal[0,1,-1]) -> Literal[0,1,-1]:
        """Check `value` against `base` to decide a carry of 1 or -1 (borrow) is needed.
        - `dir`: >0 (<0) if the `value` must be no less (more) than `base`.
        - Returns 0 if no carry, 1 if it has a carry, or -1 if it requires to borrow.
        """
        if dir > 0:
            if value < base:
                return 1
        elif dir < 0:
            if value > base:
                return -1
        return 0
    def _replace_timeonly(self, base_time: timeonly,
                          time_dir: Literal[0,1,-1]=0) -> tuple[timeonly,Literal[0,1,-1]]:
        """Replaces the fields in `base_time` with partial absolute time fields in self."""
        hour = base_time.hour if self.hour is None else self.hour
        minute = base_time.minute if self.minute is None else self.minute
        second = base_time.second if self.second is None else self.second
        msec = base_time.microsecond if self.microsecond is None else self.microsecond
        time = timeonly(hour, minute, second, msec, tzinfo=base_time.tzinfo)
        if not time_dir:
            return (time, 0)
        if self.hour is not None:
            carry = self._get_carry_by_dir(hour, base_time.hour, time_dir)
            return (time, carry)
        if self.minute is not None:
            delta = 3600 * self._get_carry_by_dir(minute, base_time.minute, time_dir)
        elif self.second is not None:
            delta = 60 * self._get_carry_by_dir(second, base_time.second, time_dir)
        elif self.microsecond is not None:
            delta = self._get_carry_by_dir(msec, base_time.microsecond, time_dir)
        else:
            return (time, 0)
        (time, carry) = seconds_to_timeonly(timeonly_to_seconds(time) + delta, time.tzinfo)
        assert carry in (0, 1, -1)
        return (time, carry)
    def _replace_dateonly(self, base_date: dateonly, date_dir: Literal[0,1,-1]=0) -> dateonly:
        """Replaces the fields in `base_date` with partial absolute time fields in self."""
        year = base_date.year if self.year is None else self.year
        # Once week is set, ignore month or day of the base date
        if self.week is not None:
            assert self.day is None or 1 <= self.day <= 7
            if self.month is None:
                # Use ISO week of year calendar of month is not set; self.day is day of week
                return dateonly.fromisocalendar(year, self.week, self.day or 1)
            # If self.month is, the first week starts on the first Monday of the month
            first = dateonly(year, self.month, 1)  # The first day of month
            # If self.week = 1, first.weekday() -> delta function is:
            #    {Mon: 0, Tue: 6, Wed: 5, Thu: 4 Fri: 3, Sat: 2, Sun: 1}
            delta = self.week * 7 - (first.weekday() or 7)
            if self.day is not None:
                delta += self.day - 1
            return first + timedelta(days=delta)
        # If week is not set, use base date's month and day as defaults
        month = base_date.month if self.month is None else self.month
        day = base_date.day if self.day is None else self.day
        if date_dir and self.year is None:
            if self.month is not None:
                year += self._get_carry_by_dir(month, base_date.month, date_dir)
            elif self.day is not None:
                month += self._get_carry_by_dir(day, base_date.day, date_dir)
        if day <= 28:
            return dateonly(year, month, day)
        return dateonly(year, month, 1) + timedelta(days=(day - 1))
    def _replace_datetime(self, date_time: datetime, dt_dir: Literal[0,1,-1]) -> datetime:
        """Replaces the fields in `date_time` with partial absolute time fields in self."""
        tm_dir = dt_dir if self.year is None and self.month is None and self.day is None else 0
        (time, carry) = self._replace_timeonly(date_time.time(), tm_dir)
        date = self._replace_dateonly(date_time.date(), dt_dir)
        if carry != 0:
            date += timedelta(days=carry)
        return datetime.combine(date, time)
    def _find_rel_weekday(self, base_date: dateonly):
        """Finds the relative weekday based on `base_date`.
        - This method uses `self.weekday`, and `self.wd_dir`.
        """
        if self.weekday is None:
            return base_date
        days = self.weekday - base_date.weekday()  # 0 is Monday and 6 is Sunday
        if self.wd_dir > 0:
            days += 7 * (self.wd_dir - int(days >= 0))
        elif self.wd_dir < 0:
            days += 7 * (self.wd_dir + int(days <= 0))
        return base_date + timedelta(days=days)
    def add_to_timeonly(self, base_time: timeonly,
                        dt_dir: Literal[0,1,-1]=0) -> tuple[timeonly,int]:
        """Adds this relative time to the absolute `base_time`.
        - `dt_dir`: 1 to look forward only, and -1 to look backward only.
        - Returns the sum and the extra offset in the number of days.
        """
        (time, carry1) = self._replace_timeonly(base_time, dt_dir)
        if self.delta is None:
            return (time, carry1)
        (time, carry2) = self.delta.add_to_timeonly(time)
        return (time, carry1 + carry2)
    def add_to_dateonly(self, base_date: dateonly, dt_dir: Literal[0,1,-1]=0) -> dateonly:
        """Adds this relative time to the absolute `base_date`.
        - `dt_dir`: 1 to look forward only, and -1 to look backward only.
        - Returns the sum and the extra offset in the number of days.
        """
        date = self._replace_dateonly(base_date, dt_dir)
        if self.delta is not None:
            date = self.delta.add_to_dateonly(date)
        return self._find_rel_weekday(date)
    def add_to_datetime(self, date_time: datetime, dt_dir: Literal[0,1,-1]=0) -> datetime:
        """Adds this relative time to the absolute `date_time`.
        - `dt_dir`: 1 to look forward only, and -1 to look backward only.
        - Returns the sum and the extra offset in the number of days.
        """
        out = self._replace_datetime(date_time, dt_dir)
        if self.delta is not None:
            out = self.delta.add_to_datetime(out)
        date = out.date()
        new_date = self._find_rel_weekday(date)
        if new_date == date:
            return out
        return datetime.combine(new_date, out.time())
