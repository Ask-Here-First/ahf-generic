import re
from datetime import timezone, timedelta

from .typing import dateonly, timeonly, datetime, DateTypes

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

