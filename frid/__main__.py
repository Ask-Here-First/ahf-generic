import os, io, sys, math, json, base64, unittest, importlib
from random import Random
from typing import Any, Literal, cast
from functools import partial

from .random import frid_random

try:
    # We have to import in the begining; otherwise static contents are not coveraged
    if __name__ == '__main__':
        print("Load the Python coverage package ...")
        import coverage
        _cov = coverage.Coverage()
        _cov.start()
        # Reload all loaded modules of name frid.* to cover all static context
        modules = [x for x in sys.modules.values() if x.__name__.startswith("frid.")]
        for module in modules:
            importlib.reload(module)
    else:
        _cov = None
except ImportError:
    _cov = None

from .typing import MISSING, PRESENT, FridBeing, FridMixin, FridValue, FridNameArgs, StrKeyMap, ValueArgs
from .chrono import DateTimeDiff, DateTimeSpec, parse_datetime, parse_timeonly, strfr_datetime
from .chrono import dateonly, timeonly, datetime, timezone, timedelta
from .strops import StringEscapeDecode, StringEscapeEncode
from .strops import escape_control_chars, revive_control_chars, str_transform, str_find_any
from .helper import Comparator, Substitute, get_func_name, get_qual_name, get_type_name
from .dumper import dump_args_str, dump_frid_tio, dump_frid_str, frid_redact
from .loader import FridParseError, load_frid_str, load_frid_tio, open_frid_tio, scan_frid_str
from .number import Quantity

class TestChrono(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(parse_datetime("2021-02-03"), dateonly(2021, 2, 3))
        self.assertEqual(parse_datetime("2021-02-03T11:22"), datetime(2021, 2, 3, 11, 22))
        self.assertEqual(parse_datetime("2021-02-03T11:22:33"),
                         datetime(2021, 2, 3, 11, 22, 33))
        self.assertEqual(parse_datetime("2021-02-03T11:22:33.1"),
                         datetime(2021, 2, 3, 11, 22, 33, 100000))
        self.assertEqual(parse_datetime("2021-02-03T112233.12"),
                         datetime(2021, 2, 3, 11, 22, 33, 120000))
        self.assertEqual(parse_datetime("2021-02-03T11:22:33.123"),
                         datetime(2021, 2, 3, 11, 22, 33, 123000))
        self.assertEqual(parse_datetime("2021-02-03T11:22:33.12345"),
                         datetime(2021, 2, 3, 11, 22, 33, 123450))
        self.assertEqual(parse_datetime("2021-02-03T112233.123456"),
                         datetime(2021, 2, 3, 11, 22, 33, 123456))
        self.assertEqual(parse_datetime("2021-02-03T11:22:33.1234567"),
                         datetime(2021, 2, 3, 11, 22, 33, 123456))
        self.assertEqual(parse_datetime("2021-02-03T11:22:33.12Z"),
                         datetime(2021, 2, 3, 11, 22, 33, 120000, timezone.utc))
        self.assertEqual(parse_datetime("11:22:33+00:00"),
                         timeonly(11, 22, 33, tzinfo=timezone(-timedelta())))
        self.assertEqual(parse_datetime("0T11:22-0530"),
                         timeonly(11, 22, tzinfo=timezone(-timedelta(hours=5, minutes=30))))
        self.assertEqual(parse_datetime("0T11:22:33.12+04:30"),
                         timeonly(11, 22, 33, 120000, timezone(timedelta(hours=4, minutes=30))))
        self.assertEqual(parse_datetime("0T112233.12+0430"),
                         timeonly(11, 22, 33, 120000, timezone(timedelta(hours=4, minutes=30))))
        # Not matching cases
        self.assertIsNone(parse_datetime(""))
        self.assertIsNone(parse_datetime("0t11"))
        self.assertIsNone(parse_timeonly("0T11,2233.12+0430"))
        self.assertIsNone(parse_timeonly("11-22-33"))

    def test_strfr(self):
        self.assertEqual(strfr_datetime(dateonly(2011, 2, 13)), "2011-02-13")
        self.assertEqual(strfr_datetime(timeonly(10, 20, 30)), "0T102030.000")
        self.assertEqual(strfr_datetime(timeonly(10, 20, 30), colon=True), "0T10:20:30.000")
        self.assertEqual(strfr_datetime(timeonly(10, 20, 30, 22, timezone.utc)),
                         "0T102030.220Z")
        self.assertEqual(strfr_datetime(datetime(2011, 2, 3, 11, 22, 33, 456789)),
                         "2011-02-03T112233.456")
        self.assertEqual(strfr_datetime(datetime(
            2011, 2, 3, 11, 22, 33, 456789, timezone(timedelta(hours=5, minutes=30))
        ), colon=True), "2011-02-03T11:22:33.456+0530")
        self.assertEqual(strfr_datetime(timeonly(11, 22, 33), precision=1), "0T112233.0")
        self.assertEqual(strfr_datetime(timeonly(11, 22, 33), precision=0), "0T112233")
        self.assertEqual(strfr_datetime(timeonly(11, 22, 33), precision=-1), "0T1122")
        self.assertEqual(strfr_datetime(timeonly(11, 22, 33), precision=-2), "0T11")
        self.assertEqual(strfr_datetime(0),
                         strfr_datetime(datetime.fromtimestamp(0, timezone.utc)))
        with self.assertRaises(ValueError):
            strfr_datetime(timeonly(11, 22, 33), precision=-3)

    def test_datetimediff(self):
        self.assertEqual(dump_frid_str(DateTimeSpec("MON")), "DateTimeSpec(MON)")
        self.assertEqual(dump_frid_str(DateTimeSpec("TUE+1", "2mo")),
                         "DateTimeSpec(+2mo, TUE+)")
        self.assertEqual(dump_frid_str(DateTimeSpec("1h3m", "WED-", month=2)),
                         "DateTimeSpec(+1h3m, WED-, month=2)")
        self.assertEqual(dump_frid_str(DateTimeSpec("+1h3m", "THU+2", day=2)),
                         "DateTimeSpec(+1h3m, THU+2, day=2)")
        self.assertEqual(dump_frid_str(DateTimeSpec("SUN-4", "")),
                         "DateTimeSpec(SUN-4)")
        self.assertEqual(DateTimeDiff("1m3h") + DateTimeDiff("+2m4d"), DateTimeDiff("+3m3h4d"))
        self.assertEqual(str(DateTimeDiff("1year2month3days4hours5minutes6.3125seconds")),
                         "+1yr2mo3d4h5m6.3125s")
        self.assertEqual(dateonly(2020, 1, 3) + DateTimeDiff("+1yr1mo"), dateonly(2021, 2, 3))
        self.assertEqual(dateonly(2020, 1, 3) + DateTimeDiff("+1.0yr1.2mo"), dateonly(2021, 2, 9))
        self.assertEqual(dateonly(2020, 1, 3) - DateTimeDiff("+1yr1mo"), dateonly(2018, 12, 3))
        self.assertEqual(dateonly(2020, 1, 3) + DateTimeDiff("-4d"), dateonly(2019, 12, 30))
        self.assertEqual(dateonly(2020, 5, 30) + DateTimeDiff("+3d20mo"), dateonly(2022, 2, 2))
        self.assertEqual(timeonly(12, 34, 56) + DateTimeDiff("+1h10m10s"), timeonly(13, 45, 6))
        self.assertEqual(timeonly(2, 4, 6) - DateTimeDiff("+10m"), timeonly(1, 54, 6))
        self.assertEqual(
            DateTimeDiff("0.000001s").add_to_timeonly(timeonly(23, 59, 59, 999999)),
            (timeonly(0, 0, 0), 1)
        )
        self.assertEqual(datetime(2020, 5, 30, 22, 50, 10) + DateTimeDiff("1mo1d1h15m55.7s"),
                         datetime(2020, 7, 2, 0, 6, 5, 700000))
        with self.assertRaises(TypeError):
            assert object() + DateTimeDiff("1d")

    def test_datetimespec(self):
        self.assertFalse(DateTimeSpec())
        self.assertTrue(DateTimeSpec("+1s"))
        self.assertTrue(DateTimeSpec(hour=4))
        self.assertTrue(DateTimeSpec("TUE"))
        self.assertEqual(dateonly(2020, 2, 4) + DateTimeSpec("+1m3h"), dateonly(2020, 2, 4))
        self.assertEqual(timeonly(10, 2, 4) + DateTimeSpec(), timeonly(10, 2, 4))
        self.assertEqual(timeonly(10, 2, 4) + DateTimeSpec("+1m3h"), timeonly(13, 3, 4))
        self.assertEqual(datetime(2020, 2, 4) + DateTimeSpec("+1m3h"),
                         datetime(2020, 2, 4, 3, 1))
        self.assertEqual(datetime(2020, 2, 4, 10, 55, 3) + DateTimeSpec(
            - DateTimeDiff("-1mo1d"), month=6, time="03:05:20"
        ), datetime(2020, 7, 5, 3, 5, 20))
        self.assertEqual(datetime(2020, 2, 4, 10, 55, 3) + DateTimeSpec(
            - DateTimeDiff("-1mo1d"), month=6, time="0T030520", minute=6, microsecond=100
        ), datetime(2020, 7, 5, 3, 6, 20, 100))
        self.assertEqual(datetime(2020, 2, 4, 10, 55, 3) + DateTimeSpec(
            - DateTimeDiff("-1mo1d"), month=6, time="T0305", hour=4, second=7
        ), datetime(2020, 7, 5, 4, 5, 7))
        self.assertEqual(DateTimeSpec(month=5).add_to_dateonly(dateonly(2024, 7, 1), 1),
                         dateonly(2025, 5, 1))
        self.assertEqual(DateTimeSpec(month=5).add_to_dateonly(dateonly(2024, 7, 1), -1),
                         dateonly(2024, 5, 1))
        self.assertEqual(DateTimeSpec(month=6).add_to_dateonly(dateonly(2024, 5, 31), 1),
                         dateonly(2024, 7, 1))
        self.assertEqual(DateTimeSpec(month=6).add_to_dateonly(dateonly(2024, 5, 31), -1),
                         dateonly(2023, 7, 1))
        self.assertEqual(DateTimeSpec(day=29).add_to_dateonly(dateonly(2023, 3, 28), -1),
                         dateonly(2023, 3, 1))
        self.assertEqual(DateTimeSpec(week=2).add_to_dateonly(dateonly(2023, 5, 7)),
                         dateonly(2023, 1, 9))
        self.assertEqual(DateTimeSpec(week=1, day=3).add_to_dateonly(dateonly(2024, 5, 7)),
                         dateonly(2024, 1, 3))
        self.assertEqual(DateTimeSpec(
            year=2024, month=4, week=1, day=4
        ).add_to_dateonly(dateonly.today()), dateonly(2024, 4, 4))
        self.assertEqual(DateTimeSpec(
            year=2024, month=5, week=2, day=1
        ).add_to_dateonly(dateonly.today()), dateonly(2024, 5, 13))
        self.assertEqual(DateTimeSpec(minute=5).add_to_timeonly(timeonly(20, 30, 40), 1),
                         (timeonly(21, 5, 40), 0))
        self.assertEqual(DateTimeSpec(minute=30).add_to_timeonly(timeonly(20, 30, 40), -1),
                         (timeonly(20, 30, 40), 0))
        self.assertEqual(DateTimeSpec(minute=30).add_to_timeonly(timeonly(0, 10, 40), -1),
                         (timeonly(23, 30, 40), -1))
        self.assertEqual(DateTimeSpec(second=30).add_to_timeonly(timeonly(23, 59, 59), 1),
                         (timeonly(0, 0, 30), 1))
        self.assertEqual(DateTimeSpec(microsecond=0).add_to_timeonly(timeonly(23, 59, 59, 1), 1),
                         (timeonly(0, 0, 0), 1))
        self.assertEqual(DateTimeSpec(month=10).add_to_timeonly(timeonly(12, 34, 56, 7890), -1),
                         (timeonly(12, 34, 56, 7890), 0))
        # 2024-07-25 is a Thursay
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("FRIDAY"), dateonly(2024, 7, 26))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("THU"), dateonly(2024, 7, 25))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("WED"), dateonly(2024, 7, 24))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("FRI+"), dateonly(2024, 7, 26))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("THU+"), dateonly(2024, 7, 25))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("WED+"), dateonly(2024, 7, 31))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("FRI-"), dateonly(2024, 7, 19))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("THU-"), dateonly(2024, 7, 25))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("WED-"), dateonly(2024, 7, 24))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("FRI+1"), dateonly(2024, 7, 26))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("THU+1"), dateonly(2024, 7, 25))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("WED+2"), dateonly(2024, 8, 7))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("FRI-1"), dateonly(2024, 7, 19))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("THU-1"), dateonly(2024, 7, 25))
        self.assertEqual(dateonly(2024, 7, 25) + DateTimeSpec("WED-2"), dateonly(2024, 7, 17))
        # Mixed
        self.assertEqual(dateonly(2024, 1, 1) + DateTimeSpec("+2d", month=7, day=23, weekday=2),
                         dateonly(2024, 7, 24))
        self.assertEqual(timeonly(20, 30, 40) + DateTimeSpec("-2m", hour=4, second=20),
                         timeonly(4, 28, 20))
        self.assertEqual(DateTimeSpec(hour=8, minute=9).add_to_datetime(
            datetime(2020, 1, 1, 10, 20, 30), 1
        ), datetime(2020, 1, 2, 8, 9, 30))
        self.assertEqual(datetime(2020, 3, 4, 5, 6, 7, 800000)
                         + DateTimeSpec("-20h", year=2024, minute=9, microsecond=20),
                         datetime(2024, 3, 3, 9, 9, 7, 20))
        self.assertEqual(datetime(2020, 4, 5, 6, 7, 8, 999999)
                         + DateTimeSpec("FRI-", "+0.1s", date="2024-07-25"),
                         datetime(2024, 7, 19, 6, 7, 9, 99999))
        with self.assertRaises(TypeError):
            assert object() + DateTimeSpec()

class TestStrops(unittest.TestCase):
    def test_str_find_any(self):
        #    012345678901234567
        s = "3.1415926535897932"
        self.assertEqual(str_find_any(s, ""), -1)
        self.assertEqual(str_find_any(s, "abc"), -1)
        self.assertEqual(str_find_any(s, "."), 1)
        self.assertEqual(str_find_any(s, ".", -1000), 1)
        self.assertEqual(str_find_any(s, "45"), 3)
        self.assertEqual(str_find_any(s, "45", 4), 5)
        self.assertEqual(str_find_any(s, ".", -len(s)), 1)
        self.assertEqual(str_find_any(s, ".", 1-len(s)), 1)
        self.assertEqual(str_find_any(s, "23", 5, 9), 7)
        self.assertEqual(str_find_any(s, "13", 5, 10), -1)
        self.assertEqual(str_find_any(s, ".", 1-len(s)), 1)
        self.assertEqual(str_find_any(s, "82", -5, -1), -1)
        self.assertEqual(str_find_any("(')')", ')', 1), 2)
        self.assertEqual(str_find_any("(')')", ')', 1, quotes="'"), 4)
        t = r"abc([]{}, ([{,;}])) [,]{;} ' ,\,' ,"
        self.assertEqual(str_find_any(
            t, ",;", paired="()[]{}", quotes="'\"", escape='\\'
        ), len(t) - 1)
        self.assertEqual(str_find_any(
            t + ")]}", ")]}", paired="()[]{}", quotes="'\"", escape='\\'
        ), len(t))
        # Skip a prefix
        self.assertEqual(str_find_any(
            "[{(" + t, ",;", 3, paired="()[]{}", quotes="'\"", escape='\\'
        ), len(t) + 3 - 1)
        self.assertEqual(str_find_any(
            t, ",;", 0, -1, paired="()[]{}", quotes="'\"", escape='\\'
        ), -1)
        self.assertEqual(str_find_any(
            r"[(\{\),;\]),]", ",;", paired="()[]{}", escape='\\'
        ), -1)
        self.assertEqual(str_find_any(
            r"..\,\;..", ",;", escape='\\'
        ), -1)
        with self.assertRaises(ValueError):
            str_find_any(
                "abc (,;])", ",;", paired="()[]{}", quotes="'\"", escape='\\'
            )
        with self.assertRaises(ValueError):
            str_find_any(
                "abc (,;)]", ",;", paired="()[]{}", quotes="'\"", escape='\\'
            )
        with self.assertRaises(ValueError):
            str_find_any(
                "abc ([,;]", ",;", paired="()[]{}", quotes="'\"", escape='\\'
            )
        with self.assertRaises(ValueError):
            str_find_any(
                "abc '([,;]", ",;", paired="()[]{}", quotes="'\"", escape='\\'
            )

    def _add_next_by_one(self, s: str, start: int, bound: int, prefix: str):
        index = start + len(prefix)
        if index >= bound:
            return (-1, '')
        return (len(prefix) + 1, prefix + chr(ord(s[index]) + 1))

    def test_str_transform(self):
        s = "a3b4c5"
        self.assertEqual(str_transform(s, {'a': self._add_next_by_one}),
                         (len(s), "a4b4c5"))
        self.assertEqual(str_transform(s, {'b': self._add_next_by_one}),
                         (len(s), "a3b5c5"))
        self.assertEqual(str_transform(s, {'a': self._add_next_by_one}),
                         (len(s), "a4b4c5"))
        self.assertEqual(str_transform(s, {'d': self._add_next_by_one}),
                         (len(s), "a3b4c5"))
        self.assertEqual(str_transform(s, {
            'a': self._add_next_by_one, 'c': self._add_next_by_one, 'd': self._add_next_by_one
        }), (len(s), "a4b4c6"))
        self.assertEqual(str_transform(
            s, {'a': self._add_next_by_one, 'c': self._add_next_by_one}, stop_at="b"
        ), (2, "a4"))
        self.assertEqual(str_transform(
            s, {'a': self._add_next_by_one, 'c': self._add_next_by_one}, stop_at="3"
        ), (6, "a4b4c6"))
        self.assertEqual(str_transform(
            s, {'a': self._add_next_by_one, 'c': self._add_next_by_one}, stop_at="4"
        ), (3, "a4b"))
        self.assertEqual(str_transform(
            s, {'a': self._add_next_by_one}, stop_at="c"
        ), (4, "a4b4"))
        self.assertEqual(str_transform(
            s, {'a': self._add_next_by_one, 'c': self._add_next_by_one}, stop_at="c"
        ), (6, "a4b4c6"))

    def test_escape_control_char(self):
        cases = {
            "\n ": r"\n ", " \r": r" \r", "abc": "abc",
            "I'm a\tgood\r\nstu\\dent.\n": r"I'm a\tgood\r\nstu\\dent.\n",
            " \a \b \t \n \r \v \f \x1b \0 ": r" \a \b \t \n \r \v \f \e \0 ",
        }
        for x, y in cases.items():
            self.assertEqual(escape_control_chars(x), y)
            self.assertEqual(revive_control_chars(y), x)
        self.assertEqual(revive_control_chars("abc \\"), "abc \\")
        self.assertEqual(revive_control_chars("abc\\!"), "abc\\!")

    def test_string_escape(self):
        s = "I'm a\tgood\r\nstudent.\n"
        t1 = r"I'm a\tgood\r\nstudent.\n"
        t2 = r"I\'m a\tgood\r\nstudent.\n"
        t3 = r"I'm a\x09good\x0d\x0astudent.\x0a"
        t4 = r"I'm a\u0009good\u000d\u000astudent.\u000a"
        t5 = r"I'm a\U00000009good\U0000000d\U0000000astudent.\U0000000a"
        encode = StringEscapeEncode("\tt\rr\nn")
        decode = StringEscapeDecode("\tt\rr\nn''")
        self.assertEqual(encode(s, ''), t1)
        self.assertEqual(decode(t1, ''), (len(t1), s))
        self.assertEqual(encode(s, "'"), t2)
        self.assertEqual(decode(t2, "'"), (len(t2), s))
        self.assertEqual(decode(t2 + "'", "'"), (len(t2), s))
        self.assertEqual(encode(s, '"'), t1)
        self.assertEqual(decode(t1, '"'), (len(t1), s))
        self.assertEqual(decode(t2 + "`'", "`'\""), (len(t2), s))
        self.assertEqual(encode(s, "'", 1, 2), r"\'")
        self.assertEqual(decode(t2, "'", 1, 3), (2, "'"))
        # No escape
        encode = StringEscapeEncode("")
        decode = StringEscapeDecode("''")
        self.assertEqual(encode(s, ""), s)
        self.assertEqual(decode(s, ""), (len(s), s))
        # 2 hexadecimalcode
        encode = StringEscapeEncode("", hex_prefix=('x', None, None))
        decode = StringEscapeDecode("", hex_prefix=('x', None, None))
        self.assertEqual(encode(s, ''), t3)
        self.assertEqual(decode(t3, ''), (len(t3), s))
        with self.assertRaises(ValueError):
            decode(t3, '', 0, len(t3)-1)
        # 4 hexadecimalcode
        encode = StringEscapeEncode("", hex_prefix=(None, 'u', None))
        decode = StringEscapeDecode("", hex_prefix=(None, 'u', None))
        self.assertEqual(encode(s, ''), t4)
        self.assertEqual(decode(t4, ''), (len(t4), s))
        with self.assertRaises(ValueError):
            decode(t4, '', 0, len(t4)-2)
        # Surrogate pair
        self.assertEqual(encode('\U00010437', ''), r"\ud801\udc37")
        # 8 hexadecimal code
        encode = StringEscapeEncode("", hex_prefix=(None, None, 'U'))
        decode = StringEscapeDecode("", hex_prefix=(None, None, 'U'))
        self.assertEqual(encode(s, ''), t5)
        self.assertEqual(decode(t5, ''), (len(t5), s))
        with self.assertRaises(ValueError):
            decode(t5, '', 0, len(t5)-1)
        with self.assertRaises(ValueError):
            decode(t1, '')

class TestHelper(unittest.TestCase):
    def test_comparator(self):
        cmp = Comparator()
        self.assertTrue(cmp(None, None))
        self.assertTrue(cmp(2, 2))
        self.assertFalse(cmp(1, "1"))
        # Unsupported data type
        self.assertFalse(cmp(cast(FridValue, self), cast(FridValue, self)))
        data = ['a', '1', 1, datetime.now(), b"345"]
        self.assertTrue(cmp(data, data))
        self.assertFalse(cmp(data, 'a'))
        self.assertFalse(cmp(data, 3))
        data = {'a': [1, True, 2.0, None, False], 'b': "Hello", 'c': [
            dateonly.today(), datetime.now(), datetime.now().time()
        ]}
        self.assertTrue(cmp(data, data))
        self.assertFalse(cmp(data, 'a'))
        self.assertFalse(cmp(data, 3))

    def test_comparator_submap(self):
        cmp = Comparator(compare_dict=Comparator.is_submap)
        self.assertTrue(cmp({'a': 1}, {'a': 1, 'b': 2}))
        self.assertFalse(cmp({'a': 1}, {'a': 3, 'b': 2}))
        self.assertFalse(cmp({'a': 1}, {}))
        self.assertFalse(cmp({'a': 1}, 3))

    def test_substitute(self):
        sub = Substitute(present="+.", missing="-.")
        self.assertEqual(sub(3), 3)
        self.assertEqual(sub("${a}", a=MISSING), "-.")
        self.assertEqual(sub("${a}", a=PRESENT), "+.")
        self.assertEqual(sub("[${a}]", a=MISSING), "[-.]")
        self.assertEqual(sub("[${a}]", a=PRESENT), "[+.]")
        self.assertEqual(sub("${a}"), "-.")
        self.assertEqual(sub("a"), "a")
        self.assertEqual(sub("The ${key}=${val} is here", {'key': "data", 'val': 3}),
                         "The data=3 is here")
        self.assertEqual(sub("The ${var} is here", var="data"), "The data is here")
        self.assertEqual(sub({
            'a': "${var1}", 'b': ["${var2}", "${var3}"]
        }, var1=3, var2=['abc', 4], var3='def'), {
            'a': 3, 'b': ['abc', 4, 'def']
        })
        self.assertEqual(sub(["${var1}", "${var2}"], var1=PRESENT, var2=MISSING), ["+.", "-."])
        with self.assertRaises(ValueError):
            sub("abc ${def")
        # This is for the pattern
        self.assertEqual(sub("${var*}", var1="x", var2="y"), {'1': "x", '2': "y"})
        self.assertEqual(sub("[${va*}]", var1="x", var2="y"), "[{r1: x, r2: y}]")

    def test_type_check(self):
        self.assertEqual(get_type_name(FridBeing), "FridBeing")
        self.assertEqual(get_type_name(PRESENT), "FridBeing")
        self.assertEqual(get_type_name(MISSING), "FridBeing")
        self.assertEqual(get_qual_name(FridBeing), "FridBeing")
        self.assertEqual(get_qual_name(PRESENT), "FridBeing")
        self.assertEqual(get_qual_name(MISSING), "FridBeing")
        self.assertEqual(get_func_name(self.test_type_check), "test_type_check()")
        self.assertEqual(get_func_name(id), "id()")
        def test(a, b, c):
            pass
        self.assertEqual(get_func_name(test), "test()")
        self.assertIsInstance(partial(test, 3), partial)
        self.assertEqual(get_func_name(partial(test)), "test()")
        self.assertEqual(get_func_name(partial(test, 3)), "test(3,...)")
        self.assertEqual(get_func_name(partial(test, b=3)), "test(...,b=3,...)")


class TestLoaderAndDumper(unittest.TestCase):
    common_cases = {
        "123": 123,     " 123 ": 123,   "-4": -4,       "   -4   ": -4,
        "0.0": 0.0,     "+0.0": 0.0,    "+0.": 0.0,     ".0": 0.0,
        "-0.0": -0.0,   "-0.": -0.0,    "-.0": -0.0,    "-0.00": -0.0,
        "0.5": 0.5,     "+0.5": 0.5,    "5E-1": 0.5,    "+0.05e1": 0.5,
        "-0.5": -0.5,   "-.5": -0.5,    "-5E-1": -0.5,  "-0.05e1": -0.5,
        "-2.0": -2.0,   "-2.": -2.0,    "-2E0": -2.0,   "-.2E1": -2.0,
        '""': '',       '   ""  ': '',
        "\"\\u20af\"": "\u20aF",        "\"\\u20aF\" ": "\u20af",
        "[]": [],       "[   ] ": [],
        "[\"\"]": [''],                 "[,] ": [''],
        "[1]": [1],     "[ 1]": [1],    "[ 1 , ] ": [1],
        "[1, 2]": [1,2],                " [ 1 ,   2]": [1,2],
        "{}": {},       "{   }": {},
    }
    json_only_cases = {
        "{\"a\": 1, \"b\": 2}": {'a': 1, 'b': 2},
        "{\"a\": 1, \"b\": 2,   }": {'a': 1, 'b': 2},
    }
    json_json5_cases = {
        "null": None,   " null ": None, "  null": None, "    null    ": None,
        "true": True,   "  true": True, "false": False, "false  ": False,
        '"a"': "a",
    }
    json5_only_cases = {
        "+Infinity": math.inf,          "Infinity": math.inf,           "-Infinity": -math.inf,
        "NaN": math.isnan,              "+NaN": math.isnan,             "-NaN": math.isnan,
        '"b"': "b",     "'b'": "b",
    }
    frid_json5_cases = {
        "3": 3,         "0x3": 3,       "-19": -19,     "-0X13": -19,
        '""': "",       "''": "",
        '"abc\\r\\ndef"': "abc\r\ndef", "'abc\\r\\ndef'": "abc\r\ndef",
        "{a: 3}": {'a': 3},             "{ a : 3 ,}": {'a': 3},
        "{ 'a' : 3}": {'a':3},
        '{",": "}"}': {',': "}"},       "{ ',': '}' }": {',': "}"},
        "{a: 1, b: 2}": {'a': 1, 'b': 2},               "{a: 1, b: 2,  }": {'a': 1, 'b': 2},
    }
    frid_only_cases = {
        # Constants
        ".": None,      " . ": None,    ". ": None,     " .  ": None,
        "+": True,      " +": True,     "-": False,     "- ": False,
        # Numbers
        "30": 30,       " 3_0 ": 30,    "2000": 2000,   " +2_000 ": 2000,
        "12345": 12345, "1_2_3_4_5": 12345,
        "-400000": -400000,             "-400_000  ": -400000,
        "0.25": 0.25,   ".25": 0.25,    "2.5E-1": 0.25,
        "++": math.inf, "--": -math.inf, "+-": math.isnan, "-+": math.isnan,
        # Unquoted strings
        '""': '',       "": '',         " ": '',        "  ": '',
        "c": "c",       "'c'": "c",     '"c"': "c",     "  `c`  ": "c",
        "abc": "abc",   " abc ": "abc", " `abc` ": "abc",
        "ab d": "ab d", " ab d ": "ab d",
        '"ab  e"': "ab  e",
        "user@admin.com": "user@admin.com",
        # Quoted strings
        '" a\\eb"': " a\033b",    "  `\\x20a\\eb`": " a\033b",
        '"\\U00010248"': "\U00010248",  " '\\U00010248' ": "\U00010248",
        '"\\e\\e\\"\'` "': "\033\033\"'` ",
        "  '\\e\\x1b\\\"\\'\\` '": "\033\033\"'` ",
        # "'''tester's test''' """: "tester's test", # Do not support triple quotes yet
        # Blob
        "..": b'',      " ..": b'',         ".. ": b'',
        "..YQ..": b"a", "..YWI.": b"ab",    "..YWJj": b"abc",
        # List
        "[3, [4, 6], abc, [\"\"], [[[]]]]": [3,[4,6],"abc",[''],[[[]]]],
        "[3, [4, 6], abc , [,], [[[]]],  ] ": [3,[4,6],"abc",[''],[[[]]]],
        # Dict
        "{a.b: c, _: \"[]\", d+e-f: g@h}": {'a.b': "c", '_': "[]", 'd+e-f': "g@h"},
        "{a.b: c, _: '[]', d+e-f: g@h  , }": {'a.b': "c", '_': "[]", 'd+e-f': "g@h"},
        "{: \"\"}": {'': ''}, "{:}": {'': ''}, "{: a}": {'': "a"}, "{:a}": {'': "a"},
        # Set: Python caveats: True == 1 and False == 0; also
        "{,}": set(), "{a}": {'a'}, "{3}": {3}, "{+}": {True}, "{-}": {False}, "{.}": {None},
        # Can't set multi value set, since set is not following insert ordering
        "{0,1,2,.}": (lambda x: x == {0, 1, 2, None}),

        # "()": (''), "(a>3)": LionExprStub('a>3'),
        # "(([{()}]))": LionExprStub("([{()}])"),
        # "(x in [a,b,c])": LionExprStub("x in [a,b,c]"),
        # "(x in '([{\\'\"\\\"')": LionExprStub("x in '([{\\'\"\\\"'"),
        # TODO: do we support non-string keys"{.:+}": {None: True}
    }
    def _do_test_positive(self, cases: StrKeyMap, json_level: Literal[0,1,5]):
        prev_value = ...
        for i, (s, t) in enumerate(cases.items()):
            try:
                v = load_frid_str(s, json_level=json_level)
                if callable(t):
                    self.assertTrue(t(v), f"[{i}] {s} ==> {t} ({json_level=})")
                    continue
                self.assertEqual(t, v, f"[{i}] {s} ==> {t} ({json_level=})")
                assert t is not ...
                if t == prev_value:
                    continue
                assert not isinstance(t, FridBeing)
                self.assertEqual(s, dump_frid_str(t, json_level=json_level),
                                f"[{i}] {s} <== {t} ({json_level=})")
                # With loose mode
                v1 = load_frid_str(s, json_level=json_level, loose_mode=True)
                self.assertEqual(v, v1)
                # With indentation
                s1 = dump_frid_str(t, json_level=json_level, indent='\t')
                v = load_frid_str(s1, json_level=json_level)
                if callable(t):
                    self.assertTrue(t(v), f"[{i}] {s} ==> {t} ({json_level=})")
                else:
                    self.assertEqual(t, v, f"[{i}] {s} ==> {t} ({json_level=})")
            except Exception:
                print(f"\nError @ [{i}] {s} <=> {t} ({json_level=})", file=sys.stderr)
                raise
            prev_value = t
    def test_positive(self):
        self._do_test_positive(self.common_cases, 0)
        self._do_test_positive(self.common_cases, 1)
        self._do_test_positive(self.common_cases, 5)
        self._do_test_positive(self.json_only_cases, 1)
        self._do_test_positive(self.json5_only_cases, 5)
        self._do_test_positive(self.json_json5_cases, 1)
        self._do_test_positive(self.json_json5_cases, 5)
        self._do_test_positive(self.frid_json5_cases, 0)
        self._do_test_positive(self.frid_json5_cases, 5)
        self._do_test_positive(self.frid_only_cases, 0)
        self.assertEqual(dump_frid_str(math.nan), "+-")
        self.assertEqual(dump_frid_str(-math.nan), "-+")
        self.assertEqual(dump_frid_str(math.nan, json_level=5), "NaN")

    def test_random(self):
        def_seed = 0
        def_runs = 256
        def_tree = 4
        runs = int(os.getenv('FRID_RANDOM_RUNS', def_runs))
        seed = os.getenv('FRID_RANDOM_SEED')
        if seed is None:
            seed = def_seed
        else:
            seed = load_frid_str(seed)
            assert isinstance(seed, int|float|bytes|str)
        tree = int(os.getenv('FRID_RANDOM_TREE', def_tree))

        if seed != def_seed or runs != def_runs or tree != def_tree:
            print(f"\nRunning random test with {runs} rounds, seed={seed}")
        rng = Random()
        rng.seed(seed)

        for _ in range(runs):
            r = rng.randint(0, 15)
            dump_args = {
                'print_real': None if r & 1 else lambda x: format(x, '+'),
                'print_date': None if r & 2 else lambda v: strfr_datetime(v, precision=6),
                'print_blob': None if r & 4 else lambda v: base64.b16encode(v).decode(),
                'ascii_only': bool(r & 8),
            }
            load_args = {
                'parse_real': None if r & 1 else lambda s: (
                    int(s, 0) if s[1:].isnumeric() and (s[0].isnumeric() or s[0] in "+-")
                    else float(s)
                ),
                'parse_date': None if r & 2 else lambda s: parse_datetime(s),
                'parse_blob': None if r & 4 else lambda s: base64.b16decode(s),
            }
            # Test with only JSON compatible values
            data = frid_random(rng, tree, for_json=1)
            text = json.dumps(data)
            self.assertEqual(data, load_frid_str(text, json_level=1), msg="Loading JSON")
            self.assertEqual(data, load_frid_str(text, json_level=5), msg="Loading JSON5")
            for json_level in (0, 1, 5):
                s = dump_frid_str(data, json_level=json_level, **dump_args)
                self.assertEqual(data, load_frid_str(s, json_level=json_level, **load_args),
                                 msg=f"{json_level=} {len(s)=}")
            # Test with only JSON-5 compatible values
            data = frid_random(rng, tree, for_json=5)
            for json_level in (0, 5):
                s = dump_frid_str(data, json_level=json_level, **dump_args)
                self.assertEqual(data, load_frid_str(s, json_level=json_level, **load_args),
                                 msg=f"{json_level=} {len(s)=}")
            # Test with only all possible frid values
            json_level = rng.choice([0, 1, 5])
            for escape_seq in ('~', "#!"):
                data = frid_random(rng, tree, for_json=0)
                s = dump_frid_str(data, json_level=json_level,
                                  escape_seq=escape_seq, **dump_args)
                self.assertEqual(data, load_frid_str(
                    s, json_level=1, escape_seq=escape_seq, **load_args
                ), msg=f"{len(s)=}")
                t = io.StringIO()
                dump_frid_tio(data, t, json_level=json_level,
                              escape_seq=escape_seq, **dump_args)
                self.assertEqual(s, t.getvalue())
                self.assertEqual(data, load_frid_tio(
                    io.StringIO(s), page=rng.randint(1, 5), json_level=1, escape_seq=escape_seq,
                    **load_args
                ), msg=f"{len(s)=}")
                # With loose mode
                self.assertEqual(data, load_frid_tio(
                    io.StringIO(s), page=rng.randint(1, 5), json_level=1, escape_seq=escape_seq,
                    loose_mode=True, **load_args
                ), msg=f"{len(s)=}")
                # With indentitation and newlines
                s = dump_frid_str(data, json_level=json_level, indent=4,
                                  escape_seq=escape_seq, **dump_args)
                self.assertEqual(data, load_frid_str(
                    s, json_level=json_level, escape_seq=escape_seq, **load_args
                ), msg=f"{len(s)=}")
                self.assertEqual(data, load_frid_tio(
                    io.StringIO(s), page=64, json_level=json_level, escape_seq=escape_seq,
                    **load_args
                ), msg=f"{len(s)=}")

    negative_load_cases = [
        # Numbers
        "3+7", "4 x 2", "  .5abc", "-6d ef  ", ".723 ghi 4", "+abc", "-invalid",
        "3_", "+_", "0x-", "0x_",
        # Strings
        "I'm here", "back`ticks`", "a\\ b ", " c(d)", "Come, here",
        "'No ending quote", "'''Mismatched end quote' '", "'wrong quotes`",
        "'\\", "'\\x2", "'\\x3'", "'\\x9k'", "'\\u37'", "'\\xyz'", "'\\U03'",
        # List
        "[1,", "[}",
        # Dict
        "{a:3,,}", "{)", "{a:1, a:2}", "{3a:3}", "{3: 4}",
        # Set
        "{3, a:}", "{b:3, +}"
        # Expr
        "(", "([})", "((())",
    ]
    def test_negative_load(self):
        # print(f"Running {len(positive_testcases)} negative testcases...")
        for i, k in enumerate(self.negative_load_cases):
            with self.assertRaises(FridParseError, msg=f"[{i}]: {k}"):
                load_frid_str(k)


    negative_json_dump_cases = [
        math.nan, math.inf, -math.inf, dateonly.today(), b"1234", {3: 4}, object(),
    ]
    def test_negative_json_dump(self):
        for i, k in enumerate(self.negative_json_dump_cases):
            with self.assertRaises(ValueError, msg=f"[{i}]: {k}"):
                dump_frid_str(k, json_level=True)

    expression_cases = {
        "()": "", "(())": "()", "(')')": "')'",
        "{a:(')'), b: (['()[]{}'])}": {'a': "')'", 'b': "['()[]{}']"}
    }
    def test_expression(self):
        for i, (k, v) in enumerate(self.expression_cases.items()):
            w = load_frid_str(k, parse_expr=lambda x, p: x)
            self.assertEqual(v, w, msg=f"[{i}]: {k}")

    comment_cases = {
        "\n123": 123, "\n[\n123,\n\n 456,]": [123, 456],
        "123 # 456": 123, "123 # 456\n": 123, "// abc\n456": 456, "/* abc */ 123": 123,
        "[123, #456,\n789]": [123, 789], "[1,/*1,\n3,*/ 4 // 5,\n # 6\n, 7]": [1,4,7],
    }
    def test_comments(self):
        for i, (k, v) in enumerate(self.comment_cases.items()):
            w = load_frid_str(k, comments=["#", "//", ("/*", "*/")])
            self.assertEqual(v, w, msg=f"[{i}]: {k}")

    scan_cases = {
        "123\n456": (123, "456"),
        "123 // 456\n789": (123, "789"),
        "123 /* 456 */\t\n789": (123, "789"),
        "123 /* 456 */ 789\n": Exception,
    }
    def test_scan(self):
        kwargs: dict[str,Any] = dict(comments=["//", ("/*", "*/")])
        for s, t in self.scan_cases.items():
            if isinstance(t, type) and issubclass(t, BaseException):
                with self.assertRaises(t):
                    scan_frid_str(s, 0, until_eol=True, **kwargs)
                with self.assertRaises(t):
                    open_frid_tio(io.StringIO(s), **kwargs)(
                        until_eol=True
                    )
                continue
            assert isinstance(t, tuple)
            (data, trailing) = t
            (value, index) = scan_frid_str(s, 0, until_eol=True, **kwargs)
            self.assertEqual(data, value, f"{s=}")
            self.assertEqual(s[index:], trailing, f"{s=}")
            value = open_frid_tio(io.StringIO(s), **kwargs)(until_eol=True)
            self.assertEqual(data, value, f"{s=}")

    class TestMixinClass(FridMixin):
        def __init__(self, *args, **kwds):
            super().__init__()
            self._args = args
            self._kwds = kwds
        def frid_repr(self) -> FridNameArgs:
            return FridNameArgs(get_type_name(self), self._args, self._kwds)
        def __repr__(self):
            return dump_args_str(self.frid_repr())
        def __eq__(self, other):
            if self is other:
                return True
            if not isinstance(other, __class__):
                return False
            return self._args == other._args and self._kwds == other._kwds
    class TestMixinClass2(TestMixinClass):
        def __init__(self, opt1, *args, opt2, **kwds):
            super().__init__(*args, **kwds)
            self._opt1 = opt1
            self._opt2 = opt2
        @classmethod
        def frid_from(cls, data: FridNameArgs, opt1, /, *, opt2):
            return cls(opt1, *data.args, opt2=opt2, **data.kwds)
        def frid_repr(self, opt1, /, *, opt2) -> FridNameArgs:
            assert opt1 == self._opt1 and opt2 == self._opt2
            return super().frid_repr()
        def __eq__(self, other):
            if not super().__eq__(other):
                return False
            return self._opt1 == other._opt1 and self._opt2 == other._opt2

    def test_mixins(self):
        test = self.TestMixinClass()
        frid = "TestMixinClass()"
        self.assertEqual(dump_frid_str(test), frid)
        self.assertEqual(load_frid_str(frid, frid_mixin=[self.TestMixinClass]), test)
        json = '"#!TestMixinClass()"'
        self.assertEqual(dump_frid_str(test, json_level=1, escape_seq="#!"), json)
        self.assertEqual(load_frid_str(
            json, frid_mixin=[self.TestMixinClass], json_level=1, escape_seq="#!"
        ), test)

        test = self.TestMixinClass("Test", a=3)
        frid = "TestMixinClass(Test, a=3)"
        self.assertEqual(dump_frid_str(test), frid)
        self.assertEqual(load_frid_str(frid, frid_mixin=[self.TestMixinClass]), test)
        json = """{"": ["#!TestMixinClass", "Test"], "a": 3}"""
        self.assertEqual(dump_frid_str(test, json_level=1, escape_seq="#!"), json)
        self.assertEqual(load_frid_str(
            json, frid_mixin=[self.TestMixinClass], json_level=1, escape_seq="#!"
        ), test)

        test = self.TestMixinClass2("1", "Test", opt2=2, a=3)
        frid = "TestMixinClass2(Test, a=3)"
        mixin_list = [ValueArgs(self.TestMixinClass2, "1", opt2=2)]
        self.assertEqual(dump_frid_str(test, mixin_args=mixin_list), frid)
        self.assertEqual(load_frid_str(frid, frid_mixin=mixin_list), test)
        json = """{"": ["#!TestMixinClass2", "Test"], "a": 3}"""
        self.assertEqual(dump_frid_str(test, mixin_args=mixin_list,
                                       json_level=1, escape_seq="#!"), json)
        self.assertEqual(load_frid_str(
            json, frid_mixin=mixin_list, json_level=1, escape_seq="#!"
        ), test)

    def test_identation(self):
        data = {'x': ["a", "b", []], 'y': 2, 'z': self.TestMixinClass(3, 4, b=5)}
        s = ("{\n\tx: [\n\t\ta,\n\t\tb,\n\t\t[\n\t\t]?\n\t],\n"
             "\ty: 2,\n\tz: TestMixinClass(3, 4, b=5)?\n}\n")
        self.assertEqual(
            dump_frid_str(data, indent='\t'),
            s.replace('?', '')
        )
        self.assertEqual(
            dump_frid_str(data, indent='\t', extra_comma=True),
            s.replace('?', ',')
        )
        data = {'a': "abc", 'b': "abc\n", 'c': "abc\ndef", 'd': "abc\ndef\n", 'e': [
            "abc", "abc\n", "abc\ndef", "abc\ndef\n", "abc\ndef\nghi", "abc\ndef\nghi\n",
        ]}
        # With break quotes
        s = dump_frid_str(data, indent='\t', break_quoted=True)
        self.assertEqual(data, load_frid_str(s), s)
        s = dump_frid_str(data, indent='\t', break_quoted=True, extra_comma=True)
        self.assertEqual(data, load_frid_str(s), s)

    def test_redact(self):
        now = datetime.now()
        self.assertEqual(frid_redact({
            'a': 3, 'b': ["ab", b"a", now, now.time(), 3.5, self.TestMixinClass()],
            'c': [], 'd': False, 'e': None, 'f': PRESENT,
        }), {
            'a': 'i', 'b': ['s2', 'b1', 'd', 't', 'f', 'TestMixinClass'], 'c': [], 'd': False,
            'e': None, 'f': PRESENT
        })
        self.assertEqual(frid_redact({
            'a': 3, 'b': ["a", "b", "c"], 'c': [], 'd': False, 'e': None,
        }, 0), {
            'a': PRESENT, 'b': [3], 'c': [], 'd': PRESENT, 'e': PRESENT,
        })


class TestQuantity(unittest.TestCase):
    @staticmethod
    def _to_dict(value, **kwargs):
        return {'': value, **kwargs}

    def test_quantity(self):
        self.assertEqual(Quantity("5ft8in").value(), {'ft': 5, 'in': 8})
        self.assertEqual(Quantity("5ft-3in ").value(dict), {'ft': 5, 'in': -3})
        self.assertEqual(Quantity("-5ft8.1in").value(), {'ft': -5, 'in': -8.1})
        self.assertEqual(Quantity("5ft+8in").value({'ft': 12, 'in': 1}), 68)

        self.assertEqual(Quantity("5ft8", ['ft', '']).value(), {'ft': 5, '': 8})
        self.assertEqual(Quantity("5ft8", ['ft', '']).value(self._to_dict), {'ft': 5, '': 8})
        self.assertEqual(Quantity("5ft8", {'foot': ['ft', 'feet'], 'inch': ['in', '']}).value(),
                        {'foot': 5, 'inch': 8})
        self.assertEqual(Quantity("5ft8.1").value({'ft': 12}), 68.1)

        for s in ("5ft8in", "5ft-8in", "-5ft+8.1in", "-5ft8.0in", "-5ft8", "-5ft+8"):
            self.assertEqual(str(Quantity(s)), s)

    def test_quantity_ops(self):
        self.assertFalse(Quantity(""))
        self.assertTrue(Quantity("5ft"))
        self.assertEqual(Quantity("5ft8in"), Quantity("8in5ft"))
        self.assertEqual(Quantity("5ft8in") + Quantity("1ft2in"), Quantity("6ft10in"))
        self.assertEqual(Quantity("5ft8in") - Quantity("1ft2in"), Quantity("4ft6in"))

    def test_quantity_negative(self):
        with self.assertRaises(ValueError):
            Quantity("3ft", ["ft", 'ft'])
        with self.assertRaises(ValueError):
            Quantity("3ft", 8) # type: ignore -- negative test with bad data type
        with self.assertRaises(ValueError):
            Quantity("3feet2meter", {"foot": ['ft', 'feet']})
        with self.assertRaises(ValueError):
            Quantity("3feet2foot", {"foot": ['ft', 'feet']})
        with self.assertRaises(ValueError):
            Quantity("                3feet  1inches @                          ")

if __name__ == '__main__':
    if _cov is not None:
        print("Running unit tests with coverage ...")
    else:
        print("Running unit tests ...")

    unittest.main(exit=False)
    unittest.main("frid.kvs.__main__", exit=False)

    if _cov is not None:
        _cov.stop()
        _cov.save()
        print("Generating HTML converage report ...")
        _cov.html_report()
        print("Report is in [ htmlcov/index.html ].")
