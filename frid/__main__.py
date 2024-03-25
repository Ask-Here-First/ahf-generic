import sys, math, unittest
from typing import cast
try:
    # We have to import in the begining; otherwise static contents are not coveraged
    if __name__ == '__main__':
        print("Load the Python coverage package ...")
        import coverage
        _cov = coverage.Coverage()
        _cov.start()
        print("Running unit tests with coverage ...")
    else:
        _cov = None
except ImportError:
    _cov = None

from .typing import JsonLevel, StrKeyMap
from .chrono import parse_datetime, parse_timeonly, strfr_datetime
from .chrono import dateonly, timeonly, datetime, timezone, timedelta
from .strops import StringEscapeDecode, StringEscapeEncode, str_transform, str_find_any
from .dumper import dump_into_str
from .loader import load_from_str

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
        self.assertEqual(strfr_datetime(timeonly(10, 20, 30, 22, timezone.utc)),
                         "0T102030.220Z")
        self.assertEqual(strfr_datetime(datetime(2011, 2, 3, 11, 22, 33, 456789)),
                         "2011-02-03T112233.456")
        self.assertEqual(strfr_datetime(datetime(
            2011, 2, 3, 11, 22, 33, 456789, timezone(timedelta(hours=5, minutes=30))
        )), "2011-02-03T112233.456+0530")
        self.assertEqual(strfr_datetime(timeonly(11, 22, 33), prec=1), "0T112233.0")
        self.assertEqual(strfr_datetime(timeonly(11, 22, 33), prec=0), "0T112233")
        self.assertEqual(strfr_datetime(timeonly(11, 22, 33), prec=-1), "0T1122")
        self.assertIsNone(strfr_datetime(cast(datetime, 0)), None)

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
        t = r"abc([]{}, ([{,;}])) [,]{;} ' ,\,' ,"
        self.assertEqual(str_find_any(
            t, ",;", paired="()[]{}", quotes="'\"", escape='\\'
        ), len(t) - 1)
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
        "++": math.inf, "--": -math.inf, "+.": math.isnan, "-.": math.isnan,
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
        # List
        "[3, [4, 6], abc, [\"\"], [[[]]]]": [3,[4,6],"abc",[''],[[[]]]],
        "[3, [4, 6], abc , [,], [[[]]],  ] ": [3,[4,6],"abc",[''],[[[]]]],
        # Dict
        "{a.b: c, _: \"[]\", d+e-f: g@h}": {'a.b': "c", '_': "[]", 'd+e-f': "g@h"},
        "{a.b: c, _: '[]', d+e-f: g@h  , }": {'a.b': "c", '_': "[]", 'd+e-f': "g@h"},
        # "{a}": {'a':"a"}, # TODO: decide what to do for set-like

        # "()": (''), "(a>3)": LionExprStub('a>3'),
        # "(([{()}]))": LionExprStub("([{()}])"),
        # "(x in [a,b,c])": LionExprStub("x in [a,b,c]"),
        # "(x in '([{\\'\"\\\"')": LionExprStub("x in '([{\\'\"\\\"'"),
        # TODO: do we support non-string keys"{.:+}": {None: True}
    }
    def _do_test_positive(self, cases: StrKeyMap, json_level:JsonLevel):
        prev_value = ...
        json_const = json_level or json_level == ''
        for i, (s, t) in enumerate(cases.items()):
            try:
                v = load_from_str(s, json_const=json_const)
                if callable(t):
                    self.assertTrue(t(v), f"[{i}] {s} ==> {t} ({json_level=})")
                    continue
                self.assertEqual(t, v, f"[{i}] {s} ==> {t} ({json_level=})")
                if t == prev_value:
                    continue
                self.assertEqual(s, dump_into_str(t, json_level=json_level),
                                f"[{i}] {s} <== {t} ({json_level=})")
            except Exception:
                print(f"\nError @ [{i}] {s} <=> {t} ({json_level=})", file=sys.stderr)
                raise
            prev_value = t

    def test_positive(self):
        self._do_test_positive(self.common_cases, None)
        self._do_test_positive(self.common_cases, True)
        self._do_test_positive(self.common_cases, 5)
        self._do_test_positive(self.json_only_cases, True)
        self._do_test_positive(self.json5_only_cases, 5)
        self._do_test_positive(self.json_json5_cases, True)
        self._do_test_positive(self.json_json5_cases, 5)
        self._do_test_positive(self.frid_json5_cases, False)
        self._do_test_positive(self.frid_json5_cases, 5)
        self._do_test_positive(self.frid_only_cases, False)

if __name__ == '__main__':
    if _cov is not None:
        unittest.main(exit=False)
        _cov.stop()
        _cov.save()
        print("Generating HTML converage report ...")
        _cov.html_report()
        print("Report is in [ htmlcov/index.html ].")
    else:
        print("Running unit tests ...")
        unittest.main()
