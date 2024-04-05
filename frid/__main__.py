import os, io, sys, math, json, string, base64, unittest, importlib
from random import Random
from typing import Literal, cast
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
        print("Running unit tests with coverage ...")
    else:
        _cov = None
except ImportError:
    _cov = None

from .typing import FridValue, StrKeyMap
from .chrono import parse_datetime, parse_timeonly, strfr_datetime
from .chrono import dateonly, timeonly, datetime, timezone, timedelta
from .strops import StringEscapeDecode, StringEscapeEncode, str_transform, str_find_any
from .helper import Comparator
from .dumper import dump_into_tio, dump_into_str
from .loader import ParseError, load_from_str, load_from_tio

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
        "{a}": {'a'}, "{3}": {3}, "{+}": {True}, "{-}": {False}, "{.}": {None},
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
                v = load_from_str(s, json_level=json_level)
                if callable(t):
                    self.assertTrue(t(v), f"[{i}] {s} ==> {t} ({json_level=})")
                    continue
                self.assertEqual(t, v, f"[{i}] {s} ==> {t} ({json_level=})")
                assert t is not ...
                if t == prev_value:
                    continue
                self.assertEqual(s, dump_into_str(t, json_level=json_level),
                                f"[{i}] {s} <== {t} ({json_level=})")
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
        self.assertEqual(dump_into_str(math.nan), "+.")
        self.assertEqual(dump_into_str(-math.nan), "-.")
        self.assertEqual(dump_into_str(math.nan, json_level=5), "NaN")

    CHAR_POOL = string.printable + "\u03b1\u03b2\u03b3\u2020\u2021\u2022" #"\U00010330"
    def random_string(self, rng: Random, for_key=False):
        n = rng.randint(0, 20)
        if for_key and rng.randint(0, 1):
            # Generate within the
            return "".join(rng.choices("abcxyz+-._", k=n))
        return "".join(rng.choices(self.CHAR_POOL, k=n))
    def random_value(self, rng: Random, depth: int, *, for_json: int=0):
        r = rng.randint(0, 32 if depth > 0 else 20)
        match r:
            case 0:
                return None
            case 1:
                return True
            case 2:
                return False
            case 3:
                if for_json:
                    return None
                return datetime.now().replace(microsecond=0)
            case 4:
                if for_json:
                    return True
                return dateonly.today()
            case 5:
                if for_json:
                    return False
                return datetime.now().time().replace(microsecond=0)
            case 6 | 7 | 8 | 9:
                return self.random_string(rng)
            case 10 | 11:
                if for_json:
                    return self.random_string(rng)
                return self.random_string(rng).encode()
            case 12 | 13 | 14:
                return rng.randint(-10000, 10000)
            case 15:
                # Cannot use NaN as NaN != NaN
                if for_json == 1:
                    return rng.choice([1.0, 0.0, -1.0, math.e, math.pi]) # no infinity
                return rng.choice([math.inf, 1.0, 0.0, -1.0, math.e, math.pi, -math.inf])
            case 16 | 17 | 18 | 19:
                return math.ldexp(rng.random() - 0.5, rng.randint(-40, 40))
            case 20 | 21 | 22 | 23 | 24 | 25:
                return [self.random_value(rng, depth - 1, for_json=for_json)
                        for _ in range(rng.randint(0, 8))]
            case 26 | 27 | 28 | 29 | 30 | 31:
                return {
                    self.random_string(rng, True):
                        self.random_value(rng, depth - 1, for_json=for_json)
                    for _ in range(rng.randint(0, 8))
                }
    def test_random(self):
        def_seed = 0
        def_runs = 256
        def_tree = 4
        runs = int(os.getenv('FRID_RANDOM_RUNS', def_runs))
        seed = os.getenv('FRID_RANDOM_SEED')
        if seed is None:
            seed = def_seed
        else:
            seed = load_from_str(seed)
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
                'print_date': None if r & 2 else lambda v: strfr_datetime(v, prec=6),
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
            data = self.random_value(rng, tree, for_json=1)
            text = json.dumps(data)
            self.assertEqual(data, load_from_str(text, json_level=1), msg="Loading JSON")
            self.assertEqual(data, load_from_str(text, json_level=5), msg="Loading JSON5")
            for json_level in (0, 1, 5):
                s = dump_into_str(data, json_level=json_level, **dump_args)
                self.assertEqual(data, load_from_str(s, json_level=json_level, **load_args),
                                 msg=f"{json_level=} {len(s)=}")
            # Test with only JSON-5 compatible values
            data = self.random_value(rng, tree, for_json=5)
            for json_level in (0, 5):
                s = dump_into_str(data, json_level=json_level, **dump_args)
                self.assertEqual(data, load_from_str(s, json_level=json_level, **load_args),
                                 msg=f"{json_level=} {len(s)=}")
            # Test with only all possible frid values
            json_level = rng.choice([0, 1, 5])
            for escape_seq in ('~', "#!"):
                data = self.random_value(rng, tree, for_json=0)
                s = dump_into_str(data, json_level=json_level,
                                  escape_seq=escape_seq, **dump_args)
                self.assertEqual(data, load_from_str(
                    s, json_level=1, escape_seq=escape_seq, **load_args
                ), msg=f"{len(s)=}")
                t = io.StringIO()
                dump_into_tio(data, t, json_level=json_level,
                              escape_seq=escape_seq, **dump_args)
                self.assertEqual(s, t.getvalue())
                t = io.StringIO(s)
                self.assertEqual(data, load_from_tio(
                    t, page=rng.randint(1, 5), json_level=1, escape_seq=escape_seq, **load_args
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
        "{,}", "{a:3,,}", "{)", "{a:1, a:2}", "{3a:3}", "{3: 4}",
        # Set
        "{3, a:}", "{b:3, +}"
        # Expr
        "(", "([})", "((())",
    ]
    def test_negative_load(self):
        # print(f"Running {len(positive_testcases)} negative testcases...")
        for i, k in enumerate(self.negative_load_cases):
            with self.assertRaises(ParseError, msg=f"[{i}]: {k}"):
                load_from_str(k)


    negative_json_dump_cases = [
        math.nan, math.inf, -math.inf, dateonly.today(), b"1234", {3: 4}, object(),
    ]
    def test_negative_json_dump(self):
        for i, k in enumerate(self.negative_json_dump_cases):
            with self.assertRaises(ValueError, msg=f"[{i}]: {k}"):
                dump_into_str(k, json_level=True)

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
