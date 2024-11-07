import random, unittest

from .dicts import CaseDict
from .quant import Quantity
from .texts import StringEscapeDecode, StringEscapeEncode
from .texts import str_decode_nonprints, str_encode_nonprints, str_find_any, str_scan_sub

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
            Quantity("3ft", 8)  # type: ignore -- negative test with bad data type
        with self.assertRaises(ValueError):
            Quantity("3feet2meter", {"foot": ['ft', 'feet']})
        with self.assertRaises(ValueError):
            Quantity("3feet2foot", {"foot": ['ft', 'feet']})
        with self.assertRaises(ValueError):
            Quantity("                3feet  1inches @                          ")

class TestTexts(unittest.TestCase):
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
        self.assertEqual(str_scan_sub(s, {'a': self._add_next_by_one}),
                         (len(s), "a4b4c5"))
        self.assertEqual(str_scan_sub(s, {'b': self._add_next_by_one}),
                         (len(s), "a3b5c5"))
        self.assertEqual(str_scan_sub(s, {'a': self._add_next_by_one}),
                         (len(s), "a4b4c5"))
        self.assertEqual(str_scan_sub(s, {'d': self._add_next_by_one}),
                         (len(s), "a3b4c5"))
        self.assertEqual(str_scan_sub(s, {
            'a': self._add_next_by_one, 'c': self._add_next_by_one, 'd': self._add_next_by_one
        }), (len(s), "a4b4c6"))
        self.assertEqual(str_scan_sub(
            s, {'a': self._add_next_by_one, 'c': self._add_next_by_one}, stop_at="b"
        ), (2, "a4"))
        self.assertEqual(str_scan_sub(
            s, {'a': self._add_next_by_one, 'c': self._add_next_by_one}, stop_at="3"
        ), (6, "a4b4c6"))
        self.assertEqual(str_scan_sub(
            s, {'a': self._add_next_by_one, 'c': self._add_next_by_one}, stop_at="4"
        ), (3, "a4b"))
        self.assertEqual(str_scan_sub(
            s, {'a': self._add_next_by_one}, stop_at="c"
        ), (4, "a4b4"))
        self.assertEqual(str_scan_sub(
            s, {'a': self._add_next_by_one, 'c': self._add_next_by_one}, stop_at="c"
        ), (6, "a4b4c6"))

    def test_escape_control_char(self):
        cases = {
            "\n ": r"\n ", " \r": r" \r", "abc": "abc",
            "I'm a\tgood\r\nstu\\dent.\n": r"I'm a\tgood\r\nstu\\dent.\n",
            " \a \b \t \n \r \v \f \x1b \0 ": r" \a \b \t \n \r \v \f \e \0 ",
        }
        for x, y in cases.items():
            self.assertEqual(str_encode_nonprints(x), y)
            self.assertEqual(str_decode_nonprints(y), x)
        self.assertEqual(str_decode_nonprints("abc \\"), "abc \\")
        self.assertEqual(str_decode_nonprints("abc\\!"), "abc\\!")

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

class TestCaseDict(unittest.TestCase):
    def test_simple(self):
        d = CaseDict()
        repr(d)
        repr(d.keys())
        repr(d.items())
        self.assertNotEqual(d, 0)
        self.assertNotEqual(d, [(0, 0)])
        self.assertNotEqual(d, [(0, 0, 0)])
        self.assertNotEqual(d.keys(), 0)
        self.assertNotEqual(d.keys(), [(0, 0)])
        self.assertNotEqual(d.keys(), [(0, 0, 0)])
        self.assertNotEqual(d.items(), 0)
        self.assertNotEqual(d.items(), [(0, 0)])
        self.assertNotEqual(d.items(), [(0, 0, 0)])
        d = CaseDict(x=0)
        repr(d)
        repr(d.keys())
        repr(d.items())
        self.assertEqual(d, [("x", 0)])
        self.assertNotEqual(d, [("x",)])
        self.assertNotEqual(d, [("x", 0, 0)])
        self.assertEqual(d.keys(), ["x"])
        self.assertEqual(d.keys(), ["X"])
        self.assertNotEqual(d.keys(), ["x", "X"])
    def randkey(self):
        key = ""
        for _ in range(random.randint(0, 6)):
            key += chr(32 + random.randint(0, 95))
        return key
    def init_keys(self):
        # Generate a list of keys different
        key_set = set()
        key_list = []
        for _ in range(128):
            key = self.randkey()
            low_key = key.lower()
            if low_key not in key_set:
                key_set.add(low_key)
                key_list.append(key)
        return key_list
    def get_random_case(self, key):
        # Not used for add
        match random.randint(0, 2):
            case 0:
                return key
            case 1:
                return key.lower()
            case 2:
                return key.upper()
    def check_one_key(self, d0: CaseDict, key, exists: bool, val=None):
        if exists:
            self.assertIn(key, d0)
            self.assertIn(key, d0.keys())
            self.assertIn((key, val), d0.items())
            self.assertEqual(d0[key], val)
            self.assertEqual(d0.get(key), val)
            self.assertEqual(d0.get(key, 3), val)
            self.assertEqual(d0[key.lower()], val)
            self.assertEqual(d0[key.upper()], val)
        else:
            self.assertNotIn(key, d0)
            self.assertNotIn(key, d0.keys())
            self.assertIsNone(d0.get(key))
            self.assertIs(d0.get(key, ...), ...)
    def compare_dicts(self, d0: CaseDict, d1: dict, key_set: set[str]):
        self.assertEqual(len(d0), len(d1))
        self.assertEqual(len(d0.keys()), len(d1.keys()))
        self.assertEqual(len(d0.items()), len(d1.items()))
        self.assertEqual(set(k.lower() for k in d0.keys()), set(d1.keys()))
        self.assertEqual(set(k.lower() for k in d0), set(k for k in d1))
        self.assertEqual(set((k.lower(), v) for k, v in d0.items()), set(d1.items()))
        self.assertEqual(set(k.lower() for k in reversed(d0.keys())), set(d1.keys()))
        self.assertEqual(set(k.lower() for k in reversed(d0)), set(d1.keys()))
        self.assertEqual(set((k.lower(), v) for k, v in reversed(d0.items())), set(d1.items()))
        self.assertEqual(d0, CaseDict(d1))
        self.assertEqual(d0.keys(), d1.keys())
        self.assertEqual(d0, CaseDict(d1.items()))
        self.assertEqual(d0.keys(), d1.keys())
        self.assertEqual(d0.items(), d1.items())
        # Prove the case are correct
        for k, v in d0.items():
            self.assertIn(k, key_set)
            self.assertIn(k.lower(), d1)
            self.assertEqual(v, d1[k.lower()])
    def test_dict_random(self):
        key_list = self.init_keys()
        key_set = set(key_list)
        d0 = CaseDict()
        d1 = dict()
        for _ in range(256):
            key = random.choice(key_list)
            exists = key.lower() in d1
            self.check_one_key(d0, key, exists, d1.get(key.lower(), ...))
            match random.randint(0, 6):
                case 0:
                    val = random.randint(0, 31)
                    d0[key] = val
                    d1[key.lower()] = val
                    exists = True
                case 1:
                    if exists:
                        del d0[self.get_random_case(key)]
                        del d1[key.lower()]
                        exists = False
                    else:
                        with self.assertRaises(KeyError):
                            del d0[key]
                case 2:
                    val = random.randint(0, 31)
                    d0.setdefault(key, val)
                    d1.setdefault(key.lower(), val)
                    exists = True
                case 3:
                    if exists:
                        d0.pop(self.get_random_case(key))
                        d1.pop(key.lower())
                        exists = False
                    else:
                        with self.assertRaises(KeyError):
                            d0.pop(self.get_random_case(key))
                case 4:
                    v0 = d0.pop(self.get_random_case(key), 999)
                    v1 = d1.pop(key.lower(), 999)
                    self.assertEqual(v0, v1)
                    exists = False
                case 5:
                    if d1:
                        (key, val) = d0.popitem()
                        v1 = d1.pop(key.lower())
                        self.assertEqual(val, v1)
                    else:
                        with self.assertRaises(KeyError):
                            d0.popitem()
                    exists = False
                case 6:
                    d0[key.upper()] = 1213
                    d0[key.lower()] = 3459
                    val = random.randint(0, 31)
                    d0[key] = val
                    d1[key.lower()] = val
                    exists = True
            self.check_one_key(d0, key, exists, d1.get(key.lower()))
            self.compare_dicts(d0, d1, key_set)
            if random.randint(0, 8):
                continue
            self.compare_dicts(d0, d1, key_set)
            # Test copy() and copy constructor
            self.compare_dicts(d0.copy(), d1, key_set)
            self.compare_dicts(CaseDict(d0), d1, key_set)
            # Test constructor with kwargs
            d2 = CaseDict(**d0)
            d3 = dict(**{k.lower(): v for k, v in d0.items()})
            self.compare_dicts(d2, d3, key_set)
            self.assertEqual(d2, CaseDict(d3))
            self.assertEqual(CaseDict(d3), d2.items())
            # Test fromkeys
            n = random.randint(0, 10000)
            self.assertEqual(CaseDict.fromkeys(d2.keys(), n), dict.fromkeys(d2.keys(), n))
            # Test update
            d4 = {k: random.randint(0, 300) for k in random.choices(
                key_list, k=random.randint(1, 7)
            )}
            match random.randint(0, 4):
                case 0:
                    d0.update(d4)
                case 1:
                    d0.update(list(d4.items()))
                case 2:
                    d0.update(**d4)
                case 3:
                    d0 |= d4
                case 4:
                    d0 = d0 | d4
            d1.update((k.lower(), v) for k, v in d4.items())
            self.compare_dicts(d0, d1, key_set)
            if random.randint(0, 8):
                continue
            # Test clear
            d0.clear()
            d1.clear()
            self.assertFalse(d0)
            self.assertEqual(len(d0), 0)
            self.assertFalse(list(d0.keys()))
            self.assertFalse(list(d0.items()))
            self.compare_dicts(d0, d1, key_set)
        # Some negative cases afterwards
        self.assertNotIn(0, d0)
        self.assertNotIn(0, d0.keys())
        self.assertNotIn(0, d0.items())
        self.assertNotIn((), d0.items())
        self.assertNotIn((0,), d0.items())
        self.assertNotIn((0, 0), d0.items())
