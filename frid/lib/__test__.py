import unittest

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
