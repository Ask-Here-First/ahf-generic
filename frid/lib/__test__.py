import unittest

from .quant import Quantity

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
