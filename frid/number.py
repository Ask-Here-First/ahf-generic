import re
from collections.abc import Mapping, Iterable, Callable
from typing import NoReturn, TypeVar, overload

_T = TypeVar('_T')

class Quantity:
    """Data for a dimensional quantity with value-unit pairs.

    The constructor accepts a string as input and parse it into a dictionary
    with the unit as key and the number as the value.
    - It allows multiple number-unit pairs for a single aggregated quantity,
     for example "5ft4in" or "5 feet 4 inch".
    - One can use positive or negative signs, for example "-4h30m" means
      -4 for hours and -30 for minutes, that is, -4.5 hours.
    - One can use positive and negative signs in the middle to update the
      sign; otherwise it use the clostest sign to the left as above.
      For example, 4h-30m means 4 hours and -30 minutes, that is, -3.5 hours.
      Also, -4h+30m means -4 hours and +30 minutes, that is, 3.5 hours.
    - The value can be a float like 4.5h.
    - Each unit can only appear once, but the parser does not enforce and
      ordering.
    - Only last pair can have an empty-string unit (i.e., string may ends with
      a number).

    Construct arguments:
    - `units`: a list of string for allowed units (including an emptry string),
      or a mapping with canonical unit as keys and list of aliases as values.
      By default, all units are accepted as different units.
    """
    def __init__(self, s: str,
                 /, units: Mapping[str,Iterable[str]|None]|Iterable[str]|None=None):
        if units is None:
            alias = None
        elif isinstance(units, Mapping):
            alias = {}
            for k, v in units.items():
                alias[k] = k
                if v is not None:
                    assert not isinstance(v, str) and isinstance(v, Iterable)
                    for x in v:
                        alias[x] = k
        elif isinstance(units, Iterable):
            alias = {}
            for v in units:
                if v in alias:
                    raise ValueError(f"Duplicated unit {v}")
                alias[v] = v
        else:
            raise ValueError(f"Invalid type for units: {type(units)}")
        self._data = self.parse(s, alias)

    @staticmethod
    def _make_error(s: str, p: int, msg: str) -> NoReturn:
        """Raise an error showing the part of the string at the location."""
        # We use unicode \u20xx for delimiters
        n = 16
        if p > n:
            s1 = "\u2026" + s[(p-n):p]
        else:
            s1 = "\u2045" + s[:p]
        if p < len(s) - n:
            s2 = s[p:(p+n)] + "\u2026"
        else:
            s2 = s[p:] + "\u2046"
        raise ValueError(f"{msg} @{p}: {s1}\u2023{s2}")
    @staticmethod
    def _num_to_str(v: float):
        """Generate a representation of a number as string, without scientific notation."""
        if isinstance(v, int):
            return str(v)
        r = format(v, ".15f").rstrip('0')
        return r + '0' if r.endswith('.') else r

    item_re = re.compile(r"\s*([+-]?)\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)?")
    @classmethod
    def parse(cls, s: str, /, alias: Mapping[str,str]|None=None) -> dict[str,float]:
        """Parses a string and returns a dictionary mapping units to its values.
        - `alias`: a map from aliases to canonical units (including entries with
          canonical units to themselves).
        """
        out = {}
        pos = 0
        negated = False
        while pos < len(s):
            if (m := cls.item_re.match(s, pos)) is None:
                break
            ns = m.group(2)
            v = float(ns) if '.' in ns else int(ns)
            match m.group(1):
                case '-':
                    negated = True
                case '+':
                    negated = False
            if negated:
                v = -v
            u = m.group(3)
            if u is None:
                u = ''
            if alias is not None:
                u = alias.get(u)
                if u is None:
                    cls._make_error(s, pos, f"Unit `{u}` is not allowed")
            if u in out:
                cls._make_error(s, m.start(3), f"Unit `{u}` appears the second time")
            out[u] = v
            pos = m.end()
            if not u:
                break
        if pos < len(s) and not s[pos:].isspace():
            cls._make_error(s, pos, f"Trailing text of {len(s) - pos} chars")
        return out

    def __str__(self):
        return self.strfr()

    def strfr(self) -> str:
        """String formated representation -- a normalized representation that can be parsed."""
        negated = False
        out = []
        for u, v in self._data.items():
            if not u:
                continue
            s = self._num_to_str(v)
            if s.startswith('-'):
                if negated:
                    s = s[1:]
                else:
                    negated = True
            elif negated:
                out.append('+')
                negated = False
            out.append(s)
            out.append(u)
        v = self._data.get('')
        if v is not None:
            s = self._num_to_str(v)
            if s.startswith('-'):
                if negated:
                    s = s[1:]
            elif negated:
                out.append('+')
            out.append(s)
        return ''.join(out)

    @overload
    def value(self, scaling: None=None, /) -> Mapping[str,float]: ...
    @overload
    def value(self, scaling: Mapping[str,float], /) -> float: ...
    @overload
    def value(self, scaling: Callable[...,_T], /) -> _T: ...
    def value(self, scaling: Mapping[str,float]|Callable|None=None, /):
        """Converts the quality to a single value according to the scaling.
        - If `scaling` is not given, just return the data as is (a mapping).
        - If `scaling` is a map mapping a unit string to a float or int value,
          then it multiplies each value in this quatity with its unit's
          corresponding value in `scaling`, and then adds them together to
          return a single float or int number. (If the scaling for empty unit,
          is not specified, it is assumed to be 1.0.)
        - If `scaling` is a callable (e.g., a constructor), then pass the
          data dictionary as key/value arguments to the callable; if the
          empty string unit exists, its value is passed as the first
          positional argument.
        """
        if scaling is None:
            return self._data
        if callable(scaling):
            if '' not in self._data:
                return scaling(**self._data)
            args = dict(self._data)
            arg1 = args.pop('')
            return scaling(arg1, **args)
        return sum(v * scaling[u] if u else v * scaling.get(u, 1)
                   for u, v in self._data.items())
