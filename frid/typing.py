import dataclasses
from abc import ABC
from datetime import date as dateonly, time as timeonly, datetime
from collections.abc import Mapping, Sequence, Set, Callable
from enum import Enum
from typing import Any, Generic, Literal, NamedTuple, TypeGuard, TypeVar, final

# Quick union types used in many places
BlobTypes = bytes|bytearray|memoryview
DateTypes = dateonly|timeonly|datetime   # Note that datetime in Python is deriveed from date

# FRID types follow (Flexibly represented inteactive data)

_T = TypeVar('_T')
_B = TypeVar('_B', bound='FridBasic')
_M = TypeVar('_M', bound='FridMixin')

class FridBeing(Enum):
    """This "being or not being" class introduces two special values, PRESENT and MISSING.
    The main purpose is to be used for values of a map. If the value
    is PRESENT for a key, it means the key is present but there is
    no meaningful associated value. If the value is MISSING for a key,
    the the entry in the map should be handled as it is not there.
    """
    PRESENT = True
    MISSING = False
    def __bool__(self):
        return self.value
    def strfr(self) -> str:
        return "+." if self.value else "-."
    @classmethod
    def parse(cls, s: str) -> 'FridBeing|None':
        match s:
            case '+.':
                return PRESENT
            case '-.':
                return MISSING
            case _:
                return None

PresentType = Literal[FridBeing.PRESENT]
MissingType = Literal[FridBeing.MISSING]
PRESENT: PresentType = FridBeing.PRESENT
MISSING: MissingType = FridBeing.MISSING

class FridBasic(ABC):
    """The abstract base class that handles some basic datatypes in string format.
    - The constructor must at least accept a single string-formated positional argument.
    - If the constractor raises an exception then the format is not accepted.
    - When a string is parsed and a list of FridBasic data types are given
      as options, their constructors will be tried one by one until the one
      raises no exception.
    - The directed class should overwrite either frid_repr() to return a string,
      or overwrite __str__() because frid_repr() calls __str__() by default.
    Note that the string representaion here if rather limited:
    - The entire string should contain only quote free characters (letters,
      numbers, and +-._ and a few other, see is_quote_free_char()).
    - It must not be a quote-free string (otherwise it is handled as a string).
    - It must not be parsed as first-order pri, like constants, numbers,
    datetimes, blobs, etc.
    - The main purpose is to allow user-defined numerical types, such as
      complex numbers, fractional numbers, dimensional quantities, etc.
    """
    def frid_repr(self) -> str:
        """Convert the data to string representation."""
        return self.__str__()
    @classmethod
    def frid_from(cls: type[_B], s: str, /, *args, **kwargs) -> _B|None:
        """Construct from string reprentation"""
        return cls(s, *args, **kwargs) # type: ignore

class FridMixin(ABC):
    """The abstract base frid class to be loadable and dumpable.

    A frid class needs to implement three methods:
    - A class method `frid_keys()` that returns a list of acceptable keys
      for the class (default includes the class name);
    - A class method `frid_from()` that constructs and object of this class
      with the name, and a set of positional and keyword arguments
      (default is to check the name against acceptable keys, and then call
      the constructor with these arguments).
    - A instance method `frid_repr()` that converts the object to a triplet:
      a name, a list of positional values, and a dict of keyword values
      (this method is abstract).
    """
    @classmethod
    def frid_keys(cls) -> Sequence[str]:
        """The list of keys that the class provides; the default containing class name only."""
        return [cls.__name__]

    @classmethod
    def frid_from(cls: type[_M], data: 'FridNameArgs|FridArray|StrKeyMap', /, **kwargs) -> _M:
        """Construct an instance with given name and arguments.
        - `data`: An array for a positional arguments or a map for keywords
          arguemtns, or a combination of them in the type of `FridNameArgs`.
        - `dc_ignore_extra` (only effective for dataclasses): if set, ignore
          extra positional arguments and non-matching keyward arguments;
          if set to a callback function, also call this function for each of
          the ignored arguments with the index or the name of the argument.
        - `dc_check_values` (only effective for dataclasses): if set, check
          if the value is a Frid value and is compatible with the dataclass
          field specification.
        """
        if isinstance(data, FridNameArgs):
            assert data.name in cls.frid_keys()
            args = data.args
            kwds = data.kwds
        elif isinstance(data, Sequence):
            args = data
            kwds = {}
        elif isinstance(data, Mapping):
            args = ()
            kwds = data
        else:
            raise ValueError(f"Invalid data type {type(data)}")
        if dataclasses.is_dataclass(cls):
            (args, kwds) = cls.__dc_check_fields(dataclasses.fields(cls), args, kwds, **kwargs)
        return cls(*args, **kwds)

    def frid_repr(self) -> 'FridNameArgs':
        """Converts an instance to a triplet of name, a list of positional values,
        and a dict of keyword values.

        The default implementation handles dataclasses derived from this Mixin,
        by converting the set of non-default data fields to a dict.
        but raises a NotImplementedError for other types of classes.
        """
        if dataclasses.is_dataclass(self):
            return FridNameArgs(self.__class__.__name__, (),
                                self.__dc_frid_to_dict(dataclasses.fields(self)))
        raise NotImplementedError

    @classmethod
    def __dc_check_fields(
            cls, fields: Sequence[dataclasses.Field], args: 'FridArray', kwds: 'StrKeyMap',
            *, dc_check_values: bool=False, dc_ignore_extra: Callable[[str|int],Any]|bool=False
    ) -> 'tuple[FridArray,StrKeyMap]':
        """Check positional and keyward argument values in `args` and `kwds` for dataclasses.
        - `fields`: the dataclass field specifications
        - Check `frid_from()` for the rest of arguments.
        """
        if dc_ignore_extra:
            if args and (
                n := next((i for i, f in enumerate(fields) if f.kw_only), len(fields))
            ) < len(args):
                if callable(dc_ignore_extra):
                    for i in range(n, len(args)):
                        dc_ignore_extra(i)
                args = args[:n]
            if kwds and (keys := set(kwds.keys()).difference(f.name for f in fields)):
                if callable(dc_ignore_extra):
                    for k in keys:
                        dc_ignore_extra(k)
                kwds = {k: v for k, v in kwds.items() if k not in keys}
        if dc_check_values:
            for i, v in enumerate(args):
                f = fields[i]
                cls.__check_frid_value(i, v)
                if type(f.type) is type and not isinstance(v, f.type):
                    raise ValueError(
                        f"Dataclass {cls.__name__}: field {f.name} is of type {type(v)}; "
                        f"expecting {f.type} for the positional argument at the index #{i}"
                    )
            f_map = {f.name: f for f in fields[len(args):]}
            for k, v in kwds.items():
                f = f_map[k]
                cls.__check_frid_value(k, v)
                if type(f.type) is type and not isinstance(v, f.type):
                    raise ValueError(
                        f"Dataclass {cls.__name__} field {f.name} is of type {type(v)}; "
                        f"expecting {f.type} for the keyword argument with the key {k}"
                    )
        return (args, kwds)

    def __dc_frid_to_dict(self, fields: Sequence[dataclasses.Field], ) -> dict[str,'FridValue']:
        kwds = {}
        for f in fields:
            v = getattr(self, f.name)
            if f.default is not dataclasses.MISSING and v == f.default:
                continue
            if f.default_factory is not dataclasses.MISSING and v == f.default_factory():
                continue
            self.__check_frid_value(f.name, v)
            kwds[f.name] = v
        return kwds

    @classmethod
    def __check_frid_value(cls, name: str|int, value):
        if not cls._is_frid_value(value):
            raise ValueError(
                f"Dataclass {cls.__name__}: bad type for the argument {name}: {type(value)}"
            )

    @staticmethod
    def _is_frid_value(data) -> TypeGuard['FridValue']:
        """This method is overwritten when guard.py is loaded"""
        raise NotImplementedError

# The Prime types must all be immutable and hashable
FridPrime = str|float|int|bool|BlobTypes|DateTypes|FridBasic|None
FridExtra = FridMixin|Set[FridPrime]  # Only set of primes, no other
FridMapVT = Mapping|Sequence|FridPrime|FridExtra|FridBeing  # Allow PRESENT/MISSING for dict
StrKeyMap = Mapping[str,FridMapVT]
FridSeqVT = StrKeyMap|Sequence|Set|FridPrime|FridMixin
FridArray = Sequence[FridSeqVT]
FridValue = StrKeyMap|FridArray|FridPrime|FridExtra

class FridNameArgs(NamedTuple):
    """This is a named tuple used to create and represent FridMixin."""
    name: str
    args: FridArray
    kwds: StrKeyMap

FridTypeName = Literal['frid','text','blob','list','dict','real','date','null','bool','']
FridTypeSize = tuple[FridTypeName,int]

@final
class ValueArgs(Generic[_T]):
    """Container to hold a value of specific type with positional and keyword arguments."""
    __slots__ = ('data', 'args', 'kwds')
    def __init__(self, data: _T, *args, **kwds):
        self.data = data
        self.args = args
        self.kwds = kwds
    def __args_to_str(self):
        sargs = [repr(x) for x in self.args]
        sargs.extend(str(k) + "=" + repr(v) for k, v in self.kwds.items())
        return "(" + ", ".join(sargs) + ")"
    def __str__(self):
        return str(self.data) + self.__args_to_str()
    def __repr__(self):
        return repr(self.data) + self.__args_to_str()
