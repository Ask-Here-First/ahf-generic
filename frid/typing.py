import types
from abc import ABC, abstractmethod
from datetime import date as dateonly, time as timeonly, datetime
from collections.abc import Mapping, Sequence, Set
from typing import TypeVar

# Quick union types used in many places
BlobTypes = bytes|bytearray|memoryview
DateTypes = dateonly|timeonly|datetime   # Note that datetime in Python is deriveed from date

# FRID types follow (Flexibly represented inteactive data)

T = TypeVar('T', bound='FridMixin')

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
    def frid_from(cls: type[T], name: str, *args: 'FridValue', **kwds: 'FridValue') -> T:
        """Construct an instance with given name and arguments."""
        assert name in cls.frid_keys()
        return cls(*args, **kwds)

    @abstractmethod
    def frid_repr(self) -> tuple[str,'FridArray','StrKeyMap']:
        """Converts an instance to a triplet of name, a list of positional values,
        and a dict of keyword values.
        """
        raise NotImplementedError

# The Prime types must all be immutable and hashable
FridPrime = str|float|int|bool|BlobTypes|DateTypes|None
FridExtra = FridMixin|Set[FridPrime]  # Only set of primes, no other
FridMapVT = Mapping|Sequence|FridPrime|FridExtra|types.EllipsisType  # Allow ... for dict value
StrKeyMap = Mapping[str,FridMapVT]
FridSeqVT = StrKeyMap|Sequence|Set|FridPrime|FridMixin
FridArray = Sequence[FridSeqVT]
FridValue = StrKeyMap|FridArray|FridPrime|FridExtra
