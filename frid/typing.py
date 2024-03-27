from abc import ABC, abstractmethod
from datetime import date as dateonly, time as timeonly, datetime
from collections.abc import Mapping, Sequence

BlobTypes = bytes|bytearray|memoryview
DateTypes = dateonly|timeonly|datetime   # Note that datetime in Python derives from date

# Frid type (Flexibly represented inteactive data)

# The Prime types including all types supported internally by default

class FridMixin(ABC):
    @classmethod
    def frid_keys(cls) -> list[str]:
        return [cls.__name__]

    @classmethod
    @abstractmethod
    def frid_from(cls, name, *args, **kwas) -> 'FridMixin':
        raise NotImplementedError

    @abstractmethod
    def frid_repr(self) -> tuple[str,Sequence['FridValue'],Mapping[str,'FridValue']]:
        raise NotImplementedError

FridPrime = str|float|int|bool|BlobTypes|DateTypes|None
StrKeyMap = Mapping[str,Mapping|Sequence|FridPrime|FridMixin]
FridValue = StrKeyMap|Sequence[StrKeyMap|Sequence|FridPrime|FridMixin]|FridPrime|FridMixin
FridArray = Sequence[FridValue]
