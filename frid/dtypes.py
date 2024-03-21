from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from datetime import date, time, datetime

BlobTypes = bytes|bytearray|memoryview
DateTypes = date|time|datetime   # Note that datetime in Python derives from date

# Frid type (Flexibly represented inteactive data)

# The Prime types including all types supported internally by default

class FridMixin(ABC):
    @abstractmethod
    def __frid__(self) -> 'FridValue':
        ...

FridPrime = str|float|int|bool|BlobTypes|DateTypes|None
StrKeyMap = Mapping[str,Mapping|Sequence|FridPrime|FridMixin]
FridValue = StrKeyMap|Sequence[StrKeyMap|Sequence|FridPrime|FridMixin]|FridPrime
FridArray = Sequence[FridValue]


