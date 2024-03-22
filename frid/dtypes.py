from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from datetime import date as dateonly, time as timeonly, datetime
from typing import Literal

BlobTypes = bytes|bytearray|memoryview
DateTypes = dateonly|timeonly|datetime   # Note that datetime in Python derives from date

# Frid type (Flexibly represented inteactive data)

# The Prime types including all types supported internally by default

class FridMixin(ABC):
    @abstractmethod
    def __frid__(self) -> 'FridValue':
        ...

FridPrime = str|float|int|bool|BlobTypes|DateTypes|None
StrKeyMap = Mapping[str,Mapping|Sequence|FridPrime|FridMixin]
FridValue = StrKeyMap|Sequence[StrKeyMap|Sequence|FridPrime|FridMixin]|FridPrime|FridMixin
FridArray = Sequence[FridValue]

# This is for JSON support: 5 means Json5, True means standard json, and a non-empty string
# for escaping JSON string. Any falsy value (None, False, empty string, 0 means Frid format)
JsonLevel = Literal[5]|bool|str|None