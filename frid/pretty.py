"""Base class for pretty print.
"""

from abc import ABC, abstractmethod
from enum import Enum, auto

class PPTokenType(Enum):
    START = auto()
    CLOSE = auto()
    LABEL = auto()
    ENTRY = auto()
    PIECE = auto()
    SEP_0 = auto()
    SEP_1 = auto()
    OPT_0 = auto()
    OPT_1 = auto()

class PrettyPrint(ABC):
    @abstractmethod
    def _print(self, token: str, /):
        raise NotImplementedError

    def print(self, token: str, ttype: PPTokenType, /):
        self._print(token)

