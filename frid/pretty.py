"""Base class for pretty print.
"""

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import TextIO

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
    """This abstract base class supports two kinds of mixins:
    - Data backend mixins. The data can be printed into a string or written
      to a stream, depending on the mixin.
    - Pretty format mixins. They constrol how whitespaces are inserted (e.g,
      spaces, new lines, and identitations). The default is not to insert any
      whitespaces.
    """
    @abstractmethod
    def _print(self, token: str, /):
        """This method is for the backend to override."""
        raise NotImplementedError

    def print(self, token: str, ttype: PPTokenType, /):
        """This method is for the formatter to override"""
        self._print(token)

class PPToStringMixin(PrettyPrint):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = []
    def _print(self, token: str, /):
        self.buffer.append(token)
    def __str__(self):
        return ''.join(self.buffer)

class PPToTextIOMixin(PrettyPrint):
    def __init__(self, stream: TextIO, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream = stream
    def _print(self, token: str, /):
        self.stream.write(token)
