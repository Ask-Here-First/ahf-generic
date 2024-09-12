"""Base class for pretty print.
"""

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import TextIO

class PPTokenType(Enum):
    START = auto()      # The block starting token, such as [{(
    CLOSE = auto()      # The block ending token, such as )}]
    LABEL = auto()      # The label (the key of dict)
    ENTRY = auto()      # The entry: prime values, including list items and dict values
    PIECE = auto()      # The partial data of an entry
    SEP_0 = auto()      # The primary separator (e.g., comma)
    SEP_1 = auto()      # The secondary separator (e.g., colon)
    OPT_0 = auto()      # The optional primary separator (e.g., comma at the end)
    OPT_1 = auto()      # The optional secondary secondary (e.g, colon with no value)

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
        """Default token print behavior:
        - Do not show optional separator.
        - Add a space after the required seqarator ',:'.
        """
        if ttype not in (PPTokenType.OPT_0, PPTokenType.OPT_1):
            self._print(token)
        if ttype in (PPTokenType.SEP_0, PPTokenType.SEP_1) and token in ':,':
            self._print(' ')

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

class MultilineFormatMixin(PrettyPrint):
    def __init__(self, *args, indent: int|str|None=None, extra_comma=False,
                 break_quoted: bool=False,
                 newline: str='\n', newline_after: str="{[", **kwargs):
        super().__init__(*args, **kwargs)
        self.indent = ' ' * indent if isinstance(indent, int) else indent
        self.newline = newline
        self.extra_comma = extra_comma
        self.break_quoted = break_quoted
        self.newline_after = newline_after
        self._level = 0
        self._delta: list[bool] = []
        self._indented_back = False
        self._start_newline = False
    def print(self, token: str, ttype: PPTokenType, /):
        if self.indent is None:
            return super().print(token, ttype)
        if self._start_newline or self._indented_back:
            prefix = self.newline + self.indent * self._level
        else:
            prefix = ''
        self._indented_back = False
        self._start_newline = False
        match ttype:
            case PPTokenType.START:
                if token in self.newline_after:
                    self._level += 1
                    self._start_newline = True
                self._delta.append(self._start_newline)
            case PPTokenType.CLOSE:
                self._indented_back = self._delta.pop()
                # Need to recompute the prefix
                if self._indented_back:
                    self._level -= 1
                    prefix = self.newline + self.indent * self._level
            case PPTokenType.SEP_0:
                prefix = ''
                self._start_newline = self._delta and self._delta[-1]
                if not self._start_newline:
                    token += ' '
            case PPTokenType.SEP_1:
                if token == ':':
                    token += ' '
            case PPTokenType.OPT_0:
                prefix = ''
                self._start_newline = self._delta and self._delta[-1]
                if not self._start_newline or not self.extra_comma:
                    token = ''
            case PPTokenType.OPT_1:
                token = ''
            case PPTokenType.ENTRY:
                if self.break_quoted:
                    lines = self._split_quoted_lines(token)
                    if lines is not None:
                        if not prefix:
                            prefix = self.newline + self.indent * (self._level + 1)
                        token = ('"' + prefix + '"').join(lines)
        if prefix:
            self._print(prefix)
        if token:
            self._print(token)
        if self._level <= 0:
            self._print(self.newline)
    def _split_quoted_lines(self, token: str) -> list[str]|None:
        """Split quoted multi-line string into multiple strings."""
        if not token.startswith('"') and not token.endswith('"'):
            return None
        lines = []
        index = 0
        prev_index = 0
        while (index := token.find("\\n", index)) >= 0:
            index += 2
            # Check if the quoted string is at the end
            if index >= len(token) or token[index] == '"':
                continue
            # Count the number of preceding backslash
            n = 0
            while index > n + 2 and token[index-n-3] == '\\':
                n += 1
            if n & 1:
                continue
            lines.append(token[prev_index:index])
            prev_index = index
        if not lines:
            return None
        if prev_index < len(token):
            lines.append(token[prev_index:])
        return lines
