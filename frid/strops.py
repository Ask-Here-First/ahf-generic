import heapq
from collections.abc import Callable, Iterable, Mapping

from frid.guards import as_kv_pairs

def str_split_ex(s, sep: str|None=None, maxsplit: int=-1):
    """String split extended version.
    - If the first argument `s` is not string, return as is.
    - Strip each entry and remove empty entries.
    """
    if not isinstance(s, str):
        return s  # non string is returned at isas is
    return [t for x in s.split(sep, maxsplit) if (t := x.strip())]

def str_sanitize(s: str, min_redacted_chars: int=8, /, retain: tuple[int,int]|int=4,
                 *, filler: str="....", length: tuple[str,str]|None=(" [", " chars]")) -> str:
    """Redact the content of a string `s` by keeping only the first few and last few chars.
    - `min_removed_chars`: minimum numer of characters to be removed,
    - `starting` and `stopping`: number of characters to keep on each side,
    - `filler`: string to be replaced into for redacted parts,
    - `length`: if not None, a pair of prefix and suffix for the length of the string.
    """
    n = len(s)
    if isinstance(retain, tuple):
        (lkeep, rkeep) = retain
    else:
        lkeep = rkeep = retain
    if n <= min_redacted_chars:
        x = filler
    elif n >= min_redacted_chars + lkeep + rkeep:
        x = s[:lkeep] + filler + s[-rkeep:]
    else:
        m = n - min_redacted_chars
        lk = lkeep * m // (lkeep + rkeep)
        rk = m - lk
        x = s[:lk] + filler + s[-rk:]
    if length:
        x += length[0] + str(n) + length[1]
    return x

def _bound_index(limit: int, index: int|None=None, /) -> int:
    """Puts the index within the bound between 0..limit.
    - If `index` is negative, it is considered to be from the limit.
    - If `index` is None, returns the `bound` itself.
    Note we allow index to be beyond the limit (in which case a triggered
    IndexError may cause a reload)
    """
    if index is None:
        return limit
    if index < 0:
        index += limit
        if index < 0:
            return 0
    return index

def _do_find_any_0(s, char_set: str, start: int, bound: int, /, escape: str="") -> int:
    """Like the `str_find_any()` below but assume 0 <= start, end <= len(s)."""
    if not char_set:
        return -1
    index = start
    while index < bound:
        if s[index] in escape:
            index += 2
            continue
        if s[index] in char_set:
            return index
        index += 1
    return -1

def _do_find_any_1(s: str, char_set: str, start: int, bound: int,
                   /, paired: str="", quotes: str="", escape: str="") -> int:
    if not quotes and not paired:
        return _do_find_any_0(s, char_set, start, bound, escape=escape)
    assert len(paired) & 1 == 0  # must be of even length
    opening = paired[0::2]
    closing = paired[1::2]
    stack = ""
    index = start
    while index < bound:
        c = s[index]
        # The char set to be searched may contain any character
        if not stack and c in char_set:
            return index
        if (j := opening.find(c)) >= 0:
            stack += closing[j]
        elif (j := closing.find(c)) >= 0:
            if not stack:
                raise ValueError(f"Unmatched closing {c} at index {index}")
            if c != stack[-1]:
                raise ValueError(f"Unmatched: expect {stack[-1]} but get {c} at index {index}")
            stack = stack[:-1]
        elif c in quotes:
            saved_index = index
            index = _do_find_any_0(s, c, index + 1, bound, escape)
            if index < 0:
                raise ValueError(f"Missing quote {c} at index {saved_index}")
            assert s[index] == c
        elif c in escape:
            if not quotes and (stack or not paired):
                index += 1
        index += 1
    if stack:
        raise ValueError(f"Expecting '{stack[::-1]}'")
    return -1

def str_find_any(s: str, char_set: str="", start: int=0, bound: int|None=None,
                 /, paired: str="", quotes: str="", escape: str="") -> int:
    """Finds in `s` the first ocurrence of any character in `char_set`.
    - `start` (inclusive) and `bound` (exclusive) gives the range of the search.
    - `paired`: a string in pairs for opening and closing delimiters,
      for example, "()[]". Character inside the delimiters will be skipped.
      The delimiters can be recursive.
    - `quotes`: a string for quotes (open and close delimiters must be the same).
      Quoted characters are ignored.
    - `escape`: the escape character resulting in the next character to be
      ignored. If `quotes` are set, `escape` is only valid in quotes;
      otherwise it is valid in `paired` if set; else it is valid globally.
    - Returns the index between `start` (inclusive), and `bound` (exclusive),
      or -1 if not found.
    """
    n = len(s)
    return _do_find_any_1(s, char_set, _bound_index(n, start), _bound_index(n, bound),
                          paired=paired, quotes=quotes, escape=escape)


_TransFunc = Callable[[str,int,int,str],tuple[int,str]]

def str_transform__heap(
        s: str, transformers: Iterable[tuple[str,_TransFunc]]|Mapping[str,_TransFunc],
        start: int, bound: int, /, stop_at: str="",
) -> tuple[int,str]:
    """This is an variant of `text_transform()` using `find` and a heap."""
    # Use a min-heap to handle indexes; entry is a tuple
    # (next_occcurences_index, handler_index, prefix, handler)
    heap = [
        (hpos, prio, text, func)
        for prio, (text, func) in enumerate(as_kv_pairs(transformers))
        if (hpos := s.find(text, start, bound)) >= 0
    ]
    heapq.heapify(heap)
    out: list[str] = []
    index = start
    while heap:
        (hpos, prio, text, func) = heapq.heappop(heap)
        assert hpos >= 0  # Negative index won't be in heap
        if hpos > index:
            # Copy the text between the current and the next index
            if (j := _do_find_any_0(s, stop_at, index, hpos)) >= 0:
                if j > index:
                    out.append(s[index:j])
                index = j
                break
            out.append(s[index:hpos])
        if hpos >= index:
            # Call the handler function to extract value and the updated index
            (count, value) = func(s, hpos, bound, text)
            if value:
                out.append(value)
            if count < 0:
                index = hpos
                break # Stop here because the handler completes the scanning
            index = hpos + count
        # TO avoid infinite loop; do not call the same handler at same place twice
        hpos = s.find(text, max(index, hpos + 1), bound)
        if hpos >= index:
            heapq.heappush(heap, (hpos, prio, text, func))
        else:
            assert hpos < 0
    else:
        if (j := _do_find_any_0(s, stop_at, index, bound)) >= index:
            if j > index:
                out.append(s[index:j])
                index = j
        else:
            out.append(s[index:bound])
            if bound > len(s):
                raise IndexError(f"The string terminates at {len(s)} before the bound {bound}")
            index = bound
    return (index - start, ''.join(out))

def str_transform(
        s: str, transformers: Iterable[tuple[str,_TransFunc]]|Mapping[str,_TransFunc],
        start: int=0, bound: int|None=None, /, stop_at: str="",
) -> tuple[int,str]:
    """Transform a part of a text string into a different one.
    - `s`: the input text.
    - `transformers`: map or key value pairs of a prefix to a transformer callback function.
    - `start` and `bound`:
    - `stop_at`: a list of characters where the transform will stop. Note that
      transformers match takes priority.
    - It returns a pair:
        + The number of chars processed.
        + The transformed string
    The transformer callback function receives the following arguments:
    - The string `s`,
    - The current index in the string,
    - The bound in the string,
    - The matched prefix (same as specified as the key in the transformers).
    - Returns a pair:
        + A count for consumed bytes.
        + The transformed string to be appended to output (empty string for append nothing).
    """
    assert transformers
    n = len(s)
    start = _bound_index(n, start)
    bound = _bound_index(n, bound)
    return str_transform__heap(s, transformers, start, bound, stop_at=stop_at)

# Using all common C/C++ escape sequence (07-13) plus \e (27) for ESCAPE as in bash
# The rest are filled with ASCII in 0-9a-z in order, skipping "coux" as used in C/C++.
# Control-Keyboard @ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_
# NON-PRINT ASCII  01234567890123456789012345678901
control_charmap = "0123456abtnvfr789dghijklmpqeswyz"

_control_trans_tables: dict[str,dict[int,str]] = {}  # This is a two level map
def _get_control_trans_table(eseq: str, /):
    table = _control_trans_tables.get(eseq)
    if table is None:
        table = {i: eseq + v for i, v in enumerate(control_charmap)}
        table[ord(eseq[0])] = eseq + eseq[0]
        _control_trans_tables[eseq] = table
    return table
def escape_control_chars(s: str, eseq='\\', /) -> str:
    """Escape all non-printable control characters in the string `s`.
    - The default sequence sequence for commonly used non-printables are
      the same as in C++ (include newline, tab, etc.)
    - The default sequence sequence prefix is backslash like many C-derived
      programming languages.
    """
    assert eseq
    if s.isprintable() and eseq not in s:
        return s
    return s.translate(_get_control_trans_table(eseq))

_revive_control_char_map = {v: chr(i) for i, v in enumerate(control_charmap)}
def _revive_control_transform(s: str, start: int, until: int, escape: str) -> tuple[int,str]:
    index = start + len(escape)
    if index >= until:
        return (0, "")
    c = s[index]
    if c == escape[0]:
        return (len(escape) + 1, c)
    c = _revive_control_char_map.get(c)
    if c is None:
        return (0, "")
    return (len(escape) + 1, c)
def revive_control_chars(s: str, eseq='\\', /) -> str:
    """Revives the control characters in escaped sequence done by escape_control_chars()."""
    assert eseq
    if eseq not in s:
        return s
    return str_transform(s, ((eseq, _revive_control_transform),))[1]

class StringEscapeEncode:
    def __init__(self, trans_pairs: str, escape_seq: str='\\',
                 hex_prefix: tuple[str|None,str|None,str|None]=(None, None, None)):
        self.escape_seq = escape_seq
        assert len(trans_pairs) & 1 == 0
        self.encode_map = {ord(x): escape_seq + y
                           for x, y in zip(trans_pairs[0::2], trans_pairs[1::2])}
        self.encode_map[ord(escape_seq[0])] = escape_seq + escape_seq[0]
        self.hex_prefix = hex_prefix

    def __call__(self, s: str, escape_quotes: str,
                 start: int=0, bound: int|None=None, /) -> str:
        if start or bound is not None:
            s = s[start:bound]
        table = self.TransTable(self._get_cp_encoding)
        for q in escape_quotes:
            table[ord(q)] = self.escape_seq + q
        return s.translate(table)

    class TransTable(dict[int,str]):
        def __init__(self, default: Callable[[int],str], *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._default = default
        def __missing__(self, key: int):
            return self._default(key)

    def _get_cp_encoding(self, cp: int):
        assert cp >= 0
        if (t := self.encode_map.get(cp)) is not None:
            return t
        if cp < 256:
            if cp >= 0x20 and cp < 0x7f:
                return chr(cp)
            if self.hex_prefix[0] is not None:
                return self.escape_seq + self.hex_prefix[0] + format(cp, "02x")
        if cp < 0x10000 and self.hex_prefix[1] is not None:
            return self.escape_seq + self.hex_prefix[1] + format(cp, "04x")
        if self.hex_prefix[2] is not None:
            return self.escape_seq + self.hex_prefix[2] + format(cp, "08x")
        if self.hex_prefix[1] is not None:
            cpx = cp - 0x10000
            assert 0 <= cpx < 0x100000
            # Return a surrogate pair
            return (
                self.escape_seq + self.hex_prefix[1] + format((cpx >> 10) + 0xD800, "04x")
                + self.escape_seq + self.hex_prefix[1] + format((cpx & 0x3ff) + 0xDC00, "04x")
            )
        return chr(cp)

class StringEscapeDecode:
    def __init__(self, trans_pairs: str, escape_seq: str='\\',
                 hex_prefix: tuple[str|None,str|None,str|None]=(None, None, None)):
        assert len(escape_seq) == 1
        self.escape_seq = escape_seq
        self.decode_map: dict[int,str] = {
            ord(x): y for x, y in zip(trans_pairs[1::2], trans_pairs[0::2])
        }
        self.decode_map[ord(escape_seq[0])] = escape_seq
        self.hex_prefix = hex_prefix

    def __call__(self, s: str, stop_at: str,
                 start: int=0, bound: int|None=None, /) -> tuple[int,str]:
        return str_transform(s, [
            (self.escape_seq, self._find_escape_seq),
            *((q, self._exit_trans_func) for q in stop_at)
        ], start, bound)

    def _find_escape_seq(self, s: str, start: int, bound: int, prefix: str) -> tuple[int,str]:
        """Finds the escape sequence, to be used by `find_transforms()`."""
        index = start + len(prefix)
        c = s[index]
        if (v := self.decode_map.get(ord(c))) is not None:
            return (len(prefix) + len(v), v)
        for i, x in enumerate(self.hex_prefix):
            if x is not None and s.startswith(x, index):
                n = 2 << i
                index += len(x)
                if index + n > bound:
                    raise ValueError(f"Less than {n} chars follows \\{c} sequence")
                if index + n > len(s):
                    raise IndexError(f"Less than {n} chars follows \\{c} in the current buffer")
                cp = int(s[index:(index + n)], 16)
                return (len(prefix) + len(x) + n, chr(cp))
        raise ValueError(f"Unexpected escape sequence '{prefix}{c}': '{s[start-4:index+8]}'")

    @staticmethod
    def _exit_trans_func(s: str, start: int, bound: int, escape: str, /) -> tuple[int,str]:
        return (-1, '')
