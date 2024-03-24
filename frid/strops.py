import heapq
from collections.abc import Callable, Iterable, Mapping

from frid.guards import as_key_value_pair

def _bound_index(limit: int, index: int|None=None, /) -> int:
    """Puts the index within the bound between 0..limit.
    - If `index` is negative, it is considered to be from the limit.
    - If `index` is None, returns the `bound` itself.
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
        if s[index] in char_set:
            return index
        if s[index] in escape:
            index += 1
        index += 1
    return -1

def _do_find_any_1(s: str, char_set: str, start: int, bound: int,
                   /, paired: str="", quotes: str="", escape: str="") -> int:
    if not quotes and not paired:
        return _do_find_any_0(s, char_set, start, bound)
    assert len(paired) & 1 == 0  # must be even
    opening = paired[0::2]
    closing = paired[1::2]
    stack = ""
    index = start
    while index < bound:
        c = s[index]
        if (j := opening.find(c)) >= 0:
            stack += closing[j]
        elif (j := closing.find(c)) >= 0:
            if not stack:
                raise ValueError(f"Unmatched closing {c}")
            if c != stack[-1]:
                raise ValueError(f"Unmatched: expect {stack[-1]} but get {c}")
            stack = stack[:-1]
        elif c in quotes:
            index = _do_find_any_0(s, c, index, bound, escape)
            if index < 0:
                raise ValueError(f"Missing quote {c}")
            index += 1
        elif c in escape and not quotes:
            index += 1
        elif c in char_set and not stack:
                return index
        index += 1
    return -1

def str_find_any(s: str, char_set: str="", start: int=0, bound: int|None=None,
                 /, paired: str="", quotes: str="", escape: str="") -> int:
    """Finds in `s` the first ocurrence of any character in `char_set`.
    - `start` (inclusive) and `bound` (exclusive) gives the range of the search.
    - Returns the index between `start` (inclusive), and `bound` (exclusive),
      or -1 if not found.
    """
    n = len(s)
    return _do_find_any_1(s, char_set, _bound_index(n, start), _bound_index(n, bound))


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
        for prio, (text, func) in enumerate(as_key_value_pair(transformers))
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
        # TO avpid infinite loop; do not call the same handler at same place twice
        hpos = s.find(text, max(index, hpos + 1), bound)
        if hpos >= index:
            heapq.heappush(heap, (hpos, prio, text, func))
        else:
            assert hpos < 0
    else:
        if (j := _do_find_any_0(s, stop_at, index, bound)) > index:
            out.append(s[index:j])
            index = j
        else:
            out.append(s[index:bound])
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
    - It returns a pair: the number of chars processed and the transformed string
    The transformer callback function receives the following arguments:
    - The string `s`,
    - The current index in the string,
    - The bound in the string,
    - The matched prefix (same as specified as the key in the transformers).
    - It returns an transfoedmed text to be appended to the output, as well as the
      next index where the caller should continue.
    """
    n = len(s)
    start = _bound_index(n, start)
    bound = _bound_index(n, bound)
    if not transformers:
        if not stop_at:
            return (bound - start, s[start:bound])
        index = _do_find_any_0(s, stop_at, start, bound)
        if index < 0:
            return (bound - start, s[start:bound])
        assert start <= index <= bound
        return (index - start, s[start:index])
    return str_transform__heap(s, transformers, start, bound, stop_at=stop_at)

class StringEscape:
    def __init__(self, trans_pairs: str, escape_seq: str='\\', *, extra_pairs: str='',
                 encode_hex=['x', 'u', 'U'], decode_hex=['x', 'u', 'U']):
        assert len(escape_seq) == 1
        self.escape_seq = escape_seq
        self.encode_map = self._from_pairs(escape_seq, trans_pairs)
        self.decode_map = self._from_pairs('', trans_pairs + extra_pairs)
        self.encode_hex = encode_hex
        self.decode_hex = decode_hex
        self._no_utf = True

    @staticmethod
    def _from_pairs(escape_seq: str, pairs: str):
        assert len(pairs) & 1 == 0
        return {ord(x): escape_seq + y for x, y in zip(pairs[0::2], pairs[1::2])}

    class TransTable(dict[int,str]):
        def __init__(self, default: Callable[[int],str], *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._default = default
        def __missing__(self, key: int):
            return self._default(key)

    def get_cp_encoding(self, cp: int):
        assert cp >= 0
        if (t := self.encode_map.get(cp)) is not None:
            return t
        if cp < 256:
            if cp >= 0x20 and cp < 0x7f:
                return chr(cp)
            if self.encode_hex[0] is not None:
                return self.escape_seq + self.encode_hex[0] + format(cp, "02x")
        if cp < 0x10000:
            if self.encode_hex[1] is not None:
                return self.escape_seq + self.encode_hex[1] + format(cp, "04x")
        else:
            if self.encode_hex[2] is not None:
                return self.escape_seq + self.encode_hex[2] + format(cp, "08x")
            if self.encode_hex[1] is not None:
                cpx = cp - 0x10000
                assert cpx < 0x100000
                # Return a surrogate pair
                return (self.escape_seq + self.encode_hex[1] + chr((cpx >> 12) + 0xD800)
                        + self.escape_seq + self.encode_hex[1] + chr((cpx & 0x3ff) + 0xDC00))
        return chr(cp)

    def _find_escape_seq(self, s: str, start: int, bound: int, prefix: str) -> tuple[int,str]:
        """Finds the escape sequence, to be used by `find_transforms()`."""
        index = len(prefix)
        c = s[start + index]
        if (v := self.decode_map.get(ord(c))) is not None:
            return (index + len(v), v)
        if (j := self.decode_hex.find(c)) < 0:
            raise ValueError(f"Invalid escape sequence \\{c}")
        # The 2 or 4 or 8 hex chars to load
        n = 1 << (j + 1)
        if start + index + n > bound:
            raise ValueError(f"Less than {n} letters follows \\{c} sequence")
        cp = int(s[(start + index):(start + index + n)], 16)
        return (index + n, chr(cp))

    @staticmethod
    def _exit_trans_func(s: str, start: int, bound: int, escape: str, /) -> tuple[int,str]:
        return (-1, '')

    def decode(self, s: str, stop_at: str,
               start: int=0, bound: int|None=None, /) -> tuple[int,str]:
        return str_transform(s, [
            (self.escape_seq, self._find_escape_seq),
            *((q, self._exit_trans_func) for q in stop_at)
        ], start, bound)

    def encode(self, s: str, escape_quotes: str,
               start: int=0, bound: int|None=None, /) -> str:
        if start or bound is not None:
            s = s[start:bound]
        table = self.TransTable(self.get_cp_encoding)
        for q in escape_quotes:
            table[ord(q)] = q
        return s.translate(table)