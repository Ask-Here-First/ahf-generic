import heapq
from collections.abc import Callable, Iterable, Mapping

from frid.checks import as_key_value_pair

def _bound_index(limit: int, index: int|None=None, /) -> int:
    """Puts the index within the bound between 0..limit.
    - If `index` is negative, it is considered to be from the bound.
    - If `index` is None, returns the `bound` itself.
    """
    if index is None:
        return limit
    if index >= limit:
        return limit
    if index < 0:
        index += limit
    if index < 0:
        return 0
    return index

def _do_find_any(s, char_set: str, start: int, bound: int) -> int:
    """Like the `str_find_any()` below but assume 0 <= start, end <= len(s)."""
    if not char_set:
        return -1
    for index in range(start, bound):
        if s[index] in char_set:
            return index
    return -1

def str_find_any(s: str, char_set: str="", start: int=0, bound: int|None=None, /) -> int:
    """Finds in `s` the first ocurrence of any character in `char_set`.
    - `start` (inclusive) and `bound` (exclusive) gives the range of the search.
    - Returns the index between `start` (inclusive), and `bound` (exclusive),
      or -1 if not found.
    """
    n = len(s)
    return _do_find_any(s, char_set, _bound_index(n, start), _bound_index(n, bound))


_TransFunc = Callable[[str,int,int,str],tuple[int,str]]

def scan_transforms__heap(
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
            if (j := _do_find_any(s, stop_at, index, hpos)) >= 0:
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
        if (j := _do_find_any(s, stop_at, index, bound)) > index:
            out.append(s[index:j])
            index = j
        else:
            out.append(s[index:bound])
            index = bound
    return (index - start, ''.join(out))

def scan_transforms(
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
        index = _do_find_any(s, stop_at, start, bound)
        if index < 0:
            return (bound - start, s[start:bound])
        assert start <= index <= bound
        return (index - start, s[start:index])
    return scan_transforms__heap(s, transformers, start, bound, stop_at=stop_at)
