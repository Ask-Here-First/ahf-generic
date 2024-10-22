
from collections.abc import Sequence

def list_find_ex(a_list: Sequence, item, start: int=0, end: int|None = None,
                 *, not_found: int=-1) -> int:
    """Find items in a list but do not raise ValueError on missing.
    - Returns the index of the first occurrence of the `item` in the list.
    - Search starting at `start` index (included) to the `end` index (encluded).
    - If not found, return `not_found` (default to -1, but can be set to,
      for example, the length of the list or the value of `end` if given).
    """
    try:
        if end is None:
            return a_list.index(item, start)
        return a_list.index(item, start, end)
    except ValueError:
        return not_found
