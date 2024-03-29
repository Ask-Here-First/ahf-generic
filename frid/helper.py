from collections.abc import Callable, Mapping, Sequence
from typing import Concatenate, Generic, ParamSpec, TypeVar

from .typing import BlobTypes, DateTypes, FridArray, FridValue, StrKeyMap

P = ParamSpec('P')
T = TypeVar('T')
CompareFunc = Callable[Concatenate[T,FridValue,P],bool]
RecursiveFunc = Callable[Concatenate[T,FridValue,Callable[...,bool],P],bool]

class Comparator(Generic[P]):
    def __init__(
            self, *, default: bool=False,
            compare_none: CompareFunc[None,P]|None=None,
            compare_bool: CompareFunc[bool,P]|None=None,
            compare_real: CompareFunc[int|float,P]|None=None,
            compare_text: CompareFunc[str,P]|None=None,
            compare_blob: CompareFunc[BlobTypes,P]|None=None,
            compare_date: CompareFunc[DateTypes,P]|None=None,
            compare_list: RecursiveFunc[FridArray,P]|None=None,
            compare_dict: RecursiveFunc[StrKeyMap,P]|None=None
    ):
        self._default: bool = default
        self._compare_none: CompareFunc[None,P] = compare_none or self.is_none
        self._compare_bool: CompareFunc[bool,P] = compare_bool or self.equal_item
        self._compare_real: CompareFunc[int|float,P] = compare_real or self.equal_item
        self._compare_text: CompareFunc[str,P] = compare_text or self.equal_item
        self._compare_blob: CompareFunc[BlobTypes,P] = compare_blob or self.equal_item
        self._compare_date: CompareFunc[DateTypes,P] = compare_date or self.equal_item
        self._compare_list: RecursiveFunc[FridArray,P] = compare_list or self.equal_list
        self._compare_dict: RecursiveFunc[StrKeyMap,P] = compare_dict or self.equal_dict

    def __call__(self, d1: FridValue, d2: FridValue,
                 /, *args: P.args, **kwargs: P.kwargs) -> bool:
        if d1 is None:
            return self._compare_none(d1, d2, *args, **kwargs)
        if isinstance(d1, bool):
            return self._compare_bool(d1, d2, *args, **kwargs)
        if isinstance(d1, int|float):
            return self._compare_real(d1, d2, *args, **kwargs)
        if isinstance(d1, str):
            return self._compare_text(d1, d2, *args, **kwargs)
        if isinstance(d1, BlobTypes):
            return self._compare_blob(d1, d2, *args, **kwargs)
        if isinstance(d1, DateTypes):
            return self._compare_date(d1, d2, *args, **kwargs)
        if isinstance(d1, Sequence):
            return self._compare_list(d1, d2, self, *args, **kwargs)
        if isinstance(d1, Mapping):
            return self._compare_dict(d1, d2, self, *args, **kwargs)
        return self._default

    @staticmethod
    def is_none(d1: None, d2: FridValue,
                /, *args: P.args, **kwargs: P.kwargs) -> bool:
        return d2 is None

    @staticmethod
    def equal_item(d1: str|int|float|DateTypes|BlobTypes, d2: FridValue,
                   /, *args: P.args, **kwargs: P.kwargs) -> bool:
        return d1 == d2

    @staticmethod
    def equal_list(d1: FridArray, d2: FridValue, /, comparator: Callable[...,bool],
                   *args: P.args, **kwargs: P.kwargs) -> bool:
        if not isinstance(d2, Sequence):
            return False
        return len(d1) == len(d2) and all(
            comparator(x, d2[i], *args, **kwargs) for i, x in enumerate(d1)
        )

    @staticmethod
    def equal_dict(d1: StrKeyMap, d2: FridValue, /, comparator: Callable[...,bool],
                   *args: P.args, **kwargs: P.kwargs) -> bool:
        if not isinstance(d2, Mapping):
            return False
        return len(d1) == len(d2) and all(
            k in d2 and comparator(v, d2[k], *args, **kwargs) for k, v in d1.items()
        )

    @staticmethod
    def is_submap(d1: StrKeyMap, d2: FridValue, /, comparator: Callable[...,bool],
                  *args: P.args, **kwargs: P.kwargs) -> bool:
        """Returns true iff `d2` is a submap of `d1`."""
        if not isinstance(d2, Mapping):
            return False
        return all(
            k in d2 and comparator(v, d2[k], *args, **kwargs) for k, v in d1.items()
        )
