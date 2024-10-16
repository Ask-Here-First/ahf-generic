import asyncio
from collections.abc import AsyncGenerator, AsyncIterable, Callable, Iterable
from typing import Concatenate, Literal, TypeVar, ParamSpec

_T = TypeVar('_T')
_R = TypeVar('_R')
_P = ParamSpec('_P')


class CountedAsyncLock(asyncio.Lock):
    """Counting the number of acquire()s and releases()s so that the lock is reentrant"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._count = 0
    async def acquire(self) -> Literal[True]:
        if not self._count:
            await super().acquire()
        self._count += 1
        return True
    def release(self):
        self._count -= 1
        if self._count <= 0:
            super().release()

def _id_func(x):
    return x

async def map_as_aiter(
        it: Iterable[_T]|AsyncIterable[_T],
        func: Callable[Concatenate[_T,_P],_R]=_id_func, *args: _P.args, **kwargs: _P.kwargs
) -> AsyncGenerator[_R,None]:
    """Similar to built-in `map` but returns an async iterator.
    - For each `X` of the input iterable `it`, call the `func(X,*args,**kwargs)`.
    """
    if isinstance(it, Iterable):
        for x in it:
            yield func(x, *args, **kwargs)
    else:
        async for x in it:
            yield func(x, *args, **kwargs)

async def gather_aiter(
        it: AsyncIterable[_T], *catch: type[BaseException]
) -> list[_T]:
    """Gathers all items of in an async iterable to a list.
    - Optionally `catch` and drop a list of exceptions
    """
    out = []
    if catch:
        try:
            async for data in it:
                out.append(data)
        except catch:
            pass
    else:
        async for data in it:
            out.append(data)
    return out

