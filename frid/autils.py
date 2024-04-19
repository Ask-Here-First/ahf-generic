import time, asyncio
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable, Sequence
from typing import TypeVar

T = TypeVar('T')

class AsyncReentrantLock(asyncio.Lock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._count = 0
    async def acquire(self) -> bool:
        if not self._count:
            await super().acquire()
        self._count += 1
        return True
    def release(self):
        self._count -= 1
        if self._count <= 0:
            super().release()

async def proxied_async_iterable(data: Iterable[T]) -> AsyncIterable[T]:
    for x in data:
        yield x

async def collect_async_iterable(
        it: AsyncIterable[T], catch: type[BaseException]|None=None
) -> list[T]:
    out = []
    if catch is None:
        async for data in it:
            out.append(data)
    else:
        try:
            async for data in it:
                out.append(data)
        except catch:
            pass
    return out

def timeout_async_iterable(timeout: float|tuple[float,float], it: AsyncIterable[T]):
    x = aiter(it)
    return timeout_multi_callable(timeout, lambda: anext(x))

async def timeout_multi_callable(
        timeout: float|tuple[float,float], func: Callable[...,Awaitable[T]], *args, **kwargs,
):
    """Convert a repeated function generator"""
    if isinstance(timeout, float):
        min_wait = timeout
        max_wait = timeout
    else:
        assert isinstance(timeout, Sequence) and len(timeout) == 2
        (min_wait, max_wait) = timeout
        assert min_wait <= max_wait
    t0 = time.time()
    t1 = t0 + min_wait
    t2 = t0 + max_wait
    t = t0
    while t < t1:
        try:
            yield await asyncio.wait_for(func(*args, **kwargs), timeout=(t2 - t))
        except StopIteration:
            break
        except StopAsyncIteration:
            break
        t = time.time()


