import math, time, asyncio
from datetime import datetime, timedelta
from collections.abc import (
    AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator
)
from typing import TypeVar

from .guards import is_list_like

_T = TypeVar('_T')

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

async def proxied_async_iterable(data: Iterable[_T]) -> AsyncIterable[_T]:
    for x in data:
        yield x

async def collect_async_iterable(
        it: AsyncIterable[_T], *catch: type[BaseException]
) -> list[_T]:
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

def timeout_async_iterable(timeout: float|tuple[float,float],
                           it: AsyncIterable[_T]) -> AsyncIterator[_T]:
    x = aiter(it)
    return timeout_multi_callable(timeout, lambda: anext(x))

async def timeout_multi_callable(
        timeout: float|tuple[float,float], func: Callable[...,Awaitable[_T]], *args, **kwargs,
) -> AsyncGenerator[_T,None]:
    """Convert a repeated function generator"""
    if isinstance(timeout, int|float):
        min_wait = timeout
        max_wait = timeout
    else:
        assert is_list_like(timeout, int|float) and len(timeout) == 2
        (min_wait, max_wait) = timeout
        assert min_wait <= max_wait
    t0 = time.time()
    t1 = t0 + min_wait
    t2 = t0 + max_wait
    t = t0
    while t < t1:
        try:
            yield await asyncio.wait_for(func(*args, **kwargs), timeout=(t2 - t))
        except asyncio.TimeoutError:
            break
        except StopIteration:
            break
        except StopAsyncIteration:
            break
        t = time.time()

def _timed_aiter_fix_timestamp(time: float|None, base_time: float, default: float) -> float:
    if time is None:
        return default
    return base_time + time

async def _timed_aiter_timestamp(
        interval: float, objects: Iterable[_T]|None=None,
        /, init_time: float|None=None, stop_time: float|None=None,
        *, max_count: int=0, factory: Callable[[int],_T]|None=None, default: _T=None,
        skip_past: bool=False,
) -> AsyncGenerator[_T,None]:
    assert interval > 0
    now = time.time()
    init_time = _timed_aiter_fix_timestamp(init_time, now, now)
    stop_time = _timed_aiter_fix_timestamp(stop_time, now, now + 1E15)  # Just something that is large enough
    if isinstance(objects, Iterable) and not isinstance(objects, Iterator):
        objects = iter(objects)
    next_ts = init_time
    if skip_past and now > next_ts:
        next_ts += math.ceil((now - next_ts) / interval) * interval
    n = 0
    while next_ts <= stop_time:
        t = time.time()
        if t < next_ts:
            await asyncio.sleep(next_ts - t)
        if objects is not None:
            try:
                yield next(objects)
            except StopIteration:
                break
        elif factory is not None:
            yield factory(n)
        else:
            yield default
        n += 1
        if n >= max_count > 0:
            break
        next_ts += interval

def _timed_aiter_fix_timedelta(time: datetime|timedelta|float|None, base_time: datetime,
                               default: datetime|timedelta|float) -> datetime:
    if time is None:
        time = default
    if isinstance(time, datetime):
        if time.tzinfo is None and base_time.tzinfo is not None:
            return time.replace(tzinfo=base_time.tzinfo)
        return time
    if isinstance(time, timedelta):
        return base_time + time
    if isinstance(time, (int, float)):
        return base_time + timedelta(seconds=time)
    raise ValueError(f"Invalid data type: {type(time)}")

async def _timed_aiter_timedelta(
        interval: timedelta, objects: Iterable[_T]|None=None,
        /, init_time: datetime|timedelta|float|None=None,
        stop_time: datetime|timedelta|float|None=None,
        *, max_count: int=0, factory: Callable[[int],_T]|None=None, default: _T=None,
        skip_past: bool=False,
) -> AsyncGenerator[_T,None]:
    assert interval.total_seconds() > 0
    tzinfo = None
    if isinstance(init_time, datetime) and init_time is not None:
        tzinfo = init_time.tzinfo
    if isinstance(stop_time, datetime) and stop_time is not None:
        tzinfo = stop_time.tzinfo
    now = datetime.now(tzinfo)
    init_time = _timed_aiter_fix_timedelta(init_time, now, now)
    stop_time = _timed_aiter_fix_timedelta(stop_time, now, datetime.max)
    if isinstance(objects, Iterable) and not isinstance(objects, Iterator):
        objects = iter(objects)
    next_time = init_time
    if skip_past:
        while now > next_time:
            next_time += interval
    n = 0
    while next_time <= stop_time:
        next_ts = next_time.timestamp()
        t = time.time()
        if t < next_ts:
            await asyncio.sleep(next_ts - t)
        if objects is not None:
            try:
                yield next(objects)
            except StopIteration:
                break
        elif factory is not None:
            yield factory(n)
        else:
            yield default
        n += 1
        if n >= max_count > 0:
            break
        next_time += interval

def timed_issuing_iterable(
        interval: timedelta|float, objects: Iterable[_T]|None=None,
        /, init_time: datetime|float|None=None, stop_time: datetime|float|None=None,
        *, max_count: int=0, factory: Callable[[int],_T]|None=None, default: _T=None,
        skip_past: bool=False,
) -> AsyncIterator[_T]:
    if isinstance(interval, timedelta):
        assert init_time is None or isinstance(init_time, (datetime, timedelta, int))
        assert stop_time is None or isinstance(stop_time, (datetime, timedelta, int))
        return _timed_aiter_timedelta(
            interval, objects, init_time, stop_time, max_count=max_count,
            factory=factory, default=default, skip_past=skip_past,
        )
    assert init_time is None or isinstance(init_time, (int,float))
    assert stop_time is None or isinstance(stop_time, (int,float))
    return _timed_aiter_timestamp(
        interval, objects, init_time, stop_time, max_count=max_count,
        factory=factory, default=default, skip_past=skip_past,
    )
