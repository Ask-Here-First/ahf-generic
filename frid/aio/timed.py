import math, time, asyncio
from datetime import datetime, timedelta
from collections.abc import (
    AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Iterator
)
from typing import TypeVar


from ..guards import is_list_like

_T = TypeVar('_T')


async def timeout_loop(
        timeout: float|tuple[float,float], func: Callable[...,Awaitable[_T]], *args, **kwargs,
) -> AsyncGenerator[_T,None]:
    """Generator to call a `func(*args, **kwargs)` repeated until `timeout`."""
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

def timeout_stop(timeout: float|tuple[float,float], it: AsyncIterable[_T]) -> AsyncIterator[_T]:
    """Returns a generator with timeout stop over another async iterator."""
    x = aiter(it)
    return timeout_loop(timeout, lambda: anext(x))


def _timestamp_aiter_fix_time(time: float|None, base_time: float, default: float) -> float:
    if time is None:
        return default
    return base_time + time

async def _timestamp_aiter(
        interval: float, objects: Iterable[_T]|None=None,
        /, init_time: float|None=None, stop_time: float|None=None,
        *, max_count: int=0, factory: Callable[[int],_T]|None=None, default: _T=None,
        skip_past: bool=False,
) -> AsyncGenerator[_T,None]:
    assert interval > 0
    now = time.time()
    init_time = _timestamp_aiter_fix_time(init_time, now, now)
    stop_time = _timestamp_aiter_fix_time(stop_time, now, now + 1E15)  # Just something that is large enough
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

def _timedelta_aiter_fix_time(time: datetime|timedelta|float|None, base_time: datetime,
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

async def _timedelta_aiter(
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
    init_time = _timedelta_aiter_fix_time(init_time, now, now)
    stop_time = _timedelta_aiter_fix_time(stop_time, now, datetime.max)
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

def recurring_events(
        interval: timedelta|float, objects: Iterable[_T]|None=None,
        /, init_time: datetime|float|None=None, stop_time: datetime|float|None=None,
        *, max_count: int=0, factory: Callable[[int],_T]|None=None, default: _T=None,
        skip_past: bool=False,
) -> AsyncIterator[_T]:
    """Generates a recurrent events at a fixed interval.
    - If `objects` is not None, use this iterable as data source for each item;
      otherwise, call the `factory(i)` with the incremental index `i` starting
      from zero for the data source; if `factory` is also not set, use `default`
      value, which itself is default to None>
    - Events are only generated between wall-clock time `init_time` and `stop_time`.
    - `max_count` is the maximum number of events to generate.
    """
    if isinstance(interval, timedelta):
        assert init_time is None or isinstance(init_time, (datetime, timedelta, int))
        assert stop_time is None or isinstance(stop_time, (datetime, timedelta, int))
        return _timedelta_aiter(
            interval, objects, init_time, stop_time, max_count=max_count,
            factory=factory, default=default, skip_past=skip_past,
        )
    assert init_time is None or isinstance(init_time, (int,float))
    assert stop_time is None or isinstance(stop_time, (int,float))
    return _timestamp_aiter(
        interval, objects, init_time, stop_time, max_count=max_count,
        factory=factory, default=default, skip_past=skip_past,
    )


