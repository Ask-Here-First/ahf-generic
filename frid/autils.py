from .aio import recurring_events, map_as_aiter, gather_aiter, timeout_loop, timeout_stop
proxied_async_iterable = map_as_aiter
collect_async_iterable = gather_aiter
timeout_multi_callable = timeout_loop
timeout_async_iterable = timeout_stop
timed_issuing_iterable = recurring_events
