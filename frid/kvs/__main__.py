import os, asyncio, unittest
from concurrent.futures import ThreadPoolExecutor


from ..typing import MISSING
from .store import VSPutFlag, ValueStore
from .basic import MemoryValueStore
from .proxy import AsyncProxyValueStore, ValueProxyAsyncStore
from .files import FileIOValueStore

class VStoreTest(unittest.TestCase):
    def check_text_store(self, store: ValueStore):
        self.assertIsNone(store.get_text("key0"))
        self.assertTrue(store.put_frid("key0", "value0"))
        self.assertEqual(store.get_text("key0"), "value0")
        self.assertEqual(store.get_bulk(["key0", "key1"], None), ["value0", None])
        self.assertEqual(store.put_bulk({"key0": "value", "key1": "value1"},
                                        VSPutFlag.ATOMICITY|VSPutFlag.NO_CHANGE), 0)
        self.assertEqual(store.get_bulk(["key0", "key1"], None), ["value0", None])
        self.assertEqual(store.put_bulk({"key0": "value", "key1": "value1"},
                                        VSPutFlag.NO_CHANGE), 1)
        self.assertEqual(store.get_bulk(["key0", "key1"]), ["value0", "value1"])
        self.assertEqual(store.put_bulk({"key0": "value", "key1": "value1"},
                                        VSPutFlag.UNCHECKED), 2)
        self.assertEqual(store.get_bulk(["key0", "key1"]), ["value", "value1"])
        self.assertTrue(store.put_frid("key0", "0", VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_text("key0"), "value0")
        self.assertEqual(store.get_meta("key0").get("key0"), ("text", 6))
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.put_frid("key0", "0", VSPutFlag.KEEP_BOTH|VSPutFlag.NO_CREATE))
        self.assertTrue(store.put_frid("key0", "0", flags=VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_text("key0"), "0")
        self.assertEqual(store.del_bulk(["key0", "key1"]), 2)
        self.assertEqual(store.get_bulk(["key0", "key1"], None), [None, None])

    def check_blob_store(self, store: ValueStore):
        self.assertIsNone(store.get_blob("key0"))
        self.assertIs(store.put_frid("key0", b"value0"), True)
        self.assertEqual(store.get_blob("key0"), b"value0")
        self.assertEqual(store.get_bulk(["key0", "key1"], None), [b"value0", None])
        self.assertEqual(store.put_bulk({"key0": b"value", "key1": b"value1"},
                                        VSPutFlag.ATOMICITY|VSPutFlag.NO_CHANGE), 0)
        self.assertEqual(store.get_bulk(["key0", "key1"], None), [b"value0", None])
        self.assertEqual(store.put_bulk({"key0": b"value", "key1": b"value1"},
                                        VSPutFlag.UNCHECKED), 2)
        self.assertEqual(store.get_bulk(["key0", "key1"]), [b"value", b"value1"])
        self.assertTrue(store.put_frid("key0", b"0", VSPutFlag.KEEP_BOTH), True)
        self.assertEqual(store.get_blob("key0"), b"value0")
        self.assertEqual(store.get_meta("key0"), {"key0": ("blob", 6)})
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.del_frid("key0"))
        self.assertFalse(store.put_frid("key0", b"0", VSPutFlag.NO_CREATE), False)
        self.assertTrue(store.put_frid("key0", b"0", VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_blob("key0"), b"0")
        self.assertEqual(store.del_bulk(["key0", "key1"]), 2)
        self.assertEqual(store.get_bulk(["key0", "key1"], None), [None, None])

    def check_list_store(self, store: ValueStore, auto_create=False):
        self.assertFalse(store.get_list("key0")) # None or [] for Redis
        self.assertIs(store.put_frid("key0", ["value00"]), True)
        self.assertEqual(store.get_list("key0"), ["value00"])
        self.assertTrue(store.put_frid("key0", ["value01", "value02"], VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_list("key0"), ["value00", "value01", "value02"])
        self.assertEqual(store.get_list("key0", 1), "value01")
        self.assertEqual(store.get_list("key0", (1, 2)), ["value01"])
        self.assertEqual(store.get_list("key0", (1, 0)), ["value01", "value02"])
        self.assertEqual(store.get_list("key0", (1, -1)), ["value01"])
        self.assertEqual(store.get_list("key0", (-2, -1)), ["value01"])
        self.assertEqual(store.get_list("key0", (-3, -1)), ["value00", "value01"])
        self.assertEqual(store.get_list("key0", (-3, 1)), ["value00"])
        self.assertEqual(store.get_list("key0", slice(1, 2)), ["value01"])
        self.assertEqual(store.get_list("key0", slice(1, None)), ["value01", "value02"])
        self.assertEqual(store.get_list("key0", slice(None, 1)), ["value00"])
        self.assertEqual(store.get_list("key0", slice(None,2)), ["value00", "value01"])
        self.assertEqual(store.get_meta("key0"), {"key0": ("list", 3)})
        self.assertTrue(store.del_frid("key0", (1, 0)))
        self.assertEqual(store.get_meta("key0"), {"key0": ("list", 1)})
        self.assertEqual(store.get_list("key0"), ["value00"])
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.get_list("key0"))
        self.assertFalse(store.put_frid("key0", ["value0"], VSPutFlag.NO_CREATE))
        self.assertTrue(store.put_frid("key0", ["value0"]))
        self.assertEqual(store.get_list("key0"), ["value0"])
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.get_list("key0"))
        self.assertFalse(store.put_frid("key0", ["value0a", "value0b"], VSPutFlag.NO_CREATE))
        self.assertIs(store.get_frid("key0", 1), MISSING)
        self.assertTrue(store.put_frid("key0", ["value01", "value01"], VSPutFlag.NO_CHANGE))
        self.assertTrue(store.get_meta("key0"), ("list", 3))
        self.assertEqual(store.get_frid("key0", 1), "value01")
        self.assertFalse(store.put_frid("key0", ["value0x", "value0y"], VSPutFlag.NO_CHANGE))
        self.assertEqual(store.get_frid("key0", 0), "value01")
        self.assertTrue(store.put_frid("key0", ["value00", "value01"], VSPutFlag.NO_CREATE))
        self.assertEqual(store.get_frid("key0", 0), "value00")
        self.assertTrue(store.put_frid("key0", ["value02", "value03"], VSPutFlag.KEEP_BOTH))
        self.assertTrue(store.put_frid("key0", ["value04"],
                                       VSPutFlag.KEEP_BOTH | VSPutFlag.NO_CREATE))
        self.assertEqual(store.get_frid("key0", (3, -2)), [])
        self.assertEqual(store.get_frid("key0", slice(3, None)), ["value03", "value04"])
        self.assertEqual(store.get_frid("key0", slice(1, 3, 2)), ["value01"])
        self.assertFalse(store.del_frid("key0", (3, -2)))
        self.assertEqual(store.get_frid("key0", slice(3, 1, -2)), ["value03"])
        self.assertTrue(store.del_frid("key0", (4, 0)))
        self.assertEqual(store.get_frid("key0", slice(3,None)), ["value03"])
        self.assertTrue(store.del_frid("key0", slice(1)))
        self.assertEqual(store.get_frid("key0"), ["value01", "value02", "value03"])
        self.assertTrue(store.del_frid("key0", (1, 2)))
        self.assertEqual(store.get_list("key0"), ["value01", "value03"])
        self.assertTrue(store.del_frid("key0", (-2, -1)))
        self.assertEqual(store.get_list("key0"), ["value03"])
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.del_frid("key0"))
        self.assertFalse(store.del_frid("key0", 3))
        self.assertFalse(store.get_list("key0"))


    def check_dict_store(self, store: ValueStore):
        self.assertFalse(store.get_dict("key0"))  # None or empty for Redis
        self.assertTrue(store.put_frid("key0", {"n0": "value00"}))
        self.assertEqual(store.get_dict("key0"), {"n0": "value00"})
        self.assertEqual(store.get_frid("key0"), {"n0": "value00"})
        self.assertEqual(store.get_dict("key0", "n0"), "value00")
        self.assertEqual(store.get_dict("key0", ["n0"]), {"n0": "value00"})
        self.assertEqual(store.get_frid("key0", ["n0"]), {"n0": "value00"})
        self.assertTrue(store.put_frid("key0", {"n1": "value01", "n2": "value02"},
                                       VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_meta("key0"), {"key0": ('dict', 3)})
        self.assertTrue(store.del_frid("key0", "n1"))
        self.assertFalse(store.del_frid("key0", "n1"))
        self.assertEqual(store.get_dict("key0"), {"n0": "value00", "n2": "value02"})
        self.assertEqual(store.get_meta("key0"), {"key0": ('dict', 2)})
        self.assertTrue(store.del_frid("key0", ["n2"]))
        self.assertEqual(store.get_dict("key0"), {"n0": "value00"})
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.put_frid("key0", {"n0": "value0"}, VSPutFlag.NO_CREATE))
        self.assertTrue(store.put_frid("key0", {"n0": "value0"}))
        self.assertEqual(store.get_dict("key0"), {"n0": "value0"})
        self.assertFalse(store.put_frid("key0", {"n0": "value1"}, VSPutFlag.NO_CHANGE))
        self.assertEqual(store.get_dict("key0"), {"n0": "value0"})
        self.assertTrue(store.put_frid("key0", {"n0": "value1"}, VSPutFlag.NO_CREATE))
        self.assertEqual(store.get_dict("key0"), {"n0": "value1"})
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.get_dict("key0"))

    def do_test_store(self, store: ValueStore, loop: asyncio.AbstractEventLoop|None=None,
                      no_proxy: bool=False):
        self.check_text_store(store)
        self.check_blob_store(store)
        self.check_list_store(store)
        self.check_dict_store(store)
        if no_proxy:
            return
        # Note we test using Sync API so we need the following to test async API
        proxy = AsyncProxyValueStore(ValueProxyAsyncStore(store), loop=loop)
        self.check_text_store(proxy)
        self.check_blob_store(proxy)
        self.check_list_store(proxy)
        self.check_dict_store(proxy)
        proxy = AsyncProxyValueStore(ValueProxyAsyncStore(store, executor=True), loop=loop)
        self.check_text_store(proxy)
        self.check_blob_store(proxy)
        self.check_list_store(proxy)
        self.check_dict_store(proxy)
        with ThreadPoolExecutor() as executor:
            proxy = AsyncProxyValueStore(ValueProxyAsyncStore(store, executor=executor),
                                         loop=loop)
            self.check_text_store(proxy)
            self.check_blob_store(proxy)
            self.check_list_store(proxy)
            self.check_dict_store(proxy)
            proxy.finalize()

    def test_memory_store(self):
        store = MemoryValueStore()
        self.assertFalse(store.all_data())
        self.do_test_store(store)
        self.assertFalse(store.all_data())
        store.finalize()

    def test_fileio_store(self):
        root_dir = "/tmp/VStoreTest"
        sub_name = "UNITTEST"
        store = FileIOValueStore(root=root_dir).substore(sub_name)
        sub_root = os.path.join(root_dir, sub_name + ".dir")
        self.assertTrue(os.path.isdir(sub_root), f"{root_dir=}")
        for name in os.listdir(sub_root):
            path = os.path.join(sub_root, name)
            if os.path.isfile(path):
                os.unlink(path)
        self.assertFalse(os.listdir(sub_root))
        self.do_test_store(store)
        self.assertFalse(os.listdir(sub_root))
        os.rmdir(sub_root)
        os.rmdir(root_dir)

    def test_sync_redis_store(self):
        try:
            from .redis import RedisValueStore
        except Exception:
            return
        host = os.getenv('REDIS_KVS_HOST')
        if not host:
            return
        store = RedisValueStore(
            host=host, port=int(os.getenv('REDIS_KVS_PORT', 6379)),
            username=os.getenv('REDIS_KVS_USER'), password=os.getenv('REDIS_KVS_PASS'),
        ).substore("UNITTEST")
        store.wipe_all()
        self.do_test_store(store)
        store.wipe_all()
        store.finalize()

    def test_async_redis_store(self):
        try:
            from .redis import RedisAsyncStore
        except Exception:
            return
        loop = asyncio.new_event_loop()
        host = os.getenv('REDIS_KVS_HOST')
        if not host:
            return
        try:
            store = RedisAsyncStore(
                host=host, port=int(os.getenv('REDIS_KVS_PORT', 6379)),
                username=os.getenv('REDIS_KVS_USER'), password=os.getenv('REDIS_KVS_PASS'),
            ).substore("UNITTEST")
            loop.run_until_complete(store.awipe_all())
            self.do_test_store(AsyncProxyValueStore(store, loop=loop), no_proxy=True)
            loop.run_until_complete(store.awipe_all())
            loop.run_until_complete(store.finalize())
        finally:
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()

if __name__ == '__main__':
    unittest.main()
