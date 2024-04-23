import os, unittest
from concurrent.futures import ThreadPoolExecutor

from .store import VSPutFlag, ValueStore
from .basic import MemoryValueStore
from .proxy import AsyncToSyncProxyStore, SyncToASyncProxyStore

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
        self.assertEqual(store.get_meta(["key0"]).get("key0"), ("text", 6))
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
        self.assertEqual(store.get_meta(["key0"]), {"key0": ("blob", 6)})
        self.assertTrue(store.del_frid("key0"))
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
        self.assertEqual(store.get_meta(["key0"]), {"key0": ("list", 3)})
        self.assertTrue(store.del_frid("key0", (1, 0)))
        self.assertEqual(store.get_meta(["key0"]), {"key0": ("list", 1)})
        self.assertEqual(store.get_list("key0"), ["value00"])
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.get_list("key0"))
        self.assertFalse(store.put_frid("key0", ["value0"], VSPutFlag.NO_CREATE))
        self.assertTrue(store.put_frid("key0", ["value0"]))
        self.assertEqual(store.get_list("key0"), ["value0"])
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.get_list("key0"))

    def check_dict_store(self, store: ValueStore, auto_create=False):
        self.assertFalse(store.get_dict("key0"))  # None or empty for Redis
        self.assertTrue(store.put_frid("key0", {"n0": "value00"}))
        self.assertEqual(store.get_dict("key0"), {"n0": "value00"})
        self.assertEqual(store.get_dict("key0", "n0"), "value00")
        self.assertEqual(store.get_dict("key0", ["n0"]), {"n0": "value00"})
        self.assertTrue(store.put_frid("key0", {"n1": "value01", "n2": "value02"},
                                       VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_meta(["key0"]), {"key0": ('dict', 3)})
        self.assertTrue(store.del_frid("key0", "n1"))
        self.assertEqual(store.get_dict("key0"), {"n0": "value00", "n2": "value02"})
        self.assertEqual(store.get_meta(["key0"]), {"key0": ('dict', 2)})
        self.assertTrue(store.del_frid("key0", ["n2"]))
        self.assertEqual(store.get_dict("key0"), {"n0": "value00"})
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.put_frid("key0", {"n0": "value0"}, VSPutFlag.NO_CREATE))
        self.assertTrue(store.put_frid("key0", {"n0": "value0"}))
        self.assertEqual(store.get_dict("key0"), {"n0": "value0"})
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.get_dict("key0"))

    def do_test_store(self, store: ValueStore, no_async=False):
        self.check_text_store(store)
        self.check_blob_store(store)
        self.check_list_store(store)
        self.check_dict_store(store)
        if no_async:
            return
        proxy = SyncToASyncProxyStore(store)
        self.check_text_store(proxy)
        self.check_blob_store(proxy)
        self.check_list_store(proxy)
        self.check_dict_store(proxy)
        # Note we test using Sync API so we need the following to test async API
        proxy = SyncToASyncProxyStore(AsyncToSyncProxyStore(store))
        self.check_text_store(proxy)
        self.check_blob_store(proxy)
        self.check_list_store(proxy)
        self.check_dict_store(proxy)
        proxy = SyncToASyncProxyStore(AsyncToSyncProxyStore(store, executor=True))
        self.check_text_store(proxy)
        self.check_blob_store(proxy)
        self.check_list_store(proxy)
        self.check_dict_store(proxy)
        with ThreadPoolExecutor() as executor:
            proxy = SyncToASyncProxyStore(AsyncToSyncProxyStore(store, executor=executor))
            self.check_text_store(proxy)
            self.check_blob_store(proxy)
            self.check_list_store(proxy)
            self.check_dict_store(proxy)

    def test_memory_store(self):
        store = MemoryValueStore()
        self.assertFalse(store.all_data())
        self.do_test_store(store)
        self.assertFalse(store.all_data())

    def test_redis_store(self):
        try:
            from .redis import RedisValueStore
        except Exception:
            return
        host = os.getenv('REDIS_KVS_HOST')
        if not host:
            return
        store = RedisValueStore(
            host=host, port=int(os.getenv('REDIS_KVS_PORT', 6379)),
            username=os.getenv('REDIS_KVS_USER'), password=os.getenv('REDIS_KVS_PASS')
        ).substore("UNITTEST")
        store.wipe_all()
        self.do_test_store(store, no_async=True)
        store.wipe_all()

if __name__ == '__main__':
    unittest.main()
