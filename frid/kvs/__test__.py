"""Unit test code for various implementations of ValueStore and AsyncStore

To run against Redis, `redis-server` must be available but not already run.
(TODO: maybe on a different port in case that the system level redis service is running?)

To run AsyncStore against PostgreSQL (with dependencies `sqlalchemy psycopg[binary]`):

```
time DBSQL_VALUE_STORE_TEST_URL='postgresql+psycopg://postgres:PASSWORD@HOSTNAME' \
    FRID_LOG_LEVEL=trace python3 -m unittest \
        frid.kvs.__test__.VStoreTestDbsql.test_dbsql_value_store
```

To run AsyncStore against PostgreSQL (with dependencies `sqlalchemy asyncpg`):
```
time DBSQL_ASYNC_STORE_TEST_URL='postgresql+asyncpg://postgres:PASSWORD@HOSTNAME' \
    FRID_LOG_LEVEL=trace python3 -m unittest \
        frid.kvs.__test__.VStoreTestDbsql.test_dbsql_async_store
```
"""

import os, time, random, asyncio, unittest, subprocess
from logging import info
from concurrent.futures import ThreadPoolExecutor


from ..typing import MISSING
from .._basic import frid_random
from ..lib import get_loglevel_str
from .store import VSPutFlag, ValueStore
from .basic import MemoryValueStore
from .proxy import AsyncProxyValueStore, ValueProxyAsyncStore
from .files import FileIOValueStore

class _VStoreTestBase(unittest.TestCase):
    def check_text_store(self, store: ValueStore):
        self.assertEqual(set(store.get_keys()), set())
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
        self.assertEqual(set(store.get_keys()), {"key0", "key1"})
        self.assertEqual(set(store.get_keys("key0")), {"key0"})
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

    def check_list_store(self, store: ValueStore):
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
        self.assertFalse(store.put_frid("key0", [], VSPutFlag.KEEP_BOTH))
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
        self.assertFalse(store.put_frid("key0", {}, VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_meta("key0"), {"key0": ('dict', 3)})
        self.assertTrue(store.del_frid("key0", "n1"))
        self.assertFalse(store.del_frid("key0", "n1"))
        self.assertEqual(store.get_dict("key0"), {"n0": "value00", "n2": "value02"})
        self.assertEqual(store.get_meta("key0"), {"key0": ('dict', 2)})
        self.assertTrue(store.del_frid("key0", ["n2"]))
        self.assertEqual(store.get_dict("key0"), {"n0": "value00"})
        self.assertTrue(store.put_frid("key0", {"n1": "value01"}))
        self.assertEqual(store.get_dict("key0"), {"n1": "value01"})
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.put_frid("key0", {"n0": "value0"}, VSPutFlag.NO_CREATE))
        self.assertTrue(store.put_frid("key0", {"n0": "value0"}))
        self.assertEqual(store.get_dict("key0"), {"n0": "value0"})
        self.assertFalse(store.put_frid("key0", {"n0": "value1"}, VSPutFlag.NO_CHANGE))
        self.assertEqual(store.get_dict("key0"), {"n0": "value0"})
        self.assertTrue(store.put_frid("key0", {"n0": "value1"}, VSPutFlag.NO_CREATE))
        self.assertEqual(store.get_dict("key0"), {"n0": "value1"})
        self.assertTrue(store.put_frid("key0", {"n0": "value02"}, VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_dict("key0"), {"n0": "value02"})
        self.assertTrue(store.put_frid("key0", {"n0": "value03", "n1": "value10"},
                                       VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_dict("key0"), {"n0": "value03", "n1": "value10"})
        self.assertTrue(store.put_frid("key0", {"n1": "value11"},
                                       VSPutFlag.KEEP_BOTH))
        self.assertEqual(store.get_dict("key0"), {"n0": "value03", "n1": "value11"})
        self.assertTrue(store.del_frid("key0"))
        self.assertFalse(store.get_dict("key0"))

    def check_random(self, store: ValueStore, *, exact=False):
        rng = random.Random(0)
        for _ in range(64):
            # Note: for some backends, falsy value are the same as empty
            data = frid_random(rng)
            if data and exact:
                self.assertTrue(store.put_frid("key", data))
                self.assertEqual(store.get_frid("key"), data)
                self.assertTrue(store.del_frid("key"))
            else:
                store.put_frid("key", data)
                if data:
                    self.assertEqual(store.get_frid("key"), data)
                else:
                    self.assertFalse(store.get_frid("key"))
                store.del_frid("key")

    def check_store(self, store: ValueStore, *, exact=False):
        self.check_text_store(store)
        self.check_blob_store(store)
        self.check_list_store(store)
        self.check_dict_store(store)
        self.check_random(store, exact=exact)

    def do_test_store(self, store: ValueStore, loop: asyncio.AbstractEventLoop|None=None,
                      no_proxy: bool=False, exact=False):
        self.check_store(store, exact=exact)
        if no_proxy:
            return
        # Note we test using Sync API so we need the following to test async API
        proxy = AsyncProxyValueStore(ValueProxyAsyncStore(store), loop=loop)
        self.check_store(proxy, exact=exact)
        proxy.finalize(1)
        proxy = AsyncProxyValueStore(ValueProxyAsyncStore(store, executor=True), loop=loop)
        self.check_store(proxy, exact=exact)
        proxy.finalize(1)
        with ThreadPoolExecutor() as executor:
            proxy = AsyncProxyValueStore(ValueProxyAsyncStore(store, executor=executor),
                                         loop=loop)
            self.check_store(proxy, exact=exact)
            proxy.finalize(1)

class VStoreTestMemoryAndFile(_VStoreTestBase):
    def test_memory_store(self):
        store = MemoryValueStore()
        self.assertFalse(store.all_data())
        self.do_test_store(store, exact=True)
        self.assertFalse(store.all_data())
        store.finalize()

    def test_fileio_store(self):
        root_dir = os.path.join(os.getenv('TEMP', "/tmp"), "VStoreTest")
        sub_name = "UNITTEST"
        store = FileIOValueStore(root_dir).substore(sub_name)
        sub_root = os.path.join(root_dir, sub_name + FileIOValueStore.SUBSTORE_EXT)
        self.assertTrue(os.path.isdir(sub_root), f"{root_dir=}")
        for name in os.listdir(sub_root):
            path = os.path.join(sub_root, name)
            if os.path.isfile(path):
                os.unlink(path)
        self.assertFalse(os.listdir(sub_root))
        self.do_test_store(store, exact=True)
        self.assertFalse(os.listdir(sub_root))
        os.rmdir(sub_root)
        os.rmdir(root_dir)

class VStoreTestRedis(_VStoreTestBase):
    @classmethod
    def setUpClass(cls):
        # Do not write anything to disk
        cmd = ["redis-server", "--save", "", "--appendonly", "no"]
        cls.cmdline = ' '.join(x or '""' for x in cmd)
        try:
            cls.process = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True
            )
        except Exception as e:
            cls.process = None
            info(f"Failed to start `{cls.cmdline}`: {e}")
        else:
            info(f"Started process {cls.process.pid}: {cls.cmdline}")
        time.sleep(1.0)
    @classmethod
    def tearDownClass(cls):
        if cls.process is None:
            return
        time.sleep(0.5)
        pid = cls.process.pid
        if cls.process.poll() is None:
            cls.process.terminate()
        exit_code = cls.process.wait()
        info(f"Completed process {pid} with code {exit_code}: {cls.cmdline}")

    def check_redis(self):
        try:
            import redis  # noqa: F401
        except ImportError:
            raise unittest.SkipTest("Skip Redis test as the `redis` package is not installed.")
        if self.process is None:
            raise unittest.SkipTest(f"Process `{self.cmdline}` was not started")
        if (exit_code := self.process.poll()) is not None:
            raise unittest.SkipTest(f"Process exits with code {exit_code}: {self.cmdline}")

    def test_redis_value_store(self):
        self.check_redis()
        from .redis import RedisValueStore
        store = RedisValueStore().substore("UNITTEST")
        store.wipe_all()
        self.do_test_store(store, exact=False)
        store.wipe_all()
        store.finalize()

    def test_redis_async_store(self):
        self.check_redis()
        from .redis import RedisAsyncStore
        loop = asyncio.new_event_loop()
        try:
            store = RedisAsyncStore().substore("UNITTEST")
            loop.run_until_complete(store.wipe_all())
            self.do_test_store(AsyncProxyValueStore(store, loop=loop),
                               no_proxy=True, exact=False)
            loop.run_until_complete(store.wipe_all())
            loop.run_until_complete(store.finalize())
        finally:
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()

class VStoreTestDbsql(_VStoreTestBase):
    def check_dbsql(self) -> bool:
        """Check if the store has all dependencies, and returns if echo is enabled."""
        try:
            import aiosqlite, sqlalchemy, greenlet  # noqa: F401
        except ImportError:
            raise unittest.SkipTest("Skip Dbsql tests as sqlalchemy is not installed")
        return get_loglevel_str() == 'trace'
    def create_tables(self, aio: bool,*, echo=False, **kwargs):
        from sqlalchemy import (
            MetaData, Table, Column, String, LargeBinary, Integer,
            UniqueConstraint
        )
        dburl = os.getenv('DBSQL_ASYNC_STORE_TEST_URL' if aio else 'DBSQL_VALUE_STORE_TEST_URL')
        if dburl:
            dbfile = None
        else:
            dbfile = "/tmp/VStoreTest.sdb"
            if aio:
                dburl = "sqlite+aiosqlite:///" + dbfile
            else:
                dburl = "sqlite+pysqlite:///" + dbfile
        table_name1 = "unittest_table1"
        table_name2 = "unittest_table2"

        self.remove_tables(dburl, dbfile, table_name1, table_name2, aio,
                           echo=echo, **kwargs)

        metadata = MetaData()
        table1 = Table(
            table_name1, metadata,
            Column('id', String, primary_key=True),
            Column('frid', String, nullable=False),
            Column('n0', String, nullable=True),
            Column('n1', LargeBinary, nullable=True),
            # The two fields are set to constants if text_field/blob_field are not set
            Column('text', String, nullable=True),
            Column('blob', LargeBinary, nullable=True),
        )
        table2 = Table(
            table_name2, metadata,
            Column('id', String, nullable=False),
            Column('frid', String, nullable=False),
            Column('n0', String, nullable=True),
            Column('n1', LargeBinary, nullable=True),
            Column('mapkey', String, nullable=True),
            Column('seqind', Integer, nullable=True),
            # Use UniqueConstrait because key cannot be null
            UniqueConstraint('id', 'mapkey', 'seqind'),
        )
        if echo:
            print("Creating the following tables")
            print("***", table_name1)
            for col in table1.c:
                print("   ", repr(col))
            print("***", table_name2)
            for col in table2.c:
                print("   ", repr(col))
        return (dburl, dbfile, table1, table2)

    def remove_tables(self, dburl: str, dbfile: str|None, table1: str, table2: str,
                      aio: bool, *, echo: bool=False, **kwargs):
        from sqlalchemy import create_engine, MetaData
        metadata = MetaData()
        if aio:
            from sqlalchemy.ext.asyncio import create_async_engine
            async def create_all():
                engine = create_async_engine(dburl, echo=echo, **kwargs)
                async with engine.begin() as conn:
                    await conn.run_sync(metadata.reflect)
                    for k in (table1, table2):
                        t = metadata.tables.get(k)
                        if t is not None:
                            await conn.run_sync(t.drop)
                await engine.dispose()
            asyncio.run(create_all())
        else:
            from sqlalchemy import create_engine, MetaData
            engine = create_engine(dburl, echo=echo, **kwargs)
            with engine.begin() as conn:
                metadata.reflect(engine)
                for k in (table1, table2):
                    t = metadata.tables.get(k)
                    if t is not None:
                        t.drop(conn)
            engine.dispose()
        if dbfile is None:
            return
        try:
            os.unlink(dbfile)
        except Exception:
            pass

    def test_dbsql_value_store(self):
        try:
            import sqlalchemy  # noqa: F401
        except ImportError:
            raise unittest.SkipTest("Skip Dbsql tests as sqlalchemy is not installed")
        echo = get_loglevel_str() == 'trace'
        from .dbsql import DbsqlValueStore

        # Log only in trace level
        (dburl, dbfile, table1, table2) = self.create_tables(False, echo=echo)

        # Single frid columm
        store = DbsqlValueStore.from_url(
            dburl, table1, engine_args={'echo': echo}, frid_field=True,
            col_values={'text': "(UNUSED)", 'blob': b"(UNUSED)"}
        )
        self.assertTrue(store._frid_column is not None
                        and store._frid_column.name == 'frid')
        self.assertTrue(store._text_column is None)
        self.assertTrue(store._blob_column is None)
        self.do_test_store(store, exact=True)
        store.finalize()

        # Separate text columm
        store = DbsqlValueStore.from_url(
            dburl, table1.name, engine_args={'echo': echo}, frid_field=True,
            text_field='text', col_values={'blob': b"(UNUSED)"}
        )
        self.assertTrue(store._frid_column is not None
                        and store._frid_column.name == 'frid')
        self.assertTrue(store._text_column is not None
                        and store._text_column.name == 'text')
        self.assertTrue(store._blob_column is None)
        self.do_test_store(store, exact=True)
        store.finalize()

        # Separate blob columm
        store = DbsqlValueStore.from_url(
            dburl, table1, engine_args={'echo': echo}, frid_field=True,
            blob_field='blob', col_values={'text': "(UNUSED)"}
        )
        self.assertIsInstance(str(store), str)
        self.assertTrue(store._frid_column is not None
                        and store._frid_column.name == 'frid')
        self.assertTrue(store._text_column is None)
        self.assertTrue(store._blob_column is not None
                        and store._blob_column.name == 'blob')
        self.do_test_store(store, exact=True)
        store.finalize()

        # Multirow for sequence and mapping
        store = DbsqlValueStore.from_url(
            dburl, table2, engine_args={'echo': echo},
            key_fields='id', frid_field='frid',
            seq_subkey='seqind', map_subkey='mapkey',
        )
        self.assertIsInstance(str(store), str)
        self.assertTrue(store._frid_column is not None
                        and store._frid_column.name == 'frid')
        self.assertTrue(store._seq_key_col is not None)
        self.assertTrue(store._map_key_col is not None)
        self.do_test_store(store, exact=True)
        store.finalize()

        self.remove_tables(dburl, dbfile, table1.name, table2.name, False, echo=echo)

    def test_dbsql_async_store(self):
        try:
            import aiosqlite, sqlalchemy, greenlet  # noqa: F401
        except ImportError:
            raise unittest.SkipTest("Skip Dbsql async tests; it needs sqlalchemy & others")
        echo = get_loglevel_str() == 'trace'
        from .dbsql import DbsqlAsyncStore

        (dburl, dbfile, table1, table2) = self.create_tables(True, echo=echo)

        loop = asyncio.new_event_loop()
        try:
            # Single frid columm
            store = loop.run_until_complete(DbsqlAsyncStore.from_url(
                dburl, table1, engine_args={'echo': echo}, frid_field=True,
                col_values={'text': "(UNUSED)", 'blob': b"(UNUSED)"}
            ))
            self.assertIsInstance(str(store), str)
            self.assertTrue(store._frid_column is not None
                            and store._frid_column.name == 'frid')
            self.assertTrue(store._text_column is None)
            self.assertTrue(store._blob_column is None)
            self.do_test_store(AsyncProxyValueStore(store, loop=loop),
                            no_proxy=True, exact=True)
            loop.run_until_complete(store.finalize())

            # Separate text columm
            store = loop.run_until_complete(DbsqlAsyncStore.from_url(
                dburl, table1.name, engine_args={'echo': echo}, frid_field=True,
                text_field='text', col_values={'blob': b"(UNUSED)"}
            ))
            self.assertIsInstance(str(store), str)
            self.assertTrue(store._frid_column is not None
                            and store._frid_column.name == 'frid')
            self.assertTrue(store._text_column is not None
                            and store._text_column.name == 'text')
            self.assertTrue(store._blob_column is None)
            self.do_test_store(AsyncProxyValueStore(store, loop=loop),
                            no_proxy=True, exact=True)
            loop.run_until_complete(store.finalize())

            # Separate blob columm
            store = loop.run_until_complete(DbsqlAsyncStore.from_url(
                dburl, table1, engine_args={'echo': echo}, frid_field=True,
                blob_field='blob', col_values={'text': "(UNUSED)"}
            ))
            self.assertIsInstance(str(store), str)
            self.assertTrue(store._frid_column is not None
                            and store._frid_column.name == 'frid')
            self.assertTrue(store._text_column is None)
            self.assertTrue(store._blob_column is not None
                            and store._blob_column.name == 'blob')
            self.do_test_store(AsyncProxyValueStore(store, loop=loop),
                            no_proxy=True, exact=True)
            loop.run_until_complete(store.finalize())

            # Multirow for sequence
            store = loop.run_until_complete(DbsqlAsyncStore.from_url(
                dburl, table2, engine_args={'echo': echo}, key_fields='id',
                frid_field='frid', seq_subkey='seqind', map_subkey='mapkey'
            ))
            self.assertIsInstance(str(store), str)
            self.assertTrue(store._frid_column is not None
                            and store._frid_column.name == 'frid')
            self.assertTrue(store._seq_key_col is not None)
            self.assertTrue(store._map_key_col is not None)
            self.do_test_store(AsyncProxyValueStore(store, loop=loop),
                            no_proxy=True, exact=True)
            loop.run_until_complete(store.finalize())
        finally:
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()

        self.remove_tables(dburl, dbfile, table1.name, table2.name, True, echo=echo)

