from collections.abc import Collection, Iterable, Mapping, Sequence
from logging import error
from typing import TypeGuard, TypeVar

from sqlalchemy import (
    Integer, Row, Table, Column, ColumnElement, CursorResult,
    Delete, Insert, Select, Update,
    LargeBinary, String, Date, DateTime, Time, Numeric, Boolean, Null, Sequence as SqlSequence,
    bindparam, create_engine, null,
    delete, insert, select, update
)
from sqlalchemy import types
from sqlalchemy.engine import Connection

from ..typing import (
    MISSING, BlobTypes, DateTypes, FridArray, FridBeing, FridSeqVT,
    FridTypeName, FridTypeSize, FridValue, MissingType, StrKeyMap
)
from ..guards import as_kv_pairs, is_frid_array, is_text_list_like
from ..chrono import datetime, dateonly, timeonly
from ..helper import frid_merge, frid_type_size
from ..dumper import dump_into_str
from ..loader import load_from_str
from .store import ValueStore
from .utils import (
    BulkInput, VSPutFlag, VStoreKey, VStoreSel, is_dict_sel, is_list_sel,
    dict_concat, list_concat, frid_delete, frid_select, list_select
)
from frid.kvs import utils

SqlTypes = str|float|int|DateTypes|bytes|bool  # Allowed data types for columns
ParTypes = Mapping[str,FridValue]|Sequence[Mapping[str,FridValue]]|None

_T = TypeVar('_T')

class _SqlBaseStore:
    def __init__(
            self, table: Table,
            *, key_fields: Sequence[str]|str|None=None, val_fields: Sequence[str]|str|None=None,
            frid_field: str|bool=False, text_field: str|bool=False, blob_field: str|bool=False,
            row_filter: Mapping[str,SqlTypes]|None=None,
            col_values: Mapping[str,SqlTypes]|None=None,
            seq_subkey: str|bool=False, map_subkey: str|bool=False,
    ):
        self._table = table
        self._where_conds: list[ColumnElement[bool]] = self._build_where(table, row_filter)
        self._insert_data: Mapping[str,SqlTypes] = dict_concat(row_filter, col_values)
        # For keys
        self._seq_key_col: Column|None = self._find_sub_key_col(table, seq_subkey, True)
        self._map_key_col: Column|None = self._find_sub_key_col(table, map_subkey, False)
        exclude: list[str] = []
        if self._seq_key_col is not None:
            exclude.append(self._seq_key_col.name)
        if self._map_key_col is not None:
            exclude.append(self._map_key_col.name)
        self._key_columns: list[Column] = self._find_key_columns(table, key_fields, exclude)
        exclude.extend(col.name for col in self._key_columns)
        # For values
        if frid_field is True and text_field is True:
            raise ValueError("frid_field and text_field cannot both be true; use column names")
        if col_values:
            exclude.extend(col_values.keys())
        self._frid_column: Column|None = self._find_column(table, frid_field, exclude, String)
        if self._frid_column is not None:
            exclude.append(self._frid_column.name)
        self._text_column: Column|None = self._find_column(table, text_field, exclude, String)
        if self._text_column is not None:
            exclude.append(self._text_column.name)
        self._blob_column: Column|None = self._find_column(table, blob_field, exclude,
                                                           LargeBinary)
        if self._blob_column is not None:
            exclude.append(self._blob_column.name)
        self._val_columns: list[Column] = self._find_val_columns(table, val_fields, exclude)
        # TODO: if row is autoincrement integer is part of primary key then it is for a list
        # If set to True, find such a column
        # self._multi_rows = table.c[multi_rows] if isinstance(multi_rows, str) else multi_rows

        self._select_cols: list[Column] = self._select_args()

    @classmethod
    def _build_where(cls, table: Table, data: Mapping[str,SqlTypes]|None):
        """Returns a list of boolean expression for extra conditions in where clause."""
        if not data:
            return []
        # items = data.items() if isinstance(data, Mapping) else data
        return [table.c[k] == v for k, v in data.items()]
    @classmethod
    def _match_dtype(cls, data, column: Column) -> TypeGuard[SqlTypes]:
        if isinstance(data, str):
            return isinstance(column.type, String)
        if isinstance(data, BlobTypes):
            return isinstance(column.type, LargeBinary)
        if isinstance(data, datetime):
            return isinstance(column.type, DateTime)
        if isinstance(data, dateonly):
            return isinstance(column.type, Date)
        if isinstance(data, timeonly):
            return isinstance(column.type, Time)
        if isinstance(data, bool):
            return isinstance(column.type, Boolean)
        if isinstance(data, int):
            return isinstance(column.type, Integer)
        if isinstance(data, float):
            return isinstance(column.type, Numeric)
        return False
    @classmethod
    def _find_sub_key_col(cls, table: Table, name: str|bool, seq_key=False) -> Column|None:
        if not name:
            return None
        if isinstance(name, str):
            col = table.c[name]
            if seq_key:
                if not isinstance(col.type, (Integer, DateTime, SqlSequence)):
                    raise ValueError(f"Column type of {name} is not for sequence: {col.type}")
            else:
                if not isinstance(col.type, String):
                    raise ValueError(f"Column type of {name} is not string: {col.type}")
            return col
        # Search from right to left
        for col in reversed(table.primary_key.columns):
            if seq_key:
                if isinstance(col.type, (Integer, DateTime, SqlSequence)):
                    return col
            else:
                if isinstance(col.type, String):
                    return col
        raise ValueError(f"Cannot find key with a {'integer' if seq_key else 'string'} type")
    @classmethod
    def _find_key_columns(cls, table: Table, names: str|Sequence[str]|None,
                          exclude: list[str]|None) -> list[Column]:
        """Returns a list of columns used as part of key, according to `names`.
        - If the `names` is not set, using the primary key as the columns,
          but the columnn name as specified by `exclude`, if set, is excluded.
        """
        if isinstance(names, str):
            return [table.c[names]]
        if names is not None:
            return [table.c[s] for s in names]
        return [
            col for col in table.primary_key.columns if not (exclude and col.name in exclude)
        ]
    @classmethod
    def _find_val_columns(cls, table: Table, names: str|Sequence[str]|None,
                          exclude: Collection|None) -> list[Column]:
        """Returns a list of columns used as a part of values, according to `names`.
        - If `names` is not set, all columns that are non-primary-key are used,
          excluding the ones in `exclude`, if set.
        """
        if isinstance(names, str):
            return [table.c[names]]
        if names is not None:
            return [table.c[s] for s in names]
        keynames = [col.name for col in table.primary_key]
        return [col for col in table.columns if col.name not in keynames and not (
            exclude and col.name in exclude
        ) and not col.primary_key]
    @classmethod
    def _find_column(cls, table: Table, field: str|bool, exclude: Collection|None,
                     col_type: type[types.TypeEngine]) -> Column|None:
        """Finds and returns the desire column in the `table`.
        - If `field` is falsy, returns None.
        - If `field` is a string, returns the column of this name.
        - If `field` is true, find a non-key column with the particular `col_type`,
          but do not only any columns with names in exclude.
        The column without a default is chosen over columns with defaults.
        An exception is raised if there are two or more choices (e.g., both
        without default or both without).
        """
        if not field:
            return None
        if isinstance(field, str):
            return table.c[field]
        keynames = [col.name for col in table.primary_key]
        required = []       # Required fields; these are of higher precedence
        optional = []       # Optional fields that has a default value
        for col in table.c:
            if col.name in keynames or col.primary_key:
                continue
            if exclude and (col.key in exclude or col.name in exclude):
                continue
            if not isinstance(col.type, col_type):
                continue
            if col.default is None:
                required.append(col)
            else:
                optional.append(col)
        if len(required) >= 2:
            raise ValueError(f"Too many non-key columns without default: {required}")
        if required:
            return required[0]
        if len(optional) >= 2:
            raise ValueError(f"Too many non-key columns: {optional}")
        if optional:
            return optional[0]
        raise ValueError(f"No field of type {type} found")
    def _select_args(self) -> list[Column]:
        """Returns the list of all value columns."""
        cols: list[Column] = [x for x in (
            self._seq_key_col, self._map_key_col,
            self._frid_column, self._text_column, self._blob_column
        ) if x is not None]
        cols.extend(self._val_columns)
        if len(set(cols)) < len(cols):
            raise ValueError(f"Duplicated columns: {cols}")
        return cols

    def _reorder_key(self, key: VStoreKey) -> tuple[SqlTypes,...]:
        """Converts the store key to a list of pairs: (key column name, key value)."""
        if isinstance(key, str):
            if len(self._key_columns) != 1:
                raise ValueError(f"{len(self._key_columns)} keys required, but 1 given")
            return (key,)
        if not isinstance(key, tuple):
            raise ValueError(f"Invalid key type: {type(key)}")
        if len(self._key_columns) != len(key):
            raise ValueError(f"{len(self._key_columns)} keys required, but {len(key)} given")
        # Check named tuple first
        if hasattr(key, '_fields'):
            return tuple(getattr(key, f.name) for f in self._key_columns)
        return key
    def _keys_ranges(self, keys: Iterable[VStoreKey]) -> list[set[SqlTypes]]:
        """Converts the list of store keys to a list of ranges for individual columns:
        (key column name, and set of possible values).
        """
        out = [set() for _ in range(len(self._key_columns))]
        for k in keys:
            data = self._reorder_key(k)
            assert len(data) == len(out)
            for i, x in enumerate(data):
                out[i].add(x)
        return out
    def _key_to_dict(self, key: VStoreKey) -> dict[str,SqlTypes]:
       """Converts the store key to a dict mapping the column names to values."""
       return {k.name: v for k, v in zip(self._key_columns, self._reorder_key(key))}
    def _val_to_dict(self, val: FridValue) -> dict[str,SqlTypes|Null]:
        """Converts the value to a dict mapping the column names to fields values.
        - If the `val` is text or blob and the text/blob column is set, put the value
          to that field.
        - If the `val` is a mapping, the fields with name matching `self._val_columns`
          are spread out into each column.
        Otherwise, if the frid column is set, it will store dumped data of other
        types, or for mapping, whatever remains after some fields are extracted.
        """
        out: dict[str,SqlTypes|Null] = {
            col.name: null() for col in self._select_cols
            if col is not self._seq_key_col  # This column needs to use its autoincrease
        }
        if isinstance(val, str):
            if self._text_column is not None:
                return {self._text_column.name: val}
        elif isinstance(val, BlobTypes):
            if self._blob_column is not None:
                return {self._blob_column.name: bytes(val)}
        elif isinstance(val, Mapping):
            val = dict(val)
            for col in self._val_columns:
                item = val.get(col.name, MISSING)
                if self._match_dtype(item, col):
                    out[col.name] = item
                    val.pop(col.name)
            if not val:
                return out
        if self._frid_column is not None:
            out[self._frid_column.name] = dump_into_str(val)
            return out
        raise ValueError(f"No column to store data of type {type(val)}")
    def _extract_row_value(
            self, row: Sequence, sel: VStoreSel
    ) -> tuple[int|DateTime|str|None,FridValue|MissingType]:
        """Extracts data from the row coming from SQL result."""
        assert len(row) == len(self._select_cols)
        key = None
        out = {}
        frid_val = MISSING
        for idx, col in enumerate(self._select_cols):
            val = row[idx]
            if val is None or val == null():
                continue
            if self._seq_key_col is not None and col.name == self._seq_key_col.name:
                assert isinstance(val, (int, datetime))
                key = round(val.timestamp() * 1E9) if isinstance(val, datetime) else val
                continue
            if self._map_key_col is not None and col.name == self._map_key_col.name:
                assert isinstance(val, str)
                key = val
                continue
            if self._text_column is not None and col.name == self._text_column.name:
                if isinstance(val, str):
                    return (key, val)
                error(f"Data in column {col.name} is not string: {type(val)}")
                continue
            if self._blob_column is not None and col.name == self._blob_column.name:
                if isinstance(val, BlobTypes):
                    return (key, val)
                error(f"Data in column {col.name} is not binary: {type(val)}")
                continue
            if self._frid_column is not None and col.name == self._frid_column.name:
                if val and isinstance(val, str):
                    frid_val = load_from_str(val)
                else:
                    error(f"Data in column {col.name} is not string: {type(val)}")
                continue
            out[col.name] = val
        if frid_val is MISSING:
            return (key, frid_select(out, sel))
        if isinstance(frid_val, Mapping):
            out.update(frid_val)
        else:
            out = frid_val
        return (key, frid_select(out, sel))

    def _make_where_args(self, key: VStoreKey, *args: ColumnElement[bool]):
        out = [k == v for k, v in zip(self._key_columns, self._reorder_key(key))]
        out.extend(args)
        out.extend(self._where_conds)
        return out
    def _make_select_cmd(self, key: VStoreKey, *args: ColumnElement[bool]) -> Select:
        return select(*self._select_cols).where(*self._make_where_args(key, *args))
    def _make_delete_cmd(self, key: VStoreKey, *args: ColumnElement[bool]) -> Delete:
        return delete(self._table).where(*self._make_where_args(key, *args))
    def _make_update_cmd(self, key: VStoreKey, val: FridValue,
                         *args: ColumnElement[bool]) -> Update:
        return update(self._table).where(*self._make_where_args(key, *args)).values(
            **self._val_to_dict(val)
        )
    def _make_insert_cmd(self, key: VStoreKey, val: FridValue,
                         extra: Mapping[str,FridValue|Null]|None=None) -> Insert:
        args: dict[str,FridValue|Null] = dict(extra) if extra is not None else {}
        # if self._seq_key_col is not None and self._seq_key_col.name not in args:
        #     args[self._seq_key_col.name] = null()
        # if self._map_key_col is not None and self._map_key_col.name not in args:
        #     args[self._map_key_col.name] = null()
        return insert(self._table).values(
            **self._key_to_dict(key), **args, **self._val_to_dict(val), **self._insert_data,
        )

    def _get_meta_select(self, keys: Iterable[VStoreKey], /) -> Select:
        """Returns the select cmd for _get_meta()."""
        return self._get_bulk_select(keys)
    def _get_meta_result(self, result: CursorResult, keys: Iterable[VStoreKey],
                         /) -> dict[VStoreKey,FridTypeSize]:
        if not isinstance(keys, Sequence):
            keys = list(keys)
        return {k: frid_type_size(v) for k, v in zip(keys, self._get_bulk_result(result, keys))
                if not isinstance(v, FridBeing)}
    def _get_frid_select(self, key: VStoreKey, sel: VStoreSel, dtype: FridTypeName) -> Select:
        """Returns the select command for get_frid()."""
        extra = []
        if self._map_key_col is not None:
            # We can only do restricted selection for mapping, but not for sequence
            if isinstance(sel, str):
                extra.append(self._map_key_col == sel)
            elif is_text_list_like(sel):
                extra.append(self._map_key_col.in_(sel))
        cmd = self._make_select_cmd(key, *extra)
        if self._seq_key_col is not None:
            cmd = cmd.order_by(self._seq_key_col)
        return cmd
    def _get_frid_result(self, result: CursorResult, sel: VStoreSel,
                         dtype: FridTypeName) -> FridValue|MissingType:
        """Processes the results by the select command for get_frid()."""
        if self._map_key_col is None and self._seq_key_col is None:
            row = result.one_or_none()
            if row is None:
                return MISSING
            (key, val) = self._extract_row_value(row, sel)
            assert key is None
            return val
        return self._proc_multi_rows(result.all(), sel, dtype)
    def _proc_multi_rows(self, datarows: Sequence[Sequence], sel: VStoreSel=None,
                         dtype: FridTypeName='') -> FridValue|MissingType:
        seq_val: FridArray = []
        map_val: StrKeyMap = {}
        out_val = MISSING
        for row in datarows:
            (key, val) = self._extract_row_value(row, None)
            if key is None:
                if out_val is not MISSING:
                    raise ValueError("Multiple values for a single entry result")
                out_val = val
            elif isinstance(key, (int, datetime)):
                assert val is not MISSING
                seq_val.append(val)
            elif isinstance(key, str):
                map_val[key] = val
        # print("===", dtype, datarows, seq_val, map_val, out_val)
        if dtype == 'list' or (not dtype and utils.is_list_sel(sel)):
            if map_val:
                raise ValueError("Found mapping data while sequence results are expected")
            if out_val is MISSING:
                out_val = seq_val
            elif seq_val:
                raise ValueError("Found regular data while sequence results are expected")
        elif dtype == 'dict' or (not dtype and utils.is_dict_sel(sel)):
            if seq_val:
                raise ValueError("Found sequence data while mapping results are expected")
            if out_val is MISSING:
                out_val = map_val
            elif map_val:
                raise ValueError("Found regular data while mapping results are expected")
        if out_val is MISSING:
            return seq_val or map_val or MISSING
        return frid_select(out_val, sel)
    def _put_frid_select(self, key: VStoreKey, val: FridValue,
                         /, flags: VSPutFlag) -> Select|None:
        """Returns the select command for put_frid for read-modify-write.
        - Returns None if select is not needed by flags.
        """
        if isinstance(val, Mapping):
            if self._map_key_col is not None:
                return select(self._map_key_col).where(*self._make_where_args(key))
        elif is_frid_array(val):
            if self._seq_key_col is not None:
                # return select(self._seq_key_col).where(
                #     *self._make_where_args(key)
                # ).order_by(self._seq_key_col)
                return None  # Do not need past for put for now because insert is not supported
        return self._make_select_cmd(key)
    def _put_frid_delete(self, key: VStoreKey, val: FridValue,
                         /, flags: VSPutFlag, datarows: Sequence[Row]|None) -> Delete|None:
        """Returns a delete command for put_frid() if a delete is needed."""
        if isinstance(val, Mapping):
            to_delete = self._map_key_col is not None and bool(datarows)
        elif is_frid_array(val):
            to_delete = self._seq_key_col is not None and bool(datarows)
        else:
            to_delete = False
        # Deleting existing data in the case of overwrite
        if to_delete and not (flags & (VSPutFlag.KEEP_BOTH | VSPutFlag.NO_CHANGE)):
            return self._make_delete_cmd(key)
        # Do not call delete for regular single row update
        return None
    def _put_frid_update(self, key: VStoreKey, val: FridValue, /, flags: VSPutFlag,
                         datarows: Sequence[Row]|None) -> list[Update]:
        """Returns a list of update commands for put_frid().
        - `datarows` is the result of the commond given by `_put_frid_select()`;
          it's none if no select was executed.
        - Returns empty if update is not required.
        """
        if isinstance(val, Mapping):
            if not val:
                return []
            if self._map_key_col is not None and datarows:
                if flags & VSPutFlag.NO_CHANGE:
                    return []
                existing = set(row[0] for row in datarows)
                return [
                    self._make_update_cmd(k, v, self._map_key_col == k)
                    for k, v in val.items()
                    if k in existing and not isinstance(v, FridBeing)
                ]
        elif is_frid_array(val):
            if not val:
                return []
            if self._seq_key_col is not None:
                return []   # Do not support insert yet
        if flags & VSPutFlag.NO_CHANGE:
            return []
        assert datarows is not None
        if not datarows:
            return []
        assert len(datarows) == 1
        if flags & VSPutFlag.KEEP_BOTH:
            (row_key, data) = self._extract_row_value(datarows[0], None)
            assert row_key is None
            val = frid_merge(data, val)
        return [self._make_update_cmd(key, val)]
    def _put_frid_insert(self, key: VStoreKey, val: FridValue, /, flags: VSPutFlag,
                         datarows: Sequence[Row]|None) -> list[Insert]:
        """Returns the insert command for put_frid.
        - `datarows` is the result of the commond given by `_put_frid_select()`;
          it's none if no select was executed.
        - Returns empty if update is not required.
        """
        if isinstance(val, Mapping):
            if self._map_key_col is not None:
                if not val:
                    return []
                if not datarows and flags & VSPutFlag.NO_CREATE:
                    return []
                existing = set(row[0] for row in datarows) if datarows else set()
                return [
                    self._make_insert_cmd(key, v, {self._map_key_col.name: k})
                    for k, v in val.items()
                    if k not in existing and not isinstance(v, FridBeing)
                ]
        elif is_frid_array(val):
            if self._seq_key_col is not None:
                if not datarows and flags & VSPutFlag.NO_CREATE:
                    return []
                return [self._make_insert_cmd(key, v) for v in val]
        if flags & VSPutFlag.NO_CREATE:
            return []
        assert datarows is not None
        if datarows:
            return []
        return [self._make_insert_cmd(key, val, (
            {self._seq_key_col.name: null()} if self._seq_key_col is not None else {}
        ))]
    def _put_frid_result(self, delete: CursorResult|None, update: Sequence[CursorResult],
                         insert: Sequence[CursorResult]) -> bool:
        """Returns the put_frid() return value according to the insert or upate result."""
        return delete is not None and bool(delete.rowcount) or any(
            r.rowcount for r in update
        ) or any(r.rowcount for r in insert)
    def _del_frid_select(self, key: VStoreKey, sel: VStoreSel, /) -> Select|None:
        if sel is None:
            return None
        if self._map_key_col is not None and is_dict_sel(sel):
            return None
        return self._make_select_cmd(key)
    def _del_frid_delete(self, key: VStoreKey, sel: VStoreSel,
                         datarows: CursorResult|None) -> Delete|None:
        """Returns the update command for del_frid.
        - Returns None if no delete should be performed, according to `key` and `sel`.
        """
        if sel is None:
            assert datarows is None
            return self._make_delete_cmd(key)
        if self._map_key_col is not None and is_dict_sel(sel):
            assert datarows is None
            if isinstance(sel, str):
                dict_sel_cond = self._map_key_col == sel
            elif isinstance(sel, Sequence):
                dict_sel_cond = self._map_key_col.in_(sel)
            else:
                raise ValueError(f"Invalid selector type for dict {type(sel)}")
            return self._make_delete_cmd(key, dict_sel_cond)
        if self._seq_key_col is not None and is_list_sel(sel):
            assert datarows is not None
            oids = [k for row in datarows.all()
                    if isinstance((k := self._extract_row_value(row, None)[0]), int)]
            assert sel is not None
            oid_sel = list_select(oids, sel)
            if oid_sel is MISSING:
                return None
            if isinstance(oid_sel, int):
                list_sel_cond = self._seq_key_col == oid_sel
            else:
                list_sel_cond = self._seq_key_col.in_(oid_sel)
            return self._make_delete_cmd(key, list_sel_cond)
        return None
    def _del_frid_update(self, key: VStoreKey, sel: VStoreSel,
                         datarows: CursorResult|None) -> Update|None:
        # Not calls if _del_frid_delete() is called; hence basically only for single row
        if datarows is None:
            return None
        data = self._get_frid_result(datarows, None, '')
        if data is MISSING:
            return None
        (data, cnt) = frid_delete(data, sel)
        if cnt == 0:
            return None
        return self._make_update_cmd(key, data)
    def _del_frid_result(self, result: CursorResult, is_update: bool, /) -> bool:
        """Returns the del_frid() return value according to the insert or upate result."""
        return bool(result.rowcount)

    def _get_bulk_select(self, keys: Iterable[VStoreKey], /) -> Select:
        """Returns the select cmd for _get_bulk()."""
        return select(*self._key_columns, *self._select_cols).where(
            *(k.in_(v) for k, v in zip(self._key_columns, self._keys_ranges(keys))),
            *self._where_conds
        )
    def _get_bulk_result(self, result: CursorResult, keys: Iterable[VStoreKey],
                         /, alt: _T=MISSING) -> list[FridValue|_T]:
        res: dict[tuple,list[Sequence]] = {}
        for row in result.all():
            prev = res.setdefault(tuple(row[:len(self._key_columns)]), [])
            prev.append(row[len(self._key_columns):])
        out = []
        for k in keys:
            v = res.get(self._reorder_key(k))
            if v is None:
                out.append(alt)
            else:
                out.append(self._proc_multi_rows(v))
        return out
    def _del_bulk_delete(self, keys: Iterable[VStoreKey], /) -> tuple[Delete,ParTypes]:
        """Returns the update command for del_frid.
        - Returns None if no delete should be performed, according to `key` and `sel`.
        """
        cmd = delete(self._table).where(
            *(k == bindparam(k.name) for k in self._key_columns),
            *self._where_conds
        )
        return (cmd, [self._key_to_dict(k) for k in keys])
    def _del_bulk_result(self, result: CursorResult, /) -> int:
        """Returns the del_frid() return value according to the insert or upate result."""
        return result.rowcount

class DbsqlValueStore(_SqlBaseStore, ValueStore):
    def __init__(self, url: str, *args, echo=False, **kwargs):
        self._engine = create_engine(url, echo=echo)
        self._dbconn: Connection|None = None
        super().__init__(*args, **kwargs)

    def substore(self, name: str, *args: str):
        raise NotImplementedError

    def get_lock(self, name: str|None=None):
        raise NotImplementedError

    def get_meta(self, *args: VStoreKey,
                 keys: Iterable[VStoreKey]|None=None) -> Mapping[VStoreKey,FridTypeSize]:
        merged_keys = list_concat(args, keys)
        cmd = self._get_bulk_select(merged_keys)
        with self._engine.begin() as conn:
            return self._get_meta_result(conn.execute(cmd), merged_keys)

    def get_frid(self, key: VStoreKey, sel: VStoreSel=None,
                 /, dtype: FridTypeName='') -> FridValue|MissingType:
        cmd = self._get_frid_select(key, sel, dtype)
        with self._engine.begin() as conn:
            return self._get_frid_result(conn.execute(cmd), sel, dtype)
    def put_frid(self, key: VStoreKey, val: FridValue, /, flags=VSPutFlag.UNCHECKED) -> bool:
        with self._engine.begin() as conn:
            return self._put_frid(conn, key, val, flags)
    def _put_frid(self, conn: Connection, key: VStoreKey, val: FridValue,
                  /, flags=VSPutFlag.UNCHECKED) -> bool:
        sel_cmd = self._put_frid_select(key, val, flags)
        sel_out = conn.execute(sel_cmd).all() if sel_cmd is not None else None
        del_cmd = self._put_frid_delete(key, val, flags, sel_out)
        del_out = conn.execute(del_cmd) if del_cmd is not None else None
        upd_cmd = self._put_frid_update(key, val, flags, sel_out)
        upd_out = [conn.execute(cmd) for cmd in upd_cmd]
        ins_cmd = self._put_frid_insert(key, val, flags, sel_out)
        ins_out = [conn.execute(cmd) for cmd in ins_cmd]
        return self._put_frid_result(del_out, upd_out, ins_out)
    def del_frid(self, key: VStoreKey, sel: VStoreSel=None, /) -> bool:
        sel_cmd = self._del_frid_select(key, sel)
        with self._engine.begin() as conn:
            if sel_cmd is not None:
                results = conn.execute(sel_cmd)
            else:
                results = None
            del_cmd = self._del_frid_delete(key, sel, results)
            if del_cmd is not None:
                return self._del_frid_result(conn.execute(del_cmd), False)
            upd_cmd = self._del_frid_update(key, sel, results)
            if upd_cmd is not None:
                return self._del_frid_result(conn.execute(upd_cmd), True)
        return False

    def get_bulk(self, keys: Iterable[VStoreKey], /, alt: _T=MISSING) -> list[FridSeqVT|_T]:
        cmd = self._get_bulk_select(keys)
        with self._engine.begin() as conn:
            return self._get_bulk_result(conn.execute(cmd), keys, alt)
    def put_bulk(self, data: BulkInput, /, flags=VSPutFlag.UNCHECKED) -> int:
        pairs = as_kv_pairs(data)
        with self._engine.begin() as conn:
            meta = self._get_meta_result(conn.execute(
                self._get_meta_select(k for k, _ in pairs),
            ), (k for k, _ in pairs))
            if not utils.check_flags(flags, len(pairs), len(meta)):
                return 0
            # If Atomicity for bulk is set and any other flags are set, we need to check
            return sum(int(self._put_frid(conn, k, v, flags)) for k, v in pairs)
    def del_bulk(self, keys: Iterable[VStoreKey]) -> int:
        (cmd, par) = self._del_bulk_delete(keys)
        with self._engine.begin() as conn:
            return self._del_bulk_result(conn.execute(cmd, par))
