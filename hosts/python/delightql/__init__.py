# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Daniel Eklund
"""DelightQL — a query language that transpiles to SQL."""

from __future__ import annotations

import ctypes
import os
from ctypes import (
    POINTER,
    c_char_p,
    c_int32,
    c_size_t,
    c_uint64,
    c_void_p,
)
from typing import List, Optional, Tuple

__all__ = ["Database", "DqlError", "split_queries"]


# ---------------------------------------------------------------------------
# Locate the compiled cdylib shipped inside the wheel
# ---------------------------------------------------------------------------

def _load_lib() -> ctypes.CDLL:
    lib_dir = os.path.join(os.path.dirname(__file__), "_lib")
    for name in os.listdir(lib_dir):
        if name.endswith((".so", ".dylib", ".dll")):
            return ctypes.CDLL(os.path.join(lib_dir, name))
    raise ImportError(
        f"Cannot find cdylib in {lib_dir} — was the package built with maturin?"
    )


_lib = _load_lib()

# ---------------------------------------------------------------------------
# C struct mirrors
# ---------------------------------------------------------------------------


class _DqlColumnInfo(ctypes.Structure):
    _fields_ = [
        ("name", c_char_p),
        ("position", c_size_t),
        ("type_name", c_char_p),
    ]


class _DqlQueryResult(ctypes.Structure):
    _fields_ = [
        ("query_id", c_uint64),
        ("columns", POINTER(_DqlColumnInfo)),
        ("num_columns", c_size_t),
    ]


class _DqlCell(ctypes.Structure):
    _fields_ = [
        ("data", POINTER(ctypes.c_uint8)),
        ("len", c_size_t),
    ]


class _DqlFetchResult(ctypes.Structure):
    _fields_ = [
        ("cells", POINTER(_DqlCell)),
        ("num_rows", c_size_t),
        ("num_cols", c_size_t),
        ("finished", c_int32),
        ("_backing", c_void_p),
    ]


class _DqlSplitResult(ctypes.Structure):
    _fields_ = [
        ("queries", POINTER(c_void_p)),  # *mut *mut c_char
        ("num_queries", c_size_t),
    ]


# ---------------------------------------------------------------------------
# Function signatures
# ---------------------------------------------------------------------------

# Use c_void_p (not c_char_p) for error_out pointers and dql_free_string.
# c_char_p auto-converts to Python bytes on read, losing the original C
# pointer.  Passing that bytes object back to dql_free_string would make
# CString::from_raw free a Python-allocated buffer → heap corruption.

# dql_open(db_path, error_out) -> *mut Handle
_lib.dql_open.restype = c_void_p
_lib.dql_open.argtypes = [c_char_p, POINTER(c_void_p)]

# dql_query(handle, dql, error_out) -> DqlQueryResult
_lib.dql_query.restype = _DqlQueryResult
_lib.dql_query.argtypes = [c_void_p, c_char_p, POINTER(c_void_p)]

# dql_fetch(handle, query_id, count, error_out) -> DqlFetchResult
_lib.dql_fetch.restype = _DqlFetchResult
_lib.dql_fetch.argtypes = [c_void_p, c_uint64, c_uint64, POINTER(c_void_p)]

# dql_close_query(handle, query_id, error_out) -> i32
_lib.dql_close_query.restype = c_int32
_lib.dql_close_query.argtypes = [c_void_p, c_uint64, POINTER(c_void_p)]

# dql_destroy(handle)
_lib.dql_destroy.restype = None
_lib.dql_destroy.argtypes = [c_void_p]

# dql_free_string(s) — takes a raw pointer, NOT c_char_p
_lib.dql_free_string.restype = None
_lib.dql_free_string.argtypes = [c_void_p]

# dql_free_query_result(result)
_lib.dql_free_query_result.restype = None
_lib.dql_free_query_result.argtypes = [POINTER(_DqlQueryResult)]

# dql_free_fetch_result(result)
_lib.dql_free_fetch_result.restype = None
_lib.dql_free_fetch_result.argtypes = [POINTER(_DqlFetchResult)]

# dql_split_queries(source, error_out) -> DqlSplitResult
_lib.dql_split_queries.restype = _DqlSplitResult
_lib.dql_split_queries.argtypes = [c_char_p, POINTER(c_void_p)]

# dql_free_split_result(result)
_lib.dql_free_split_result.restype = None
_lib.dql_free_split_result.argtypes = [POINTER(_DqlSplitResult)]


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------



class DqlError(Exception):
    """Raised when a C-ABI call signals an error."""


def _check_error(error_out: ctypes.Array) -> None:
    """If *error_out* is non-null, read the message, free it, and raise."""
    ptr = error_out[0]
    if ptr:
        # Read the C string via the raw pointer, then free the original.
        msg = ctypes.string_at(ptr).decode("utf-8", errors="replace")
        _lib.dql_free_string(ptr)
        raise DqlError(msg)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class QueryResult:
    """Thin wrapper around a pending query's metadata."""

    __slots__ = ("query_id", "columns")

    def __init__(self, query_id: int, columns: List[str]) -> None:
        self.query_id = query_id
        self.columns = columns

    def __repr__(self) -> str:
        return f"QueryResult(query_id={self.query_id}, columns={self.columns!r})"


class Database:
    """Context-manager wrapper around a DQL database handle.

    Usage::

        with Database("my.db") as db:
            cols, rows = db.execute("users(*)")
    """

    def __init__(self, path: str) -> None:
        error_out = (c_void_p * 1)()
        self._handle = _lib.dql_open(path.encode("utf-8"), error_out)
        _check_error(error_out)
        if not self._handle:
            raise DqlError("dql_open returned NULL without setting an error")

    # -- context manager ----------------------------------------------------

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # -- core operations ----------------------------------------------------

    def query(self, dql: str) -> QueryResult:
        """Compile and begin executing *dql*, returning column metadata."""
        self._ensure_open()
        error_out = (c_void_p * 1)()
        result = _lib.dql_query(self._handle, dql.encode("utf-8"), error_out)
        _check_error(error_out)
        columns: List[str] = []
        for i in range(result.num_columns):
            col = result.columns[i]
            columns.append(col.name.decode("utf-8") if col.name else "")
        qid = result.query_id
        _lib.dql_free_query_result(ctypes.byref(result))
        return QueryResult(query_id=qid, columns=columns)

    # Cell = str | None (raw UTF-8 from the protocol, no type coercion)
    CellValue = Optional[str]

    def fetch(
        self, query_id: int, count: int = 256
    ) -> Tuple[List[List["Database.CellValue"]], bool]:
        """Fetch up to *count* rows.  Returns ``(rows, finished)``."""
        self._ensure_open()
        error_out = (c_void_p * 1)()
        result = _lib.dql_fetch(self._handle, query_id, count, error_out)
        _check_error(error_out)
        rows: list = []
        try:
            ncols = result.num_cols
            for r in range(result.num_rows):
                row: list = []
                for c in range(ncols):
                    cell = result.cells[r * ncols + c]
                    if cell.data:
                        raw = ctypes.string_at(cell.data, cell.len)
                        row.append(raw.decode("utf-8", errors="replace"))
                    else:
                        row.append(None)
                rows.append(row)
            finished = bool(result.finished)
        finally:
            _lib.dql_free_fetch_result(ctypes.byref(result))
        return rows, finished

    def close_query(self, query_id: int) -> None:
        """Release server-side resources for a query."""
        self._ensure_open()
        error_out = (c_void_p * 1)()
        rc = _lib.dql_close_query(self._handle, query_id, error_out)
        _check_error(error_out)
        if rc != 0:
            raise DqlError(f"dql_close_query returned {rc}")

    def execute(
        self, dql: str
    ) -> Tuple[List[str], List[List["Database.CellValue"]]]:
        """Convenience: query → fetch all → close.  Returns ``(columns, rows)``."""
        qr = self.query(dql)
        all_rows: list = []
        while True:
            batch, finished = self.fetch(qr.query_id)
            all_rows.extend(batch)
            if finished:
                break
        self.close_query(qr.query_id)
        return qr.columns, all_rows

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        """Destroy the underlying handle.  Safe to call multiple times."""
        if self._handle:
            _lib.dql_destroy(self._handle)
            self._handle = None

    def _ensure_open(self) -> None:
        if not self._handle:
            raise DqlError("Database is closed")

    def __del__(self) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Standalone functions
# ---------------------------------------------------------------------------


def split_queries(dql: str) -> List[str]:
    """Split multi-query DQL source into individual queries using tree-sitter."""
    error_out = (c_void_p * 1)()
    result = _lib.dql_split_queries(dql.encode("utf-8"), error_out)
    _check_error(error_out)
    queries: List[str] = []
    try:
        for i in range(result.num_queries):
            ptr = result.queries[i]
            queries.append(ctypes.string_at(ptr).decode("utf-8"))
    finally:
        _lib.dql_free_split_result(ctypes.byref(result))
    return queries
