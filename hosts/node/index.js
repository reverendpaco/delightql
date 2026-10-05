// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
'use strict';

const path = require('path');
const koffi = require('koffi');

// ---------------------------------------------------------------------------
// Library loading
// ---------------------------------------------------------------------------

const libName = process.platform === 'darwin'
    ? 'libdelightql_cabi.dylib'
    : 'libdelightql_cabi.so';

const fs = require('fs');

const libPath = process.env.DELIGHTQL_LIB_PATH
    || [
        path.join(__dirname, '..', '..', 'target', 'release', libName),
        path.join(__dirname, '..', '..', 'target', 'debug', libName),
    ].find(p => fs.existsSync(p))
    || path.join(__dirname, '..', '..', 'target', 'release', libName);

const lib = koffi.load(libPath);

// ---------------------------------------------------------------------------
// Struct definitions (mirrors crates/delightql-cabi/src/types.rs)
// ---------------------------------------------------------------------------

const DqlColumnInfo = koffi.struct('DqlColumnInfo', {
    name: 'char *',
    position: 'uintptr_t',
    type_name: 'char *',
    minted: 'uint8_t',
});

const DqlQueryResult = koffi.struct('DqlQueryResult', {
    query_id: 'uint64_t',
    columns: koffi.pointer(DqlColumnInfo),
    num_columns: 'uintptr_t',
});

const DqlCell = koffi.struct('DqlCell', {
    data: 'void *',
    len: 'uintptr_t',
});

const DqlFetchResult = koffi.struct('DqlFetchResult', {
    cells: koffi.pointer(DqlCell),
    num_rows: 'uintptr_t',
    num_cols: 'uintptr_t',
    finished: 'int32_t',
    _backing: 'void *',
});

const DqlSplitResult = koffi.struct('DqlSplitResult', {
    queries: 'void *',
    num_queries: 'uintptr_t',
});

// ---------------------------------------------------------------------------
// Function declarations
// ---------------------------------------------------------------------------

const dql_open = lib.func('void *dql_open(const char *path, _Out_ void **err)');
const dql_query = lib.func('DqlQueryResult dql_query(void *h, const char *dql, _Out_ void **err)');
const dql_fetch = lib.func('DqlFetchResult dql_fetch(void *h, uint64_t qid, uint64_t count, _Out_ void **err)');
const dql_close_query = lib.func('int32_t dql_close_query(void *h, uint64_t qid, _Out_ void **err)');
const dql_destroy = lib.func('void dql_destroy(void *h)');
const dql_free_string = lib.func('void dql_free_string(void *s)');
const dql_free_query_result = lib.func('void dql_free_query_result(DqlQueryResult *r)');
const dql_free_fetch_result = lib.func('void dql_free_fetch_result(DqlFetchResult *r)');
const dql_split_queries = lib.func('DqlSplitResult dql_split_queries(const char *src, _Out_ void **err)');
const dql_free_split_result = lib.func('void dql_free_split_result(DqlSplitResult *r)');
const dql_digest_version = lib.func('const char *dql_digest_version()');
// Returns void * (not char *) so the original pointer can be freed.
const dql_digest_rows = lib.func(
    'void *dql_digest_rows(const uint8_t *data, uintptr_t data_len, const int64_t *lens, '
    + 'uintptr_t num_rows, uintptr_t num_cols, _Out_ void **err)'
);

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

class DqlError extends Error {
    constructor(message) {
        super(message);
        this.name = 'DqlError';
    }
}

function checkError(errBuf) {
    const ptr = errBuf[0];
    if (ptr) {
        const msg = koffi.decode(ptr, 'char', -1);
        dql_free_string(ptr);
        throw new DqlError(msg);
    }
}

// ---------------------------------------------------------------------------
// Database class
// ---------------------------------------------------------------------------

class Database {
    constructor(dbPath) {
        const errBuf = [null];
        this._handle = dql_open(dbPath, errBuf);
        checkError(errBuf);
        if (!this._handle) {
            throw new DqlError('dql_open returned NULL without setting an error');
        }
    }

    _ensureOpen() {
        if (!this._handle) {
            throw new DqlError('Database is closed');
        }
    }

    query(dql) {
        this._ensureOpen();
        const errBuf = [null];
        const result = dql_query(this._handle, dql, errBuf);
        checkError(errBuf);

        // minted[i] is true when the compiler minted columns[i]: its
        // spelling moves between compilations, so do not key on it.
        const columns = [];
        const minted = [];
        if (result.num_columns > 0 && result.columns) {
            const colArray = koffi.decode(result.columns, DqlColumnInfo, result.num_columns);
            for (const col of colArray) {
                columns.push(col.name || '');
                minted.push(col.minted === 1);
            }
        }
        const queryId = result.query_id;
        dql_free_query_result(result);
        return { queryId, columns, minted };
    }

    // A raw cell is the protocol cell itself: a Buffer of its bytes, or null
    // for SQL NULL.
    fetchRaw(queryId, count) {
        this._ensureOpen();
        if (count === undefined) count = 256;
        const errBuf = [null];
        const result = dql_fetch(this._handle, queryId, count, errBuf);
        checkError(errBuf);

        const rows = [];
        try {
            const numCols = result.num_cols;
            if (result.num_rows > 0 && result.cells) {
                const cellArray = koffi.decode(result.cells, DqlCell, result.num_rows * numCols);
                for (let r = 0; r < result.num_rows; r++) {
                    const row = [];
                    for (let c = 0; c < numCols; c++) {
                        const cell = cellArray[r * numCols + c];
                        if (!cell.data) {
                            row.push(null);
                        } else if (cell.len === 0) {
                            row.push(Buffer.alloc(0));
                        } else {
                            row.push(Buffer.from(koffi.decode(cell.data, 'uint8_t', cell.len)));
                        }
                    }
                    rows.push(row);
                }
            }
        } finally {
            dql_free_fetch_result(result);
        }
        return { rows, finished: result.finished !== 0 };
    }

    // Cells decoded as UTF-8 (lossy for bytes that are not UTF-8), null being
    // SQL NULL.
    fetch(queryId, count) {
        const { rows, finished } = this.fetchRaw(queryId, count);
        return { rows: rows.map(decodeRow), finished };
    }

    closeQuery(queryId) {
        this._ensureOpen();
        const errBuf = [null];
        const rc = dql_close_query(this._handle, queryId, errBuf);
        checkError(errBuf);
        if (rc !== 0) {
            throw new DqlError(`dql_close_query returned ${rc}`);
        }
    }

    executeRaw(dql) {
        const qr = this.query(dql);
        const allRows = [];
        while (true) {
            const { rows, finished } = this.fetchRaw(qr.queryId);
            allRows.push(...rows);
            if (finished) break;
        }
        this.closeQuery(qr.queryId);
        return { columns: qr.columns, rows: allRows };
    }

    execute(dql) {
        const { columns, rows } = this.executeRaw(dql);
        return { columns, rows: rows.map(decodeRow) };
    }

    close() {
        if (this._handle) {
            dql_destroy(this._handle);
            this._handle = null;
        }
    }
}

function decodeRow(row) {
    return row.map(cell => (cell === null ? null : cell.toString('utf-8')));
}

// ---------------------------------------------------------------------------
// Result digest
// ---------------------------------------------------------------------------

// The name of the digest framing digestRows computes.
function digestVersion() {
    return dql_digest_version();
}

// The result digest of raw rows (Buffers, null being SQL NULL) as lowercase
// hex: the digest the CLI's `-f hash` prints. The library computes it; pass
// the Buffers fetchRaw returned, never a decoding of them.
function digestRows(rows, numCols) {
    const lens = [];
    const chunks = [];
    for (const row of rows) {
        if (row.length !== numCols) {
            throw new DqlError(`a row of ${row.length} cells in a ${numCols}-column result`);
        }
        for (const cell of row) {
            if (cell === null) {
                lens.push(-1n);
            } else {
                lens.push(BigInt(cell.length));
                chunks.push(cell);
            }
        }
    }
    const data = Buffer.concat(chunks);
    const errBuf = [null];
    const ptr = dql_digest_rows(
        data.length > 0 ? data : null,
        data.length,
        lens.length > 0 ? BigInt64Array.from(lens) : null,
        rows.length,
        numCols,
        errBuf,
    );
    checkError(errBuf);
    if (!ptr) {
        throw new DqlError('dql_digest_rows returned NULL without setting an error');
    }
    try {
        return koffi.decode(ptr, 'char', -1);
    } finally {
        dql_free_string(ptr);
    }
}

// ---------------------------------------------------------------------------
// splitQueries
// ---------------------------------------------------------------------------

function splitQueries(dql) {
    const errBuf = [null];
    const result = dql_split_queries(dql, errBuf);
    checkError(errBuf);

    const queries = [];
    try {
        if (result.num_queries > 0 && result.queries) {
            const ptrs = koffi.decode(result.queries, 'void *', result.num_queries);
            for (const ptr of ptrs) {
                queries.push(koffi.decode(ptr, 'char', -1));
            }
        }
    } finally {
        dql_free_split_result(result);
    }
    return queries;
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

module.exports = { Database, DqlError, digestRows, digestVersion, splitQueries };
