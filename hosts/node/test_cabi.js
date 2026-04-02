// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
#!/usr/bin/env node
'use strict';

/**
 * Node.js C-ABI stress test for DelightQL test balls.
 *
 * Exercises the same .db test balls used by test.sh / dql-pack-man, but through
 * the Node/C-ABI path (libdelightql_cabi.so via koffi).  The goal is to
 * stress the C-ABI under real workloads, not to replace the existing test runner.
 */

const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');

const BetterSqlite3 = require('better-sqlite3');
const fzstd = require('fzstd');

const { Database, splitQueries } = require('.');

// ---------------------------------------------------------------------------
// Hash computation (mirrors pack-man's compute_data_hash + hex2hash)
// ---------------------------------------------------------------------------

function floatToRustString(f) {
    if (Number.isNaN(f)) return 'NaN';
    if (f === Infinity) return 'inf';
    if (f === -Infinity) return '-inf';
    if (Object.is(f, -0)) return '-0';
    if (Number.isInteger(f) && Math.abs(f) < 2 ** 53) {
        return String(Math.trunc(f));
    }
    return String(f);
}

function cellToText(value) {
    if (value === null || value === undefined) return 'NULL';
    if (typeof value === 'string') return value || 'NULL';
    if (typeof value === 'bigint') return String(value);
    if (typeof value === 'number') {
        if (Number.isInteger(value)) return String(value);
        return floatToRustString(value);
    }
    if (Buffer.isBuffer(value)) return `<blob ${value.length} bytes>`;
    return String(value);
}

function computeDataHash(rows) {
    const rowHashes = [];

    for (const row of rows) {
        const h = crypto.createHash('sha256');
        for (const cell of row) {
            const text = cellToText(cell);
            h.update(text || 'NULL', 'utf-8');
            h.update('|', 'utf-8');
        }
        rowHashes.push(h.digest('hex'));
    }

    rowHashes.sort();

    const dataHasher = crypto.createHash('sha256');
    dataHasher.update('ROWS:', 'utf-8');
    for (const rh of rowHashes) {
        dataHasher.update(rh, 'utf-8');
        dataHasher.update('\n', 'utf-8');
    }
    return dataHasher.digest('hex');
}

function hex2hash(hexStr) {
    const raw = Buffer.from(hexStr, 'hex');
    let b64 = raw.toString('base64');
    b64 = b64.replace(/\//g, '_').replace(/\+/g, '-');
    return b64.slice(0, 8);
}

// ---------------------------------------------------------------------------
// TestResult
// ---------------------------------------------------------------------------

class TestResult {
    constructor() {
        this.passed = 0;
        this.failed = 0;
        this.errors = 0;
        this.meh = 0;
        this.output = [];
    }

    recordPass(label) {
        this.output.push(`  [PASS] ${label}`);
        this.passed++;
    }

    recordFail(label, detail) {
        this.output.push(`  [FAIL] ${label} (${detail})`);
        this.failed++;
    }

    recordError(label, detail) {
        this.output.push(`  [ERROR] ${label} (${detail})`);
        this.errors++;
    }

    recordMeh(label) {
        this.output.push(`  [MEH]  ${label}`);
        this.meh++;
    }

    merge(other) {
        this.passed += other.passed;
        this.failed += other.failed;
        this.errors += other.errors;
        this.meh += other.meh;
        this.output.push(...other.output);
    }
}

// ---------------------------------------------------------------------------
// Fixture extraction
// ---------------------------------------------------------------------------

function extractFixtureDbs(conn) {
    const rows = conn.prepare(
        'SELECT d.id, d.filename, c.blob ' +
        'FROM data_database d ' +
        'JOIN data_database_contents c ON c.dbid = d.id ' +
        'ORDER BY d.id'
    ).all();

    const dbs = new Map();
    for (const row of rows) {
        const decompressed = Buffer.from(fzstd.decompress(new Uint8Array(row.blob)));
        dbs.set(row.id, { filename: row.filename, data: decompressed });
    }
    return dbs;
}

function writeFixtureDbs(fixtures, tmpdir) {
    const paths = new Map();
    for (const [dbid, { filename, data }] of fixtures) {
        const dest = path.join(tmpdir, filename);
        fs.mkdirSync(path.dirname(dest), { recursive: true });
        fs.writeFileSync(dest, data);
        paths.set(dbid, dest);
    }
    return paths;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function executeAndHash(db, dqlText) {
    const { columns, rows } = db.execute(dqlText);
    const hexHash = computeDataHash(rows);
    return { columns, rows, hexHash };
}

function checkResult(result, label, shouldFail, expectedHash, runFn) {
    if (shouldFail) {
        try {
            runFn();
        } catch (_) {
            result.recordPass(label);
            return;
        }
        result.recordFail(label, 'expected error, got success');
        return;
    }

    let actualHex;
    try {
        actualHex = runFn();
    } catch (exc) {
        result.recordError(label, String(exc.message || exc));
        return;
    }

    if (expectedHash === null || expectedHash === undefined) {
        result.recordMeh(label);
    } else {
        const actualShort = hex2hash(actualHex);
        if (actualShort === expectedHash) {
            result.recordPass(label);
        } else {
            result.recordFail(label, `expected ${expectedHash}, got ${actualShort}`);
        }
    }
}

// ---------------------------------------------------------------------------
// SEF runner
// ---------------------------------------------------------------------------

function runSefBall(ballPath) {
    const result = new TestResult();
    const conn = new BetterSqlite3(ballPath, { readonly: true });

    const fixtures = extractFixtureDbs(conn);

    // Detect schema: legacy balls have dbid directly on side_effect_free,
    // newer balls use a run join table.
    const tables = new Set(
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(r => r.name)
    );
    const hasRunTable = tables.has('run');

    const cases = hasRunTable
        ? conn.prepare(
            'SELECT sef.file, sef.dql, sef.hash, r.dbid, sef.should_fail ' +
            'FROM side_effect_free sef ' +
            'JOIN run r ON sef.run_id = r.id ' +
            'ORDER BY r.dbid, sef.file'
        ).all()
        : conn.prepare(
            'SELECT file, dql, hash, dbid, should_fail ' +
            'FROM side_effect_free ' +
            'ORDER BY dbid, file'
        ).all();

    if (cases.length === 0) {
        conn.close();
        return result;
    }

    const tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), 'dql-cabi-sef-'));
    try {
        const dbPaths = writeFixtureDbs(fixtures, tmpdir);

        let currentDbid = null;
        let db = null;

        try {
            for (const { file, dql, hash: expectedHash, dbid, should_fail } of cases) {
                if (dbid !== currentDbid) {
                    if (db !== null) {
                        db.close();
                        db = null;
                    }
                    const dbPath = dbPaths.get(dbid);
                    if (dbPath === undefined) {
                        result.recordError(file, `unknown dbid ${dbid}`);
                        continue;
                    }
                    try {
                        db = new Database(dbPath);
                    } catch (exc) {
                        result.recordError(file, `mount failed: ${exc.message || exc}`);
                        db = null;
                        currentDbid = null;
                        continue;
                    }
                    currentDbid = dbid;
                }

                if (db === null) {
                    result.recordError(file, 'no database');
                    continue;
                }

                const theDb = db;
                const theDql = dql;
                checkResult(result, file, !!should_fail, expectedHash, () => {
                    const { hexHash } = executeAndHash(theDb, theDql);
                    return hexHash;
                });
            }
        } finally {
            if (db !== null) db.close();
        }
    } finally {
        fs.rmSync(tmpdir, { recursive: true, force: true });
    }

    conn.close();
    return result;
}

// ---------------------------------------------------------------------------
// SES runner
// ---------------------------------------------------------------------------

function createTempDb(dbPath, sql) {
    for (const suffix of ['', '-wal', '-shm', '-journal']) {
        const p = dbPath + suffix;
        if (fs.existsSync(p)) fs.unlinkSync(p);
    }
    const conn = new BetterSqlite3(dbPath);
    conn.exec('CREATE TABLE _dql_init(x); DROP TABLE _dql_init;');
    conn.exec(sql);
    conn.close();
}

function executeSequential(db, dql) {
    const queries = splitQueries(dql);
    let lastHex = '';
    for (const q of queries) {
        const { hexHash } = executeAndHash(db, q);
        lastHex = hexHash;
    }
    return lastHex;
}

function runSesBall(ballPath) {
    const result = new TestResult();
    const conn = new BetterSqlite3(ballPath, { readonly: true });

    const fixtures = extractFixtureDbs(conn);

    // Check if setup_sql column exists
    const colInfo = conn.pragma('table_info(side_effectful_on_system)');
    const colNames = new Set(colInfo.map(r => r.name));
    const hasSetupSql = colNames.has('setup_sql');

    let cases;
    if (hasSetupSql) {
        cases = conn.prepare(
            'SELECT id, file, dql, hash, dbid, should_fail, setup_sql ' +
            'FROM side_effectful_on_system ORDER BY id'
        ).all();
    } else {
        cases = conn.prepare(
            'SELECT id, file, dql, hash, dbid, should_fail ' +
            'FROM side_effectful_on_system ORDER BY id'
        ).all().map(r => ({ ...r, setup_sql: null }));
    }

    // Read DDL files: {test_id: [{filename, content}, ...]}
    const ddlMap = new Map();
    for (const row of conn.prepare(
        'SELECT test_id, filename, content FROM side_effectful_on_system_ddl'
    ).all()) {
        if (!ddlMap.has(row.test_id)) ddlMap.set(row.test_id, []);
        ddlMap.get(row.test_id).push({ filename: row.filename, content: row.content });
    }

    if (cases.length === 0) {
        conn.close();
        return result;
    }

    const tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), 'dql-cabi-ses-'));
    try {
        const dbPaths = writeFixtureDbs(fixtures, tmpdir);

        for (const { id: testId, file, dql, hash: expectedHash, dbid, should_fail, setup_sql } of cases) {
            // Write DDL files for this test case
            const ddlFiles = ddlMap.get(testId) || [];
            for (const { filename: ddlFilename, content: ddlContent } of ddlFiles) {
                const ddlDest = path.join(tmpdir, ddlFilename);
                fs.mkdirSync(path.dirname(ddlDest), { recursive: true });
                fs.writeFileSync(ddlDest, ddlContent, 'utf-8');
            }

            let useDbPath;
            if (setup_sql) {
                const tempDbPath = path.join(tmpdir, `_setup_${testId}.db`);
                try {
                    createTempDb(tempDbPath, setup_sql);
                } catch (exc) {
                    result.recordError(file, `setup_sql failed: ${exc.message || exc}`);
                    continue;
                }
                useDbPath = tempDbPath;
            } else {
                useDbPath = dbPaths.get(dbid);
                if (useDbPath === undefined) {
                    result.recordError(file, `unknown dbid ${dbid}`);
                    continue;
                }
            }

            const theDql = dql;
            const theDbPath = useDbPath;
            const theTmpdir = tmpdir;

            checkResult(result, file, !!should_fail, expectedHash, () => {
                const savedCwd = process.cwd();
                try {
                    process.chdir(theTmpdir);
                    const db = new Database(theDbPath);
                    try {
                        return executeSequential(db, theDql);
                    } finally {
                        db.close();
                    }
                } finally {
                    process.chdir(savedCwd);
                }
            });

            // Clean up temp DB if we created one
            if (setup_sql) {
                const tempDbPath = path.join(tmpdir, `_setup_${testId}.db`);
                for (const suffix of ['', '-wal', '-shm', '-journal']) {
                    const p = tempDbPath + suffix;
                    if (fs.existsSync(p)) fs.unlinkSync(p);
                }
            }
        }
    } finally {
        fs.rmSync(tmpdir, { recursive: true, force: true });
    }

    conn.close();
    return result;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function main() {
    const testSuiteDir = process.argv[2]
        ? path.resolve(process.argv[2])
        : path.resolve(__dirname, '..', '..', 'test_suite');

    let ballFiles = fs.readdirSync(testSuiteDir)
        .filter(f => f.endsWith('.db'))
        .sort()
        .map(f => path.join(testSuiteDir, f));

    if (ballFiles.length === 0) {
        process.stderr.write(`No .db files found in ${testSuiteDir}\n`);
        process.exit(1);
    }

    // Skip pre-split balls when split variants exist
    const ballNames = new Set(ballFiles.map(f => path.basename(f)));
    const skip = new Set();
    for (const name of ballNames) {
        const stem = name.replace(/\.db$/, '');
        if (ballNames.has(`${stem}-a.db`) && ballNames.has(`${stem}-b.db`)) {
            skip.add(name);
        }
    }
    ballFiles = ballFiles.filter(f => !skip.has(path.basename(f)));

    const total = new TestResult();

    for (const ballPath of ballFiles) {
        const conn = new BetterSqlite3(ballPath, { readonly: true });
        const tables = new Set(
            conn.prepare(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).all().map(r => r.name)
        );
        conn.close();

        let ballType, runner;
        if (tables.has('side_effect_free')) {
            ballType = 'Side-Effect-Free';
            runner = runSefBall;
        } else if (tables.has('side_effectful_on_system')) {
            ballType = 'Side-Effectful';
            runner = runSesBall;
        } else {
            continue;
        }

        console.log(`\n--- ${ballType} (${path.basename(ballPath)}) ---`);
        const result = runner(ballPath);

        for (const line of result.output) {
            console.log(line);
        }
        total.merge(result);
    }

    console.log();
    console.log('=== Node.js C-ABI Test Results ===');
    console.log(
        `PASS: ${total.passed}  ` +
        `FAIL: ${total.failed}  ` +
        `ERROR: ${total.errors}  ` +
        `MEH: ${total.meh}`
    );

    return 0;
}

process.exit(main());
