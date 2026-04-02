// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
'use strict';

const { Database, splitQueries } = require('.');

const dbPath = process.argv[2];
if (!dbPath) {
    console.error('Usage: node smoke.js <path/to/db.db> [query]');
    process.exit(1);
}

const db = new Database(dbPath);
const dql = process.argv[3] || 'users(*)';
const { columns, rows } = db.execute(dql);
console.log('columns:', columns);
console.log('rows:', rows.length);
rows.slice(0, 5).forEach(r => console.log(' ', r));
db.close();

// Quick split test
console.log('split:', splitQueries('users(*)\norders(*)'));
