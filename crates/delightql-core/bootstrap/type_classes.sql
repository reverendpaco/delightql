-- THE DOCUMENT DENYLIST's target fact: which declared column types each
-- target family classes numeric, date or boolean. A column so declared holds
-- no document, and a document operation over it refuses; every other
-- declaration (textual, unknown, undeclared) lowers. `{table}` is the one
-- placeholder: each host writes the name its own SQL substrate holds the
-- table under (native: the bootstrap catalog's `type_classes`, activated as
-- sys::targeting.type_classes).
--
-- dialect    the family the row belongs to. Every family states its own
--            vocabulary; no row is shared between families.
-- reads      how the row matches a column declaration:
--              affinity   the affinity SQLite's rule gives the declaration
--                         (datatype3 §3.1, stated once in the product) is
--                         `type_name`;
--              type_name  the declaration's leading words, the parameter
--                         list dropped, are `type_name`; the longest run of
--                         them a row names (`DECIMAL(10, 2)` is `decimal`,
--                         `TIMESTAMP WITH TIME ZONE` is `timestamp`,
--                         `DOUBLE PRECISION` is `double precision`).
--            A declaration takes the class of the type_name row it matches,
--            else of the affinity row it matches; one it matches no row of
--            is unknown, and lowers.
-- type_name  an affinity's name, or a type name, in lower case, words
--            separated by one space.
-- class      numeric | date | boolean.
--
-- SQLite reads a declaration by positive affinity plus named rows. A name
-- SQLite's rule positively gives INTEGER affinity (it contains "INT") or
-- REAL affinity (it contains "REAL", "FLOA" or "DOUB", and none of rule 2's
-- or rule 3's substrings) is numeric: the engine coerces what the column
-- stores, so a document read of it would be a silent NULL. NUMERIC affinity
-- has no row: SQLite gives it to every name no rule recognises (`JSON`, an
-- author's own word), and such a name lowers. The standard names below are
-- numeric, date or boolean by name. Every other family reads type names
-- only, from its own engine's closed vocabulary.

CREATE TABLE {table} (
    dialect    TEXT NOT NULL
        CHECK (dialect IN ('sqlite', 'postgres', 'mysql', 'sqlserver', 'duckdb')),
    reads      TEXT NOT NULL CHECK (reads IN ('affinity', 'type_name')),
    type_name  TEXT NOT NULL
        CHECK (type_name <> '' AND type_name = lower(type_name)),
    class      TEXT NOT NULL CHECK (class IN ('numeric', 'date', 'boolean')),
    PRIMARY KEY (dialect, reads, type_name)
);

INSERT INTO {table} (dialect, reads, type_name, class) VALUES
    -- SQLite: the affinities its rule positively derives.
    ('sqlite',    'affinity',  'integer',                  'numeric'),
    ('sqlite',    'affinity',  'real',                     'numeric'),
    -- SQLite: the standard names its rule does not positively derive.
    ('sqlite',    'type_name', 'numeric',                  'numeric'),
    ('sqlite',    'type_name', 'decimal',                  'numeric'),
    ('sqlite',    'type_name', 'dec',                      'numeric'),
    ('sqlite',    'type_name', 'fixed',                    'numeric'),
    ('sqlite',    'type_name', 'money',                    'numeric'),
    ('sqlite',    'type_name', 'smallmoney',               'numeric'),
    ('sqlite',    'type_name', 'serial',                   'numeric'),
    ('sqlite',    'type_name', 'smallserial',              'numeric'),
    ('sqlite',    'type_name', 'bigserial',                'numeric'),
    ('sqlite',    'type_name', 'serial2',                  'numeric'),
    ('sqlite',    'type_name', 'serial4',                  'numeric'),
    ('sqlite',    'type_name', 'serial8',                  'numeric'),
    ('sqlite',    'type_name', 'date',                     'date'),
    ('sqlite',    'type_name', 'time',                     'date'),
    ('sqlite',    'type_name', 'timetz',                   'date'),
    ('sqlite',    'type_name', 'timestamp',                'date'),
    ('sqlite',    'type_name', 'timestamptz',              'date'),
    ('sqlite',    'type_name', 'datetime',                 'date'),
    ('sqlite',    'type_name', 'datetime2',                'date'),
    ('sqlite',    'type_name', 'smalldatetime',            'date'),
    ('sqlite',    'type_name', 'datetimeoffset',           'date'),
    ('sqlite',    'type_name', 'year',                     'date'),
    ('sqlite',    'type_name', 'boolean',                  'boolean'),
    ('sqlite',    'type_name', 'bool',                     'boolean'),
    ('sqlite',    'type_name', 'logical',                  'boolean'),

    -- DuckDB: its types and their aliases.
    ('duckdb',    'type_name', 'tinyint',                  'numeric'),
    ('duckdb',    'type_name', 'int1',                     'numeric'),
    ('duckdb',    'type_name', 'smallint',                 'numeric'),
    ('duckdb',    'type_name', 'int2',                     'numeric'),
    ('duckdb',    'type_name', 'int16',                    'numeric'),
    ('duckdb',    'type_name', 'short',                    'numeric'),
    ('duckdb',    'type_name', 'integer',                  'numeric'),
    ('duckdb',    'type_name', 'int4',                     'numeric'),
    ('duckdb',    'type_name', 'int32',                    'numeric'),
    ('duckdb',    'type_name', 'int',                      'numeric'),
    ('duckdb',    'type_name', 'signed',                   'numeric'),
    ('duckdb',    'type_name', 'bigint',                   'numeric'),
    ('duckdb',    'type_name', 'int8',                     'numeric'),
    ('duckdb',    'type_name', 'int64',                    'numeric'),
    ('duckdb',    'type_name', 'long',                     'numeric'),
    ('duckdb',    'type_name', 'hugeint',                  'numeric'),
    ('duckdb',    'type_name', 'int128',                   'numeric'),
    ('duckdb',    'type_name', 'utinyint',                 'numeric'),
    ('duckdb',    'type_name', 'uint8',                    'numeric'),
    ('duckdb',    'type_name', 'usmallint',                'numeric'),
    ('duckdb',    'type_name', 'uint16',                   'numeric'),
    ('duckdb',    'type_name', 'uinteger',                 'numeric'),
    ('duckdb',    'type_name', 'uint32',                   'numeric'),
    ('duckdb',    'type_name', 'ubigint',                  'numeric'),
    ('duckdb',    'type_name', 'uint64',                   'numeric'),
    ('duckdb',    'type_name', 'uhugeint',                 'numeric'),
    ('duckdb',    'type_name', 'uint128',                  'numeric'),
    ('duckdb',    'type_name', 'varint',                   'numeric'),
    ('duckdb',    'type_name', 'bignum',                   'numeric'),
    ('duckdb',    'type_name', 'decimal',                  'numeric'),
    ('duckdb',    'type_name', 'numeric',                  'numeric'),
    ('duckdb',    'type_name', 'float',                    'numeric'),
    ('duckdb',    'type_name', 'float4',                   'numeric'),
    ('duckdb',    'type_name', 'real',                     'numeric'),
    ('duckdb',    'type_name', 'double',                   'numeric'),
    ('duckdb',    'type_name', 'float8',                   'numeric'),
    ('duckdb',    'type_name', 'date',                     'date'),
    ('duckdb',    'type_name', 'time',                     'date'),
    ('duckdb',    'type_name', 'timetz',                   'date'),
    ('duckdb',    'type_name', 'time_ns',                  'date'),
    ('duckdb',    'type_name', 'timestamp',                'date'),
    ('duckdb',    'type_name', 'datetime',                 'date'),
    ('duckdb',    'type_name', 'timestamptz',              'date'),
    ('duckdb',    'type_name', 'timestamp_s',              'date'),
    ('duckdb',    'type_name', 'timestamp_ms',             'date'),
    ('duckdb',    'type_name', 'timestamp_us',             'date'),
    ('duckdb',    'type_name', 'timestamp_ns',             'date'),
    ('duckdb',    'type_name', 'interval',                 'date'),
    ('duckdb',    'type_name', 'boolean',                  'boolean'),
    ('duckdb',    'type_name', 'bool',                     'boolean'),
    ('duckdb',    'type_name', 'logical',                  'boolean'),

    -- PostgreSQL: its numeric, date/time and boolean types.
    ('postgres',  'type_name', 'smallint',                 'numeric'),
    ('postgres',  'type_name', 'integer',                  'numeric'),
    ('postgres',  'type_name', 'bigint',                   'numeric'),
    ('postgres',  'type_name', 'int',                      'numeric'),
    ('postgres',  'type_name', 'int2',                     'numeric'),
    ('postgres',  'type_name', 'int4',                     'numeric'),
    ('postgres',  'type_name', 'int8',                     'numeric'),
    ('postgres',  'type_name', 'decimal',                  'numeric'),
    ('postgres',  'type_name', 'numeric',                  'numeric'),
    ('postgres',  'type_name', 'real',                     'numeric'),
    ('postgres',  'type_name', 'float',                    'numeric'),
    ('postgres',  'type_name', 'float4',                   'numeric'),
    ('postgres',  'type_name', 'float8',                   'numeric'),
    ('postgres',  'type_name', 'double precision',         'numeric'),
    ('postgres',  'type_name', 'smallserial',              'numeric'),
    ('postgres',  'type_name', 'serial',                   'numeric'),
    ('postgres',  'type_name', 'bigserial',                'numeric'),
    ('postgres',  'type_name', 'serial2',                  'numeric'),
    ('postgres',  'type_name', 'serial4',                  'numeric'),
    ('postgres',  'type_name', 'serial8',                  'numeric'),
    ('postgres',  'type_name', 'money',                    'numeric'),
    ('postgres',  'type_name', 'date',                     'date'),
    ('postgres',  'type_name', 'time',                     'date'),
    ('postgres',  'type_name', 'timetz',                   'date'),
    ('postgres',  'type_name', 'timestamp',                'date'),
    ('postgres',  'type_name', 'timestamptz',              'date'),
    ('postgres',  'type_name', 'interval',                 'date'),
    ('postgres',  'type_name', 'boolean',                  'boolean'),
    ('postgres',  'type_name', 'bool',                     'boolean'),

    -- MySQL: its numeric, date/time and boolean types (BOOLEAN is
    -- TINYINT(1)).
    ('mysql',     'type_name', 'tinyint',                  'numeric'),
    ('mysql',     'type_name', 'smallint',                 'numeric'),
    ('mysql',     'type_name', 'mediumint',                'numeric'),
    ('mysql',     'type_name', 'int',                      'numeric'),
    ('mysql',     'type_name', 'integer',                  'numeric'),
    ('mysql',     'type_name', 'bigint',                   'numeric'),
    ('mysql',     'type_name', 'serial',                   'numeric'),
    ('mysql',     'type_name', 'decimal',                  'numeric'),
    ('mysql',     'type_name', 'dec',                      'numeric'),
    ('mysql',     'type_name', 'numeric',                  'numeric'),
    ('mysql',     'type_name', 'fixed',                    'numeric'),
    ('mysql',     'type_name', 'float',                    'numeric'),
    ('mysql',     'type_name', 'double',                   'numeric'),
    ('mysql',     'type_name', 'real',                     'numeric'),
    ('mysql',     'type_name', 'bit',                      'numeric'),
    ('mysql',     'type_name', 'date',                     'date'),
    ('mysql',     'type_name', 'time',                     'date'),
    ('mysql',     'type_name', 'datetime',                 'date'),
    ('mysql',     'type_name', 'timestamp',                'date'),
    ('mysql',     'type_name', 'year',                     'date'),
    ('mysql',     'type_name', 'boolean',                  'boolean'),
    ('mysql',     'type_name', 'bool',                     'boolean'),

    -- SQL Server: its exact and approximate numerics and its date and time
    -- types. `timestamp` there is a row version (binary), so it has no row.
    ('sqlserver', 'type_name', 'tinyint',                  'numeric'),
    ('sqlserver', 'type_name', 'smallint',                 'numeric'),
    ('sqlserver', 'type_name', 'int',                      'numeric'),
    ('sqlserver', 'type_name', 'bigint',                   'numeric'),
    ('sqlserver', 'type_name', 'bit',                      'numeric'),
    ('sqlserver', 'type_name', 'decimal',                  'numeric'),
    ('sqlserver', 'type_name', 'dec',                      'numeric'),
    ('sqlserver', 'type_name', 'numeric',                  'numeric'),
    ('sqlserver', 'type_name', 'money',                    'numeric'),
    ('sqlserver', 'type_name', 'smallmoney',               'numeric'),
    ('sqlserver', 'type_name', 'float',                    'numeric'),
    ('sqlserver', 'type_name', 'real',                     'numeric'),
    ('sqlserver', 'type_name', 'double precision',         'numeric'),
    ('sqlserver', 'type_name', 'date',                     'date'),
    ('sqlserver', 'type_name', 'time',                     'date'),
    ('sqlserver', 'type_name', 'datetime',                 'date'),
    ('sqlserver', 'type_name', 'datetime2',                'date'),
    ('sqlserver', 'type_name', 'datetimeoffset',           'date'),
    ('sqlserver', 'type_name', 'smalldatetime',            'date');
