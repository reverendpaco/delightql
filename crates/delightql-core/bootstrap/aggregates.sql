-- The a priori aggregate facts: one row per (dialect, functor, resolved
-- arity) whose call reduces the rows it stands over. `{table}` is the one
-- placeholder: each host writes the name its own SQL substrate holds the
-- table under (native: the bootstrap catalog's `aggregates`, activated as
-- sys::targeting.aggregates).
--
-- A row is POSITIVE knowledge. A callable with no row is unknown, never
-- scalar: the target surface stays open and an unknown call is the author's
-- assertion of whatever grade its position asks for.
--
-- dialect         '*' is every supported dialect; a family name is that
--                 dialect alone. For the same functor and arity the exact
--                 family's row is read in place of the '*' row.
-- functor_name    the functor as DelightQL spells the call, folded to
--                 lower case — the name before any render rule renames it
--                 for a target (postgres renders group_concat as string_agg,
--                 so its row says group_concat).
-- arity           the resolved argument row's members, the whole-operand
--                 star of count:(*) counted as one.
-- can_be_globbed  1 when the whole-operand star may stand as the call's
--                 argument. It is permission for that one form, not a
--                 second grade: sum:(x) is an aggregate and sum:(*) is not
--                 admitted.
-- can_be_windowed 1 when the target evaluates the call over a window
--                 (`OVER (…)`); 0 when it evaluates the aggregate only as a
--                 grouped reduction (mysql GROUP_CONCAT, sqlserver
--                 STRING_AGG). Permission for that one form, like the star.
--
-- A target row is seeded only where that target is known to have the
-- aggregate. Copying a name to every family would turn a guess into a
-- refusal on the families that lack it.

CREATE TABLE {table} (
    dialect         TEXT NOT NULL
        CHECK (dialect IN ('*', 'sqlite', 'postgres', 'mysql', 'sqlserver', 'duckdb')),
    functor_name    TEXT NOT NULL
        CHECK (functor_name <> '' AND functor_name = lower(functor_name)),
    arity           INTEGER NOT NULL CHECK (arity >= 0),
    can_be_globbed  INTEGER NOT NULL CHECK (can_be_globbed IN (0, 1)),
    can_be_windowed INTEGER NOT NULL CHECK (can_be_windowed IN (0, 1)),
    PRIMARY KEY (dialect, functor_name, arity)
);

INSERT INTO {table} (dialect, functor_name, arity, can_be_globbed, can_be_windowed) VALUES
    ('*',         'count',              1, 1, 1),
    ('*',         'sum',                1, 0, 1),
    ('*',         'avg',                1, 0, 1),
    ('*',         'min',                1, 0, 1),
    ('*',         'max',                1, 0, 1),

    ('sqlite',    'total',              1, 0, 1),
    ('sqlite',    'group_concat',       1, 0, 1),
    ('sqlite',    'group_concat',       2, 0, 1),
    ('sqlite',    'string_agg',         2, 0, 1),
    ('sqlite',    'json_group_array',   1, 0, 1),
    ('sqlite',    'json_group_object',  2, 0, 1),
    ('sqlite',    'jsonb_group_array',  1, 0, 1),
    ('sqlite',    'jsonb_group_object', 2, 0, 1),

    ('duckdb',    'string_agg',         1, 0, 1),
    ('duckdb',    'string_agg',         2, 0, 1),
    ('duckdb',    'group_concat',       1, 0, 1),
    ('duckdb',    'group_concat',       2, 0, 1),
    ('duckdb',    'listagg',            1, 0, 1),
    ('duckdb',    'list',               1, 0, 1),
    ('duckdb',    'array_agg',          1, 0, 1),
    ('duckdb',    'json_group_array',   1, 0, 1),
    ('duckdb',    'json_group_object',  2, 0, 1),
    ('duckdb',    'bool_and',           1, 0, 1),
    ('duckdb',    'bool_or',            1, 0, 1),
    ('duckdb',    'bit_and',            1, 0, 1),
    ('duckdb',    'bit_or',             1, 0, 1),
    ('duckdb',    'bit_xor',            1, 0, 1),
    ('duckdb',    'median',             1, 0, 1),
    ('duckdb',    'mode',               1, 0, 1),
    ('duckdb',    'stddev',             1, 0, 1),
    ('duckdb',    'stddev_pop',         1, 0, 1),
    ('duckdb',    'stddev_samp',        1, 0, 1),
    ('duckdb',    'variance',           1, 0, 1),
    ('duckdb',    'var_pop',            1, 0, 1),
    ('duckdb',    'var_samp',           1, 0, 1),

    ('postgres',  'group_concat',       1, 0, 1),
    ('postgres',  'group_concat',       2, 0, 1),
    ('postgres',  'json_group_object',  2, 0, 1),
    ('postgres',  'string_agg',         2, 0, 1),
    ('postgres',  'array_agg',          1, 0, 1),
    ('postgres',  'json_agg',           1, 0, 1),
    ('postgres',  'jsonb_agg',          1, 0, 1),
    ('postgres',  'json_object_agg',    2, 0, 1),
    ('postgres',  'jsonb_object_agg',   2, 0, 1),
    ('postgres',  'bool_and',           1, 0, 1),
    ('postgres',  'bool_or',            1, 0, 1),
    ('postgres',  'every',              1, 0, 1),
    ('postgres',  'bit_and',            1, 0, 1),
    ('postgres',  'bit_or',             1, 0, 1),
    ('postgres',  'stddev',             1, 0, 1),
    ('postgres',  'stddev_pop',         1, 0, 1),
    ('postgres',  'stddev_samp',        1, 0, 1),
    ('postgres',  'variance',           1, 0, 1),
    ('postgres',  'var_pop',            1, 0, 1),
    ('postgres',  'var_samp',           1, 0, 1),

    ('mysql',     'group_concat',       1, 0, 0),
    ('mysql',     'group_concat',       2, 0, 0),
    ('mysql',     'json_arrayagg',      1, 0, 1),
    ('mysql',     'json_objectagg',     2, 0, 1),
    ('mysql',     'bit_and',            1, 0, 1),
    ('mysql',     'bit_or',             1, 0, 1),
    ('mysql',     'bit_xor',            1, 0, 1),
    ('mysql',     'std',                1, 0, 1),
    ('mysql',     'stddev',             1, 0, 1),
    ('mysql',     'stddev_pop',         1, 0, 1),
    ('mysql',     'stddev_samp',        1, 0, 1),
    ('mysql',     'variance',           1, 0, 1),
    ('mysql',     'var_pop',            1, 0, 1),
    ('mysql',     'var_samp',           1, 0, 1),

    ('sqlserver', 'string_agg',         2, 0, 0),
    ('sqlserver', 'count_big',          1, 1, 1),
    ('sqlserver', 'stdev',              1, 0, 1),
    ('sqlserver', 'stdevp',             1, 0, 1),
    ('sqlserver', 'var',                1, 0, 1),
    ('sqlserver', 'varp',               1, 0, 1);
