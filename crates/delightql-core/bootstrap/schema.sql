-- DelightQL Bootstrap Schema
-- This file defines all metadata tables for the DDL-LIGHT cartridge/entity/namespace system
-- See: documentation/design/ddl/SYS-NS-CARTRIDGE-ER-DESIGN.md

-- Enable foreign key constraints
PRAGMA foreign_keys = ON;

-- ============================================================================
-- REFERENCE TABLES (Pre-installed enumeration types)
-- ============================================================================

-- Language/Dialect variants (DQL/standard, SQL/postgres, etc.)
CREATE TABLE language (
    id INTEGER PRIMARY KEY,
    language TEXT NOT NULL,
    dialect TEXT,
    version TEXT
);

-- Source type variants (file, filebin, db, bin)
CREATE TABLE source_type_enum (
    id INTEGER PRIMARY KEY,
    variant TEXT NOT NULL,
    explanation TEXT
);

-- Entity type variants (DQLFunctionExpression, DBPermanentTable, etc.)
CREATE TABLE entity_type_enum (
    id INTEGER PRIMARY KEY,
    variant TEXT NOT NULL,
    is_ho INTEGER NOT NULL DEFAULT 0,  -- boolean: is higher-order
    is_fn INTEGER NOT NULL DEFAULT 0   -- boolean: is function
);

-- Connection type variants (how to physically connect)
CREATE TABLE connection_type_enum (
    id INTEGER PRIMARY KEY,
    variant TEXT NOT NULL,
    explanation TEXT
);

-- ============================================================================
-- CONNECTION TABLES (Physical database connection management)
-- ============================================================================

-- Connection: Represents a physical database connection
-- Multiple cartridges can share the same connection_id, enabling cross-schema queries
--
-- Three orthogonal facts (URI-DESIGN.md §4), not one overloaded string:
--   resource_uri — WHAT the user named (worldly spelling; the literal
--                  label 'session:primary' for the pre-mount placeholder)
--   mechanism    — HOW DelightQL reaches it (in-process | fatboy | siso | attach)
--   identity     — what the resource ASSERTS about itself, obtained at
--                  connect, method-prefixed (pg-system-id:…, realpath:…).
-- Identity is the unique key when present; resource/mechanism need not be
-- unique (two spellings may reach one server — identity catches that).
CREATE TABLE connection (
    id INTEGER PRIMARY KEY,
    resource_uri TEXT NOT NULL,
    mechanism TEXT NOT NULL DEFAULT 'in-process',
    identity TEXT,
    connection_type INTEGER NOT NULL,
    description TEXT,
    FOREIGN KEY (connection_type) REFERENCES connection_type_enum(id)
);
CREATE UNIQUE INDEX connection_identity_uq ON connection(identity)
    WHERE identity IS NOT NULL;

-- ============================================================================
-- CARTRIDGE TABLES (Cartridge metadata and source management)
-- ============================================================================

-- Cartridge: Represents a source of definitions (code or data)
CREATE TABLE cartridge (
    id INTEGER PRIMARY KEY,
    language INTEGER NOT NULL,
    source_type_enum INTEGER NOT NULL,
    source_uri TEXT NOT NULL,
    source_ns TEXT,
    connected INTEGER NOT NULL DEFAULT 0,  -- boolean
    creation_time INTEGER DEFAULT (strftime('%s', 'now')),
    connection_id INTEGER,  -- NULL for universal cartridges
    is_universal INTEGER NOT NULL DEFAULT 0,  -- boolean: works on all connections
    FOREIGN KEY (language) REFERENCES language(id),
    FOREIGN KEY (source_type_enum) REFERENCES source_type_enum(id),
    FOREIGN KEY (connection_id) REFERENCES connection(id),
    CHECK (
        -- Either connected to a specific connection OR universal (not both)
        (is_universal = 1 AND connection_id IS NULL) OR
        (is_universal = 0 AND connection_id IS NOT NULL)
    )
);

-- ============================================================================
-- ENTITY TABLES (Entity metadata, references, and attributes)
-- ============================================================================

-- Entity: Stores entity definitions (views, functions, tables, etc.)
CREATE TABLE entity (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    type INTEGER NOT NULL,
    cartridge_id INTEGER NOT NULL,
    doc TEXT,
    FOREIGN KEY (type) REFERENCES entity_type_enum(id),
    FOREIGN KEY (cartridge_id) REFERENCES cartridge(id)
);

-- Entity Clause: Stores individual definition clauses for an entity.
-- Single-clause entities (most views, functions) have one row.
-- Multi-clause entities (disjunctive functions, sigma predicates, facts) have multiple rows.
CREATE TABLE entity_clause (
    id INTEGER PRIMARY KEY,
    entity_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    definition TEXT NOT NULL,
    location TEXT,
    FOREIGN KEY (entity_id) REFERENCES entity(id)
);

-- Referenced Entity: Stores references found in entity definitions
-- Each occurrence gets its own row, even if they look identical
CREATE TABLE referenced_entity (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    namespace TEXT,
    apparent_type INTEGER,
    containing_entity_id INTEGER NOT NULL,
    location TEXT,
    FOREIGN KEY (apparent_type) REFERENCES entity_type_enum(id),
    FOREIGN KEY (containing_entity_id) REFERENCES entity(id)
);

-- Entity Attribute: Stores columns/parameters/domains for entities
CREATE TABLE entity_attribute (
    id INTEGER PRIMARY KEY,
    entity_id INTEGER NOT NULL,
    attribute_name TEXT NOT NULL,
    attribute_type TEXT NOT NULL,  -- 'input_param', 'output_column', 'context_param'
    data_type TEXT,
    position INTEGER,
    is_nullable INTEGER DEFAULT 1,  -- boolean
    default_value TEXT,
    FOREIGN KEY (entity_id) REFERENCES entity(id),
    UNIQUE (entity_id, attribute_name, attribute_type)
);

-- HO view parameters with kind metadata
CREATE TABLE ho_param (
    id INTEGER PRIMARY KEY,
    entity_id INTEGER NOT NULL,
    param_name TEXT NOT NULL,
    position INTEGER NOT NULL,
    kind TEXT NOT NULL,  -- 'glob', 'argumentative', 'scalar', 'ground_scalar'
    ground_mode TEXT,    -- 'pure_ground', 'mixed_ground', 'pure_unbound', 'input_only'
    column_name TEXT,    -- canonical name from free-var clauses (NULL for table params)
    FOREIGN KEY (entity_id) REFERENCES entity(id)
);

-- Column schema for argumentative functor parameters
CREATE TABLE ho_param_column (
    id INTEGER PRIMARY KEY,
    ho_param_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    column_position INTEGER NOT NULL,
    FOREIGN KEY (ho_param_id) REFERENCES ho_param(id)
);

-- Per-clause ground values for GroundScalar positions
CREATE TABLE ho_param_ground_value (
    ho_param_id     INTEGER NOT NULL,
    clause_ordinal  INTEGER NOT NULL,
    ground_value    TEXT NOT NULL,
    FOREIGN KEY (ho_param_id) REFERENCES ho_param(id),
    PRIMARY KEY (ho_param_id, clause_ordinal)
);

-- ER-context rule metadata: stores table pair and context for ER-join rules
CREATE TABLE er_rule (
    id INTEGER PRIMARY KEY,
    entity_id INTEGER NOT NULL,
    left_table TEXT NOT NULL,
    right_table TEXT NOT NULL,
    context_name TEXT NOT NULL,
    clause_ordinal INTEGER NOT NULL,
    FOREIGN KEY (entity_id) REFERENCES entity(id)
);

-- Interior Entity: Tracks interior relations (tree group columns) within entities.
-- When a view produces a tree group column (e.g., ~> {name, type} as entities),
-- an interior_entity row links the parent entity to the column name.
CREATE TABLE interior_entity (
    id INTEGER PRIMARY KEY,
    parent_entity_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    FOREIGN KEY (parent_entity_id) REFERENCES entity(id)
);

-- Interior Entity Attribute: Columns within an interior entity.
-- For nested interior relations (e.g., entities with a nested columns tree group),
-- child_interior_entity_id points to another interior_entity row.
CREATE TABLE interior_entity_attribute (
    id INTEGER PRIMARY KEY,
    interior_entity_id INTEGER NOT NULL,
    attribute_name TEXT NOT NULL,
    position INTEGER NOT NULL,
    child_interior_entity_id INTEGER,
    FOREIGN KEY (interior_entity_id) REFERENCES interior_entity(id),
    FOREIGN KEY (child_interior_entity_id) REFERENCES interior_entity(id)
);

-- ============================================================================
-- Entity Resolution: Tracks when a reference resolves to a definition
CREATE TABLE entity_resolution (
    entity_id INTEGER NOT NULL,
    referenced_entity_id INTEGER NOT NULL,
    FOREIGN KEY (entity_id) REFERENCES entity(id),
    FOREIGN KEY (referenced_entity_id) REFERENCES referenced_entity(id),
    PRIMARY KEY (entity_id, referenced_entity_id)
);

-- ============================================================================
-- NAMESPACE TABLES (Namespace hierarchy and entity activation)
-- ============================================================================

-- Namespace: Hierarchical namespace tree
CREATE TABLE namespace (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    pid INTEGER,
    fq_name TEXT,
    default_data_ns TEXT,
    kind TEXT NOT NULL DEFAULT 'unknown',
    provenance TEXT,
    source_path TEXT,
    writable INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (pid) REFERENCES namespace(id)
);

-- Activated Entity: Tracks which entities are active in which namespaces
CREATE TABLE activated_entity (
    entity_id INTEGER NOT NULL,
    activation_time INTEGER DEFAULT (strftime('%s', 'now')),
    namespace_id INTEGER NOT NULL,
    cartridge_id INTEGER NOT NULL,
    FOREIGN KEY (entity_id) REFERENCES entity(id),
    FOREIGN KEY (namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (cartridge_id) REFERENCES cartridge(id),
    PRIMARY KEY (entity_id, namespace_id)
);

-- Enlisted Entity: Entity aliased into another namespace
CREATE TABLE enlisted_entity (
    name TEXT,
    entity_id INTEGER NOT NULL,
    from_namespace_id INTEGER NOT NULL,
    to_namespace_id INTEGER NOT NULL,
    FOREIGN KEY (entity_id) REFERENCES entity(id),
    FOREIGN KEY (from_namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (to_namespace_id) REFERENCES namespace(id)
);

-- Enlisted Namespace: Entire namespace enlisted into another
CREATE TABLE enlisted_namespace (
    from_namespace_id INTEGER NOT NULL,
    to_namespace_id INTEGER NOT NULL,
    PRIMARY KEY (from_namespace_id, to_namespace_id),
    FOREIGN KEY (from_namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (to_namespace_id) REFERENCES namespace(id)
);

-- Namespace Local Enlist: Records which namespaces were enlisted inside a DDL file.
-- These are scoped to the DDL's namespace — they don't leak to the caller.
-- Used by the resolver to activate dependencies when resolving a view body.
CREATE TABLE namespace_local_enlist (
    namespace_id INTEGER NOT NULL,       -- The DDL's own namespace
    enlisted_namespace_id INTEGER NOT NULL, -- The namespace that was enlisted inside the DDL
    PRIMARY KEY (namespace_id, enlisted_namespace_id),
    FOREIGN KEY (namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (enlisted_namespace_id) REFERENCES namespace(id)
);

-- Namespace Local Alias: Records which aliases were created inside a DDL file.
-- These are scoped to the DDL's namespace — they don't leak to the caller.
-- Used by the resolver to activate alias dependencies when resolving a view body.
CREATE TABLE namespace_local_alias (
    namespace_id INTEGER NOT NULL,
    alias TEXT NOT NULL,
    target_namespace_id INTEGER NOT NULL,
    PRIMARY KEY (namespace_id, alias),
    FOREIGN KEY (namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (target_namespace_id) REFERENCES namespace(id)
);

-- Exposed Namespace: Records which child namespaces a DDL re-exports
-- through its facade. When someone enlists the parent, exposed children's
-- entities become visible too.
CREATE TABLE exposed_namespace (
    exposing_namespace_id INTEGER NOT NULL,
    exposed_namespace_id INTEGER NOT NULL,
    PRIMARY KEY (exposing_namespace_id, exposed_namespace_id),
    FOREIGN KEY (exposing_namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (exposed_namespace_id) REFERENCES namespace(id)
);

-- Namespace Alias: Short alias for a namespace (e.g., "l" → "lib::math")
CREATE TABLE namespace_alias (
    alias TEXT NOT NULL PRIMARY KEY,
    target_namespace_id INTEGER NOT NULL,
    FOREIGN KEY (target_namespace_id) REFERENCES namespace(id)
);

-- Grounding: Tracks which namespaces borrow from which data/lib namespaces.
-- Used for ownership enforcement: a namespace cannot be destroyed while borrowed.
CREATE TABLE grounding (
    id INTEGER PRIMARY KEY,
    grounded_namespace_id INTEGER NOT NULL,
    data_namespace_id INTEGER NOT NULL,
    lib_namespace_id INTEGER NOT NULL,
    FOREIGN KEY (grounded_namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (data_namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (lib_namespace_id) REFERENCES namespace(id)
);

-- ============================================================================
-- VIEWS (Derived/computed data)
-- ============================================================================

-- Grounded Entity: Entities where all references (direct and transitive) are resolved
-- An entity is grounded when it has no dangling references
CREATE VIEW GroundedEntity AS
SELECT DISTINCT e.id as entity_id, e.cartridge_id
FROM entity e
WHERE NOT EXISTS (
    -- Has no unresolved references
    SELECT 1 FROM referenced_entity re
    WHERE re.containing_entity_id = e.id
      AND NOT EXISTS (
          -- Reference is resolved
          SELECT 1 FROM entity_resolution er
          WHERE er.referenced_entity_id = re.id
      )
);

-- External Namespaces: All external namespaces mentioned in entity definitions
-- Shows which external cartridges need to be loaded
CREATE VIEW ExternalNamespaces AS
SELECT DISTINCT re.namespace, e.id as entity_id, e.cartridge_id
FROM referenced_entity re
JOIN entity e ON re.containing_entity_id = e.id
WHERE re.namespace IS NOT NULL;

-- ============================================================================
-- EXECUTION DIAGNOSTICS TABLES (sys::execution)
-- ============================================================================

-- Compilation: One row per query compilation attempt (success or failure).
-- Records DQL input, generated SQL, error info, and derived metrics.
CREATE TABLE compilation (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    dql_input   TEXT NOT NULL,
    sql_output  TEXT,
    sql_length  INTEGER,
    cte_count   INTEGER,
    error       TEXT,
    timestamp   TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now'))
);

-- Stack: Per-function max recursion depth reached during each compilation.
CREATE TABLE stack (
    compilation_id  INTEGER NOT NULL
                    REFERENCES compilation(id) ON DELETE CASCADE,
    function_name   TEXT NOT NULL,
    max_depth       INTEGER NOT NULL,
    PRIMARY KEY (compilation_id, function_name)
);

-- Ring buffer: keep the most recent 1000 compilations, auto-delete oldest.
-- ON DELETE CASCADE on stack cleans up child rows automatically.
CREATE TRIGGER IF NOT EXISTS trim_compilation_history
AFTER INSERT ON compilation
BEGIN
    DELETE FROM compilation
    WHERE id <= (SELECT MAX(id) - 1000 FROM compilation);
END;

-- ============================================================================
-- TARGETING RULE TABLES (sys::targeting)
-- ============================================================================
-- Data-driven multi-target transpilation rules (ALL-SQL-TARGETING-DESIGN.md §4).
-- SQLite is the canonical baseline and needs NO rows here; these tables carry
-- only per-dialect DELTAS from canonical (DESIGN §7.10 — defaults stay in
-- code, tables are the patch layer). Rules key on dialect FAMILY (the
-- language.dialect spelling: 'postgres', 'mysql', 'sqlserver', 'duckdb') plus
-- an optional version range — versions are additive rows, never a
-- dialect×version cross product (DESIGN §5).
-- Consumed per-compile via pipeline::dialect_pack (DESIGN §7.11); loaded as a
-- universal dialect-pack cartridge. DQL-queryable registration under a
-- sys::targeting namespace lands with the system-table plumbing item
-- (ALL-SQL-TARGETING-PLAN.md §1 Track B).

-- Per-form lowering rules (Axis A). form_type = entity_type_enum (the form
-- taxonomy); entity_id NULL = form-wide dialect default, set = per-functor
-- override. Precedence: entity+form+dialect → form+dialect → canonical code.
CREATE TABLE dialect_form_rule (
    form_type    INTEGER NOT NULL,
    dialect      TEXT NOT NULL,
    entity_id    INTEGER,
    rule_kind    TEXT NOT NULL,      -- 'template' | 'rust_handler' (v1); 'lua'/'mustache' reserved
    body         TEXT NOT NULL,
    min_version  TEXT,
    max_version  TEXT,
    FOREIGN KEY (form_type) REFERENCES entity_type_enum(id),
    FOREIGN KEY (entity_id) REFERENCES entity(id)
);

-- Per-dialect spelling of leaves (Axis B): operators, literals, keywords, SQL
-- builtin functions. Form-independent, node-local. render_key is NAME-BASED —
-- no arity (DESIGN §7.8): variadic fns take one '{*}' template; arity
-- overloads (max/min) are form distinctions, not render splits.
CREATE TABLE dialect_render (
    dialect      TEXT NOT NULL,
    render_key   TEXT NOT NULL,      -- 'op.not_equal', 'lit.bool_true', 'ident.quoted', 'fn.json_extract'
    rule_kind    TEXT NOT NULL,      -- 'template' | 'rust_handler' (v1); 'lua'/'mustache' reserved
    body         TEXT NOT NULL,      -- '<>' | 'TRUE' | '[{0}]' | '{0} ->> {1}'
    min_version  TEXT,
    max_version  TEXT,
    PRIMARY KEY (dialect, render_key, min_version)
);

-- Capability gates AND clause strategies (the §2.C stratum). value is TEXT,
-- not boolean: pure gates use 'true'/'false'; clause strategies use an enum
-- value the skeleton-assembly code branches on ('limit_style' = 'suffix' |
-- 'top_prefix' | 'fetch_first' | 'rownum_subquery').
CREATE TABLE dialect_capability (
    dialect      TEXT NOT NULL,
    capability   TEXT NOT NULL,
    value        TEXT NOT NULL,
    min_version  TEXT,
    max_version  TEXT,
    PRIMARY KEY (dialect, capability, min_version)
);

-- ----------------------------------------------------------------------------
-- Seed rows: the M1 generator deltas (previously `match dialect` arms in
-- generator_v3/{operators,literals,identifiers}.rs). Canonical (SQLite)
-- spellings stay in code: != , || , 1/0 booleans, "..." quoting.
-- ----------------------------------------------------------------------------
INSERT INTO dialect_render (dialect, render_key, rule_kind, body) VALUES
    ('postgres',  'lit.bool_true',   'template', 'TRUE'),
    ('postgres',  'lit.bool_false',  'template', 'FALSE'),
    -- op.* bodies: a bare token swaps the infix token; a '{'-body is a full
    -- template over both rendered operands, for spellings that change SHAPE
    -- (DIALECT-CONTRACT.md B3/B4): mysql CONCAT is a function, and mysql has
    -- no IS [NOT] DISTINCT FROM — its null-safe equality is the <=> operator
    -- (token) and the negation needs a NOT wrap (template). Templates own
    -- their own parentheses.
    ('mysql',     'op.not_equal',    'template', '<>'),
    ('mysql',     'op.concatenate',  'template', 'CONCAT({0}, {1})'),
    ('mysql',     'op.is_not_distinct_from', 'template', '<=>'),
    ('mysql',     'op.is_distinct_from',     'template', 'NOT ({0} <=> {1})'),
    ('mysql',     'ident.quoted',    'template', '`{0}`'),
    ('sqlserver', 'op.not_equal',    'template', '<>'),
    ('sqlserver', 'op.concatenate',  'template', '+'),
    ('sqlserver', 'lit.bool_true',   'template', 'TRUE'),
    ('sqlserver', 'lit.bool_false',  'template', 'FALSE'),
    ('sqlserver', 'ident.quoted',    'template', '[{0}]');

-- ----------------------------------------------------------------------------
-- Seed rows: the json/agg function family — the registry's first measured
-- tenant (ALL-SQL-TARGETING-PLAN.md §2: PG 264 / DuckDB 183 failing pairs).
-- fn.* body shapes: a bare NAME renames the call (shape and DISTINCT kept);
-- a body containing '{' is a full positional template over rendered args.
-- Deliberately NOT seeded (need `rust_handler` rules, not templates):
--   postgres fn.json_extract  — '$.a.b' path literal must be transformed,
--     not substituted;
--   fn.group_concat           — 1-arg form needs a default separator arg
--     (string_agg is 2-ary); a name-keyed template cannot add an argument.
-- ----------------------------------------------------------------------------
INSERT INTO dialect_render (dialect, render_key, rule_kind, body) VALUES
    ('postgres', 'fn.json_object',       'template', 'json_build_object({*})'),
    ('postgres', 'fn.json_array',        'template', 'json_build_array'),
    ('postgres', 'fn.json_group_object', 'template', 'json_object_agg'),
    ('duckdb',   'fn.json_extract',      'template', 'json_extract_string');

-- ----------------------------------------------------------------------------
-- Seed rows: scalar-form overloads (`fn.__dql_scalar_*`, `fn.__dql_round_2`).
-- The transformer's SQL-AST constructor stamps these when arity reveals the
-- form (2+-arg max/min = sqlite scalar max, not the aggregate; 2-arg round).
-- Canonical spelling on a lookup miss is max/min/round (naming.rs), so
-- sqlite/duckdb rows are unnecessary (both have the scalar overloads).
--   pg scalar max/min are rust_handlers, NOT bare GREATEST/LEAST renames:
--   sqlite's scalar max/min return NULL when ANY argument is NULL, pg's
--   GREATEST/LEAST ignore NULLs (measured divergence) — the handler wraps
--   a variadic NULL guard the fidelity rule demands and a template cannot.
--   pg round accepts only (numeric, int); the template casts both — the
--   value (harmless when already numeric) and the digits (sqlite accepts a
--   double digits arg and TRUNCATES it; pg's int cast ROUNDS — a counted
--   fractional-digits corner, same hazard family as cast: target semantics).
-- ----------------------------------------------------------------------------
INSERT INTO dialect_render (dialect, render_key, rule_kind, body) VALUES
    ('postgres', 'fn.__dql_scalar_max', 'rust_handler', 'pg_scalar_max'),
    ('postgres', 'fn.__dql_scalar_min', 'rust_handler', 'pg_scalar_min'),
    ('postgres', 'fn.__dql_round_2',    'template',
     'round(CAST({0} AS numeric), CAST({1} AS integer))');

-- ----------------------------------------------------------------------------
-- Seed rows: the arbitrary-witness form (`fn.__dql_arbitrary`). The
-- transformer stamps bare `<~` delegate columns (arbitrary row's value);
-- canonical/sqlite spelling is the bare column under relaxed GROUP BY
-- (unwrapped in code — identity isn't a rename), strict targets must say it:
-- any_value() (SQL:2023; postgres 16+, duckdb native) is exactly DQL's
-- promised semantic. Counted witness divergences (all legal under
-- "arbitrary"): sqlite's lone-min/max rule picks the winning row's
-- companions; sqlite's bare column can surface NULL where any_value prefers
-- non-null. Wanting a SPECIFIC row is the ordered delegate's (`<~ #()`) job.
-- ----------------------------------------------------------------------------
INSERT INTO dialect_render (dialect, render_key, rule_kind, body) VALUES
    ('postgres', 'fn.__dql_arbitrary', 'template', 'any_value({0})'),
    ('duckdb',   'fn.__dql_arbitrary', 'template', 'any_value({0})');

-- ----------------------------------------------------------------------------
-- Seed rows: rust_handler rules — renders a positional template cannot
-- express (DESIGN §4.4). Bodies name compiled handlers in
-- pipeline/dialect_pack.rs (rust_render_handler).
--   pg json paths: '$.a.b' literal is TRANSFORMED to '{a,b}';
--     fn.json_extract (user scalar read) -> #>> (text flavor);
--     fn.__dql_json_extract_raw (native-json provenance) -> #> (stays json).
--   pg group_concat: 1-arg form SYNTHESIZES the implicit ',' separator
--     (string_agg is 2-ary) + ::text coercion.
-- ----------------------------------------------------------------------------
INSERT INTO dialect_render (dialect, render_key, rule_kind, body) VALUES
    ('postgres', 'fn.json_extract',            'rust_handler', 'pg_json_path_text'),
    ('postgres', 'fn.__dql_json_extract_raw',  'rust_handler', 'pg_json_path_jsonb'),
    ('postgres', 'fn.group_concat',            'rust_handler', 'pg_group_concat');

-- ----------------------------------------------------------------------------
-- Seed rows: TVF spellings (`tvf.*`). Same contract as `fn.*`: internal
-- `__dql_*` names key under their own render key and spell canonically
-- (json_each) on a lookup miss, so sqlite/duckdb rows are unnecessary.
--   pg __dql_json_each_array: sqlite's json_each is polymorphic
--     (object|array), pg's is object-only — the array-provenance sites
--     (melt packets, narrow/drill/destructure) become a LATERAL derived
--     table over jsonb_array_elements. WITH ORDINALITY - 1 reproduces
--     sqlite's 0-based `key`; the template renders the whole FROM item,
--     code appends the alias. Works in both join shapes: after LEFT/CROSS
--     JOIN, and comma-joined (LATERAL grants the preceding-item reference
--     either way). (ALL-SQL-TARGETING-PLAN.md §2, json_each inventory.)
-- ----------------------------------------------------------------------------
--   pg __dql_json_each_object: the metadata-tree-group sites iterate a
--     JSON_GROUP_OBJECT map — pg's jsonb_each is object-each exactly, and
--     its natural output columns are already (key, value), so the plain
--     call form suffices (function-call FROM items are implicitly LATERAL).
INSERT INTO dialect_render (dialect, render_key, rule_kind, body) VALUES
    ('postgres', 'tvf.__dql_json_each_array', 'template',
     'LATERAL (SELECT e.ordinality - 1 AS key, e.value AS value FROM jsonb_array_elements(CAST({0} AS jsonb)) WITH ORDINALITY AS e)'),
    ('postgres', 'tvf.__dql_json_each_object', 'template',
     'jsonb_each(CAST({0} AS jsonb))');

-- ----------------------------------------------------------------------------
-- Seed rows: cast type-name spellings (`type.*`). Canonical = the uppercased
-- DQL type word (INTEGER/REAL/TEXT/NUMERIC/BOOLEAN); rows carry only deltas.
-- SQLite REAL is an 8-byte float, so the faithful spelling is DOUBLE
-- PRECISION on postgres and DOUBLE on duckdb (their REAL is 4-byte).
-- ----------------------------------------------------------------------------
INSERT INTO dialect_render (dialect, render_key, rule_kind, body) VALUES
    ('postgres', 'type.real', 'template', 'DOUBLE PRECISION'),
    ('duckdb',   'type.real', 'template', 'DOUBLE');
