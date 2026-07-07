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

-- ----------------------------------------------------------------------------
-- sys::help ring 2 — the identifier registry as burned rows
-- (SYS-HELP-DESIGN.md phase 1). AUTHORED-AS-DATA: these rows are the
-- SOURCE of truth for `dql explain` and every future projection (the
-- former uri_registry.rs Rust static is gone; spelling-normalization
-- stays in code). One upstream per table — never also generate these.
--
-- Invariants (PORCELAIN-AND-PLUMBING.md): (kind, hierarchy) is a FROZEN
-- identity — append-only, a hierarchy once minted is never reused or
-- reworded (URI-DESIGN.md §3); summary/explanation are porcelain and
-- may improve freely. kind ∈ error | danger | config.
-- Addressed as sys::help.identifier(*) (registered in system.rs
-- alongside the other sys tables).
-- ----------------------------------------------------------------------------
CREATE TABLE identifier (
    kind        TEXT NOT NULL,
    hierarchy   TEXT NOT NULL,
    summary     TEXT NOT NULL,
    explanation TEXT NOT NULL,
    PRIMARY KEY (kind, hierarchy)
);

INSERT INTO identifier (kind, hierarchy, summary, explanation) VALUES
    ('error', 'parse', 'The source text is structurally invalid.', 'Parse errors mean the query text could not be read as DelightQL at all — the grammar rejected it before any meaning was assigned. Check delimiter balance, operator spelling, and clause order. Hook family: (~~error://parse ~~) matches every parse error.'),
    ('error', 'parse/ddl', 'A definition (DDL) source failed to parse.', 'The text of a definition — a consulted rules file, a view/rule body, or inline DDL — contains syntax the DDL grammar rejects. The message carries the offending line and tree-sitter''s recovery note. Common causes: operator ambiguity needing spaces (`x / 2`, not `x/2`), unbalanced delimiters, or query-mode clause syntax (`… : name`) used in a rules file where definition syntax (`name(*) :- …`) is required.'),
    ('error', 'parse/sigil', 'A sigil expression contains syntax errors.', 'A sigil-introduced expression (the compact operator forms) parsed as structurally invalid. Check the sigil''s expected operand shape and delimiter balance near the reported position.'),
    ('error', 'semantic', 'The structure is valid but the meaning is wrong.', 'Semantic errors mean the query parsed, but a name failed to resolve, an arity was wrong, or a constraint was violated during compilation. The subhierarchy names what went wrong: resolution/ (name binding), constraint/, arity, limitation/ (known gaps).'),
    ('error', 'dml', 'A data-modification query violated DML shape rules.', 'DML errors cover insert!/update!/delete!/keep! shape and marker rules: marker/ (the !! mutation marker — missing, multiple, forbidden, mismatch), shape/ (required or meaningless clauses), source/ (what may feed a mutation).'),
    ('error', 'operational', 'The query is valid but this session refuses to run it.', 'Operational errors are policy, not meaning: the query compiled, but session configuration forbids executing it (e.g. federation-prohibited: a query may touch only one connection).'),
    ('error', 'runtime', 'Compilation succeeded; execution failed.', 'Runtime errors happen after SQL generation: the database rejected the SQL, an assertion failed, a connection dropped, or I/O failed. Subhierarchy: assertion, connection, io, bug (internal), relay/transport (protocol channel).'),
    ('error', 'target', 'The foreign engine rejected or failed the query.', 'Target errors originate in the mounted engine, not in DelightQL: target/<engine>/<class>/<code> embeds the world''s taxonomy as the leaf (Postgres: SQLSTATE, e.g. target/postgres/undefined-object/42883). Hook family: (~~error://target/postgres ~~) matches any Postgres-side failure. Lifecycle members: connect, orientation, unimplemented.'),
    ('error', 'parse/general', 'Generic parse failure.', 'The grammar rejected the text and no more specific parse category applied. The caret in the message marks the first unreadable token.'),
    ('error', 'semantic/resolution/table', 'A named table (or relation) was not found.', 'The name does not exist in the current namespace. Check spelling, the mounted namespace prefix (ns.table), and whether the relation needs a mount!/consult! first.'),
    ('error', 'semantic/resolution/column', 'A named column was not found in scope.', 'The column does not exist in the relation''s schema at this pipeline stage. Note that |> projection changes the visible columns: a filter AFTER |> (a, b) sees only a and b.'),
    ('error', 'semantic/resolution/ambiguous', 'A name matches more than one column in scope.', 'After a join, an unqualified column name exists on more than one side. Qualify it with the relation alias (u.id).'),
    ('error', 'semantic/arity', 'Wrong number of arguments.', 'A function or predicate was called with the wrong number of arguments for its declared arity.'),
    ('error', 'semantic/cast', 'Invalid cast:() usage.', 'cast:(expr, type) takes a bare type name from the v1 vocabulary: integer, real, text, numeric, boolean. Target engines apply their own cast semantics (Postgres rounds real→integer; SQLite truncates) — see the book''s cast page.'),
    ('error', 'semantic/recursion', 'A recursive definition breaks the recursion contract.', 'Family for refusals of recursive forms the language does not permit (RECURSION-CONTRACT.md). DelightQL recursion is a generator (co-recursion): each recursive clause sees only the previous iteration''s rows — never the accumulated result, never itself as a callable. Forms outside that contract are refused here, each with its rewrite path.'),
    ('error', 'semantic/recursion/nonlinear', 'A recursive rule references itself more than once.', 'The frontier cannot join with itself (or with the accumulated result) — forward evaluation carries one previous iteration. Carry the values you need as columns of one frontier row instead: the tupling transformation. fib is the canonical example — two self-calls become one two-column state, (a, b) stepping to (b, a+b). RECURSION-CONTRACT.md N1.'),
    ('error', 'semantic/recursion/aggregate', 'Aggregation inside a recursive rule.', 'An aggregate over the frontier would need the accumulated set, which a recursive rule never sees. Aggregate after the fixpoint — strata are textual, so a later pipe stage aggregates the finished recursion — or carry a running value as a column of the frontier row when the aggregation is per-path. RECURSION-CONTRACT.md N3.'),
    ('error', 'semantic/recursion/self_subquery', 'A recursive rule references itself inside a subquery.', 'Semi/anti-joins, IN, scalar subqueries, or derived tables against the definition itself would need the accumulated set — a recursive rule sees only the previous iteration''s rows, as a direct source. Track visited state in the frontier row (the visited-string idiom), or deduplicate/filter after the fixpoint. RECURSION-CONTRACT.md N4.'),
    ('error', 'semantic/recursion/argumentative_binding', 'Argumentative binding on a recursive self-reference.', 'Renames and constraints on the self-reference (''c(m)'' inside c''s own definition) do not bind inside a recursive definition yet — refused rather than returning wrong results. Use glob binding ''c(*)'' and rename or filter in a pipe stage. The proper fix (the rename-hoist legalization: WITH c(m) AS (…)) is pending. RECURSION-CONTRACT.md B2.'),
    ('error', 'semantic/recursion/limit_bound', '#<N inside a recursive rule has no spelling on this target.', 'DelightQL defines a row limit inside a recursive rule as a TOTAL-ROW CAP on the fixpoint — a demand bound on the unfold. SQLite and MySQL spell it natively (a trailing LIMIT on the recursive member); this target has no single-statement equivalent, and the near-miss spellings silently change meaning (a subquery LIMIT becomes per-iteration — non-terminating). Rewrite the bound as a filter condition on the recursive rule: a depth counter carried in the frontier row, or a value predicate.'),
    ('error', 'semantic/recursion/consulted_clause_order', 'Circular consulted-definition expansion (recursive clause before base, or an indirect view cycle).', 'While inlining a consulted definition, the resolver re-encountered a name it was already expanding — the self-reference did not resolve as the in-progress recursive CTE, so expansion would never terminate (this used to hang the compiler). The common cause: in a consulted rules file, the recursive clause appears BEFORE the base clause — clause order matters; a self-reference is only recursive once a prior clause has established the name. Put the base (non-recursive) clause first. If the cycle runs through another view (a uses v, v uses a), break the cycle. The error message shows the expansion chain. RECURSION-CONTRACT.md B5.'),
    ('error', 'semantic/compound/scalar_column', 'A compound-value tool aimed at a plainly-scalar column.', 'Pathing (''col:{.field}'', ''col:[0]'') reaches into a value; narrowing (''|> .col{.field}'') iterates one. A column declared as a plain scalar (INTEGER, REAL, BOOLEAN, dates) has no insides to reach into and no rows to iterate — aiming these tools at it used to fail target-dependently at runtime (sqlite: ''malformed JSON'', or silent NULLs when the scalar happened to parse as JSON). Refused at compile time instead. TEXT columns stay permissive: documents live in TEXT, and declarations cannot be trusted to deny it. Aim the tool at a compound value: something built with {...}/[...], a tree-group, or a document column.'),
    ('error', 'runtime/assertion', 'An assertion hook did not hold.', 'The main query executed, but an assertion hook attached in the source ((~~assert ...~~)) returned false. Hookable for tests: (~~error://runtime/assertion ~~).'),
    ('error', 'internal/panic', 'dql itself crashed. This is a bug in dql, not in your query.', 'An internal invariant failed (a Rust panic). The CLI catches it and emits this record instead of a raw backtrace; rerun with RUST_BACKTRACE=1 for the developer trace, and please report the message and the query that triggered it. Your query may be perfectly valid — do not rewrite it to dodge this error; the bug is ours.'),
    ('error', 'runtime/connection', 'A database connection failed or was poisoned.', 'The connection to a mounted or primary database was lost or unusable at execution time.'),
    ('error', 'operational/federation-prohibited', 'One query may touch only one connection.', 'The query references namespaces served by different connections. DelightQL deliberately does not federate: split the query, or mount the data into one engine.'),
    ('danger', 'cardinality/nulljoin', 'NULL-matching join equality (NULL = NULL → true).', 'OFF (default): join equality is SQL equality, where NULL never matches. ON: NULLs match each other in join keys, which can multiply rows AND changes what the join means — so this gate is semantic-class: inline-only ((~~danger://cardinality/nulljoin ON~~)), never a CLI flag. Consult sys.danger(*) for this session''s states.'),
    ('danger', 'cardinality/cartesian', 'Unrestricted cartesian product.', 'OFF (default): a join with no usable key is an error (the classic accidental row explosion). ON: the cartesian product is allowed. Guardrail-class: may be opened from the CLI (--danger cardinality/cartesian=ON) or inline.'),
    ('danger', 'termination/unbounded', 'Unbounded recursive query.', 'OFF (default): recursive queries must be bounded. ON: unbounded recursion is allowed (the query may not terminate). Guardrail-class: CLI-overridable.'),
    ('danger', 'semantics/min_multiplicity', 'True INTERSECT ALL via ROW_NUMBER (min-multiplicity).', 'Changes what a set operator MEANS (bag semantics via minimum multiplicity), so it is semantic-class: inline-only ((~~danger://semantics/min_multiplicity ON~~)), never a CLI flag — a flag that silently changes query meaning would make the same text mean different things in different shells.'),
    ('config', 'generation/rule/inlining/view', 'Inline consulted view rules instead of emitting CTEs.', 'Strategy selection, not meaning: with this ON the compiler inlines view-rule bodies as subqueries rather than emitting CTEs. Results are identical either way; generated SQL shape differs. Inline: (~~config://generation/rule/inlining/view ON~~); CLI: --config.'),
    ('config', 'generation/rule/inlining/fact', 'Inline consulted fact rules instead of emitting CTEs.', 'As generation/rule/inlining/view, for fact rules.'),
    ('diagnostic', 'autoload', 'Health of the embedded autoload (stdlib) modules.', 'The autoload provider (dql selftest) force-loads every embedded .dql module through the real loader and reports failures. Members: autoload/parse_failed, autoload/consult_failed.'),
    ('diagnostic', 'autoload/parse_failed', 'An autoload module did not parse.', 'The .dql text was rejected by the DDL grammar, so the module loaded nothing and its rules resolve as Table not found. Most common cause: a bare `--` line comment, which collides with the `---` anonymous-table separator — move prose into a (~~docs ~~) hook. The finding''s detail carries the offending line and tree-sitter''s recovery note.'),
    ('diagnostic', 'autoload/consult_failed', 'An autoload module parsed but failed to register.', 'The module parsed, but consulting it (registering its rules/entities) failed — typically a rule references a relation or namespace that does not exist. Check the referenced names in the module against what is available at load time. The finding''s detail carries the consult error.'),
    ('diagnostic', 'catalog', 'Integrity of the entity catalog.', 'The catalog provider (dql selftest) checks that the compiler''s own system tables are properly placed in the catalog. Members: catalog/orphaned_entity.'),
    ('diagnostic', 'catalog/orphaned_entity', 'A system table has no namespace address.', 'A physical system table exists (and is queryable by direct name via the schema fallback) but has no activated_entity row, so it lives in no sys:: namespace and is invisible to the namespace-organized views (sys::util.tables_as_d2, catalog enumeration). Doctrine: everything the compiler or runtime uses should be dogfood-exposed — there are no intentional hidden internals. Fix: activate the table into its namespace (import/activation.rs + import/namespace.rs), as sys::targeting did for the dialect_* tables.');

-- ----------------------------------------------------------------------------
-- sys::help ring 1 — the CLI's own shape (SYS-HELP-DESIGN.md phase 2).
-- GENERATED AT SESSION INIT by the host binary from its live clap tree
-- (api::HelpSurface → DelightQLSystem::seed_help_surface): runtime
-- generation means these rows structurally cannot drift from the binary
-- that serves them. One upstream per table — never also author rows
-- here. Headless hosts (wasm, cabi) have no CLI surface; their ring-1
-- tables are legitimately empty.
--
-- class/grade columns carry the porcelain/plumbing declaration
-- (PORCELAIN-AND-PLUMBING.md): class ∈ porcelain | plumbing |
-- 'porcelain+semantic-warranty'; grade ∈ frozen | versioned. NULL =
-- not an output surface (most flags).
-- ----------------------------------------------------------------------------
CREATE TABLE command (
    name    TEXT NOT NULL,
    parent  TEXT,
    alias   TEXT,
    summary TEXT NOT NULL,
    PRIMARY KEY (name, parent)
);
CREATE TABLE option (
    command       TEXT NOT NULL,
    long          TEXT NOT NULL,
    short         TEXT,
    value_name    TEXT,
    default_value TEXT,
    global        INTEGER NOT NULL,
    repeatable    INTEGER NOT NULL,
    summary       TEXT NOT NULL,
    PRIMARY KEY (command, long)
);
CREATE TABLE option_value (
    command TEXT NOT NULL,
    option  TEXT NOT NULL,
    value   TEXT NOT NULL,
    summary TEXT,
    class   TEXT,
    grade   TEXT,
    PRIMARY KEY (command, option, value)
);
CREATE TABLE dot_command (
    name    TEXT PRIMARY KEY,
    summary TEXT NOT NULL
);
CREATE TABLE env (
    name            TEXT PRIMARY KEY,
    effect          TEXT NOT NULL,
    equivalent_flag TEXT
);
CREATE TABLE exit_code (
    code    INTEGER NOT NULL,
    context TEXT NOT NULL,
    meaning TEXT NOT NULL,
    class   TEXT,
    grade   TEXT,
    PRIMARY KEY (code, context)
);

-- sys::help ring 2: man pages, seeded by the host via HelpSurface
-- (phase 3). troff is the source (authored in the host's man/ tree,
-- embedded at compile time); plain is scrubbed from it AT SEED TIME
-- by the host's closed-dialect scrubber — in sync by construction,
-- the last rung of the dql-man rendering chain. Sections per the
-- ruling: 1 = commands, 7 = language/concepts.
CREATE TABLE man_page (
    name    TEXT NOT NULL,
    section INTEGER NOT NULL,
    troff   TEXT NOT NULL,
    plain   TEXT NOT NULL,
    PRIMARY KEY (name, section)
);
