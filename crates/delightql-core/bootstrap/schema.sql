-- DelightQL Bootstrap Schema
-- This file defines all metadata tables for the DDL-LIGHT cartridge/entity/namespace system
-- See: documentation/design/ddl/SYS-NS-CARTRIDGE-ER-DESIGN.md

-- Foreign-key ENFORCEMENT is a connection setting, not schema: every
-- connection that opens this catalog is configured by
-- `bootstrap::configure_connection` before any statement runs on it.
-- A pragma here would be a second judgment, and one that is silently a
-- no-op inside a transaction.

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
    -- The identifier law's other half: 1 = the authored name was stropped
    -- and keeps its exact identity; 0 = it folds. Agreement is computed
    -- over (name, name_stropped), never by collation alone.
    name_stropped INTEGER NOT NULL DEFAULT 0,
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

-- Clause ordinals are authored order and unique within their family.
CREATE UNIQUE INDEX entity_clause_ordinal_uq ON entity_clause(entity_id, ordinal);

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
    column_name TEXT,    -- canonical name from free-var clauses (NULL for table params)
    stropped INTEGER NOT NULL DEFAULT 0,  -- the declared identifier's strop bit: exact when set
    FOREIGN KEY (entity_id) REFERENCES entity(id)
);

-- Column schema for argumentative functor parameters
CREATE TABLE ho_param_column (
    id INTEGER PRIMARY KEY,
    ho_param_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    column_position INTEGER NOT NULL,
    stropped INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (ho_param_id) REFERENCES ho_param(id)
);

-- Edge catalog (GROUNDING-AND-MENTION.md "Persistence"): each row is a
-- declared ER edge — the context symbol and the two ground terms as
-- NAKED canonical spellings (inside a catalog everything is data; the
-- :`…` wrapper is code syntax and would be noise). Rows are DERIVED
-- from consulted declarations — re-emitted at consult, never migrated.
CREATE TABLE join_edge (
    id INTEGER PRIMARY KEY,
    entity_id INTEGER NOT NULL,
    left_spelling TEXT NOT NULL,
    right_spelling TEXT NOT NULL,
    context_name TEXT NOT NULL,
    clause_ordinal INTEGER NOT NULL,
    FOREIGN KEY (entity_id) REFERENCES entity(id)
);

-- Functional Dependency: THE DECLARED MODE of a fact function.
-- `f(a, b -> c, d ---- …)` declares that the inputs determine the outputs.
-- These rows are the callable signature. The entity type separately records
-- whether the complete definition has a finite relational face; a default-
-- bearing family is callable-only.
-- `stropped` carries the authored identifier's identity: a stropped name
-- compares verbatim, an unstropped one folds, and the pick is by exact
-- agreement either way.
CREATE TABLE functional_dependency (
    id INTEGER PRIMARY KEY,
    entity_id INTEGER NOT NULL,
    role TEXT NOT NULL,
    position INTEGER NOT NULL,
    attribute_name TEXT NOT NULL,
    stropped INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (entity_id) REFERENCES entity(id),
    UNIQUE (entity_id, role, position),
    CHECK (role IN ('input', 'output'))
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
-- id is AUTOINCREMENT: the liminal-program compensation boundary is a
-- namespace-id high-water mark, and its
-- "created since" scan is exact only if ids are NEVER reused. Plain
-- INTEGER PRIMARY KEY would let SQLite hand a deleted max rowid to the
-- next insert, hiding that namespace from the failure teardown.
CREATE TABLE namespace (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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

-- Mount binding: one row per mounted namespace.  This is the authoritative
-- catalog fact for mount identity.
-- connection_id is deliberately derived through cartridge.connection_id:
-- keeping a second authoritative copy here would recreate the identity
-- disagreement this relation is intended to remove.
CREATE TABLE mount (
    namespace_id INTEGER PRIMARY KEY REFERENCES namespace(id),
    cartridge_id INTEGER NOT NULL UNIQUE REFERENCES cartridge(id),
    -- The PHYSICAL attachment handle. NOT unique: one file may be named by
    -- more than one namespace, and naming it twice must not OPEN it twice —
    -- one connection holding two handles on one file cannot write through
    -- either while the other reads, and reports "database is locked" from a
    -- statement with no second party in it. So the second namespace binds
    -- the schema the first one is already using, and teardown refcounts:
    -- a schema is detached when the last binding on it goes, the rule
    -- mount_tree!'s shared connection already follows one level up.
    attach_alias TEXT,
    -- WHO OPENED the schema this binding names. 'owned' = this mount
    -- attached it and may detach it; 'borrowed' = it was already open and
    -- this mount only named it.
    --
    -- Refcounting and ownership answer different questions and neither
    -- substitutes for the other: refcounting says whether anyone is still
    -- using the schema, ownership says whether closing it was ever this
    -- binding's to do. A borrowed schema may be SQLite's own `main`, which
    -- cannot be detached at all, or another owner's attachment that is
    -- still being read.
    attachment TEXT CHECK (attachment IN ('owned', 'borrowed')),
    qualification TEXT NOT NULL CHECK (
        qualification IN ('unqualified', 'aliased', 'engine_schema')
    ),
    engine_schema TEXT,
    class TEXT NOT NULL CHECK (class IN ('attach', 'external')),
    CHECK (class != 'attach' OR attach_alias IS NOT NULL),
    -- An attach-class binding always states who opened its handle; an
    -- external one has no handle to state it for.
    CHECK (class != 'attach' OR attachment IS NOT NULL),
    CHECK (class != 'external' OR attachment IS NULL),
    CHECK (class != 'attach' OR engine_schema IS NULL),
    CHECK (qualification != 'aliased' OR class = 'attach'),
    CHECK (engine_schema IS NULL OR qualification = 'engine_schema'),
    CHECK (
        qualification != 'engine_schema'
        OR (class = 'external' AND engine_schema IS NOT NULL)
    ),
    CHECK (
        qualification != 'unqualified'
        OR engine_schema IS NULL
    )
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

-- ============================================================================
-- THE DEFINITION CATALOG IS CURRENT. A consulted namespace holds exactly
-- the definition families its current source declares: consult! writes
-- them, reconsult! deletes them and writes the replacement inside one
-- savepoint, unconsult! deletes them with the namespace. No row outlives
-- the load that declared it, so there is no historical revision for a
-- later statement to keep following. Rows are written only inside the
-- bootstrap authorizer's catalog window (the lifecycle writers open one;
-- compilation never does).

-- ONE CANONICAL NAME, ONE FAMILY, per namespace — the store's own copy of
-- the clause-agreement law (identifier folding included): activating a
-- second authored family under a name the namespace already answers
-- refuses, whatever its category or arity. An authored family activates
-- only with at least one clause. The authored kinds are the Dql definition
-- families 1,2,3,4,8,9,16,17,20 (asserted against
-- EntityType::is_authored_definition by a unit test); served rows (bins,
-- introspected objects, materialization products, reflected directives)
-- are outside this law.
CREATE TRIGGER definition_family_identity
BEFORE INSERT ON activated_entity
WHEN (SELECT type FROM entity WHERE id = NEW.entity_id) IN (1, 2, 3, 4, 8, 9, 16, 17, 20)
  AND EXISTS (
      SELECT 1 FROM activated_entity ae
      JOIN entity e ON e.id = ae.entity_id
      WHERE ae.namespace_id = NEW.namespace_id
        AND e.type IN (1, 2, 3, 4, 8, 9, 16, 17, 20)
        AND (CASE WHEN e.name_stropped = 1 THEN e.name ELSE lower(e.name) END)
            = (SELECT CASE WHEN name_stropped = 1 THEN name ELSE lower(name) END
               FROM entity WHERE id = NEW.entity_id))
BEGIN
    SELECT RAISE(ABORT, 'definition_family_identity: one canonical name identifies one definition family in a namespace');
END;

CREATE TRIGGER definition_family_requires_clauses
BEFORE INSERT ON activated_entity
WHEN (SELECT type FROM entity WHERE id = NEW.entity_id) IN (1, 2, 3, 4, 8, 9, 16, 17, 20)
  AND NOT EXISTS (SELECT 1 FROM entity_clause c WHERE c.entity_id = NEW.entity_id)
BEGIN
    SELECT RAISE(ABORT, 'an activated definition family requires at least one clause');
END;

-- Namespace Local Enlist: the enlistments a consulted file declared for
-- its own namespace. They stand while the load stands; reconsult replaces
-- them with the replacement file's.
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

-- Liminal Receipt: THE LIMINAL RELATION's storage (EFFECT-ALGEBRA §8).
-- One row per executed liminal directive of the namespace's OWN file, in
-- file-appearance order — rowid IS the insertion order (the engine-courtesy
-- ordering contract; the presented ledger carries no sequence column).
-- `receipt` is the receipt row as a JSON object (success, operation, then
-- the directive's echo columns); `echoes` is the ordered JSON array of this
-- receipt's echo column names, from which the ledger's corresponding-union
-- presentation schema is computed at drill time.
-- Session-scoped catalog state: rows are written inside the consult
-- transaction (abort rolls the ledger away with the namespace), die with the
-- namespace (destroy_namespace) and are replaced whole on reconsult
-- (clear_namespace_contents). Pinned by effects/liminal--43, --45 and the
-- liminal_ledger_* tests in bin_cartridge/prelude/consult.rs.
CREATE TABLE liminal_receipt (
    id INTEGER PRIMARY KEY,
    namespace_id INTEGER NOT NULL,
    operation TEXT NOT NULL,
    echoes TEXT NOT NULL,
    receipt TEXT NOT NULL,
    FOREIGN KEY (namespace_id) REFERENCES namespace(id)
);

-- Grounding: THE DERIVED WORLD'S CLOSURE. One row per derivative a
-- `ground!` made: the grounded namespace standing for one exact source
-- (lib) namespace, bound to one data namespace, under the root the
-- `ground!` named (the root's own row has root = itself). The row set of
-- one root IS the reachable lexical definition closure that grounding
-- derived; lifecycle roads (reconsult rebuild, refresh re-admission,
-- unconsult/unmount borrow refusal, imprint's borrow check) read it and
-- nothing re-derives it by spelling.
CREATE TABLE grounding (
    id INTEGER PRIMARY KEY,
    grounded_namespace_id INTEGER NOT NULL UNIQUE,
    data_namespace_id INTEGER NOT NULL,
    lib_namespace_id INTEGER NOT NULL,
    root_namespace_id INTEGER NOT NULL,
    FOREIGN KEY (grounded_namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (data_namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (lib_namespace_id) REFERENCES namespace(id),
    FOREIGN KEY (root_namespace_id) REFERENCES namespace(id)
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

-- Compiler limits: the resource policies a compilation runs under.
-- Addressed as sys::execution.compiler_limit(*).
--
-- A limit is a RESOURCE policy, never a rule of the language: no row here
-- says what a valid query may be, only how much of this process one may
-- spend. Every row therefore carries the identity its refusal reports, so an
-- operator who meets a refusal can find the setting from the badge it wore.
--
-- `default_value` is what an unconfigured process uses; `hard_ceiling` is
-- what ordinary runtime configuration cannot raise past. `hard_ceiling`
-- bounds CONFIGURATION, not physics: it does not promise that its own value
-- is survivable, only that no environment variable or host setter reaches
-- past it. Every column is NOT NULL, so no reader has to ask whether a limit
-- has a ceiling; each one does.
--
-- NO ROW IS AUTHORED HERE. The schema declares the table; the engine writes
-- every column of every row from the typed policy the guards themselves
-- enforce (crate::compiler_limits), at compilation entry. A row copied into
-- this file would be a second authority — one a later safety adjustment can
-- leave stating a default or a ceiling the guard has stopped using, with
-- both sides still compiling.
--
-- `effective_value` is the column that moves between compilations, and it is
-- the value the COMPILATION READING THIS ROW armed with — not a later read
-- of process policy, which is a different number whenever a host changes a
-- setting after that compilation started.
--
-- The rows are DIFFERENT budgets and must stay so. They measure different
-- objects at different times — the authored parse tree before any walk, and
-- active refiner frames while refinement runs — and raising one does not
-- raise the other. Separate rows carrying separate error identities is that
-- fact as data rather than as prose in two doc comments.
CREATE TABLE compiler_limit (
    name            TEXT PRIMARY KEY,
    default_value   INTEGER NOT NULL,
    effective_value INTEGER NOT NULL,
    hard_ceiling    INTEGER NOT NULL,
    unit            TEXT NOT NULL,
    error           TEXT NOT NULL
);

-- ============================================================================
-- THE TYPED EFFECT PLAN, MATERIALIZED (sys::execution — D4,
-- DOGFOODING-EFFECT-EXECUTION-PLAN §4; Q-D3/Q-D4 as amended)
-- ============================================================================
-- Engine-owned OBSERVATIONAL PROJECTION of the in-memory typed plan
-- (Q-D11: the typed Rust plan stays the single executable source; these
-- rows execute nothing). Lifecycle (§7 as amended): populated when a
-- plan compiles (run or explain), rows PERSIST for post-mortem
-- inspection, and clear at the START of the next compile — the
-- fresh-scratch-per-run precedent. Only the engine writes these rows.

-- Scheduled steps ONLY (guards are definitions, not steps — Q-D3).
CREATE TABLE effect_plan (
    plan_id       INTEGER NOT NULL,
    step_id       INTEGER NOT NULL,
    ordinal       INTEGER NOT NULL,
    occurrence_id TEXT    NOT NULL,   -- the demand-expansion path (Q-D2)
    step_kind     TEXT    NOT NULL,   -- effect | return | control
    action_kind   TEXT    NOT NULL,   -- dml | ddl | sql | host
    operation     TEXT    NOT NULL,
    route         INTEGER,
    sql_display   TEXT    NOT NULL,
    PRIMARY KEY (plan_id, step_id)
);

-- Guard DEFINITIONS: no ordinal, no occurrence; sampled at each
-- dependent (Q-D1), shared by any number of requirements.
CREATE TABLE effect_guard (
    plan_id     INTEGER NOT NULL,
    guard_id    INTEGER NOT NULL,
    sql_display TEXT    NOT NULL,
    PRIMARY KEY (plan_id, guard_id)
);

-- Mutable execution state (D5; Q-D5 as amended): tracked IN MEMORY
-- during the walk, materialized best-effort at the run's boundary
-- (success, abort, exit), persisting for post-mortem inspection until
-- the next compile clears it with the plan. Final statuses: done |
-- skipped (an edge sampled closed; detail says which) | error (the
-- aborting step; detail carries the message) | pending (never reached —
-- the run stopped earlier).
CREATE TABLE effect_run (
    plan_id INTEGER NOT NULL,
    step_id INTEGER NOT NULL,
    status  TEXT    NOT NULL,
    detail  TEXT,
    PRIMARY KEY (plan_id, step_id)
);

-- Requirement edges. `always` is the ABSENCE of a row, never a third
-- polarity value.
CREATE TABLE effect_requirement (
    plan_id  INTEGER NOT NULL,
    step_id  INTEGER NOT NULL,
    guard_id INTEGER NOT NULL,
    polarity TEXT    NOT NULL,        -- present | absent
    reason   TEXT    NOT NULL,        -- diagnostics only
    PRIMARY KEY (plan_id, step_id, guard_id)
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
-- generator/{operators,literals,identifiers}.rs). Canonical (SQLite)
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
    ('mysql',     'ident.escape',    'template', '`'),
    -- The POLARITY OBSERVATION. `IS [NOT] TRUE` is the canonical spelling
    -- and three families have it; SQL Server has no boolean value at all, so
    -- the collapse is written as the CASE that produces one. Both rows keep
    -- the equipartition: a predicate answering UNKNOWN takes the ELSE.
    ('sqlserver', 'op.is_true',      'template', 'CASE WHEN {0} THEN 1 ELSE 0 END = 1'),
    ('sqlserver', 'op.is_not_true',  'template', 'CASE WHEN {0} THEN 1 ELSE 0 END = 0'),
    ('sqlserver', 'op.not_equal',    'template', '<>'),
    ('sqlserver', 'op.concatenate',  'template', '+'),
    ('sqlserver', 'lit.bool_true',   'template', 'TRUE'),
    ('sqlserver', 'lit.bool_false',  'template', 'FALSE'),
    ('sqlserver', 'ident.quoted',    'template', '[{0}]'),
    ('sqlserver', 'ident.escape',    'template', ']');

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
     'LATERAL (SELECT e.ordinality - 1 AS key, e.value AS value FROM jsonb_array_elements(CASE WHEN jsonb_typeof(CAST({0} AS jsonb)) = ''array'' THEN CAST({0} AS jsonb) END) WITH ORDINALITY AS e)'),
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
-- Seed row: effect-plan scratch qualification (`scratch.schema`) — R-T2's
-- layer-1 dialect slot (EFFECTS-ON-TARGETS-PLAN.md §1, ratified 2026-07-11).
-- The session-temp schema qualifier every plan-scratch REFERENCE takes
-- (receipt shells/reads, the __exit peek and wrap-guards, replace/trailing
-- drops). Canonical (SQLite) is `temp.` in code; DuckDB accepts the SQLite
-- spelling verbatim (REPORT-T-P3 §B) so it carries no row; PG spells
-- `pg_temp.` — never `pg_temp_N` (REPORT-T-P1 §B: the alias always names
-- the session's own schema, and full qualification is immune to user
-- search_path exotica). Consumed by the effect transformer's
-- `scratch_schema` (pipeline/effect_transformer); pinned by
-- pg_shells_move_in_bracket_with_on_commit_drop_and_pg_temp_spelling.
-- ----------------------------------------------------------------------------
INSERT INTO dialect_render (dialect, render_key, rule_kind, body) VALUES
    ('postgres', 'scratch.schema', 'template', 'pg_temp');

-- ----------------------------------------------------------------------------
-- sys::identifiers — the engine identifier registry as burned rows.
-- AUTHORED-AS-DATA: these rows are the
-- SOURCE of truth for `dql explain` and every future projection;
-- spelling-normalization stays in code. One upstream per table — never
-- also generate these.
--
-- Invariants (PORCELAIN-AND-PLUMBING.md): summary/explanation are
-- porcelain and may improve freely. (kind, hierarchy) is identity, and
-- its permanence begins at the first public release or an explicit
-- earlier vocabulary freeze (URI-DESIGN.md §3): from that boundary on, a
-- hierarchy is never reassigned or deleted, and a rename is a permanent
-- alias. BEFORE it, a hierarchy that has appeared in no released version
-- may simply be deleted — pre-release vocabulary work owes no aliases,
-- tombstones, or succession rows to an identifier no user could have
-- received. kind ∈ error | danger | config.
-- Addressed as sys::identifiers.identifier(*) (registered in system.rs
-- alongside the other sys tables).
-- ----------------------------------------------------------------------------
CREATE TABLE identifier (
    kind        TEXT NOT NULL,
    hierarchy   TEXT NOT NULL,
    summary     TEXT NOT NULL,
    explanation TEXT NOT NULL,
    -- The declared population of an error hierarchy: family, leaf,
    -- family_leaf (a family whose own path is emitted), external_root (a
    -- provider-owned code space stands under it), retired. Error rows are
    -- projected from the typed hierarchy at bootstrap (bootstrap/mod.rs),
    -- never authored here; the other kinds are authored below as leaves.
    role        TEXT NOT NULL DEFAULT 'leaf',
    PRIMARY KEY (kind, hierarchy)
);

INSERT INTO identifier (kind, hierarchy, summary, explanation) VALUES
    ('danger', 'cardinality/cartesian', 'Unrestricted cartesian product.', 'DECLARED, NOT YET ENFORCED (2026-07-17, R-1): the intended OFF behavior — a join with no usable key refuses (the classic accidental row explosion) — is not built; today a condition-less join compiles and runs as a cartesian product regardless of this gate. When enforcement lands, OFF will refuse and ON will allow. Guardrail-class: may be opened from the CLI (--danger cardinality/cartesian=ON) or inline.'),
    ('danger', 'termination/unbounded', 'Unbounded recursive query.', 'DECLARED, NOT YET ENFORCED (2026-07-17, R-1): the intended OFF behavior — recursive queries must be provably bounded — is not built; today an unbounded recursion compiles without warning (and may not terminate). When enforcement lands, OFF will refuse and ON will allow. Guardrail-class: CLI-overridable.'),
    ('danger', 'semantics/min_multiplicity', 'True INTERSECT ALL via ROW_NUMBER (min-multiplicity).', 'Changes what a set operator MEANS (bag semantics via minimum multiplicity), so it is semantic-class: inline-only ((~~danger://semantics/min_multiplicity ON~~)), never a CLI flag — a flag that silently changes query meaning would make the same text mean different things in different shells.'),
    ('danger', 'scope/duplicate', 'Two live scopes sharing one answering name.', 'TWO LIVE SCOPES NEVER SHARE A NAME: by default, two relations in one lexical environment answering to one canonical name refuse at scope activation (delightql-error://semantic/scope/duplicate). Acknowledging this gate admits the co-activation: qualified references over the shared name resolve against whichever occurrences remain distinguishable, and the ambiguity is the author''s. Guardrail-class: CLI-overridable (--danger scope/duplicate=ON) or inline.'),
    ('config', 'generation/rule/inlining/view', 'Inline consulted view rules instead of emitting CTEs.', 'Strategy selection, not meaning: with this ON the compiler inlines view-rule bodies as subqueries rather than emitting CTEs. Results are identical either way; generated SQL shape differs. Inline: (~~config://generation/rule/inlining/view ON~~); CLI: --config.'),
    ('config', 'generation/rule/inlining/fact', 'Inline consulted fact rules instead of emitting CTEs.', 'As generation/rule/inlining/view, for fact rules.'),
    ('diagnostic', 'autoload', 'Health of the embedded autoload (stdlib) modules.', 'The autoload provider (dql selftest) force-loads every embedded .dql module through the real loader and reports failures. Members: autoload/parse_failed, autoload/consult_failed.'),
    ('diagnostic', 'autoload/parse_failed', 'An autoload module did not parse.', 'The .dql text was rejected by the DDL grammar, so the module loaded nothing and its rules resolve as Table not found. Most common cause: a bare `--` line comment, which collides with the `---` anonymous-table separator — move prose into a (~~docs ~~) hook. The finding''s detail carries the offending line and tree-sitter''s recovery note.'),
    ('diagnostic', 'autoload/consult_failed', 'An autoload module parsed but failed to register.', 'The module parsed, but consulting it (registering its rules/entities) failed — typically a rule references a relation or namespace that does not exist. Check the referenced names in the module against what is available at load time. The finding''s detail carries the consult error.'),
    ('diagnostic', 'catalog', 'Integrity of the entity catalog.', 'The catalog provider (dql selftest) checks that the compiler''s own system tables are properly placed in the catalog. Members: catalog/orphaned_entity.'),
    ('diagnostic', 'catalog/orphaned_entity', 'A system table has no namespace address.', 'A physical system table exists (and is queryable by direct name via the schema fallback) but has no activated_entity row, so it lives in no sys:: namespace and is invisible to the namespace-organized views (sys::util.tables_as_d2, catalog enumeration). Doctrine: everything the compiler or runtime uses should be dogfood-exposed — there are no intentional hidden internals. Fix: activate the table into its namespace (import/activation.rs + import/namespace.rs), as sys::targeting did for the dialect_* tables.');


-- ----------------------------------------------------------------------------
-- sys::format — the formatter's style bundles as burned rows.
-- AUTHORED-AS-DATA: the 'book' row IS the frozen default style; the
-- delightql-cli weld asserts it never drifts from the formatter's
-- FormatConfig::default(), and the column set never drifts from the
-- formatter's knob registry (rules::KNOBS). NULL in a non-book row
-- means "inherit from book". Every column governs WHITESPACE only —
-- no bundle can change a token. Resolution order at `dql format`:
-- code defaults, then the selected bundle row, then .dql-format.
-- Addressed as sys::format.bundle(*) (registered in system.rs
-- alongside the other sys tables).
-- ----------------------------------------------------------------------------
CREATE TABLE bundle (
    bundle                     TEXT NOT NULL PRIMARY KEY,
    projection_length          INTEGER,
    continuation_length        INTEGER,
    pipe_indent                INTEGER,
    continuation_indent        INTEGER,
    map_cover_extra_indent     INTEGER,
    aggregation_arrow_indent   INTEGER,
    cte_indent                 INTEGER,
    cte_columnar_padding       INTEGER,
    curly_member_indent        INTEGER,
    curly_inducer_indent       INTEGER,
    case_arm_indent            INTEGER,
    pipe_break_width           INTEGER,
    member_landing_pad         INTEGER,
    pipe_break                 TEXT,
    comma_clause_break         TEXT,
    comma_join_args            TEXT,
    brace_padding              TEXT,
    member_landing             TEXT,
    closer_placement           TEXT,
    tree_inducer_break         TEXT,
    member_value_break         TEXT,
    annotation_placement       TEXT,
    blank_lines                TEXT,
    cte_style                  TEXT,
    curly_opening_brace_inline INTEGER
);

INSERT INTO bundle (bundle, projection_length, continuation_length, pipe_indent, continuation_indent, map_cover_extra_indent, aggregation_arrow_indent, cte_indent, cte_columnar_padding, curly_member_indent, curly_inducer_indent, case_arm_indent, pipe_break_width, member_landing_pad, pipe_break, comma_clause_break, comma_join_args, brace_padding, member_landing, closer_placement, tree_inducer_break, member_value_break, annotation_placement, blank_lines, cte_style, curly_opening_brace_inline)
VALUES ('book', 72, 40, 2, 2, 4, 2, 3, 7, 5, 3, 3, 80, 2, 'fit', 'cascade', 'oxford', 'none', 'offset', 'own_line', 'always', 'always', 'inline', 'preserve', 'subordinate', 0);
