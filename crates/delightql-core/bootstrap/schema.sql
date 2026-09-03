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
    PRIMARY KEY (kind, hierarchy)
);

INSERT INTO identifier (kind, hierarchy, summary, explanation) VALUES
    ('error', 'parse', 'The source text is structurally invalid.', 'Parse errors mean the query text could not be read as DelightQL at all — the grammar rejected it before any meaning was assigned. Check delimiter balance, operator spelling, and clause order. Hook family: (~~error://parse ~~) matches every parse error.'),
    ('error', 'parse/ddl', 'A definition (DDL) source failed to parse.', 'The text of a definition — a consulted rules file, a view/rule body, or inline DDL — contains syntax the DDL grammar rejects. The message carries the offending line and tree-sitter''s recovery note. Common causes: bare infix arithmetic or `%` needing parentheses (`(price * 2) > 0`, `(n % 2) = 0` — sigil collision), unbalanced delimiters, or query-mode clause syntax (`… : name`) used in a rules file where definition syntax (`name(*) :- …`) is required.'),
    ('error', 'parse/sigil', 'A sigil expression contains syntax errors.', 'A sigil-introduced expression (the compact operator forms) parsed as structurally invalid. Check the sigil''s expected operand shape and delimiter balance near the reported position.'),
    ('error', 'parse/pony', 'Mixed operators without grouping (no PEMDAS).', 'DelightQL has NO operator precedence: `a * b + c` has no reading, because the language refuses to rank `*` over `+` (the PONY rule). Every composition is grouped explicitly — `((a * b) + c)` or `(a * (b + c))` — so the meaning is always on the page. The parser cannot accept the ungrouped form even to complain about it; this diagnosis is recovered from the failed parse''s token stream.'),
    ('error', 'parse/is_null', 'SQL `is null` used; DelightQL spells it `= null`.', 'There is no `is null` / `is not null` operator. `=` is the null-safe equality (compiles to IS NOT DISTINCT FROM), so `col = null` is the null check and `col != null` its negation. (`==` is the traditional SQL equality, where NULL never matches.)'),
    ('error', 'parse/anon_space', 'Space between `_` and `(` in an anonymous table.', 'The anonymous table constructor is ONE token: `_(id @ 1)`. With a space (`_ (id @ 1)`) the parser sees a discard followed by a parenthesized expression and rejects the statement. Remove the space.'),
    ('error', 'parse/anon/empty', 'There is no empty anonymous table.', '`_()` names no relation: it has no columns and no rows, so there is nothing for it to be. It is not the union identity either — that is the empty relation OF THE MATCHING SCHEMA, whose typed spelling (`_(cols @)`) is reserved and not yet available. Write the relation you mean: `_(id @ 1)` for a row, or a header form `_(a, b ---- 1, 2)`. This diagnosis is recovered from the failed parse''s token stream.'),
    ('error', 'parse/comment', 'SQL `--` comment used; DelightQL comments are `//`.', 'There is no `--` line comment (and no `/* */` block comment): `--` lexes as two `-` operators and breaks the parse. The line comment is `//`, in both query mode and rules files. Inside a string literal `--` is ordinary text. If subtraction of a negative was meant, group it explicitly: `a - (-b)`. This diagnosis is recovered from the failed parse''s token stream.'),
    ('error', 'parse/sort_minus', 'Minus-prefix descending sort; the spelling is `col desc`.', 'There is no `#(-col)` descending shorthand. Descending is spelled per key with `desc`: `#(col desc)`, `#(a desc, b)` — in pipe sorts and window specs alike. A unary minus meant as arithmetic needs explicit grouping: `#((0 - col))`. This diagnosis is recovered from the failed parse''s token stream.'),
    ('error', 'parse/retired_operator', 'Retired `==` / `!==` glyph; DelightQL equality is `=`, target SQL equality is `+sql_eq`.', '`==` and `!==` are no longer DelightQL syntax. DelightQL equality is `=` and inequality `!=`, both null-safe. The target engine''s own three-valued comparison — unknown on null, target coercions and collations — is the explicit prelude sigma predicate: `+sql_eq(l, r)` lowers to SQL `=` and `+sql_ne(l, r)` to SQL `<>`. Choose by intent: a filter or join relationship that is not about the engine''s null answer migrates to `=`; a fixture that deliberately asks the engine becomes `+sql_eq(l, r)`. This diagnosis is recovered from the failed parse''s token stream.'),
    ('error', 'semantic', 'The structure is valid but the meaning is wrong.', 'Semantic errors mean the query parsed, but a name failed to resolve, an arity was wrong, or a constraint was violated during compilation. The subhierarchy names what went wrong: resolution/ (name binding), constraint/, arity, limitation/ (known gaps).'),
    ('error', 'semantic/constraint', 'A compilation-time rule of the language was violated.', 'The general validation family: the query parsed and its names resolved, but a rule of the language refused the composition — a shape a road cannot carry, an option that contradicts another, a structure the release does not support. Where a violation has its own identifier the message carries the more specific badge; semantic/constraint is the family every remaining validation refusal reports under. Hook family: (~~error://semantic/constraint ~~).'),
    ('error', 'semantic/resolution', 'A name failed to bind.', 'Resolution errors are name-binding failures: a table, column, alias, or qualifier that does not name anything in scope, or names two things at once (ambiguous). Members include table, column, ambiguous, ho/ (higher-order landing and access), setop/ (set-operation correlation ownership), er/ (edge selection).'),
    ('error', 'semantic/resolution/ho', 'A higher-order call''s shape is ill-formed.', 'The higher-order family: how arguments and piped relations land at a functor''s parameters. Every parameter is inbound and must be supplied before the body opens. Members include incomplete_application, pipe_landing, and the closed relation/rule-value actual refusals.'),
    ('error', 'semantic/resolution/ho/incomplete_application', 'A higher-order application did not exactly complete its parameter row.', 'The parameter row is entirely inbound. Supply every scalar, relation, and rule-value parameter exactly once before the body opens, and supply no surplus members. A bare scalar name is an actual only when it resolves to one exact caller value; clause-head literals, binders, and same-spelled body columns do not supply it and are never published as an omitted argument.'),
    ('error', 'semantic/resolution/ho_access', 'A higher-order entity was accessed with the wrong shape.', 'Access-shape errors for HO entities: the call site''s parentheses, pattern, or argument count do not match the entity''s declared parameters. The message names the declared shape.'),
    ('error', 'semantic/resolution/setop', 'A set-operation correlation reference is ill-owned.', 'Correlation references after a union-flavored operator address the operands'' own headings — pads are output shape, never addressable. Member: correlation_owner (no operand answers to the qualifier, or a bare name is carried by more than one operand).'),
    ('error', 'semantic/grounding/er', 'An entity-relationship edge operation failed.', 'The ER family: edges are pair-sets selected by their ground terms'' exact canonical spellings in a declared context. Members: unknown_context, edge_miss, pair_schema (the body must publish both endpoints'' headings), self_pair (the two sides must be distinguishable), chain_normal_form and chain_shared_repeat (the written direct chain''s merge restrictions).'),
    ('error', 'semantic/grounding/head', 'A rule head violated a head law.', 'Head errors: a head is an ordered projection of its body''s heading (HEADS.md) and a grounding surface (GROUNDING-AND-MENTION.md). Members refuse heads that compute, name absent columns, or misuse ground positions.'),
    ('error', 'semantic/cte', 'A query-scoped rule (CTE) was mis-declared or misused.', 'The CTE family covers query-scoped rules declared with the : neck. Members: head/ (the head-law refusals — names only, body must carry them).'),
    ('error', 'semantic/cte/head', 'A CTE head must list names its body carries.', 'A head is an ordered projection of its body''s heading — it never computes and never renames. Members: names_only (literals, calls, aliases, and placeholders refuse in head positions).'),
    ('error', 'semantic/mention', 'A mention (uninterpreted name) was misused.', 'Mentions are names passed by spelling, never evaluated: :`delimited`, ::light, and functor terms in edge declarations. Members: term/ (term spelling and canonicalization).'),
    ('error', 'semantic/mention/term', 'A mention term''s spelling is ill-formed.', 'Term errors: the delimited or light mention spelling could not canonicalize, or the term shape does not match what the operation requires. Identity is decided by canonical bytes; the message shows the canonical spelling it derived.'),
    ('error', 'semantic/interior', 'An interior (nested) relation was misused.', 'The interior family covers relation-valued columns: drills, narrows, and interior-scoped operations. Members: topn/ (ordered-slice restrictions inside interiors).'),
    ('error', 'semantic/interior/topn', 'An ordered slice inside an interior hit a restriction.', 'Top-N-in-interior restrictions: what #<N and ordered slices may do inside a correlated interior in this release. Member: noneq_correlation (non-equality correlation under a top-N interior refuses — ruled deferred, the lateral road is future work).'),
    ('error', 'semantic/limit', 'A row-bound value is invalid.', 'Limit and offset operators require an integer bound. A bare identifier is admitted only while expanding a higher-order body and only when it names an integer scalar binding.'),
    ('error', 'semantic/limit/value', 'A limit or offset bound is not an integer value.', 'Write an integer literal, or bind the identifier to an integer scalar parameter in the active higher-order call. Missing bindings, fractional numbers, and values outside the integer range refuse instead of silently becoming zero.'),
    ('error', 'semantic/narrowing', 'A JSON narrowing operation was ill-typed or ill-aimed.', 'Narrowing reads interior values out of JSON-carrying columns (JSON-SUBSTRATE.md): owned at release points, contained elsewhere; non-array and malformed values narrow to ZERO ROWS by the null-interior road. Members refuse narrowing aimed at plainly scalar columns or ill-formed object literals.'),
    ('error', 'semantic/transform', 'The compiler''s own lowering failed an internal law.', 'The transform family is the self-check: post-lowering verification of the SQL the compiler is about to ship (qualifier visibility, column existence). These are internal invariant violations — a dql bug, not a user error; the message asks for a report.'),
    ('error', 'semantic/compound', 'A compound (multi-statement) source violated sequencing rules.', 'Compound errors cover multi-statement sources: what may follow what, and which statement kinds may share a submission.'),
    ('error', 'namespace/name', 'A namespace name hit the reserved-name guard.', 'Members: reserved (bare system names sys/std/home, sys*/std* prefixes, `_`-prefixed machinery segments), system_subtree (creation under sys::/std::). `main` is exempt; under home the prefix rule relaxes while the `_` reservation stays strict.'),
    ('error', 'imprint/manifest', 'An imprint manifest is missing or malformed.', 'Manifest members cover the imprint!''s companion blocks: the schema/constraint/default sections a persistable namespace requires, and their agreement with the rules being imprinted.'),
    ('error', 'imprint/blueprint', 'An archived blueprint namespace refused animation.', 'imprint! is linear: the consumed source namespace archives as an inert {target}::_N_blueprint. Blueprint members refuse operations that would animate that archive — consult it, enlist it, or imprint it again.'),
    ('error', 'internal', 'A dql invariant broke.', 'Internal errors are the compiler catching itself: a panic converted to a report, an invariant that should be unreachable. Never a user error — please report it with the query that produced it. Member: panic.'),
    ('error', 'dml', 'A data-modification query violated DML shape rules.', 'DML errors cover insert!/update!/delete! shape and marker rules: marker/ (the !! mutation marker — missing, multiple, forbidden, mismatch), shape/ (required or meaningless clauses), source/ (what may feed a mutation).'),
    ('error', 'operational', 'The query is valid but this session refuses to run it.', 'Operational errors are policy, not meaning: the query compiled, but session configuration forbids executing it (e.g. federation-prohibited: a query may touch only one connection).'),
    ('error', 'runtime', 'Compilation succeeded; execution failed.', 'Runtime errors happen after SQL generation: the database rejected the SQL, an assertion failed, a connection dropped, or I/O failed. Subhierarchy: assertion, connection, io, bug (internal), relay/transport (protocol channel).'),
    ('error', 'target', 'The foreign engine rejected or failed the query.', 'Target errors originate in the mounted engine, not in DelightQL: target/<engine>/<class>/<code> embeds the world''s taxonomy as the leaf (Postgres: SQLSTATE, e.g. target/postgres/undefined-object/42883). Hook family: (~~error://target/postgres ~~) matches any Postgres-side failure. Lifecycle members: connect, orientation, unimplemented.'),
    ('error', 'parse/general', 'Generic parse failure.', 'The grammar rejected the text and no more specific parse category applied. The caret in the message marks the first unreadable token.'),
    ('error', 'semantic/resolution/table', 'A named table (or relation) was not found.', 'The name does not exist in the current namespace. Check spelling, the mounted namespace prefix (ns.table), and whether the relation needs a mount!/consult! first.'),
    ('error', 'semantic/resolution/fact_function/relational_face', 'A default-bearing fact function was used as a relation.', 'A `_ -> outputs` arm makes a fact function total over an unbounded input domain. The family is callable only: call it with `name:(inputs)`, or map that call over a separately supplied finite relation. Without a default, the explicit arms remain a finite relational face; an explicit `null -> outputs` arm is one ordinary finite row.'),
    ('error', 'semantic/resolution/column', 'A named column was not found in scope.', 'The column does not exist in the relation''s schema at this pipeline stage. Note that |> projection changes the visible columns: a filter AFTER |> (a, b) sees only a and b.'),
    ('error', 'semantic/resolution/schema', 'A relation has no structural schema for name binding.', 'Filters and other column-reading operations bind authored names to column identities. An opaque passthrough relation or unknown table-valued function may be carried without a heading, but it cannot be filtered until its columns are introspectable. Check the relation spelling or make its schema available.'),
    ('error', 'semantic/resolution/ambiguous', 'A name matches more than one column in scope.', 'After a join, an unqualified column name exists on more than one side. Qualify it with the relation alias (u.id).'),
    ('error', 'semantic/ground/name_intersection', 'ground!''s library and data namespace share an entity name.', 'The ground namespace and groundable namespace cannot share entity names: if both define the name, every use of it is ambiguous, so grounding refuses whole and creates nothing (No intersection). Rename the library''s entity or ground against a data namespace that does not define it.'),
    ('error', 'semantic/grounding/data_hole_unbound', 'A free data name of a consulted world that no ground! has bound.', 'A consulted body reads its own definitions and the data world an explicit grounding published — never the caller''s tables, CTEs, or session database ambiently. ground! binds the world''s data holes to a data namespace, or enlist! inside the consulted file links the namespaces it reads.'),
    ('error', 'semantic/ground/unresolved_reference', 'ground!''s strict validation found a dangling qualified reference.', 'ground! validates ALL of the library''s references — a qualified reference must resolve where it points, and an unqualified free reference must exist in the data namespace. If any reference dangles, the entire operation fails and nothing is created.'),
    ('error', 'semantic/recursion/parameter-widening', 'A recursive definition''s self-reference changed a parameter actual.', 'The semantic actuals of a parameterized recursive definition select ONE fixpoint instance and stay invariant for that instance: a self-reference with the same actuals re-enters the active fixpoint, and one with different actuals never opens another specialization. State that changes between recursive iterations belongs in the recursive relation''s ordinary columns — a definition that needs n to become n + 1 carries n as a column instead of calling itself under a new actual (SEMANTICS/recursion-contract-law.md, MONOMORPHIC PARAMETERS).'),
    ('error', 'semantic/resolution/choe/recursion', 'A common higher-order expression''s body reaches itself.', 'A common higher-order expression (CHOE) — `p(T(*))(*) : body` — is the query-scoped parameterized rule. Its body is the query''s own text and sees the bindings declared before it, itself included, so a self-reference selects the definition being expanded. A CHOE has no fixpoint to re-enter: self-reference refuses. Write the recursion as a consulted rule, or bind the recursive relation with an ordinary `%`-badged CTE.'),
    ('error', 'semantic/resolution/choe/head_agreement', 'The clauses of one common higher-order expression disagree.', 'Repeated heads of one common higher-order expression are clauses of ONE query-local definition and accumulate by UNION ALL, under CLAUSE AGREEMENT: every clause declares the same number of parameters and publishes one agreed heading — the same names, in the same positions. A clause whose heading differs is a different relation; give it its own name, or conform its heading with head `as` or a projection in the body.'),
    ('error', 'semantic/resolution/callable_unknown', 'A qualified callable name that no DQL entity answers.', 'DelightQL''s catalog is closed and the target engine''s callable surface is open: an UNQUALIFIED call that selects no DQL entity receives the default target transpilation, caveat emptor. A QUALIFIED name states where the callable lives in DelightQL''s own world, so a miss refuses instead of guessing. To call the target engine''s function explicitly, write sys::target.name:(args).'),
    ('error', 'semantic/constraint/positional_alias', 'An `as` alias stood in a positional slot.', 'THE WRITTEN NAME IS THE NAMING. A slot binds by POSITION: a bare name binds a fresh column, a qualified one reuses an existing value, and a term constrains the column and is consumed. None of them publishes a name for `as` to change. Rename where names are published — in a projection: `f(…) |> (col as name)`.'),
    ('error', 'semantic/identifier/deixis', 'Exact `_` written as an authored name.', 'Exact `_` is reserved deixis: it points at the one unnamed pipe stage in reference position and disregards a slot in binding position. It is never an authored identifier or alias, and stropping is spelling — `` `_` `` does not release the reservation. Longer underscore spellings such as `__` and `_fn` are ordinary names.'),
    ('error', 'semantic/identifier/keyword', 'A reserved word written as a bare name.', 'A reserved word — DelightQL keyword vocabulary or a reserved word of a supported SQL target — is an identifier only when stropped. The judgment is case-insensitive and target-independent: `select` refuses as a bare alias on every backend, and `` `select` `` is an ordinary exact name on every backend. Strop the word to use it as a name.'),
    ('error', 'semantic/scope/duplicate', 'Two live scopes share one answering name.', 'TWO LIVE SCOPES NEVER SHARE A NAME: when two relations in one lexical environment answer to the same canonical name — two members aliased `q`, or one table accessed twice bare — a qualified reference could name either, so the activation refuses before any consumer can choose. Give one of them its own name with `as`, or acknowledge delightql-danger://scope/duplicate to admit the ambiguity.'),
    ('error', 'semantic/arity', 'Wrong number of arguments.', 'A function, predicate, or POSITIONAL TABLE PATTERN received the wrong number of arguments. The commonest case: a table access like users(name, age) is not a projection — it is a positional pattern that must supply one slot per column (full arity). Fill unwanted positions with _ (users(_, name, _)) or keep everything with *. For functions and rules, the runtime error names the exact expected/actual counts.'),
    ('error', 'semantic/cast', 'Invalid cast:() usage.', 'cast:(expr, type) takes a bare type name from the v1 vocabulary: integer, real, text, numeric, boolean. Target engines apply their own cast semantics (Postgres rounds real→integer; SQLite truncates) — see the book''s cast page.'),
    ('error', 'semantic/recursion', 'A recursive definition breaks the recursion contract.', 'Family for refusals of recursive forms the language does not permit (RECURSION-CONTRACT.md). DelightQL recursion is a generator (co-recursion): each recursive clause sees only the previous iteration''s rows — never the accumulated result, never itself as a callable. Forms outside that contract are refused here, each with its rewrite path.'),
    ('error', 'semantic/recursion/nonlinear', 'A recursive rule references itself more than once.', 'The frontier cannot join with itself (or with the accumulated result) — forward evaluation carries one previous iteration. Carry the values you need as columns of one frontier row instead: the tupling transformation. fib is the canonical example — two self-calls become one two-column state, (a, b) stepping to (b, a+b). RECURSION-CONTRACT.md N1.'),
    ('error', 'semantic/recursion/aggregate', 'Aggregation inside a recursive rule.', 'An aggregate over the frontier would need the accumulated set, which a recursive rule never sees. Aggregate after the fixpoint — strata are textual, so a later pipe stage aggregates the finished recursion — or carry a running value as a column of the frontier row when the aggregation is per-path. RECURSION-CONTRACT.md N3.'),
    ('error', 'semantic/consult/liminal/declaration', 'A liminal statement declared something the load cannot spend.', 'A sidecar belongs to the form that wrote it, and at LOAD there is one road that can spend one: a relational goal (`?- body`) compiles and executes, so its danger/config acknowledgments and subordinate blocks travel into its own compilation. A session directive compiles no query, so a declaration on one has no evaluator; and an expected-error hook has no meaning anywhere in a load, which ABORTS on failure rather than recording one. Move the declaration to the goal it is about, to file scope, or to the statement that demands the load.'),
    ('error', 'semantic/consult/goal/unspellable', 'A consulted goal has no canonical spelling.', 'THE LIMINAL RELATION names each goal by its body''s canonical spelling, so a ledger scan knows which goal was which across layout and reconsult. The canonicalizer is the format engine under its frozen default style; a body it passes through has no canonical identity, and keeping the authored bytes instead would be a second spelling authority that agrees with the first only by accident.'),
    ('error', 'semantic/consult/witness/read_only', 'A consulted goal compiled to a statement that writes.', 'THE LIMINAL SPACE: loading a file may READ user data only, through a top-level goal (`?- body`) that runs in the consultation and records a YES/NO witness; it may never write. The grammar already bars a directive from a relational goal, so reaching this refusal means the goal''s body lowered to a mutation — state the write as an effect rule and demand it after the load.'),
    ('error', 'semantic/recursion/mixed_badge', 'Clauses of one target disagree about the fixpoint badge.', 'THE BADGE CHOOSES THE UNION: a fixpoint flavor is one claim about the TARGET, and the target is the whole definition — so every clause of it wears the same badge. An unbadged clause beside a `%`-badged one is two claims about one thing. Badge every clause of the target, or none of them. RECURSION-CONTRACT.md THE BADGE CHOOSES THE UNION.'),
    ('error', 'semantic/recursion/false_fixpoint', 'A `%` badge on a definition that does not reference itself.', '`%` names the DEDUPLICATING fixpoint, and a fixpoint flavor on a non-fixpoint is a false statement. The definition has no self-reference, so there is no unfold for the badge to choose the union of. Drop the badge; to deduplicate an ordinary definition, spell the distinct view in the body (`|> %(*)`). RECURSION-CONTRACT.md THE BADGE CHOOSES THE UNION.'),
    ('error', 'semantic/recursion/self_subquery', 'A recursive rule references itself inside a subquery.', 'Semi/anti-joins, IN, scalar subqueries, or derived tables against the definition itself would need the accumulated set — a recursive rule sees only the previous iteration''s rows, as a direct source. Track visited state in the frontier row (the visited-string idiom), or deduplicate/filter after the fixpoint. RECURSION-CONTRACT.md N4.'),
    ('error', 'semantic/grounding', 'A head-grounding or mention rule was violated.', 'Family for the grounding/mention doctrine (GROUNDING-AND-MENTION.md): rule heads ground on literals and mentions by canonical spelling; contexts are symbols; edges are declared, finite, and selected by their terms'' exact canonical spellings. Subhierarchy: head/ (clause selection), er/ (the entity-relationship operators & and &&).'),
    ('error', 'semantic/grounding/head/provable_miss', 'A literal ground argument matches no clause head.', 'A provable miss is an error, not an empty relation. The argument is written at the call site (a literal or mention), the parameter position is ground in every clause, and no clause head spells that value — emptiness by absent DECLARATION, which the catalog proves at compile time. The message enumerates the declared spellings. A data-borne value (a column, not a literal) keeps relational semantics and misses to empty; a free clause at the position makes every call satisfiable.'),
    ('error', 'semantic/grounding/er/unknown_context', 'No edge is declared in this context.', 'A context exists exactly where an edge declares it — the edge set per context is finite and declared, so an unknown context is a hard error at first use, never an empty result. The message lists the contexts that DO have declared edges in the enlisted scope. Declare edges with A(*) &(::ctx) B(*) :- body, and enlist!() the namespace that declares them.'),
    ('error', 'semantic/grounding/er/edge_miss', 'No edge declared for this pair of terms.', 'An edge is selected by its ground terms'' exact CANONICAL spellings — people(, age >= 18) is a different term from people(, 18 <= age), deliberately, because identity decidable by bytes is the point of matching by encoding. Whitespace and identifier case normalize; semantics never do. Restriction is downstream: select a declared edge (in practice declared over glob terms), then filter its relation. The message lists the declared edges.'),
    ('error', 'semantic/grounding/er/chain_normal_form', 'A transitive chain needs join/filter edge bodies.', 'A transitive chain (&&) merges its edge bodies into ONE join before resolution, so each body must be join/filter normal form: relations and conditions only. A body carrying a pipe stage (|>), a set operation, or a nested edge call cannot be merged without discarding its semantics, so the chain refuses instead. The direct call (&) places no such restriction — it resolves the body whole. Restructure the edge body, or call the edges directly with & and compose the results.'),
    ('error', 'semantic/grounding/er/pair_schema', 'The edge body does not publish an endpoint''s columns.', 'An edge is a PAIR-SET: its body may derive the pairs freely — filter, helper joins, computed keys, distinct, aggregate-and-filter — but its final heading must carry both endpoints'' columns, because schema(A) + schema(B) IS the edge''s published schema. The boundary exports exactly those columns and hides everything else (helpers and computed body columns never cross). A body that renames or projects an endpoint away has not derived a different edge; it has derived a non-edge — spell an arbitrary derivation as a rule instead, and rename or narrow at the call site, after selection.'),
    ('error', 'semantic/grounding/er/self_pair', 'A self-pair edge''s sides cannot yet be told apart.', 'The edge publishes the same table at both endpoints, so the two sides share every column name: the endpoint exports would bind one operand twice and the pairs would come back silently self-joined. Until the boundary can mask the sides apart, spell one side as a renamed rule view — boss(*) :- employees(*) — and declare the edge over the distinct terms; the call site then reads employees(*) &(::mgr) boss(*), each side addressable by its own name.'),
    ('error', 'semantic/resolution/setop/minus_heading', 'A minus''s two operands do not publish the same exact heading.', 'Minus is an exact name-aligned anti-match: every dimension on the left answers exactly one on the right and the reverse. Two operands of different widths, a pairing that is not one-to-one, or an operand whose heading cannot be enumerated leave nothing to anti-match on. Declare the dimensions at the mention so both operands publish the same exact heading.'),
    ('error', 'semantic/resolution/setop/correlation_owner', 'A set-operation correlation reference has no clear operand owner.', 'A correlation after a union-flavored operator addresses the OPERANDS'' OWN HEADINGS — the NULL pads of the corresponding output shape are output artifacts, never addressable. Every reference must belong to exactly one operand: a qualified reference''s qualifier must name an operand (table name, alias, or answering name), and a bare reference''s column must be carried by exactly one operand''s heading. A bare name both operands carry is ambiguous — qualify it (x.col = y.col) to say which side it addresses.'),
    ('error', 'semantic/grounding/er/chain_shared_repeat', 'A chain repeats a relation beyond the shared endpoint.', 'Adjacent edge bodies in a transitive chain share exactly their common endpoint — that one occurrence merges, once. Any other repeated relation (a self-join inside a body, a helper table used by two bodies, a cyclic chain revisiting an endpoint) cannot be aliased apart during composition, and dropping an occurrence would silently rewrite the join, so the chain refuses. Restructure the bodies so only the shared endpoint repeats, or call the edges directly with &.'),
    ('error', 'semantic/grounding/er/bare_operator', 'An ER operator without its context symbol.', 'The & and && operators take their context as a symbol on the operator: &(::your_context), &&(::your_context) — spelled with no space before the parenthesis. Contexts are symbols; the edge set per context is finite and declared.'),
    ('error', 'semantic/grounding/er/mixed_contexts', 'One chain names more than one context.', 'One chain, one context. Split the chain into separate expressions, or declare the edges in one context.'),
    ('error', 'semantic/grounding/er/operand_term', 'An ER operand is not a relation-access term.', 'Edges are selected by their terms'' canonical spellings, so each operand of & and && must be a relation-access term — people(*), people(, age >= 18). The alias stays OUTSIDE the term (people(*) as p selects by people(*), exports answer to p), and so does the outer marker (orders?(*)).'),
    ('error', 'semantic/grounding/er/alias_in_declaration', 'An alias inside an edge-declaration term.', 'Declaration-side terms are naked: the alias is call-site export vocabulary (people(*) as p & …), never part of the term that names the edge.'),
    ('error', 'semantic/resolution/ho/pipe_landing', 'The pipe''s landing at this call is ill-formed.', 'R8, strict: a piped relation lands at the FIRST formal parameter, or at exactly one explicit @ — never a search for a table parameter elsewhere, never displacement of a supplied argument. If the first parameter is already occupied, say where the pipe goes: f("arg", @). Two @ placeholders refuse — one pipe, one landing.'),
    ('error', 'semantic/resolution/ho/relation_actual_form', 'A higher-order relation actual is not a closed relation value.', 'A relation-valued argument is a CLOSED relation value: a whole named relation or parameterized application, an anonymous relation of any degree (one column included), or an explicit interior over its own source. An argumentative access — `f(users(_, dept, name))` — is not one: its names are logical binders, and letting them leak or turn private would change the access law. Construct the relation with a closed interior, `users(, cond |> (cols))`, or bind it first with `:` and pass the whole named relation.'),
    ('error', 'semantic/resolution/ho/relation_actual_capture', 'A higher-order relation actual reads the calling row.', 'A relation actual is closed: its interior may read its own source columns, literals and the statement''s definitions, never a caller lvar, a sibling member''s column, or a caller qualifier. Pass the value as an ordinary argument and read it inside the definition, or construct and name the relation first.'),
    ('error', 'parse/ho/relation_actual', 'A compound relation expression stands in a higher-order argument list.', 'The mixed argument list does not embed the relation grammar: a set expression, a pipeline, or a join has no derivation inside `f(…)`. Bind the relation first — `… : name` — and pass the whole named access, `f(name(*))(*)`. This diagnosis is recovered from the failed parse''s token stream.'),
    ('error', 'semantic/resolution/ho_access/pattern_shape', 'An access-pattern element has an unsupported shape.', 'The trailing access group on a parameterized-rule call is ordinary argumentative access over the declared heading: bare names bind, _ discards, a repeated name self-unifies (null-safe), a literal filters. Other element shapes (qualified references, expressions) are not access patterns — compute in a pipe stage instead.'),
    ('error', 'semantic/resolution/pipe', 'The deictic `_` did not select exactly one unnamed pipe output.', 'A pipe stage publishes a relation with no authored name, and `_` POINTS at it (LVARS.md). It is deixis, not a name: it performs no name lookup, and it requires exactly one visible unnamed pipe output. Members: no_unnamed_pipe (none is in view) and two_unnamed_pipes (more than one is). Naming a stage with `as` removes it from what `_` can point at, which is how the second is settled.'),
    ('error', 'semantic/resolution/pipe/no_unnamed_pipe', '`_` was written where no unnamed pipe output is in view.', 'No pipe has run at this point, so there is nothing for `_` to point at. This is not a misspelled qualifier — `_` never names anything, so there is no name to have got wrong. Write the relation''s own name (users.id), or pipe first. A pipe whose output was named with `as` is no longer unnamed and is reached by that name instead.'),
    ('error', 'semantic/resolution/pipe/two_unnamed_pipes', '`_` was written with more than one unnamed pipe output in view.', 'One spelling cannot stand for two relations, and a writer who meant a particular one has no way to say so. Name one of the stages with `as` — an alias replaces the anonymous form, leaving exactly one thing for `_` to point at.'),
    ('error', 'semantic/narrowing/object_literal', 'Narrowing a column whose every row is a single object.', 'Narrowing (.col{...}) iterates a SEQUENCE — every row of this column is a single object literal, a record, not a sequence of records. Path into the record instead ((col:{.field})), or spell the one-element sequence ([{...}]). Data-borne non-arrays the compiler cannot see contribute zero rows at runtime (JSON-SUBSTRATE.md).'),
    ('error', 'semantic/cte/head/names_only', 'A CTE head lists column names, nothing else.', 'A head is an ordered projection of its body''s heading (HEADS.md): names must exist in the body, head order is output order, unmentioned columns hide, and a head never renames or computes. Compute, rename, or filter in the body, then name the result: name(cols) : body |> (expr as col).'),
    ('error', 'semantic/transform/self_check', 'The transpiler''s own output failed verification.', 'The post-lowering self-check verifies every shipped statement''s name bindings on the exact SQL AST handed to the generator: every qualified reference must name a scope visible on its path, and a column read from an enumerable derived scope must exist in it. A failure here is a compiler invariant violation, never a user error — please report the query that produced it.'),
    ('error', 'semantic/transform/self_check/dangling_qualifier', 'The transpiler emitted a dangling qualifier.', 'A generated reference names a scope that is not visible anywhere on its path. Internal invariant violation — please report the query.'),
    ('error', 'semantic/transform/self_check/unknown_column', 'The transpiler referenced a column its scope does not output.', 'A generated reference reads a column absent from its derived scope''s enumerable output list. Internal invariant violation — please report the query.'),
    ('error', 'semantic/mention/term/not_a_term', 'The mention''s interior is not an admitted term.', 'A term is the interior of a mention — the committed extent is table functors only: a single relation-access term such as people(*), people(, age >= 30), or orders(id, _, total). Namespace paths, function terms, pipelines, and joins are not terms; new term kinds are admitted by ruling, never by drift.'),
    ('error', 'semantic/mention/term/unformattable', 'The canonicalizer cannot emit this term.', 'The term parses but the format engine takes no position on part of it — and unformatted bytes never become a match key or a stored spelling, because the canonical form is both.'),
    ('error', 'semantic/interior/topn/noneq_correlation', 'Interior top-N requires equality correlation.', 'RULED (this release): under non-equality correlation (<=, <, ...) each outer row sees a DIFFERENT candidate population, and the pre-ranked lowering would rank the wrong one — so the form refuses rather than answer arbitrarily. Spell the ranking explicitly with a post-join row_number window. A lateral per-outer-row lowering is a possible future release.'),
    ('error', 'semantic/recursion/argumentative_binding', 'Argumentative binding on a recursive self-reference.', 'Renames and constraints on the self-reference (''c(m)'' inside c''s own definition) do not bind inside a recursive definition yet — refused rather than returning wrong results. Use glob binding ''c(*)'' and rename or filter in a pipe stage. The proper fix (the rename-hoist legalization: WITH c(m) AS (…)) is pending. RECURSION-CONTRACT.md B2.'),
    ('error', 'semantic/recursion/limit_bound', '#<N inside a recursive rule has no spelling on this target.', 'DelightQL defines a row limit inside a recursive rule as a TOTAL-ROW CAP on the fixpoint — a demand bound on the unfold. SQLite and MySQL spell it natively (a trailing LIMIT on the recursive member); this target has no single-statement equivalent, and the near-miss spellings silently change meaning (a subquery LIMIT becomes per-iteration — non-terminating). Rewrite the bound as a filter condition on the recursive rule: a depth counter carried in the frontier row, or a value predicate.'),
    ('error', 'semantic/recursion/consulted_clause_order', 'Circular consulted-definition expansion (recursive clause before base, or an indirect view cycle).', 'While inlining a consulted definition, the resolver re-encountered a name it was already expanding — the self-reference did not resolve as the in-progress recursive CTE, so expansion would never terminate (this used to hang the compiler). The common cause: in a consulted rules file, the recursive clause appears BEFORE the base clause — clause order matters; a self-reference is only recursive once a prior clause has established the name. Put the base (non-recursive) clause first. If the cycle runs through another view (a uses v, v uses a), break the cycle. The error message shows the expansion chain. RECURSION-CONTRACT.md B5.'),
    ('error', 'semantic/recursion/mutual', 'Two or more definitions form a recursion cycle.', 'Mutual recursion is not supported. Each recursive definition may re-enter only its own established frontier; reaching an earlier open definition through another family closes a cycle in the definition-instance graph. The refusal reports the complete cycle from either entry point. Break the cycle or combine the state into one recursive relation. SEMANTICS/recursion-contract-law.md, NO MUTUAL RECURSION.'),
    ('error', 'semantic/recursion/set_operator', 'A union-family operator appears inside a recursive clause.', 'Clause accumulation is the fixpoint''s own operation and uses the flavor declared by the recursive target. A union-family operator cannot stand inside one recursive member. Finish the fixpoint first, then use its result as an ordinary set-operation arm. SEMANTICS/recursion-contract-law.md, THE BADGE CHOOSES THE UNION.'),
    ('error', 'semantic/compound/scalar_column', 'A compound-value tool aimed at a plainly-scalar column.', 'Pathing (''col:{.field}'', ''col:[0]'') reaches into a value; narrowing (''|> .col{.field}'') iterates one. A column declared as a plain scalar (INTEGER, REAL, BOOLEAN, dates) has no insides to reach into and no rows to iterate — aiming these tools at it used to fail target-dependently at runtime (sqlite: ''malformed JSON'', or silent NULLs when the scalar happened to parse as JSON). Refused at compile time instead. TEXT columns stay permissive: documents live in TEXT, and declarations cannot be trusted to deny it. Aim the tool at a compound value: something built with {...}/[...], a tree-group, or a document column.'),
    ('error', 'semantic/ho', 'A higher-order view was parameterized wrongly.', 'Family for refusals in higher-order definition bodies. Higher-order scalar parameters bind by AST substitution at the call — a parameter name is a supplied value spliced into the body, never a column of it. Member: param_shadows_column.'),
    ('error', 'semantic/ho/param_shadows_column', 'A scalar parameter name collides with a body column.', 'A higher-order view''s scalar parameter is spliced into the body as a value, not a column. When its name equals a column the body would otherwise resolve to (e.g. `g(age)(*) :- users(*), age > 40` where users has an age column), the substitution silently CAPTURES the column: constraints on it tautologize (`age > 40` becomes `50 > 40`) and the column drops from the output — both silent. Refused loudly at expansion (call time), where body relations carry real schemas, so both concretely-named bodies and glob-param bodies (T(*)) are caught. Remedy: rename the parameter so it no longer shadows the column. Pinned by ddl/465.'),
    ('error', 'runtime/assertion', 'An asserted property did not hold.', 'The ordinary assert! effect applied its closed pure property rule to the established input relation and received no witness rows. The current run rolled back, later effects did not run, and the same session remains usable. Hookable for tests: (~~error://runtime/assertion ~~).'),
    ('error', 'internal/panic', 'dql itself crashed. This is a bug in dql, not in your query.', 'An internal invariant failed (a Rust panic). The CLI catches it and emits this record instead of a raw backtrace; rerun with RUST_BACKTRACE=1 for the developer trace, and please report the message and the query that triggered it. Your query may be perfectly valid — do not rewrite it to dodge this error; the bug is ours.'),
    ('error', 'internal/unbadged', 'An error crossed the session boundary without an identity.', 'Every error a session reports should carry a delightql-error:// identity. One that reaches the client with none is recorded under this identity in sys::diagnostics.finding so the hole is visible in the log rather than invisible in the message text. The message is the error''s own; the missing badge is dql''s omission, not yours.'),
    ('error', 'runtime/connection', 'A database connection failed or was poisoned.', 'The connection to a mounted or primary database was lost or unusable at execution time.'),
    ('error', 'runtime/execution', 'The database engine refused the generated SQL at run time.', 'Compilation succeeded, but the engine rejected the statement while executing it — e.g. a JSON function received malformed text, a constraint fired, or a transaction statement failed. The message carries the engine''s own error text. Hookable: (~~error://runtime/execution ~~).'),
    ('error', 'client/worker/unavailable', 'The REPL parser containment worker could not serve.', 'The prompt''s per-keystroke parses and the submission preflight run in a separate worker process so a parser freeze cannot take the terminal with it. This identity records the worker failing to spawn, breaking the framed protocol, or being replaced. Optional assistance (coloring, well-formedness prompts) falls back quietly; the mandatory preflight refuses the submission rather than cross an unkillable in-process parser without a verdict.'),
    ('error', 'client/worker/budget', 'A prompt parse exceeded its containment budget.', 'The exact input, the operation, the entrance, the budget that applied and how containment ended it (cooperative cancel or worker kill) are one row in repl::errors.incident, deduplicated by specimen with an occurrence count. The input is retained verbatim for reproduction; `.bug` ships it.'),
    ('error', 'client/assistance/disabled', 'Optional REPL parser assistance was switched off after an incident.', 'The breaker for the OPTIONAL per-keystroke probes (syntax coloring, parse-aware prompts, continuation navigation) trips on the first incident from one of them. Submission preflight is mandatory and stays on. `.repl helpers on` re-enables the optional probes.'),
    ('error', 'client/preflight/refused', 'A submission was refused before it reached the compiler.', 'Every prompt submission is parsed in the containment worker first. When that parse exceeds its budget, the worker is unavailable, or the worker panics on these exact bytes, the submission is refused with its input recorded here — never handed to the in-process parser without a verdict. The compiler never saw it, so this row is the only record of the refusal.'),
    ('error', 'client/ledger/write_lost', 'A client-database write was lost.', 'The client database (repl::*) records inputs, options and incidents through a bounded pending queue when its connection is busy. A write that could neither apply nor queue is reported here rather than silently dropped; the typed in-memory value still governs behavior, only its queryable projection is missing.'),
    ('error', 'client/namespace/install', 'The repl::* namespace could not be installed on this session.', 'The client database is mounted into every session as repl::data with fixed projections. When the mount or a projection fails, the session still serves the user''s database; repl::* is unavailable until the next session reset, and this row says why.'),
    ('error', 'client/config', 'A client configuration resource could not be read or written.', 'The history file, the config directory, or a highlights file was unavailable. The prompt continues without it.'),
    ('error', 'client/terminal', 'A terminal-side failure in the interactive client.', 'The Ctrl-C handler could not be installed, a line could not be read, or the multi-pane TUI failed. Not a query error: the client''s own terminal handling.'),
    ('error', 'client/argument', 'A command-line argument was accepted with a warning.', 'An unrecognized debug option, or an install proceeding without adapter digests: the invocation continues, and the warning is recorded against the process''s argv (repl::context.argument).'),
    ('error', 'client/format', 'dql format returned the input unchanged.', 'The formatter speaks the query grammar; a definition library, unparseable input, an unhandled node, or a token-stream change all pass the input through unchanged, with exit code 2 when asked to fail on unformatted input.'),
    ('error', 'client/sanitize/disabled', 'Output sanitization is off; terminal control sequences pass verbatim.', '`--no-sanitize` and `-f raw` to a terminal write bytes the terminal may interpret. A deliberate choice, warned once per process and recorded so a transcript shows the terminal was exposed.'),
    ('error', 'client/database/unavailable', 'The client database could not be created.', 'The in-memory SQLite engine refused to open the per-process client database. Nothing can be recorded in this process — the one failure with nowhere to land — so it is said on stderr and repl::* is unavailable.'),
    ('error', 'client/unbadged', 'An error without an identity reached the process boundary.', 'Every error should carry a delightql-error:// identity. One that reaches main() without one — or with a protocol prefix instead of a full badge — is recorded under this identity so the hole is visible in the exit log rather than invisible in stderr scrollback.'),
    ('error', 'client/report/description', 'The words a person attached to a bug report.', '`.bug <words>` stores the description as an info row in repl::errors.incident, so it travels in error.log with the incidents it describes and needs no side file.'),
    ('error', 'operational/resource/nesting', 'The query nests deeper than this session budgets for.', 'Depth is a RESOURCE policy, not a rule of the language (S11): nothing about DelightQL forbids a deeply nested query. The compiler''s phase walks recurse, and a walk deeper than the stack it runs on aborts the process instead of answering — which, for a compiler embedded in another program through the C interface, takes the HOST down. So the depth is measured on the parse tree, before any recursive walk, and refused. Raise the budget with DELIGHTQL_MAX_NESTING (or the host''s own setter), or flatten the query: a chain of pipe stages costs no depth where nested parentheses do.'),
    ('error', 'operational/resource/refinement-depth', 'Refinement recursed deeper than this session budgets for.', 'The refiner walks a chain recursively, and its walks carry stacksafe — so a walk that stops making progress does not overflow the stack and fail promptly; it grows stack segments and clones AST state until the MACHINE gives out. A compilation therefore spends a bounded number of active refiner frames (512 by default, measured against a corpus maximum of 101), and the frame that would exceed the budget is refused instead of entered. This is a RESOURCE policy, not a rule of the language, and it is a DIFFERENT budget from operational/resource/nesting: that one measures the authored parse tree before any walk, this one measures refinement while it runs, and raising one does not raise the other. Meeting this refusal usually means one of two things: an unusually deep query, which an operator may afford by raising DELIGHTQL_MAX_REFINEMENT_DEPTH up to the ceiling of 4096; or a cycle in the compiler, which is a bug worth reporting with the query that found it. sys::execution.compiler_limit(*) reports the default, the effective value, and the ceiling. The session stays usable: the refusal ends the one compilation and nothing else.'),
    ('error', 'operational/federation-prohibited', 'One query may touch only one connection.', 'The query references namespaces served by different connections. DelightQL deliberately does not federate: split the query, or mount the data into one engine.'),
    ('error', 'imprint', 'An imprint! lifecycle rule was violated.', 'Imprint errors cover the linear lifecycle of imprint!/imprint_replace!. imprint! is linear: it consumes the source namespace, archiving it as an inert {target}::_N_blueprint. blueprint/ members refuse operations that would animate that archive.'),
    ('error', 'imprint/blueprint/inert', 'An archived blueprint namespace is inert.', 'imprint! is linear: it consumed the source namespace into {target}::_N_blueprint and vacated the original path. The archive stays VISIBLE through the sys::meta catalog functor ({blueprint}::(*) still lists its entities) but is INERT — resolving an entity through it (blueprint.rule, blueprint::sub.rule), enlisting it, or grounding it is refused, because the archived blueprint and the live target tables it produced would otherwise drift. Re-consult the source path to obtain a fresh, live copy. Pinned by companion_linear--70 (query), --71 (enlist), --73 (ground); the visible half by --61.'),
    ('error', 'imprint/manifest/materialization', 'An imprinting() materialization value is not recognized.', 'The _internal imprinting() manifest declares each entity''s materialization; only "table" and "view" are valid. A typo (e.g. "veiw") is rejected at manifest-read rather than silently falling through to a table. Pinned by companion_linear--75 and manifest::tests::materialization_rejects_typo.'),
    ('error', 'imprint/manifest/extent', 'An imprinting() extent value is not recognized.', 'The _internal imprinting() manifest declares each entity''s extent; only "permanent" and "temporary" are valid. A typo (e.g. "temp") is rejected at manifest-read rather than silently meaning permanent. Pinned by companion_linear--76 and manifest::tests::extent_rejects_typo.'),
    ('error', 'imprint/manifest/entity_name', 'An imprint entity name contains a double quote.', 'imprint entity names are interpolated into quoted SQL identifiers; the declared-table branch routes through the DDL generator, which does not escape an embedded double quote. Such a name (only reachable via a triple-quoted DQL literal) is refused at manifest-read. Pinned by manifest::tests::entity_name_rejects_embedded_quote.'),
    ('error', 'namespace', 'A namespace-creation target hit the system name guard.', 'The top level of the namespace tree stays open to user names, but USER-facing creation verbs (consult!/consult_tree!/mount!/ground!/named (~~ddl:"name" ~~) scratch) refuse the reserved system name pool. Members: name/reserved (a bare system name sys/std/home, or a sys*/std* prefix, or any `_`-prefixed machinery segment), name/system_subtree (creation under sys::/std::). `main` is exempt — the primary data namespace arrives through the same mount! verb; under home the prefix rule relaxes (home::sysinfo is legal) while the `_` reservation stays strict.'),
    ('error', 'namespace/name/reserved', 'A namespace-creation target used a reserved system name.', 'USER-facing namespace creation refused the target because it (a) IS a bare system name (sys/std/home), (b) begins with a reserved system prefix (sys*/std*, case-insensitive — sysinfo, stdlib, std2, SYS_foo), or (c) contains a segment beginning `_` (the _internal/_N_blueprint machinery convention, reserved on ANY segment EVERYWHERE, including under home). The message names the offending segment and the rule it hit. Fixes: choose a top-level name not beginning with sys/std and not equal to a system name; author scratch under home:: (where the sys*/std* prefix relaxes); never begin a segment with `_`. `main` is exempt (Deviation #4). Pinned by namespace_guard--01..05 and system::name_guard_tests.'),
    ('error', 'namespace/name/system_subtree', 'A namespace-creation target nested under sys:: or std::.', 'USER-facing namespace creation refused a target under the sys:: or std:: subtree — reserved for system machinery. (Before the guard this was only incidentally refused, and consult!/ground! into sys::/std:: actually SUCCEEDED, silently minting a flat-pid namespace beside the system rows.) Create your namespace at the top level, or under home::, instead. Pinned by namespace_guard--06 and system::name_guard_tests.'),
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

INSERT INTO identifier (kind, hierarchy, summary, explanation) VALUES
    ('error', 'parse/session_position', 'A session directive stood inside a query.', 'Session directives (mount!, consult!, enlist!, and their kin) change what the compilation can see, so they are legal at the REPL/CLI top level or in a liminal program — never nested in a data position, where their ordering relative to the query around them would be undefined.'),
    ('error', 'parse/assertion/retired', 'The retired assertion annotation was written.', 'Define a pure property rule and demand assert!(property)(*) on the relation being checked. Assertions are ordinary effects; the annotation sidecar no longer exists.'),
    ('error', 'parse/metadata_induction', 'A metadata group was induced under a data key.', '`"key": ~>` promises an interior TABLE — an array per group — while a metadata group yields an interior RECORD, one object per group. A metadata group stands under a fixed key by its own spelling: in a PATTERN `"key": ~> c:~> {…}` (or the braced nesting `"key": { c:~> {…} }`), and in a CONSTRUCTION `"key": c:~> {…}` — the group directly, with no second induction between.'),
    ('error', 'parse/iteration_binder', 'A bare iteration binder has no derivation.', 'Iteration derives a record or a tuple to destructure into; a bare name names nothing to destructure. To bind each plain value of an array, write the binder inside brackets: `"key": ~> [v]`.'),
    ('error', 'parse/pattern_qualified', 'A pattern member cannot be qualified.', 'A pattern extracts values; a qualified name in a pattern member would assert an equality with an existing column instead, and that is not what patterns do. Reach into the document with a path binding — `.person.first` publishes `person_first`, and `as` renames. Construction position may qualify freely.'),
    ('error', 'parse/path_variable', 'The json accessor was handed something other than one literal path.', 'There is one accessor door and it takes exactly ONE path, spelled with its steps: `x:{.a.b}`. A path is spec, not a value — it never evaluates alone and nothing produces one at runtime, so a bare name inside the braces can never be fed. `"$…"` is the target engine''s own path sub-language and stays with the target. `x:[1]` says the same thing as `x:{.1}` with a shape that reads as a type: an accessor reads, so it takes the one path spelling.'),
    ('error', 'parse/effect/purity', 'A pure head carried an effectful body.', 'A relational rule''s body is a relex and an effect rule''s is an effrelex, so a head without `!` whose body demands a directive has no derivation. Declare the effect in the head — `name!(*) :- …` — or take the directive out of the body.'),
    ('error', 'parse/directive/position', 'A directive stood where only a relation derives.', 'Under an effect head the law admits a directive inside a predicate subquery; what is missing is its lowering, so nothing derives there yet. Lift it out of the predicate and demand it as its own step.'),
    ('error', 'parse/effect/label', 'An effectful body was bound under a pure label.', 'A label ASSERTS what its body is. A binding whose body demands a directive is an effect binding, so its label carries the mark: write `: name!`.'),
    ('error', 'parse/structural_head', 'A head parameter named a shape to destructure.', 'Structural head grounding is reserved. A head parameter names a relation or a scalar; destructuring a shape in a head has no derivation.'),
    ('error', 'parse/guard_grouping', 'A guard composed operators without grouping.', 'DelightQL has no operator precedence, and a bare `%` reads as the group-modulo sigil wherever a relational reading is possible. Parenthesize the arithmetic: `f:(n | (n % 2) = 0)`.'),
    ('error', 'parse/glob_argument', 'A bare glob stood where a higher-order argument stands.', 'How many groups follow the name decides what the first one is. With ONE group, `f(*)` is ordinary access and the glob names the whole heading. With TWO, the left group supplies the callee''s parameters, and `*` names no actual for any parameter — the same glyph does not mean access in one left group and an unspecified value in another. Supply every scalar, relation, and rule-value actual; use an ordinary relation when those positions must be enumerable.'),
    ('error', 'parse/head_computes', 'A defining head contained a computation.', 'A head is an ordered projection of its body''s heading: each position holds a name or a ground term, optionally labeled with `as`. A call or expression computes, and a computation is not a name — a head that computes is not a head. Compute in the body and label the result: `h(*) : body |> (count:(a) as n)`.'),
    ('error', 'parse/lift_tail', 'A lift tail stood in a one-group call.', '`&` bounds arguments only in a two-group call, where the lifted rows follow it and dissolve into an anonymous-table argument (`f(users(*) & 1, 2)(*)`). A one-group call''s parentheses are its arguments alone, so a `&` tail there has no meaning. The projection the tail reaches for belongs to the ACCESS group: `json_each(doc, path)(value, type)`.'),
    ('error', 'parse/value_naming', 'A definition''s body was a row of named values.', 'A definition''s body is ONE domain expression. `as` names a publication position — a projection item, an embed, a stage — and a parenthesized list of named values is a row, which a value definition does not produce. Publish the columns from the caller''s projection instead, applying the function per column.');

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
