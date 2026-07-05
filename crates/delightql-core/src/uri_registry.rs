// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// URI registry — the compiler-owned catalog behind `dql explain`.
//
// URI-DESIGN.md §3: "the authority is generated from the compiler
// registry" — this module IS that registry. `dql explain` reads it today;
// the delightql.org/uri/ pages are generated from it later, so the CLI
// and the website can never disagree.
//
// Identifiers are append-only (§3): entries may gain text or successors,
// but a hierarchy, once minted, is never reused for a different meaning.

/// One identifier kind (one compound scheme).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriKind {
    Error,
    Danger,
    Config,
}

impl UriKind {
    pub fn scheme(&self) -> &'static str {
        match self {
            UriKind::Error => "delightql-error://",
            UriKind::Danger => "delightql-danger://",
            UriKind::Config => "delightql-config://",
        }
    }

    pub fn word(&self) -> &'static str {
        match self {
            UriKind::Error => "error",
            UriKind::Danger => "danger",
            UriKind::Config => "config",
        }
    }

    pub fn all() -> &'static [UriKind] {
        &[UriKind::Error, UriKind::Danger, UriKind::Config]
    }
}

/// A registry entry: one documented identifier (or family node).
pub struct RegistryEntry {
    pub kind: UriKind,
    /// Bare hierarchy, e.g. "semantic/resolution/column".
    pub hierarchy: &'static str,
    /// One-line summary.
    pub summary: &'static str,
    /// Longer explanation shown by `dql explain`.
    pub explanation: &'static str,
}

/// Parse any accepted identifier spelling into (kind, bare hierarchy).
///
/// Accepted: the badge form (`delightql-error://semantic/cast`), the
/// canonical URL (`https://delightql.org/uri/error/semantic/cast`), or a
/// bare hierarchy (searched across all kinds — kind-ambiguous input is
/// the caller's problem to disambiguate via [`find_bare`]).
pub fn parse_identifier(input: &str) -> Option<(UriKind, String)> {
    for kind in UriKind::all() {
        if let Some(rest) = input.strip_prefix(kind.scheme()) {
            return Some((*kind, rest.trim_matches('/').to_string()));
        }
    }
    for base in ["https://delightql.org/uri/", "http://delightql.org/uri/"] {
        if let Some(rest) = input.strip_prefix(base) {
            let rest = rest.trim_matches('/');
            let (word, hier) = rest.split_once('/')?;
            for kind in UriKind::all() {
                if kind.word() == word {
                    return Some((*kind, hier.to_string()));
                }
            }
            return None;
        }
    }
    None
}

/// The canonical https form of an identifier (URI-DESIGN.md §2 binding).
pub fn canonical_url(kind: UriKind, hierarchy: &str) -> String {
    format!("https://delightql.org/uri/{}/{}", kind.word(), hierarchy)
}

/// Exact-match lookup.
pub fn find(kind: UriKind, hierarchy: &str) -> Option<&'static RegistryEntry> {
    REGISTRY
        .iter()
        .find(|e| e.kind == kind && e.hierarchy == hierarchy)
}

/// Bare-hierarchy search across all kinds (for kind-less input).
pub fn find_bare(hierarchy: &str) -> Vec<&'static RegistryEntry> {
    REGISTRY
        .iter()
        .filter(|e| e.hierarchy == hierarchy)
        .collect()
}

/// Registered descendants of a hierarchy (segment-prefix semantics —
/// the same family matching error hooks use).
pub fn children(kind: UriKind, hierarchy: &str) -> Vec<&'static RegistryEntry> {
    let prefix = format!("{}/", hierarchy);
    REGISTRY
        .iter()
        .filter(|e| e.kind == kind && e.hierarchy.starts_with(&prefix))
        .collect()
}

/// Whether a danger gate may be overridden from the CLI. Semantic-class
/// gates (they change what the query MEANS) are inline-only. Delegates to
/// the compiler's own enforcement so `dql explain` can never advertise a
/// spelling the CLI would reject.
pub fn danger_cli_overridable(hierarchy: &str) -> bool {
    crate::pipeline::danger_gates::is_cli_overridable(
        &crate::pipeline::danger_gates::canonical_danger_uri(hierarchy),
    )
}

/// The mintable top segments of the error kind — the closed set ratified
/// by the vocabulary audit (URI-DESIGN.md §7). `error_uri()` mints only
/// under these; the soundness test below keeps the registry inside them.
pub const ERROR_TOP_SEGMENTS: &[&str] = &[
    "parse",
    "semantic",
    "dml",
    "operational",
    "runtime",
    "target",
];

/// The registry. Curated by hand; append-only. Entries cover the
/// structural family nodes and the identifiers users actually meet
/// (corpus-surveyed); compiler-minted identifiers without an entry are
/// still valid — `dql explain` shows their structure and canonical URL
/// and says the prose is pending.
pub const REGISTRY: &[RegistryEntry] = &[
    // ---- error: family nodes -------------------------------------------
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "parse",
        summary: "The source text is structurally invalid.",
        explanation: "Parse errors mean the query text could not be read as \
DelightQL at all — the grammar rejected it before any meaning was \
assigned. Check delimiter balance, operator spelling, and clause order. \
Hook family: (~~error://parse ~~) matches every parse error.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic",
        summary: "The structure is valid but the meaning is wrong.",
        explanation: "Semantic errors mean the query parsed, but a name \
failed to resolve, an arity was wrong, or a constraint was violated \
during compilation. The subhierarchy names what went wrong: resolution/ \
(name binding), constraint/, arity, limitation/ (known gaps).",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "dml",
        summary: "A data-modification query violated DML shape rules.",
        explanation: "DML errors cover insert!/update!/delete!/keep! shape \
and marker rules: marker/ (the !! mutation marker — missing, multiple, \
forbidden, mismatch), shape/ (required or meaningless clauses), source/ \
(what may feed a mutation).",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "operational",
        summary: "The query is valid but this session refuses to run it.",
        explanation: "Operational errors are policy, not meaning: the query \
compiled, but session configuration forbids executing it (e.g. \
federation-prohibited: a query may touch only one connection).",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "runtime",
        summary: "Compilation succeeded; execution failed.",
        explanation: "Runtime errors happen after SQL generation: the \
database rejected the SQL, an assertion failed, a connection dropped, or \
I/O failed. Subhierarchy: assertion, connection, io, bug (internal), \
relay/transport (protocol channel).",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "target",
        summary: "The foreign engine rejected or failed the query.",
        explanation: "Target errors originate in the mounted engine, not in \
DelightQL: target/<engine>/<class>/<code> embeds the world's taxonomy as \
the leaf (Postgres: SQLSTATE, e.g. target/postgres/undefined-object/42883). \
Hook family: (~~error://target/postgres ~~) matches any Postgres-side \
failure. Lifecycle members: connect, orientation, unimplemented.",
    },
    // ---- error: common leaves ------------------------------------------
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "parse/general",
        summary: "Generic parse failure.",
        explanation: "The grammar rejected the text and no more specific \
parse category applied. The caret in the message marks the first \
unreadable token.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/resolution/table",
        summary: "A named table (or relation) was not found.",
        explanation: "The name does not exist in the current namespace. \
Check spelling, the mounted namespace prefix (ns.table), and whether the \
relation needs a mount!/consult! first.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/resolution/column",
        summary: "A named column was not found in scope.",
        explanation: "The column does not exist in the relation's schema at \
this pipeline stage. Note that |> projection changes the visible \
columns: a filter AFTER |> (a, b) sees only a and b.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/resolution/ambiguous",
        summary: "A name matches more than one column in scope.",
        explanation: "After a join, an unqualified column name exists on \
more than one side. Qualify it with the relation alias (u.id).",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/arity",
        summary: "Wrong number of arguments.",
        explanation: "A function or predicate was called with the wrong \
number of arguments for its declared arity.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/cast",
        summary: "Invalid cast:() usage.",
        explanation: "cast:(expr, type) takes a bare type name from the v1 \
vocabulary: integer, real, text, numeric, boolean. Target engines apply \
their own cast semantics (Postgres rounds real→integer; SQLite \
truncates) — see the book's cast page.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/recursion",
        summary: "A recursive definition breaks the recursion contract.",
        explanation: "Family for refusals of recursive forms the language \
does not permit (RECURSION-CONTRACT.md). DelightQL recursion is a \
generator (co-recursion): each recursive clause sees only the previous \
iteration's rows — never the accumulated result, never itself as a \
callable. Forms outside that contract are refused here, each with its \
rewrite path.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/recursion/nonlinear",
        summary: "A recursive rule references itself more than once.",
        explanation: "The frontier cannot join with itself (or with the \
accumulated result) — forward evaluation carries one previous iteration. \
Carry the values you need as columns of one frontier row instead: the \
tupling transformation. fib is the canonical example — two self-calls \
become one two-column state, (a, b) stepping to (b, a+b). \
RECURSION-CONTRACT.md N1.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/recursion/aggregate",
        summary: "Aggregation inside a recursive rule.",
        explanation: "An aggregate over the frontier would need the \
accumulated set, which a recursive rule never sees. Aggregate after the \
fixpoint — strata are textual, so a later pipe stage aggregates the \
finished recursion — or carry a running value as a column of the frontier \
row when the aggregation is per-path. RECURSION-CONTRACT.md N3.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/recursion/self_subquery",
        summary: "A recursive rule references itself inside a subquery.",
        explanation: "Semi/anti-joins, IN, scalar subqueries, or derived \
tables against the definition itself would need the accumulated set — a \
recursive rule sees only the previous iteration's rows, as a direct \
source. Track visited state in the frontier row (the visited-string \
idiom), or deduplicate/filter after the fixpoint. \
RECURSION-CONTRACT.md N4.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/recursion/argumentative_binding",
        summary: "Argumentative binding on a recursive self-reference.",
        explanation: "Renames and constraints on the self-reference \
('c(m)' inside c's own definition) do not bind inside a recursive \
definition yet — refused rather than returning wrong results. Use glob \
binding 'c(*)' and rename or filter in a pipe stage. The proper fix (the \
rename-hoist legalization: WITH c(m) AS (…)) is pending. \
RECURSION-CONTRACT.md B2.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "semantic/recursion/limit_bound",
        summary: "#<N inside a recursive rule has no spelling on this target.",
        explanation: "DelightQL defines a row limit inside a recursive rule \
as a TOTAL-ROW CAP on the fixpoint — a demand bound on the unfold. SQLite \
and MySQL spell it natively (a trailing LIMIT on the recursive member); \
this target has no single-statement equivalent, and the near-miss \
spellings silently change meaning (a subquery LIMIT becomes per-iteration \
— non-terminating). Rewrite the bound as a filter condition on the \
recursive rule: a depth counter carried in the frontier row, or a value \
predicate.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "runtime/assertion",
        summary: "An --assert query did not hold.",
        explanation: "The main query executed, but an assertion attached to \
the run returned false. Hookable for tests: (~~error://runtime/assertion ~~).",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "runtime/connection",
        summary: "A database connection failed or was poisoned.",
        explanation: "The connection to a mounted or primary database was \
lost or unusable at execution time.",
    },
    RegistryEntry {
        kind: UriKind::Error,
        hierarchy: "operational/federation-prohibited",
        summary: "One query may touch only one connection.",
        explanation: "The query references namespaces served by different \
connections. DelightQL deliberately does not federate: split the query, \
or mount the data into one engine.",
    },
    // ---- danger ---------------------------------------------------------
    RegistryEntry {
        kind: UriKind::Danger,
        hierarchy: "cardinality/nulljoin",
        summary: "NULL-matching join equality (NULL = NULL → true).",
        explanation: "OFF (default): join equality is SQL equality, where \
NULL never matches. ON: NULLs match each other in join keys, which can \
multiply rows AND changes what the join means — so this gate is \
semantic-class: inline-only ((~~danger://cardinality/nulljoin ON~~)), \
never a CLI flag. Consult sys.danger(*) for this session's states.",
    },
    RegistryEntry {
        kind: UriKind::Danger,
        hierarchy: "cardinality/cartesian",
        summary: "Unrestricted cartesian product.",
        explanation: "OFF (default): a join with no usable key is an error \
(the classic accidental row explosion). ON: the cartesian product is \
allowed. Guardrail-class: may be opened from the CLI (--danger \
cardinality/cartesian=ON) or inline.",
    },
    RegistryEntry {
        kind: UriKind::Danger,
        hierarchy: "termination/unbounded",
        summary: "Unbounded recursive query.",
        explanation: "OFF (default): recursive queries must be bounded. ON: \
unbounded recursion is allowed (the query may not terminate). \
Guardrail-class: CLI-overridable.",
    },
    RegistryEntry {
        kind: UriKind::Danger,
        hierarchy: "semantics/min_multiplicity",
        summary: "True INTERSECT ALL via ROW_NUMBER (min-multiplicity).",
        explanation: "Changes what a set operator MEANS (bag semantics via \
minimum multiplicity), so it is semantic-class: inline-only \
((~~danger://semantics/min_multiplicity ON~~)), never a CLI flag — a \
flag that silently changes query meaning would make the same text mean \
different things in different shells.",
    },
    // ---- config ----------------------------------------------------------
    RegistryEntry {
        kind: UriKind::Config,
        hierarchy: "generation/rule/inlining/view",
        summary: "Inline consulted view rules instead of emitting CTEs.",
        explanation: "Strategy selection, not meaning: with this ON the \
compiler inlines view-rule bodies as subqueries rather than emitting \
CTEs. Results are identical either way; generated SQL shape differs. \
Inline: (~~config://generation/rule/inlining/view ON~~); CLI: --config.",
    },
    RegistryEntry {
        kind: UriKind::Config,
        hierarchy: "generation/rule/inlining/fact",
        summary: "Inline consulted fact rules instead of emitting CTEs.",
        explanation: "As generation/rule/inlining/view, for fact rules.",
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_accepted_spellings() {
        assert_eq!(
            parse_identifier("delightql-error://semantic/cast"),
            Some((UriKind::Error, "semantic/cast".to_string()))
        );
        assert_eq!(
            parse_identifier("https://delightql.org/uri/danger/cardinality/nulljoin"),
            Some((UriKind::Danger, "cardinality/nulljoin".to_string()))
        );
        assert_eq!(parse_identifier("no-scheme-here"), None);
        assert_eq!(parse_identifier("mailto://x"), None);
    }

    #[test]
    fn canonical_url_is_the_binding() {
        assert_eq!(
            canonical_url(UriKind::Error, "semantic/cast"),
            "https://delightql.org/uri/error/semantic/cast"
        );
    }

    #[test]
    fn registry_lookups() {
        assert!(find(UriKind::Error, "semantic/resolution/column").is_some());
        assert!(find(UriKind::Error, "not/a/thing").is_none());
        // family listing
        let kids = children(UriKind::Error, "semantic/resolution");
        assert!(kids.len() >= 3);
        // bare search
        assert_eq!(find_bare("cardinality/nulljoin").len(), 1);
    }

    #[test]
    fn every_registered_danger_and_config_exists_in_its_runtime_registry() {
        use crate::pipeline::{danger_gates, option_map};
        for e in REGISTRY {
            match e.kind {
                UriKind::Danger => assert!(
                    danger_gates::known_danger_hierarchies().contains(&e.hierarchy),
                    "registry documents unknown danger {}",
                    e.hierarchy
                ),
                UriKind::Config => assert!(
                    option_map::known_config_hierarchies().contains(&e.hierarchy),
                    "registry documents unknown config {}",
                    e.hierarchy
                ),
                UriKind::Error => {}
            }
        }
    }

    #[test]
    fn error_entries_stay_inside_the_mintable_top_segments() {
        // The error side can't be checked exhaustively (identities are
        // minted dynamically), but soundness CAN be: a registry entry
        // whose hierarchy starts outside the ratified top set documents
        // a phantom the compiler can never mint.
        for e in REGISTRY {
            if e.kind == UriKind::Error {
                let top = e.hierarchy.split('/').next().unwrap();
                assert!(
                    ERROR_TOP_SEGMENTS.contains(&top),
                    "error registry entry '{}' is outside the mintable top segments",
                    e.hierarchy
                );
            }
        }
    }

    #[test]
    fn every_runtime_gate_and_config_is_documented() {
        use crate::pipeline::{danger_gates, option_map};
        for h in danger_gates::known_danger_hierarchies() {
            assert!(
                find(UriKind::Danger, h).is_some(),
                "danger {} has no registry entry — document it",
                h
            );
        }
        for h in option_map::known_config_hierarchies() {
            assert!(
                find(UriKind::Config, h).is_some(),
                "config {} has no registry entry — document it",
                h
            );
        }
    }
}
