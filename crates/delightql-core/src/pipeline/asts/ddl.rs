// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! DDL AST — typed in-memory representation of definitions.
//!
//! The DDL AST is ephemeral: produced by parsing definition text, used for
//! validation and body extraction, then discarded. The database stores text;
//! ASTs are re-parsed on demand.
//!
//! Bodies reference regular DQL AST types (`DomainExpression`, `Chain`)
//! in the `Unresolved` phase — definitions are parsed before resolution context exists.
//!
//! The DDL AST itself is NOT phase-parameterized. It's a static structural
//! container. Only the DQL expressions it references carry the phase marker.
//!
//! The head is `core::definitions::Head` — the same type the `:` neck
//! carries. There is no DDL-side head family: what differs between a
//! function, a view, an effect rule, and an edge is the DECLARED kind and
//! the subject, never the shape of the head.
//!
//! The unit that leaves this module is the `DefinitionGroup` — a subject
//! and its assembled clauses. A clause vector is not a definition: nothing
//! downstream may pick a clause and read the group's identity off it.

use super::core::{
    AnonRelation, AnonTable, Chain, ContextMode, Datum, DomainExpression, FunctionApplication,
    Grelex, LiteralValue, NamedReference, OutValue, Query, Reference, TabularBody, TabularRow,
    TruthExpression, Unresolved,
};
use crate::error::{DelightQLError, Result};
use delightql_types::SqlIdentifier;

pub use super::core::definitions::{
    DefKind, GroundNaming, Head, HeadAssembly, HeadItem, HeadItems, HeadedClause, HoParam, Neck,
    Supply,
};

/// One clause of a definition: everything the neck introduces.
///
/// A clause has no subject, no declared kind, and no heading of its own —
/// those belong to the group, which is the only thing that can decide them.
///
/// Never stored; always ephemeral.
///
/// Lifecycle:
/// - Consult time: parse → assemble → store text → discard AST
/// - Query time: read text → re-parse → assemble → extract body → resolve → discard
#[derive(Debug, Clone)]
pub struct Clause {
    pub head: Head,
    /// The neck this clause was written with. A definition IS its neck; no
    /// pass may search text for one. Carried and unread: `:-` and `:=`
    /// select the same registration today, so the distinction has no
    /// consumer yet — dropping the field would put that gap back where
    /// only a text search could find it.
    #[allow(dead_code)]
    pub neck: Neck,
    pub body: DdlBody,
    /// The authored characters, PROVENANCE only: persistence stores them
    /// and re-parsing reconstructs the definition from them. No semantic
    /// decision reads this field — there is a typed neck and a typed head
    /// for every question asked of it.
    pub full_source: String,
    pub doc: Option<String>,
}

/// One clause as the builder DECLARES it, before its siblings are known:
/// the front matter it was written with, plus its body. This is assembler
/// input and nothing else — the only thing that can be made of a `Vec` of
/// them is a `DefinitionGroup`.
#[derive(Debug, Clone)]
pub struct ClauseDecl {
    pub front: DefinitionFront,
    pub neck: Neck,
    pub body: DdlBody,
    pub full_source: String,
    pub doc: Option<String>,
    /// A headerless fact clause's per-row heading offers (`1 as a`), in row
    /// order — spent by the assembler's fact elaboration, where each row
    /// becomes one ground-headed clause carrying its own labels. Empty for
    /// every other clause: a stacked fact's offers are judged against its
    /// header at build and are already consumed.
    pub fact_row_offers: Vec<Vec<Option<SqlIdentifier>>>,
}

/// A definition's front matter: everything left of the neck. Head-only
/// consumers (arity checks, entity registration) read this without paying
/// for a body parse.
#[derive(Debug, Clone)]
pub struct DefinitionFront {
    pub kind: DefKind,
    pub subject: DefSubject,
    pub head: Head,
}

impl DefinitionFront {
    /// The catalog spelling this front matter registers under.
    pub fn name(&self) -> String {
        self.subject.catalog_name()
    }

    /// The subject AS THE IDENTIFIER it is — strop bit intact — for the
    /// catalog's agreement. An edge subject has a composed catalog name and
    /// no single identifier.
    pub fn name_identifier(&self) -> Option<&delightql_types::SqlIdentifier> {
        match &self.subject {
            DefSubject::Named(name) => Some(name),
            DefSubject::Edge { .. } => None,
        }
    }

    pub fn into_clause_decl(
        self,
        neck: Neck,
        body: DdlBody,
        full_source: String,
        doc: Option<String>,
    ) -> ClauseDecl {
        ClauseDecl {
            front: self,
            neck,
            body,
            full_source,
            doc,
            fact_row_offers: Vec::new(),
        }
    }
}

/// §9 — a head baptizes ONE name; an edge names a PAIR and baptizes
/// nothing. That is the one deliberate second subject shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefSubject {
    Named(delightql_types::SqlIdentifier),
    Edge {
        /// IDENTITY IS THE CANONICAL SPELLING: the selection keys and the
        /// stored keys are the same bytes — canonical term spellings, never
        /// compared any other way. The pair is held in sorted order, which
        /// is what makes edge lookup symmetric — an edge names a pair, and
        /// a pair has no first.
        left: String,
        right: String,
        context: String,
    },
}

impl DefSubject {
    /// An edge over `left` and `right` in `context`, ordered so that the
    /// pair is the same subject however it was written.
    pub fn edge(left: String, right: String, context: String) -> DefSubject {
        let (left, right) = if left <= right {
            (left, right)
        } else {
            (right, left)
        };
        DefSubject::Edge {
            left,
            right,
            context,
        }
    }

    /// The catalog spelling this subject is stored under. For an edge that
    /// is the canonical composite of its pair and context; the edge still
    /// baptizes nothing.
    pub fn catalog_name(&self) -> String {
        match self {
            DefSubject::Named(name) => name.as_str().to_string(),
            DefSubject::Edge {
                left,
                right,
                context,
            } => format!("{left} &(::{context}) {right}"),
        }
    }
}

/// §9 — a definition: one subject and its clauses, assembled.
///
/// `DefinitionGroup::assemble` is the ONLY constructor and it is fallible.
/// Subject and declared kind, parameter arity, the per-position name-offer
/// contest, the Ground-Position rule, and output-heading collision are
/// decided HERE, once, for every definition form — before any catalog row
/// or resolution scope carries the subject's identity. There is no second
/// place clauses meet, so a first-clause heading and a silent NULL-padded
/// union have nowhere to happen.
#[derive(Debug, Clone)]
pub struct DefinitionGroup {
    subject: DefSubject,
    kind: DefKind,
    clauses: Vec<Clause>,
    assembly: HeadAssembly,
}

impl DefinitionGroup {
    /// The one door. `decls` are one subject's clauses in authored order.
    pub fn assemble(decls: Vec<ClauseDecl>) -> Result<DefinitionGroup> {
        let Some(first) = decls.first() else {
            return Err(DelightQLError::parse_error(
                "a definition group has at least one clause",
            ));
        };
        let subject = first.front.subject.clone();
        let name = subject.catalog_name();

        // One subject, or it is not one definition. Callers group by the
        // catalog spelling, so a disagreement here means two definitions
        // were handed over as one — a caller bug, refused rather than
        // silently registered under the first one's name.
        for decl in decls.iter().skip(1) {
            if decl.front.subject != subject {
                return Err(DelightQLError::validation_error_categorized(
                    "ddl/group/mixed_subject",
                    format!(
                        "definition group '{}': a later clause declares the subject \
                         '{}'. One group is one subject.",
                        name,
                        decl.front.name()
                    ),
                    "mixed subjects in one definition",
                ));
            }
        }

        // One declared kind — with the one ruled union: fact clauses stand
        // beside relational rule clauses, elaborating into the same ground
        // relational bodies. Every other mix still refuses.
        let kind = declared_group_kind(&name, &decls)?;

        // One signature arity, counting every position. Clauses may fix
        // DIFFERENT positions with ground constants; what they may not do
        // is disagree about how many positions there are.
        let first_arity = first.front.head.param_count();
        for (idx, decl) in decls.iter().enumerate().skip(1) {
            let arity = decl.front.head.param_count();
            if arity != first_arity {
                return Err(DelightQLError::validation_error_categorized(
                    "ddl/head/param_arity",
                    format!(
                        "Disjunctive definition '{}': clause {} has {} parameter(s) but \
                         clause 1 has {}. All clauses must have the same arity.",
                        name,
                        idx + 1,
                        arity,
                        first_arity
                    ),
                    "mixed clause arity in one definition",
                ));
            }
        }

        // FACT ELABORATION — once, here, into the ordinary relational clause
        // shape. After this point a fact clause is indistinguishable from a
        // hand-written view clause, which is what makes the contest, the
        // desugar law, and the UNION ALL combination downstream free.
        let decls = if decls.iter().any(|d| d.front.kind == DefKind::Fact) {
            let mut elaborated = Vec::with_capacity(decls.len());
            for decl in decls {
                if decl.front.kind == DefKind::Fact {
                    elaborated.extend(elaborate_fact_clause(&name, decl)?);
                } else {
                    elaborated.push(decl);
                }
            }
            elaborated
        } else {
            decls
        };

        // Fact syntax authenticates its positions: a fact-only definition's
        // unoffered position receives the canonical fact name instead of the
        // Ground-Position refusal.
        let ground_naming = if kind == DefKind::Fact {
            GroundNaming::FactCanonical
        } else {
            GroundNaming::Refuse
        };
        let heads: Vec<&Head> = decls.iter().map(|d| &d.front.head).collect();
        let assembly = super::core::definitions::assemble(&name, &heads, ground_naming)?;

        let clauses = decls
            .into_iter()
            .map(|decl| Clause {
                head: decl.front.head,
                neck: decl.neck,
                body: decl.body,
                full_source: decl.full_source,
                doc: decl.doc,
            })
            .collect();

        Ok(DefinitionGroup {
            subject,
            kind,
            clauses,
            assembly,
        })
    }

    /// The catalog spelling this group registers under.
    pub fn name(&self) -> String {
        self.subject.catalog_name()
    }

    /// The subject AS THE IDENTIFIER it is — strop bit intact — for the
    /// catalog's agreement. An edge subject has a composed catalog name and
    /// no single identifier.
    pub fn name_identifier(&self) -> Option<&delightql_types::SqlIdentifier> {
        match &self.subject {
            DefSubject::Named(name) => Some(name),
            DefSubject::Edge { .. } => None,
        }
    }

    pub fn subject(&self) -> &DefSubject {
        &self.subject
    }

    /// The assembled public heading, in output order — `None` for a glob
    /// group, whose heading is its body's. A pin's accessor: production
    /// consumes the assembly through `spend_heads` alone.
    #[cfg(test)]
    pub fn canonical_names(&self) -> Option<&[SqlIdentifier]> {
        self.assembly.canonical_names.as_deref()
    }

    /// What kind of entity this group's heads DECLARE. Nothing downstream
    /// re-derives it from the Rust type of a body.
    pub fn kind(&self) -> DefKind {
        self.kind
    }

    pub fn clauses(&self) -> &[Clause] {
        &self.clauses
    }

    pub fn into_clauses(self) -> Vec<Clause> {
        self.clauses
    }

    /// The group's first clause — for the per-clause readers a group of one
    /// still has (a scalar body, a context mode). It is NOT a metadata
    /// road: everything the clauses must agree on is on the group.
    pub fn first(&self) -> &Clause {
        self.clauses.first().expect("a group has a first clause")
    }

    /// How the signature captures names beyond the ones it declares. Part
    /// of what the assembler made agree.
    pub fn context(&self) -> &ContextMode {
        &self.first().head.context
    }

    /// The signature's parameters. The assembler made every clause agree on
    /// their COUNT; a clause still binds its own positions its own way.
    pub fn params(&self) -> &[HoParam] {
        self.first().params()
    }

    /// The parameter names a call site may supply, ground positions
    /// excluded.
    pub fn bound_param_names(&self) -> Vec<&SqlIdentifier> {
        self.first().head.bound_param_names()
    }

    /// THE DECLARED MODE, when this group declares one.
    ///
    /// A fact function is one clause by construction — the arms are its rows
    /// and the head's two lists are its declaration — so there is one mode to
    /// read and no cross-clause agreement to take.
    pub fn declared_mode(&self) -> Option<&super::core::FactFunctionMode<Unresolved>> {
        match &self.first().body {
            DdlBody::FactFunction(mode) => Some(mode),
            DdlBody::Scalar(_)
            | DdlBody::Truth(_)
            | DdlBody::Relational(_)
            | DdlBody::Deferred { .. } => None,
        }
    }

    /// The catalog's one documentation slot: the first clause that carries
    /// a doc comment. `doc` is per-clause in the AST and singular in the
    /// catalog, so the flattening is stated here rather than assumed at a
    /// read site.
    pub fn doc(&self) -> Option<&str> {
        self.clauses.iter().find_map(|c| c.doc.as_deref())
    }

    /// Does this road own the group's OUTPUT head?
    ///
    /// A higher-order output head belongs to the machinery that substitutes
    /// arguments: projecting before the parameters exist is a different act
    /// from projecting a first-order body. The AGREEMENT laws still ran for
    /// it — the assembler has no exceptions — only the desugar waits.
    fn output_head_is_ours(&self) -> bool {
        !matches!(self.kind, DefKind::HoView | DefKind::Effect)
    }

    /// Apply S04's one desugar law: each clause body carries the projection
    /// its own head declares, and the head is spent. Returns the clauses.
    pub fn spend_heads(self) -> Result<Vec<Clause>> {
        if !self.output_head_is_ours() {
            return Ok(self.clauses);
        }
        let subject = self.subject.catalog_name();
        super::core::definitions::spend_heads(self.clauses, &self.assembly, &subject)
    }
}

impl HeadedClause for Clause {
    fn head(&self) -> &Head {
        &self.head
    }

    fn body_publishes_names(&self) -> bool {
        use super::core::definitions::chain_publishes_names;
        match &self.body {
            // A scalar or truth body has no heading; the listed-head laws
            // do not reach it, and `spend_head` returns it untouched.
            DdlBody::Scalar(_) | DdlBody::Truth(_) => true,
            // Held characters cannot be read until substitution; the
            // question is asked again on the substituted body.
            DdlBody::Deferred { .. } => true,
            DdlBody::Relational(query) => chain_publishes_names(&query.body),
            // The declared heading names every position, so the elaborated
            // relation publishes names by construction.
            DdlBody::FactFunction(_) => true,
        }
    }

    fn spend_head(mut self, items: &[HeadItem], canonical_names: &[SqlIdentifier]) -> Clause {
        let DdlBody::Relational(query) = self.body else {
            // A scalar body has no heading to project. The listed-head
            // heading laws are the assembler's, and a scalar body reaches
            // here only through a head that declared none.
            return self;
        };
        self.body = DdlBody::Relational(project_query_through_head(query, items, canonical_names));
        self.head.items = HeadItems::Glob;
        self
    }
}

/// Append the head's projection to a body query, keeping the body's own
/// let-block where it is: the projection consumes the whole relex.
fn project_query_through_head(
    mut query: Query<Unresolved>,
    items: &[HeadItem],
    canonical_names: &[SqlIdentifier],
) -> Query<Unresolved> {
    use super::core::definitions::project_body_through_head;
    query.body = project_body_through_head(query.body, items, canonical_names);
    query
}

/// Cross-clause analysis of a single HO parameter position.
/// Computed at consult time from all clauses, stored in sys tables.
#[derive(Debug, Clone)]
pub struct HoPositionInfo {
    pub position: usize,
    /// Unified column kind across all clauses
    pub column_kind: HoColumnKind,
    /// How ground values distribute across clauses
    pub ground_mode: HoGroundMode,
    /// Ground constant values (one per clause that has a ground param at this pos)
    pub ground_values: Vec<(usize, String)>, // (clause_ordinal, value)
    /// Canonical column name (from free-variable clauses; None for PureGround)
    pub column_name: Option<String>,
}

/// What kind of HO column this position carries, unified across all clauses.
#[derive(Debug, Clone, PartialEq)]
pub enum HoColumnKind {
    /// T(*) in every clause
    TableGlob,
    /// T(x,y) in every clause
    TableArgumentative(Vec<String>),
    /// Scalar/ground across clauses
    Scalar,
}

/// How ground values distribute across clauses at a single position.
#[derive(Debug, Clone, PartialEq)]
pub enum HoGroundMode {
    /// Every clause: ground — free+unbound at call site IS valid
    PureGround,
    /// Some ground, some free — call site MUST provide concrete value
    MixedGround,
    /// Every clause: free — standard parameter
    PureUnbound,
    /// Table parameter (Glob/Argumentative) — always input-moded
    InputOnly,
}

/// Definition body — the DQL expression(s) after the neck.
#[derive(Debug, Clone)]
pub enum DdlBody {
    /// VALUE body: a function definition computes one value per tuple —
    /// a domain expression, or the licensed crossing at the same carrier a
    /// publication position uses.
    Scalar(OutValue<Unresolved>),
    /// TRUTH body: a sigma rule's body accepts or rejects a tuple, so it is
    /// carried as the truth it is. A value standing here has no derivation,
    /// and the parse-level category already refuses one.
    Truth(TruthExpression<Unresolved>),
    /// Relational body: view/ho-view definitions produce full queries (may include CTEs)
    Relational(Query<Unresolved>),
    /// THE DECLARED MODE, from the one carrier that serves both of its
    /// faces. Relationally it publishes the declared inputs followed by the
    /// declared outputs, with the explicit arms as its finite rows; callably
    /// it is the null-safe case law those arms spell. Storing the elaborated
    /// relation beside the mode would put the arms in the tree twice and let
    /// the two disagree, so the relation is BUILT from the mode where a
    /// relational body is asked for.
    FactFunction(super::core::FactFunctionMode<Unresolved>),
    /// A higher-order TEMPLATE whose text cannot be parsed until its
    /// parameters are substituted — the authored characters, held as such.
    ///
    /// A body may be deferred; a SUBJECT may not. A clause carrying this
    /// still went through the assembler with its siblings, because what the
    /// assembler decides — subject, kind, arity, the head algebra — is
    /// written left of the neck and needs no substitution to read. Only the
    /// body waits, and it says so in the type rather than by being absent.
    Deferred { source: String },
}

impl Clause {
    /// The output head's items, when the head lists them.
    pub fn head_items(&self) -> Option<&[HeadItem]> {
        self.head.items.listed()
    }

    /// The signature's parameters, when the head has a parameter group.
    pub fn params(&self) -> &[HoParam] {
        self.head.ho_params.as_deref().unwrap_or_default()
    }

    /// The value body, in the carrier that says whether it crossed.
    pub fn as_out_value(&self) -> Option<&OutValue<Unresolved>> {
        match &self.body {
            DdlBody::Scalar(expr) => Some(expr),
            DdlBody::Truth(_)
            | DdlBody::Relational(_)
            | DdlBody::FactFunction(_)
            | DdlBody::Deferred { .. } => None,
        }
    }

    /// The body as the TRUTH it is, for a sigma rule. A value body answers
    /// `None`: the two categories are decided at the parse, and a caller that
    /// wants one must say what it does with the other.
    pub fn as_truth_expr(&self) -> Option<&TruthExpression<Unresolved>> {
        match &self.body {
            DdlBody::Truth(expr) => Some(expr),
            DdlBody::Scalar(_)
            | DdlBody::Relational(_)
            | DdlBody::FactFunction(_)
            | DdlBody::Deferred { .. } => None,
        }
    }

    /// Consume the definition and return the value body, crossing included.
    pub fn into_out_value(self) -> Option<OutValue<Unresolved>> {
        match self.body {
            DdlBody::Scalar(expr) => Some(expr),
            DdlBody::Truth(_)
            | DdlBody::Relational(_)
            | DdlBody::FactFunction(_)
            | DdlBody::Deferred { .. } => None,
        }
    }

    /// Consume the definition and return the body as a full `Query` (may include CTEs).
    pub fn into_query(self) -> Option<Query<Unresolved>> {
        match self.body {
            DdlBody::Relational(query) => Some(query),
            // FACT ELABORATION, taken from the one carrier that holds the
            // arms — never from a second copy stored beside it.
            DdlBody::FactFunction(mode) => Some(mode.relational_body()),
            DdlBody::Scalar(_) | DdlBody::Truth(_) | DdlBody::Deferred { .. } => None,
        }
    }
}

/// The group's ONE declared kind, with the one ruled union.
///
/// A fact elaborates into the same ground relational clause body an ordinary
/// relational clause carries, so a name whose clauses mix facts and
/// relational rules is one relational definition (a view). Every other mix
/// is still two kinds of entity under one spelling and refuses.
fn declared_group_kind(name: &str, decls: &[ClauseDecl]) -> Result<DefKind> {
    let first = &decls[0];
    let mixed_kind = |idx: usize, kind: DefKind, first_kind: DefKind| {
        DelightQLError::validation_error_categorized(
            "ddl/head/mixed_kind",
            format!(
                "Disjunctive definition '{}': clause {} is a {} but clause 1 is a {}. \
                 All clauses must be the same kind.",
                name,
                idx + 1,
                kind_name(kind),
                kind_name(first_kind)
            ),
            "mixed clause kinds in one definition",
        )
    };
    if decls.iter().any(|d| d.front.kind == DefKind::Fact) {
        for (idx, decl) in decls.iter().enumerate() {
            if !matches!(decl.front.kind, DefKind::Fact | DefKind::View) {
                return Err(mixed_kind(idx, decl.front.kind, DefKind::Fact));
            }
        }
        return Ok(if decls.iter().all(|d| d.front.kind == DefKind::Fact) {
            DefKind::Fact
        } else {
            DefKind::View
        });
    }
    let first_type_id = entity_type_id(first.front.kind, &first.front.head.context);
    for (idx, decl) in decls.iter().enumerate().skip(1) {
        if entity_type_id(decl.front.kind, &decl.front.head.context) != first_type_id {
            return Err(mixed_kind(idx, decl.front.kind, first.front.kind));
        }
    }
    Ok(first.front.kind)
}

/// FACT ELABORATION — a fact clause becomes ordinary relational clauses.
///
/// A stacked fact (a header) becomes ONE clause whose head plumbs the header
/// names over the table body. A standard fact (no header) becomes one
/// ground-headed clause PER ROW over a unit body — each row's own `as`
/// labels ride as that clause's heading offers, so row disagreement is the
/// ordinary clause name-offer conflict and a duplicate row is a duplicate
/// clause, which the UNION ALL combination keeps as a duplicate proof.
fn elaborate_fact_clause(subject: &str, decl: ClauseDecl) -> Result<Vec<ClauseDecl>> {
    let ClauseDecl {
        front,
        neck,
        body,
        full_source,
        doc,
        fact_row_offers,
    } = decl;
    let DdlBody::Relational(query) = body else {
        return Err(DelightQLError::parse_error(format!(
            "fact '{subject}': a fact's body is its data table"
        )));
    };
    let chain = query.into_bare_body().map_err(|_| {
        DelightQLError::parse_error(format!("fact '{subject}': a fact's body is its data table"))
    })?;
    let (Grelex::Literal(anon), true) = (chain.head, chain.continuations.is_empty()) else {
        return Err(DelightQLError::parse_error(format!(
            "fact '{subject}': a fact's body is its data table"
        )));
    };
    let table = anon.table;

    if let Some(header) = &table.body.header {
        // STACKED: the header names the positions and the head plumbs them.
        // The datum offers were judged against this header at build.
        let mut items = Vec::with_capacity(header.len());
        for item in header.0.iter() {
            let Some(DomainExpression::Reference(Reference::Named(NamedReference(column)))) =
                item.term()
            else {
                return Err(DelightQLError::validation_error_categorized(
                    "ddl/head/fact_header",
                    format!("fact '{subject}': a fact's header names its columns"),
                    "a fact header item is a column name",
                ));
            };
            items.push(HeadItem::plumb(column.name.clone()));
        }
        let table_body =
            Query::relational(Chain::ground(Grelex::Literal(AnonRelation::plain(table))));
        return Ok(vec![ClauseDecl {
            front: DefinitionFront {
                kind: front.kind,
                subject: front.subject,
                head: Head::listed(items),
            },
            neck,
            body: DdlBody::Relational(table_body),
            full_source,
            doc,
            fact_row_offers: Vec::new(),
        }]);
    }

    // STANDARD: one ground-headed clause per row over a unit body — the
    // constant is placed into the body when the head is spent (SUPPLY IS
    // ELABORATION), and each row's labels are that clause's offers.
    let rows = table.body.rows.into_vec();
    let mut clauses = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.into_iter().enumerate() {
        let offers = fact_row_offers.get(row_index);
        let mut items = Vec::with_capacity(row.len());
        for (position, datum) in row.0.into_vec().into_iter().enumerate() {
            let Datum::Value(DomainExpression::Application(FunctionApplication::Ground(value))) =
                datum
            else {
                return Err(DelightQLError::parse_error(format!(
                    "fact '{subject}': a fact datum is a ground term"
                )));
            };
            items.push(HeadItem {
                supply: Supply::Ground(value),
                label: offers.and_then(|row| row.get(position).cloned().flatten()),
            });
        }
        clauses.push(ClauseDecl {
            front: DefinitionFront {
                kind: front.kind,
                subject: front.subject.clone(),
                head: Head::listed(items),
            },
            neck,
            body: DdlBody::Relational(unit_body()),
            full_source: full_source.clone(),
            doc: doc.clone(),
            fact_row_offers: Vec::new(),
        });
    }
    Ok(clauses)
}

/// The unit body a ground-headed clause projects over: `_(1)` — one row, so
/// the head's constants supply exactly one proof.
fn unit_body() -> Query<Unresolved> {
    let row = TabularRow(Box::new(
        crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![Datum::Value(
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Number(
                "1".to_string(),
            ))),
        )])
        .expect("one datum"),
    ));
    let rows = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![row]).expect("one row");
    Query::relational(Chain::ground(Grelex::Literal(AnonRelation::plain(
        AnonTable {
            body: TabularBody { header: None, rows },
            cpr_schema: (),
        },
    ))))
}

/// Entity type integer for storage in the bootstrap database.
///
/// Maps declared kind → entity_type_enum.id. A context-capturing function
/// is a different entity type from a plain one because the call protocol
/// differs, which is why the head's capture mode is read here.
pub fn entity_type_id(kind: DefKind, context: &ContextMode) -> i32 {
    match kind {
        DefKind::Function => {
            if matches!(context, ContextMode::None) {
                1
            } else {
                3
            }
        }
        DefKind::View => 4,
        DefKind::HoView => 8,
        DefKind::Sigma => 9,
        // A fact function IS a fact relationally; what makes it callable is
        // the functional dependency stored beside it, not a second kind of
        // entity with its own relational road.
        DefKind::Fact | DefKind::FactFunction => 16,
        DefKind::Edge => 17,
        // 20 = DqlEffectRule (enums.rs) — effect rules are their own entity type.
        DefKind::Effect => 20,
    }
}

/// The human word for a declared kind, for teaching a mixed-kind group.
pub fn kind_name(kind: DefKind) -> &'static str {
    match kind {
        DefKind::Function => "function",
        DefKind::View => "view",
        DefKind::HoView => "higher-order view",
        DefKind::Sigma => "sigma predicate",
        DefKind::Fact => "fact",
        DefKind::FactFunction => "fact function",
        DefKind::Edge => "er-context rule",
        DefKind::Effect => "effect rule",
    }
}
