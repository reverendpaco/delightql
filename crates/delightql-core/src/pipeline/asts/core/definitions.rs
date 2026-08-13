// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Definitions — one head, typed necks, one fallible assembler.
//!
//! A definition is a subject and its clauses; a clause
//! is a head, a neck, and a body. The head is the SAME type under every
//! neck — `:-`, `:=`, and `:` alike — so head-`as`, ground supply, and the
//! naming algebra mean one thing wherever they are written.
//!
//! The head is a heading, not a computation: an item SUPPLIES a value (a
//! reference into the body, or a ground constant) and OFFERS a name. What
//! the offers make of each other is the assembler's business, and it is the
//! only place that business happens.

use super::expressions::chain::{Continuation, Grelex, StructuralForm};
use super::literals::LiteralValue;
use super::operators::PipeOp;
use super::phases::{Phase, Unresolved};
use super::queries::ContextMode;
use super::specs::GroupSpec;
use super::{Chain, DomainExpression, TruthExpression};
use crate::error::{DelightQLError, Result};
use delightql_types::SqlIdentifier;

/// The neck, typed: `:-`, `:=`, `:`. There is no source text to search for
/// a neck in — a definition carries the one it was written with.
pub use crate::pipeline::asts::vocabulary::Neck;

/// What KIND of entity a definition declares. Declared by the head form at
/// build time; never re-derived downstream from the Rust type of a body
/// (sigma-by-body-type has no road).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefKind {
    /// `name:(params) neck expr` — a scalar function.
    Function,
    /// `name(params) neck boolean` — a sigma predicate.
    Sigma,
    /// `name(head) neck body` — a relation.
    View,
    /// `name(ho_params)(head) neck body` — a higher-order relation.
    HoView,
    /// `name(data)` — inline data, no neck.
    Fact,
    /// `name(inputs -> outputs ---- arms)` — inline data whose `->` declares
    /// a functional mode. A fact by its relational face and a callable by the
    /// declaration, from ONE carrier.
    FactFunction,
    /// `name!(…) neck body` — a user directive.
    Effect,
    /// `A(*) &(::ctx) B(*) neck body` — an edge, which names a PAIR and
    /// baptizes nothing.
    Edge,
}

/// What a head position supplies to its output column.
#[derive(Debug, Clone, PartialEq)]
pub enum Supply {
    /// A reference into the body's heading. The body must offer it: a head
    /// never renames, so an absent name is a refusal, not a rename.
    Ref(SqlIdentifier),
    /// A ground term: a constant injected into every row. It supplies a
    /// value and, unlabeled, ABSTAINS from naming the position.
    Ground(LiteralValue),
}

impl Supply {
    /// Whether this position REFERENCES the body rather than supplying a
    /// value of its own.
    pub fn is_reference(&self) -> bool {
        matches!(self, Supply::Ref(_))
    }

    /// The authored spelling, for teaching.
    pub fn spelling(&self) -> String {
        match self {
            Supply::Ref(name) => name.to_string(),
            Supply::Ground(value) => match value {
                LiteralValue::String(s) => format!("\"{s}\""),
                other => other.to_string(),
            },
        }
    }
}

/// One head position: what it supplies, and the name it offers.
///
/// `as` is one uniform rule under every neck — the left side supplies, the
/// right side names. A labeled `Ref` stops offering its own name (the lvar
/// becomes pure plumbing); a labeled `Ground` stops abstaining.
#[derive(Debug, Clone, PartialEq)]
pub struct HeadItem {
    pub supply: Supply,
    pub label: Option<SqlIdentifier>,
}

impl HeadItem {
    pub fn plumb(name: impl Into<SqlIdentifier>) -> Self {
        HeadItem {
            supply: Supply::Ref(name.into()),
            label: None,
        }
    }

    /// The naming OFFER this position makes, if any. An unlabeled ground
    /// term abstains — it is not a name at all.
    pub fn offered_name(&self) -> Option<&SqlIdentifier> {
        match (&self.supply, &self.label) {
            (_, Some(label)) => Some(label),
            (Supply::Ref(name), None) => Some(name),
            (Supply::Ground(_), None) => None,
        }
    }
}

/// The output head. A glob passes the body's heading through, names and
/// order untouched; a listed head is an ordered projection of it.
#[derive(Debug, Clone, PartialEq)]
pub enum HeadItems {
    Glob,
    Listed(Vec<HeadItem>),
}

impl HeadItems {
    pub fn listed(&self) -> Option<&[HeadItem]> {
        match self {
            HeadItems::Glob => None,
            HeadItems::Listed(items) => Some(items),
        }
    }
}

/// A parameter a definition's signature BAPTIZES — the other half of the
/// heads-reference/signatures-baptize line. One typed family: a relation
/// parameter, a scalar binder, or a clause-fixed ground constant.
#[derive(Debug, Clone, PartialEq)]
pub enum HoParam {
    /// `T(*)` or `T(x, y)` — a relation parameter. `cols` is `Glob` for the
    /// structural form and `Listed` for the positionally-typed one.
    Relation {
        name: SqlIdentifier,
        cols: HeadItems,
    },
    /// `x`, `x > 3`, `f:()` — a scalar binder. The guard is a filter the
    /// clause applies to its own argument; `callable` marks the `f:()`
    /// higher-order spelling.
    Scalar {
        name: SqlIdentifier,
        guard: Option<TruthExpression<Unresolved>>,
        callable: bool,
    },
    /// `"value"` or `42` — a ground constant fixing this position in THIS
    /// clause. Sibling clauses may bind the same position freely.
    ///
    /// `text` is the AUTHORED token, and it is the match key: the catalog
    /// selects a clause by comparing these bytes, so a re-spelling of the
    /// same value is a different key. A mention canonicalizes on the way
    /// in, which is why one spelling reaches here.
    Ground { name: SqlIdentifier, text: String },
}

impl HoParam {
    pub fn name(&self) -> &SqlIdentifier {
        match self {
            HoParam::Relation { name, .. }
            | HoParam::Scalar { name, .. }
            | HoParam::Ground { name, .. } => name,
        }
    }

    /// Ground positions carry no binder, so they are absent from the names
    /// a call site may supply.
    pub fn is_ground(&self) -> bool {
        matches!(self, HoParam::Ground { .. })
    }
}

/// One head type for every neck, pure and effect alike. The `!` is the
/// subject's mark, not a second head family.
#[derive(Debug, Clone, PartialEq)]
pub struct Head {
    /// The FIRST paren group of a higher-order head. `None` is not an empty
    /// list: a first-order head has no such group to be empty.
    pub ho_params: Option<Vec<HoParam>>,
    /// How the signature's paren group captures names beyond the ones it
    /// declares. Only a function signature can capture; every other head
    /// carries `None`.
    pub context: ContextMode,
    /// The output head.
    pub items: HeadItems,
}

impl Head {
    pub fn glob() -> Self {
        Head {
            ho_params: None,
            context: ContextMode::None,
            items: HeadItems::Glob,
        }
    }

    pub fn listed(items: Vec<HeadItem>) -> Self {
        Head {
            ho_params: None,
            context: ContextMode::None,
            items: HeadItems::Listed(items),
        }
    }

    /// A head whose output is a glob and whose signature baptizes `params`.
    pub fn signature(params: Vec<HoParam>) -> Self {
        Head {
            ho_params: Some(params),
            context: ContextMode::None,
            items: HeadItems::Glob,
        }
    }

    /// A higher-order head: a signature and an output head.
    pub fn higher_order(params: Vec<HoParam>, items: HeadItems) -> Self {
        Head {
            ho_params: Some(params),
            context: ContextMode::None,
            items,
        }
    }

    pub fn with_context(mut self, context: ContextMode) -> Self {
        self.context = context;
        self
    }

    pub fn is_glob(&self) -> bool {
        matches!(self.items, HeadItems::Glob)
    }

    /// The parameter names a call site may supply, ground positions
    /// excluded.
    pub fn bound_param_names(&self) -> Vec<&SqlIdentifier> {
        self.ho_params
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|p| !p.is_ground())
            .map(|p| p.name())
            .collect()
    }

    /// Every parameter position, ground included — the arity a call site
    /// must match.
    pub fn param_count(&self) -> usize {
        self.ho_params.as_deref().map_or(0, <[HoParam]>::len)
    }
}

// ---------------------------------------------------------------------------
// The assembler — the one door every definition form goes through
// ---------------------------------------------------------------------------

/// What the assembler decided for one subject's clauses.
#[derive(Debug, Clone, PartialEq)]
pub struct HeadAssembly {
    /// The subject's public column names, in output order. `None` for a
    /// glob group: a glob head publishes the body's heading untouched, so
    /// there is no head-declared heading to publish instead.
    pub canonical_names: Option<Vec<SqlIdentifier>>,
}

impl HeadAssembly {
    pub fn glob() -> Self {
        HeadAssembly {
            canonical_names: None,
        }
    }
}

/// WHERE AN OFFER CAME FROM.
///
/// Offers meet at a position from more than one direction: sibling clause
/// heads, and — inside one stacked fact — its header row against a datum's
/// own `as` label. The refusal is one refusal either way, so what varies is
/// only which two directions it must name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offered {
    /// The head of a clause, indexed from zero and taught from one.
    Clause(usize),
    /// A stacked fact's header row.
    Header,
    /// A row of a stacked fact's table, indexed from zero and taught from one.
    Row(usize),
}

impl std::fmt::Display for Offered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Offered::Clause(index) => write!(f, "clause {}", index + 1),
            Offered::Header => write!(f, "the header"),
            Offered::Row(index) => write!(f, "row {}", index + 1),
        }
    }
}

/// An identifier as the author would write it: a stropped name keeps its
/// delimiters, because they are what make it a different name.
fn spelled(name: &SqlIdentifier) -> String {
    match name.is_stropped() {
        true => format!("`{name}`"),
        false => name.to_string(),
    }
}

/// THE ONE HEADING-CONFLICT REFUSAL.
///
/// A position's public name is the unanimous offer of everything that offers
/// one. Two differing offers have no rule for choosing, and choosing anyway
/// is the failure this refuses: a public name that depends on which offer was
/// read first. One URI, one teaching, whichever two directions the offers
/// arrived from.
pub fn name_conflict(
    subject: &str,
    position: usize,
    first: (&SqlIdentifier, Offered),
    second: (&SqlIdentifier, Offered),
) -> DelightQLError {
    let (first_name, first_from) = first;
    let (second_name, second_from) = second;
    // SHOWN AS WRITTEN. `Tag` and Tag are two names and print the same
    // characters; a teaching that spells both bare states a contradiction the
    // author cannot act on.
    let first_name = &spelled(first_name);
    let second_name = &spelled(second_name);
    // A clause offers through its head; a fact's header and its datum offer
    // through spellings the author can simply make one. The conforming advice
    // names whichever of the two the author is actually holding.
    let conform = match (first_from, second_from) {
        (Offered::Clause(_), Offered::Clause(_)) => format!(
            "Conform the differing clause in its head with `{second_name} as {first_name}`, \
             or with a body rename-cover `|> *({second_name} as {first_name})`."
        ),
        _ => format!(
            "A stacked fact's header and its datum's `as` label offer the same position: \
             spell one name — drop the label, or write {first_name} in both places."
        ),
    };
    DelightQLError::validation_error_categorized(
        "ddl/head/name_conflict",
        format!(
            "Entity '{subject}': position {} carries conflicting name offers \
             '{first_name}' ({first_from}) and '{second_name}' ({second_from}). A position's public \
             name must be singular, deterministic, and independent of \
             clause order. {conform}",
            position + 1,
        ),
        "Head name conflict",
    )
}

/// What an all-ground unnamed position receives.
///
/// Fact syntax authenticates its positions: `f(1, 2)` is legal while the
/// ordinary rule `f(*) :- _(1, 2)` refuses under the Ground-Position Naming
/// Rule. The policy is the GROUP's provenance — chosen where every clause is
/// known — not a per-clause exception inside the contest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroundNaming {
    /// The Ground-Position Naming Rule: refuse the unnamed position.
    Refuse,
    /// A fact-only definition where nobody offers: position N receives the
    /// canonical fact name `subject|N|` — the same bytes ordinal addressing
    /// spells, so the published name and the canonical address coincide.
    FactCanonical,
}

/// The one fallible assembler: arity, the per-position name-offer
/// contest, the Ground-Position rule, and output-heading collision run
/// HERE, once, for every neck, before any scope is minted. There is no
/// second place clauses meet, so a silent NULL-padded union and a
/// first-wins heading have nowhere to happen.
///
/// `subject` names the entity for teaching. `heads` are the group's clause
/// heads in authored order.
pub fn assemble(
    subject: &str,
    heads: &[&Head],
    ground_naming: GroundNaming,
) -> Result<HeadAssembly> {
    let Some(first) = heads.first() else {
        return Ok(HeadAssembly::glob());
    };

    // A glob head is open and a listed head is a closed contract. One
    // subject cannot be both.
    let globs = heads.iter().filter(|h| h.is_glob()).count();
    if globs != 0 && globs != heads.len() {
        return Err(DelightQLError::validation_error_categorized(
            "ddl/head/mixed_forms",
            format!(
                "Entity '{subject}': cannot mix glob (*) and argumentative head forms \
                 across clauses. Use all glob or all argumentative."
            ),
            "Head form mismatch",
        ));
    }
    if first.is_glob() {
        return Ok(HeadAssembly::glob());
    }

    let listed: Vec<&[HeadItem]> = heads
        .iter()
        .map(|h| h.items.listed().expect("every head listed"))
        .collect();

    let arity = listed[0].len();
    for (idx, items) in listed.iter().enumerate().skip(1) {
        if items.len() != arity {
            return Err(DelightQLError::validation_error_categorized(
                "ddl/head/arity",
                format!(
                    "Entity '{}': clause {} has {} head item(s) but clause 1 has {}. \
                     All argumentative clauses must have the same arity.",
                    subject,
                    idx + 1,
                    items.len(),
                    arity
                ),
                "Head arity mismatch",
            ));
        }
    }

    // A position's public name is the unanimous OFFER of its clauses. Two
    // DIFFERING offers refuse: a public name must be singular, deterministic,
    // and independent of clause order.
    let mut canonical: Vec<SqlIdentifier> = Vec::with_capacity(arity);
    for pos in 0..arity {
        let mut winner: Option<(&SqlIdentifier, usize)> = None;
        for (clause_idx, items) in listed.iter().enumerate() {
            let Some(offer) = items[pos].offered_name() else {
                continue;
            };
            match winner {
                Some((existing, existing_idx)) if existing != offer => {
                    return Err(name_conflict(
                        subject,
                        pos,
                        (existing, Offered::Clause(existing_idx)),
                        (offer, Offered::Clause(clause_idx)),
                    ));
                }
                Some(_) => {}
                None => winner = Some((offer, clause_idx)),
            }
        }

        // The Ground-Position Naming Rule: a position every clause supplies
        // with ground terms must carry a name. An unnamed position is the
        // only state a public name can silently spring from — a later lvar
        // clause would rename it with no warning. Fact syntax authenticates
        // its positions, so a fact-only group receives the canonical fact
        // name instead of the refusal.
        let Some((name, _)) = winner else {
            if ground_naming == GroundNaming::FactCanonical {
                canonical.push(SqlIdentifier::new(format!("{subject}|{}|", pos + 1)));
                continue;
            }
            let n = heads.len();
            return Err(DelightQLError::validation_error_categorized(
                "ddl/head/unnamed_ground_position",
                format!(
                    "Entity '{}': head position {} is supplied only by ground terms — \
                     every one of its {} {} abstains from naming it (no lvar, no \
                     `as`-label). A position supplied only by ground terms must carry a \
                     name — every clause abstained (the Ground-Position Naming Rule, \
                     clause-head-catechism.md §II). Name it in the head, e.g. `{} as tag`: \
                     the literal still supplies, the label only names the position. Why \
                     loud: an unnamed position is the only state from which a public name \
                     can silently spring into existence — a later lvar clause would rename \
                     it with no warning; naming it now makes that a caught contest instead \
                     of a 3am surprise in someone's jq pipeline.",
                    subject,
                    pos + 1,
                    n,
                    if n == 1 { "clause" } else { "clauses" },
                    listed[0][pos].supply.spelling(),
                ),
                "Unnamed ground position",
            ));
        };
        canonical.push(name.clone());
    }

    // Two positions cannot publish one name. The heading a subject offers
    // is what everything downstream addresses, so a collision here is
    // decided at the head rather than discovered inside a projection.
    for (pos, name) in canonical.iter().enumerate() {
        if let Some(earlier) = canonical[..pos].iter().position(|seen| seen == name) {
            return Err(DelightQLError::validation_error_categorized(
                "ddl/head/name_collision",
                format!(
                    "Entity '{}': head positions {} and {} both publish the name '{}'. \
                     A heading names each column once. Name one of them differently in \
                     the head, e.g. `{} as {}_2`.",
                    subject,
                    earlier + 1,
                    pos + 1,
                    name,
                    name,
                    name,
                ),
                "Head name collision",
            ));
        }
    }

    Ok(HeadAssembly {
        canonical_names: Some(canonical),
    })
}

/// A clause the desugar law can be applied to. The law needs exactly this
/// much of a clause — its head, and a way to push that head's projection
/// onto whatever body shape the clause carries — so the consulted `:-`/`:=`
/// road and the local `:` road share one implementation instead of two
/// loops that agree by inspection.
pub trait HeadedClause: Sized {
    fn head(&self) -> &Head;
    /// Push the head's projection onto the body and leave a glob behind:
    /// the contract has been applied, so the head is spent.
    fn spend_head(self, items: &[HeadItem], canonical_names: &[SqlIdentifier]) -> Self;

    /// Whether this body can answer to a NAME at all.
    ///
    /// A head is a reference into its body, so a body that
    /// names nothing cannot be referenced by name — an anonymous table
    /// written without a header publishes unpinnable mints, and no head
    /// item can reach one. Bodies that do publish names answer this true
    /// and are checked the ordinary way, by resolution. See
    /// [`chain_publishes_names`] for what a continuation does to the answer.
    fn body_publishes_names(&self) -> bool;
}

/// Whether a chain's output answers to a NAME at all.
///
/// One body publishes nothing nameable: the headerless anonymous table.
/// `_(2, 3)` writes rows whose columns are unpinnable mints, so no
/// reference reaches one. Every other head answers, and so does any chain
/// whose continuations put an authored heading in front of one.
///
/// The chain is folded, because a continuation is a function on the
/// heading and only some of those functions introduce a name. A σ, an
/// ordering, a row bound, a reposition — these choose or move columns that
/// already exist; putting one after `_(2, 3)` leaves its columns exactly as
/// unreachable as they were.
///
/// The classification below leans toward TRUE where an operator's answer is
/// not obvious, and the asymmetry is deliberate: a false yes costs a
/// less-pointed refusal from resolution, while a false no refuses a query
/// the language allows.
#[stacksafe::stacksafe]
pub fn chain_publishes_names<P: Phase>(chain: &Chain<P>) -> bool {
    let mut publishes = match &chain.head {
        Grelex::Literal(table) => table.table.body.header.is_some(),
        Grelex::Reference(_) => true,
    };
    for continuation in &chain.continuations {
        publishes = match continuation {
            // An access asks a relation for dimensions it already has. It
            // narrows, activates, or merges a heading; it cannot put a name
            // where the operand had none.
            Continuation::Access { .. } => publishes,
            // Codd's σ: it chooses rows. Every column it hands on is a
            // column it received, under the name it received it with.
            // A correlation names two arms; like σ it publishes nothing new.
            Continuation::Restrict { .. }
            | Continuation::Bound { .. }
            | Continuation::Correlate { .. } => publishes,
            // A destructure ADDS columns, and it names every one of them.
            Continuation::Destructure { .. } => true,
            // A join publishes both operands' columns, so a named right
            // operand names part of the result whatever the left one did.
            Continuation::Member { rhs, .. } => publishes || chain_publishes_names(rhs),
            // The arms are union-compatible and the left one names the
            // result; a nameless left arm makes a nameless union.
            Continuation::BagOp { .. } => publishes,
            // An edge expands into ordinary members over catalog relations.
            Continuation::ErJoin(_) => true,
            Continuation::Pipe { operator, .. } => publishes || operator_publishes_names(operator),
            Continuation::Structural(step) => match &step.form {
                // Ordering and reposition permute a heading they were given;
                // neither can put a name where there was none.
                StructuralForm::Ordering { .. } | StructuralForm::Reposition { .. } => publishes,
                // Fixed headings of their own — (scope, column_name, ordinal)
                // and `met` — and the narrowing's and drill's named fields.
                StructuralForm::Meta
                | StructuralForm::Witness { .. }
                | StructuralForm::SignedWitness
                | StructuralForm::Drill { .. }
                | StructuralForm::Narrow { .. } => true,
            },
        };
    }
    publishes
}

/// Whether a pipe operator's output answers to names of its own, whatever
/// its operand answered to.
///
/// Exhaustive on purpose — no wildcard arm. An operator added to the
/// language has to say which side it falls on, because the alternative is
/// inheriting an answer nobody chose.
fn operator_publishes_names<P: Phase>(operator: &PipeOp<P>) -> bool {
    use PipeOp as Op;
    match operator {
        // These build a heading out of what is written in them.
        Op::Project(_)
        | Op::Embed(_)
        | Op::Rename(_)
        | Op::Transform { .. }
        | Op::EmbedMapCover(_) => true,
        // A reduction names its output; bare `%(…)` is DISTINCT, which
        // publishes the columns it was handed.
        Op::Group(spec) => match spec {
            GroupSpec::Distinct { .. } => false,
            // A delegate SELECTS from a representative row rather than
            // naming a fresh output, so only the other members publish.
            GroupSpec::Reduce { reductions, .. } => reductions
                .iter()
                .any(|item| !matches!(item, super::ReductionItem::Delegate(_))),
        },
        // Each of these permutes, narrows, or annotates a heading it was
        // given. None of them can put a name where there was none.
        Op::MapCover { .. } | Op::ProjectOut(_) => false,
    }
}

/// Spend a group's heads (the one desugar law), given what the assembler
/// decided. A glob group has no declared heading to enforce and comes back
/// untouched.
pub fn spend_heads<C: HeadedClause>(
    clauses: Vec<C>,
    assembly: &HeadAssembly,
    subject: &str,
) -> Result<Vec<C>> {
    let Some(canonical_names) = assembly.canonical_names.as_deref() else {
        return Ok(clauses);
    };
    clauses
        .into_iter()
        .map(
            |clause| match clause.head().items.listed().map(<[HeadItem]>::to_vec) {
                Some(items) => {
                    // The reference-only law, refused where both facts are
                    // in hand. Left to resolution it surfaced as a bare
                    // "Column not found: x" wrapped in a runtime error,
                    // which names neither the head nor the remedy.
                    if !clause.body_publishes_names()
                        && items.iter().any(|item| item.supply.is_reference())
                    {
                        return Err(unreachable_head_reference(subject, &items));
                    }
                    Ok(clause.spend_head(&items, canonical_names))
                }
                None => Ok(clause),
            },
        )
        .collect()
}

/// The reference-only law's teaching, with the `@`-header remedy it asks for.
fn unreachable_head_reference(subject: &str, items: &[HeadItem]) -> DelightQLError {
    let named: Vec<String> = items
        .iter()
        .filter_map(|item| match &item.supply {
            Supply::Ref(name) => Some(name.to_string()),
            Supply::Ground(_) => None,
        })
        .collect();
    DelightQLError::validation_error_categorized(
        "ddl/head/unresolved_reference",
        format!(
            "Entity '{subject}': the head names {names} but its body answers to no \
             name — an anonymous table written without a header publishes columns \
             nothing can reach. A head is a REFERENCE into its body, never a binder. \
             Give the table a header, which baptizes the names the head then \
             references: `_({remedy} @ …)`.",
            names = named
                .iter()
                .map(|name| format!("`{name}`"))
                .collect::<Vec<_>>()
                .join(", "),
            remedy = named.join(", ")
        ),
        "clause heads are reference-only",
    )
}

// ---------------------------------------------------------------------------
// The desugar law — one projection, every neck
// ---------------------------------------------------------------------------

/// The one desugar law: a listed head is an ordered projection of its
/// body's heading, so a clause under ANY neck compiles as `body |> (…)`.
/// Refusals, ordering, and hiding are then the projection machinery's,
/// identical by construction — the assembler carries no per-neck exception.
///
/// Each item aliases its SUPPLY to the position's canonical name. The
/// `as`-label never appears here: it did its work in the assembler by
/// supplying the offer that BECAME the canonical name.
/// A head item republished under the canonical name its declaration fixed.
fn named_out(
    expr: DomainExpression<Unresolved>,
    naming: delightql_types::SqlIdentifier,
) -> crate::pipeline::asts::core::OutItem<Unresolved> {
    crate::pipeline::asts::core::OutItem::One(crate::pipeline::asts::core::OneOut {
        expr: crate::pipeline::asts::core::OutValue::Domain(expr),
        naming: Some(naming),
        output: (),
    })
}

pub fn project_body_through_head(
    body: Chain<Unresolved>,
    items: &[HeadItem],
    canonical_names: &[SqlIdentifier],
) -> Chain<Unresolved> {
    let items: Vec<crate::pipeline::asts::core::OutItem<Unresolved>> = items
        .iter()
        .zip(canonical_names.iter())
        .map(|(item, canonical)| match &item.supply {
            Supply::Ref(name) if name == canonical => crate::pipeline::asts::core::OutItem::plain(
                DomainExpression::lvar_builder(name.clone()).build(),
                (),
            ),
            Supply::Ref(name) => named_out(
                DomainExpression::lvar_builder(name.clone()).build(),
                canonical.clone().into(),
            ),
            Supply::Ground(value) => named_out(
                DomainExpression::<Unresolved>::literal_builder(value.clone()).build(),
                canonical.as_str().into(),
            ),
        })
        .collect();

    // A head is nonempty by grammar; an itemless projection would refuse at
    // resolution, so the empty case answers with the body unchanged.
    let Some(items) = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(items) else {
        return body;
    };
    Chain::pipe_builder(body, ()).with_projection(items).build()
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

impl crate::lispy::ToLispy for Head {
    fn to_lispy(&self) -> String {
        match &self.ho_params {
            Some(params) => {
                let params: Vec<String> = params.iter().map(HoParam::to_lispy).collect();
                format!(
                    "(head (params {}) {})",
                    params.join(" "),
                    self.items.render()
                )
            }
            None => format!("(head {})", self.items.render()),
        }
    }
}

impl HeadItems {
    fn render(&self) -> String {
        match self {
            HeadItems::Glob => "(glob)".to_string(),
            HeadItems::Listed(items) => {
                let items: Vec<String> = items.iter().map(HeadItem::render).collect();
                format!("(items {})", items.join(" "))
            }
        }
    }
}

impl HeadItem {
    fn render(&self) -> String {
        match &self.label {
            Some(label) => format!("(item {} as \"{}\")", self.supply.render(), label),
            None => format!("(item {})", self.supply.render()),
        }
    }
}

impl Supply {
    fn render(&self) -> String {
        match self {
            Supply::Ref(name) => format!("(ref \"{name}\")"),
            Supply::Ground(_) => format!("(ground \"{}\")", self.spelling()),
        }
    }
}

impl HoParam {
    fn to_lispy(&self) -> String {
        match self {
            HoParam::Relation { name, cols } => {
                format!("(relation \"{name}\" {})", cols.render())
            }
            HoParam::Scalar { name, guard, .. } => match guard {
                Some(_) => format!("(scalar \"{name}\" guarded)"),
                None => format!("(scalar \"{name}\")"),
            },
            HoParam::Ground { text, .. } => format!("(ground \"{text}\")"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ground(text: &str) -> Supply {
        Supply::Ground(LiteralValue::String(text.to_string()))
    }

    fn item(supply: Supply, label: Option<&str>) -> HeadItem {
        HeadItem {
            supply,
            label: label.map(SqlIdentifier::new),
        }
    }

    fn listed(items: Vec<HeadItem>) -> Head {
        Head::listed(items)
    }

    fn names(assembly: &HeadAssembly) -> Vec<String> {
        assembly
            .canonical_names
            .as_ref()
            .expect("listed group")
            .iter()
            .map(|n| n.to_string())
            .collect()
    }

    #[test]
    fn a_glob_group_declares_no_heading() {
        let head = Head::glob();
        let assembly = assemble("g", &[&head, &head], GroundNaming::Refuse).expect("glob group assembles");
        assert_eq!(assembly.canonical_names, None);
    }

    #[test]
    fn mixing_glob_and_listed_heads_refuses() {
        let glob = Head::glob();
        let one = listed(vec![HeadItem::plumb("a")]);
        let err = assemble("m", &[&glob, &one], GroundNaming::Refuse).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/mixed_forms"
        );
    }

    #[test]
    fn clause_arity_must_agree() {
        let one = listed(vec![HeadItem::plumb("a")]);
        let two = listed(vec![HeadItem::plumb("a"), HeadItem::plumb("b")]);
        let err = assemble("p", &[&one, &two], GroundNaming::Refuse).unwrap_err();
        assert_eq!(err.error_uri(), "delightql-error://semantic/ddl/head/arity");
    }

    #[test]
    fn differing_offers_at_one_position_refuse() {
        let left = listed(vec![HeadItem::plumb("id")]);
        let right = listed(vec![HeadItem::plumb("age")]);
        let err = assemble("q", &[&left, &right], GroundNaming::Refuse).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/name_conflict"
        );
    }

    #[test]
    fn a_label_conforms_a_differing_clause() {
        let left = listed(vec![HeadItem::plumb("id")]);
        let right = listed(vec![item(
            Supply::Ref(SqlIdentifier::new("age")),
            Some("id"),
        )]);
        let assembly = assemble("q", &[&left, &right], GroundNaming::Refuse).expect("the label conforms");
        assert_eq!(names(&assembly), vec!["id"]);
    }

    #[test]
    fn a_position_every_clause_grounds_must_be_named() {
        let head = listed(vec![item(ground("VIP"), None), HeadItem::plumb("id")]);
        let err = assemble("b", &[&head], GroundNaming::Refuse).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/unnamed_ground_position"
        );
    }

    #[test]
    fn a_labelled_ground_position_publishes() {
        let head = listed(vec![
            item(ground("VIP"), Some("tag")),
            HeadItem::plumb("id"),
        ]);
        let assembly = assemble("b", &[&head], GroundNaming::Refuse).expect("the label names the position");
        assert_eq!(names(&assembly), vec!["tag", "id"]);
    }

    #[test]
    fn one_naming_clause_carries_a_ground_sibling() {
        // Position 1 is ground in clause 1 and an lvar in clause 2: the
        // lvar's offer names it, and clause 1's literal still supplies.
        let grounded = listed(vec![item(ground("VIP"), None)]);
        let named = listed(vec![HeadItem::plumb("tag")]);
        let assembly = assemble("b", &[&grounded, &named], GroundNaming::Refuse).expect("one offer suffices");
        assert_eq!(names(&assembly), vec!["tag"]);
    }

    #[test]
    fn a_label_beats_a_sibling_clauses_abstention() {
        // `"x" as tag` offers; bare `"y"` abstains. The offer names it.
        let labelled = listed(vec![
            item(ground("x"), Some("tag")),
            HeadItem::plumb("last"),
        ]);
        let bare = listed(vec![item(ground("y"), None), HeadItem::plumb("last")]);
        let assembly = assemble("t", &[&labelled, &bare], GroundNaming::Refuse).expect("one offer suffices");
        assert_eq!(names(&assembly), vec!["tag", "last"]);
    }

    #[test]
    fn a_label_may_agree_with_a_sibling_lvar() {
        // Clause 1 plumbs `country`; clause 2 labels a literal `country`.
        // Unanimous, so no contest.
        let plumbed = listed(vec![HeadItem::plumb("country")]);
        let laundered = listed(vec![item(ground("x"), Some("country"))]);
        let assembly = assemble("c", &[&plumbed, &laundered], GroundNaming::Refuse).expect("agreement is not conflict");
        assert_eq!(names(&assembly), vec!["country"]);
    }

    #[test]
    fn two_positions_may_not_publish_one_name() {
        let head = listed(vec![HeadItem::plumb("id"), HeadItem::plumb("id")]);
        let err = assemble("c", &[&head], GroundNaming::Refuse).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/name_collision"
        );
    }

    #[test]
    fn a_label_can_collide_too() {
        let head = listed(vec![
            HeadItem::plumb("id"),
            item(Supply::Ref(SqlIdentifier::new("other")), Some("id")),
        ]);
        let err = assemble("c", &[&head], GroundNaming::Refuse).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/name_collision"
        );
    }

    #[test]
    fn an_unlabelled_ref_offers_its_own_name() {
        assert_eq!(
            HeadItem::plumb("a").offered_name().map(|n| n.to_string()),
            Some("a".to_string())
        );
    }

    #[test]
    fn a_label_replaces_the_refs_own_offer() {
        let labelled = item(Supply::Ref(SqlIdentifier::new("nation")), Some("country"));
        assert_eq!(
            labelled.offered_name().map(|n| n.to_string()),
            Some("country".to_string())
        );
    }

    #[test]
    fn an_unlabelled_ground_abstains() {
        assert!(item(ground("VIP"), None).offered_name().is_none());
    }
}
