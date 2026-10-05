// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! AN EDGE BODY IS SIMPLE (`shape`): two endpoint reads, at least one
//! connection, filters, nothing else — judged on the text when the edge is
//! declared.

pub(in crate::defuse) mod shape;


use crate::diagnostic::Er;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use delightql_types::SqlIdentifier;

/// THE OMITTED CONTEXT. A bare `&`, `&&`, or edge declaration means this
/// context and no other: the normalizer writes it where the symbol was
/// omitted, so a bare and an explicitly normal spelling are one identity
/// before selection ever sees them. Omission never searches other
/// contexts.
pub(crate) const DEFAULT_CONTEXT: &str = "normal";

/// One TERM of an ER path: the canonical spelling that selects an edge
/// rule, and the READ that spelling denotes — the one read a body lends
/// when it spells the term, and the relation whose name the term's
/// endpoint answers to. Both are derived from the spelling; nothing
/// supplies a table beside it.
#[derive(Debug, Clone)]
pub(in crate::defuse) struct ErTerm {
    spelling: String,
    read: ast_unresolved::Chain,
}

impl ErTerm {
    /// The term a canonical spelling denotes. The spelling is the one
    /// canonicalizer's output — a catalog row or a normalized operand — so
    /// it parses as a single relation-access read; anything else is not a
    /// term.
    pub(in crate::defuse) fn spelled(spelling: &str) -> Result<ErTerm> {
        let read = term_read(spelling).ok_or_else(|| {
            DelightQLError::from(Er::OperandTerm {
                message: format!(
                    "'{spelling}' is not a relation-access term — an edge term is \
                     a single read such as people(*) or people(, age >= 18), with \
                     the alias and the outer mark outside it"
                ),
            })
        })?;
        Ok(ErTerm {
            spelling: spelling.to_string(),
            read,
        })
    }

    pub(in crate::defuse) fn spelling(&self) -> &str {
        &self.spelling
    }

    /// The relation the term reads, as written: the name its endpoint
    /// answers to, under the namespace path the term spelled.
    pub(in crate::defuse) fn identifier(&self) -> &ast_unresolved::QualifiedName {
        match self.read.as_read_relation() {
            Some(ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named { identifier, .. },
                ..
            })
            | Some(ast_unresolved::Relation::InnerRelation {
                pattern: ast_unresolved::InnerRelationPattern::Indeterminate { identifier, .. },
                ..
            }) => identifier,
            _ => unreachable!("a term's read names its relation by construction"),
        }
    }

    /// The relation's name: what the term's endpoint answers to.
    pub(in crate::defuse) fn name(&self) -> &SqlIdentifier {
        &self.identifier().name
    }

    /// The read the spelling denotes, for the composition's judgment.
    pub(in crate::defuse) fn read(&self) -> &ast_unresolved::Chain {
        &self.read
    }
}

/// Normalize a canonical spelling back into the read it denotes: one
/// named relation access standing alone, unaliased and unmarked — a bare
/// ground read (`people(*)`, `ns.people(*)`), or the shaped read a slot row
/// with a constraint normalizes to (`people(, age >= 18)`), which names its
/// relation the same way. Every term the canonicalizer admits is admitted
/// here; this reads the term back, it does not judge it again.
fn term_read(spelling: &str) -> Option<ast_unresolved::Chain> {
    let tree = crate::pipeline::parse::query_sequence(spelling).ok()?;
    let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let normalized = crate::pipeline::normalize::query_sequence(&tree, registry).ok()?;
    let mut queries = normalized.into_queries();
    if queries.len() != 1 {
        return None;
    }
    let body = queries
        .remove(0)
        .into_query()
        .into_bare_body()
        .ok()?;
    is_bare_read(&body).then_some(body)
}

/// Whether a chain is one named relation read standing alone: unaliased,
/// unmarked, neither a mutation target nor a passthrough. What a term's
/// read is, and what a body's read of a term must be before its spelling
/// is compared — the alias and the mark stand outside the read's bytes.
pub(super) fn is_bare_read(body: &ast_unresolved::Chain) -> bool {
    matches!(
        body.as_read_relation(),
        Some(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named {
                alias: None,
                mutation_target: false,
                passthrough: false,
                ..
            },
        }) | Some(ast_unresolved::Relation::InnerRelation {
            pattern: ast_unresolved::InnerRelationPattern::Indeterminate { .. },
            alias: None,
        })
    )
}

/// THE DECLARATION'S JUDGMENT: an edge body outside the simple shape
/// refuses when it is declared, naming the offending part. The
/// normalizer's edge arm asks this of every body it admits.
pub(crate) fn judge_declared_body(
    body: &ast_unresolved::Query,
    reads: &crate::term_spec::BodyReads,
    left_spelling: &str,
    right_spelling: &str,
    context: &str,
) -> Result<()> {
    let left = ErTerm::spelled(left_spelling)?;
    let right = ErTerm::spelled(right_spelling)?;
    let pair = format!("{left_spelling} & {right_spelling} in '::{context}'");
    shape::judge(body, reads, &left, &right, &pair).map(|_| ())
}

