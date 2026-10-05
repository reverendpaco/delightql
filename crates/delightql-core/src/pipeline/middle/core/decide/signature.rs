// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A family's signature: the facts its clauses must state together
//! (heads-law CLAUSE AGREEMENT; top-grammar FN.48;
//! recursion-contract-law THE BADGE CHOOSES THE UNION).
//!
//! A family is one entity, so its parameter row, its context capture and
//! its fixpoint badge are facts about the entity that every clause states.
//! Each fact is read off all clauses at once: it holds when they state it
//! unanimously; otherwise the family refuses, and the refusal reports every
//! distinct statement with the clauses that make it. No clause is the
//! reference the others are measured against, so permuting the clauses
//! never changes whether, or under which identity, the family refuses.
//!
//! What stays clause-local is not stated here: scalar formal names, a
//! relation formal's name and its positional column pattern, a ground
//! member's value, and a guard.

use crate::pipeline::middle::core::decide::rule;
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::refuse::{self, Refusal};

/// What a clause's parameter position receives.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Receives {
    /// One value per call: a scalar binder, or a ground member, which is
    /// the same inbound value compared against the clause's constant.
    Value,
    /// A function (`g:()`).
    Function,
    /// A relation (`T(*)`, `T(a, b)`).
    Relation,
    /// A closed residual rule value under this contract: its remaining
    /// positions in order and the heading its completion publishes.
    Rule(rule::Signature),
}

/// Where a value function's free names come from.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Capture {
    Nothing,
    /// `..`: free names read the caller's row.
    CallerRow,
    /// `..{a, b}`: these caller columns, in this order (a positional call
    /// supplies them first).
    Declared(Vec<Name>),
}

/// What one clause's head states about its family's signature.
#[derive(Clone, Debug)]
pub(crate) struct ClauseSignature {
    /// The parameter row; `None` when the head has no parameter group,
    /// which is not the same statement as an empty group.
    pub(crate) parameters: Option<Vec<Receives>>,
    pub(crate) capture: Capture,
    /// Whether the head wears the fixpoint badge `%`.
    pub(crate) badged: bool,
}

/// The family's one signature, or the refusal of the law its clauses break.
///
/// The parameter row is the call interface, so it is judged before the
/// badge: first its length, then each argument position from the left,
/// then the capture. A position at which any clause states a rule-valued
/// contract is judged by the rule-value law (`rule_contract`), whatever
/// the other clauses state there; any other disagreement about the row is
/// `param_arity`.
pub(crate) fn judge(subject: &str, clauses: &[ClauseSignature]) -> Result<(), Refusal> {
    let row_differs = |what: &str, statements: String| {
        refuse::parameter_rows_differ(subject, &format!("its clauses {what} ({statements})"))
    };
    let width = unanimous(clauses.iter().map(|c| c.parameters.as_ref().map(Vec::len)))
        .map_err(|stated| row_differs("declare rows of different lengths", each(&stated, width_of)))?
        .flatten();
    if let Some(width) = width {
        // Every clause states a row of this width.
        let rows: Vec<&[Receives]> = clauses.iter().filter_map(|c| c.parameters.as_deref()).collect();
        for position in 1..=width {
            unanimous(rows.iter().map(|row| &row[position - 1])).map_err(|stated| {
                let statements = each(&stated, |r| receives_of(r));
                match stated.iter().any(|(r, _)| matches!(r, Receives::Rule(_))) {
                    true => refuse::rule_contracts_differ(subject, position, &statements),
                    false => row_differs(&format!("receive different arguments at position {position}"), statements),
                }
            })?;
        }
    }
    unanimous(clauses.iter().map(|c| &c.capture))
        .map_err(|stated| row_differs("capture different context", each(&stated, |c| capture_of(c))))?;
    unanimous(clauses.iter().map(|c| c.badged))
        .map_err(|stated| refuse::badges_differ(subject, &each(&stated, |badged| badge_of(*badged))))?;
    Ok(())
}

/// The one statement every clause makes of a fact, or each distinct
/// statement with the clauses (numbered from 1) that make it, in order of
/// first appearance.
fn unanimous<T: PartialEq>(stated: impl IntoIterator<Item = T>) -> Result<Option<T>, Vec<(T, Vec<usize>)>> {
    let mut statements: Vec<(T, Vec<usize>)> = Vec::new();
    for (index, statement) in stated.into_iter().enumerate() {
        match statements.iter_mut().find(|(s, _)| *s == statement) {
            Some((_, clauses)) => clauses.push(index + 1),
            None => statements.push((statement, vec![index + 1])),
        }
    }
    if statements.len() > 1 {
        return Err(statements);
    }
    Ok(statements.pop().map(|(statement, _)| statement))
}

/// "clause 1: a value; clauses 2 and 3: a relation".
fn each<T>(stated: &[(T, Vec<usize>)], spell: impl Fn(&T) -> String) -> String {
    let parts: Vec<String> =
        stated.iter().map(|(statement, clauses)| format!("{}: {}", clauses_of(clauses), spell(statement))).collect();
    parts.join("; ")
}

fn clauses_of(clauses: &[usize]) -> String {
    let numbers: Vec<String> = clauses.iter().map(usize::to_string).collect();
    match numbers.len() {
        1 => format!("clause {}", numbers[0]),
        _ => format!("clauses {}", listing(&numbers)),
    }
}

fn listing(parts: &[String]) -> String {
    match parts.split_last() {
        None => String::new(),
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
    }
}

fn width_of(width: &Option<usize>) -> String {
    match *width {
        None => "no parameter row".to_string(),
        Some(1) => "1 parameter".to_string(),
        Some(n) => format!("{n} parameters"),
    }
}

fn receives_of(receives: &Receives) -> String {
    match receives {
        Receives::Value => "a value".to_string(),
        Receives::Function => "a function".to_string(),
        Receives::Relation => "a relation".to_string(),
        Receives::Rule(contract) => format!("a rule value `{}`", contract_of(contract)),
    }
}

fn contract_of(contract: &rule::Signature) -> String {
    let remaining: Vec<String> = contract
        .modes
        .iter()
        .map(|mode| match mode {
            rule::Mode::Scalar => "value".to_string(),
            rule::Mode::Relation(None) => "relation(*)".to_string(),
            rule::Mode::Relation(Some(names)) => format!("relation({})", names_of(names)),
            rule::Mode::Rule => "rule".to_string(),
        })
        .collect();
    let output = match &contract.output {
        None => "*".to_string(),
        Some(names) => names_of(names),
    };
    format!("(... {})({output})", remaining.join(", "))
}

fn badge_of(badged: bool) -> String {
    match badged {
        true => "`%`".to_string(),
        false => "unbadged".to_string(),
    }
}

fn capture_of(capture: &Capture) -> String {
    match capture {
        Capture::Nothing => "none".to_string(),
        Capture::CallerRow => "the caller's row `..`".to_string(),
        Capture::Declared(names) => format!("`..{{{}}}`", names_of(names)),
    }
}

fn names_of(names: &[Name]) -> String {
    names.iter().map(Name::as_str).collect::<Vec<_>>().join(", ")
}
