// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What a spread in a selector addresses, and the name a name template
//! gives one column: each decided once against the heading of the stage
//! input the selector reads.

use super::{Heading, Name};
use crate::pipeline::middle::core::refuse::{self, Refusal};

/// THE COLUMN REGEX (domain-expressions-grammar FN.43): the positions whose
/// published name the pattern matches as a substring, without regard to
/// case unless `exact`. A position no name answers to is matched by
/// nothing.
pub(crate) fn regex(heading: &Heading, pattern: &str, exact: bool) -> Result<Vec<usize>, Refusal> {
    let matcher = column_regex(pattern, exact)?;
    Ok(heading
        .positions()
        .iter()
        .enumerate()
        .filter(|(_, p)| p.answering_name().is_some_and(|n| matcher.is_match(n.as_str())))
        .map(|(i, _)| i)
        .collect())
}

/// The names lost by collision in `heading` that a column regex would
/// match were they published: they answer to nothing, so the regex
/// addresses none of their positions.
pub(crate) fn regex_lost(heading: &Heading, pattern: &str, exact: bool) -> Result<Vec<Name>, Refusal> {
    let matcher = column_regex(pattern, exact)?;
    let mut lost: Vec<Name> = Vec::new();
    for p in heading.positions() {
        if let super::NameState::Lost(name) = &p.name {
            if matcher.is_match(name.as_str()) && !lost.contains(name) {
                lost.push(name.clone());
            }
        }
    }
    Ok(lost)
}

fn column_regex(pattern: &str, exact: bool) -> Result<regex::Regex, Refusal> {
    regex::RegexBuilder::new(pattern)
        .case_insensitive(!exact)
        .build()
        .map_err(|e| refuse::column(&format!("/{pattern}/"), &format!("the column regex does not compile: {e}")))
}

/// A positional span (`|a:b|`): the displayed positions from its start to
/// its end, both included, each counted from the first position or, when
/// reversed, from the last; an open end runs to that end of the heading.
pub(crate) fn span(heading: &Heading, start: Option<(u16, bool)>, end: Option<(u16, bool)>) -> Result<Vec<usize>, Refusal> {
    let displayed: Vec<usize> = heading.displayed().map(|(i, _)| i).collect();
    let len = displayed.len();
    let at = |bound: (u16, bool)| -> Option<usize> {
        let p = usize::from(bound.0);
        if p == 0 || p > len {
            return None;
        }
        Some(if bound.1 { len - p } else { p - 1 })
    };
    let spelled = || {
        let side = |b: Option<(u16, bool)>| match b {
            Some((p, true)) => format!("-{p}"),
            Some((p, false)) => p.to_string(),
            None => String::new(),
        };
        format!("|{}:{}|", side(start), side(end))
    };
    let first = match start {
        Some(bound) => at(bound).ok_or_else(|| refuse::column(&spelled(), "past the displayed heading"))?,
        None => 0,
    };
    let last = match end {
        Some(bound) => at(bound).ok_or_else(|| refuse::column(&spelled(), "past the displayed heading"))?,
        None => len.checked_sub(1).ok_or_else(|| refuse::column(&spelled(), "past the displayed heading"))?,
    };
    if first > last {
        return Err(refuse::column(&spelled(), "the span ends before it starts"));
    }
    Ok(displayed[first..=last].to_vec())
}

/// THE REPOSITION (pipe-operators `reposition`; final positions): over
/// `width` published positions, each move takes its source position to its
/// target (1-based; a negative target counts from the end), and the
/// positions no move names fill the remaining places in their order. The
/// answer is the source position of each place. Two moves to one place, one
/// source moved twice, and a target past the width have no reading the law
/// states.
pub(crate) fn reposition(width: usize, moves: &[(usize, i32)]) -> Result<Vec<usize>, Refusal> {
    let unruled = || {
        refuse::unruled(
            "where a reposition puts columns whose moves conflict (two to one place, one moved twice, a place past \
             the width)",
        )
    };
    let mut placed: Vec<Option<usize>> = vec![None; width];
    let mut moved: Vec<usize> = Vec::with_capacity(moves.len());
    for (source, target) in moves {
        let w = width as i64;
        let t = i64::from(*target);
        let at = match t {
            1.. if t <= w => (t - 1) as usize,
            ..=-1 if -t <= w => (w + t) as usize,
            _ => return Err(unruled()),
        };
        if placed[at].is_some() || moved.contains(source) {
            return Err(unruled());
        }
        placed[at] = Some(*source);
        moved.push(*source);
    }
    let mut rest = (0..width).filter(|p| !moved.contains(p));
    placed
        .into_iter()
        .map(|slot| slot.or_else(|| rest.next()).ok_or_else(|| refuse::contract("a reposition place no column fills")))
        .collect()
}

/// A name template's name for one column (pipe-operators-grammar FN.26):
/// `{@}` stands for the column's published name and `{#}` for its 1-based
/// displayed position, the number positional addressing uses. A template
/// must vary with its column, so one with no placeholder refuses. A `{@}`
/// over a column no name answers to has no reading the law states.
pub(crate) fn template_name(template: &str, column: Option<&Name>, position: usize) -> Result<Name, Refusal> {
    if !template.contains("{@}") && !template.contains("{#}") {
        return Err(refuse::template_without_placeholder(template));
    }
    let named = if template.contains("{@}") {
        let Some(column) = column else {
            return Err(refuse::unruled("what `{@}` spells for a column whose name is minted or lost"));
        };
        template.replace("{@}", column.as_str())
    } else {
        template.to_string()
    };
    Ok(Name::new(named.replace("{#}", &position.to_string())))
}
