// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE SUBSTITUTION LAW — spent here, for every composition family, and
//! nowhere later.
//!
//! ONE LAW, TWO CARRIERS. `pipe-operators-grammar.md` states it once: an
//! authored argument row binds a complete LEFT PREFIX of the callee's mode,
//! and the pipe supplies the one formal still remaining — the FINAL one.
//! The relational pipe carries a relation, the function pipe a value, the
//! effect pipe a relation into a directive; the landing is the same act in
//! all three, so both carriers spend it here and neither level can choose a
//! direction of its own.
//!
//! A form with an argument row that wrote no hole takes that default. A form
//! with no argument row (a lambda, an open string) must write the hole:
//! nothing else can receive the value, so a hole-less one discards it and
//! refuses toward the constant.
//!
//! THE SLOT IS ONE. FN.4 and FN.34: the bare hole lands once, and a value
//! that stands at more than one place takes the named binder
//! (`:(|x| x - avg:(x))`). A second bare hole would spell the flowing value
//! twice under a glyph that names nothing, so it refuses toward the binder —
//! which now exists, so the refusal names a spelling the author can move to.
//!
//! After this the pipe is GONE. A piped call and a directly-written call are
//! the same nested application, so nothing downstream branches on how the
//! call arrived.

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::{
    transform_standard_application, walk_transform_domain, AstTransform,
};
use crate::pipeline::ast_visit::{
    walk_visit_domain, walk_visit_standard_application, AstVisit, Descent,
};
use crate::pipeline::asts::core::operators::HoArgument;
use crate::pipeline::asts::core::{
    Callable, DomainExpression, FunctionApplication, StandardApplication, Unresolved,
};

type Domex = DomainExpression<Unresolved>;

/// THE DEFAULT LANDING, over any argument row: the row's FINAL place.
///
/// This is the whole of the default. There is no direction to select, no
/// per-family layout to look up and no caller that may supply one — a
/// composition family chooses its VALUE, never where the value lands, so a
/// second answer here would be a second application judgment.
pub(crate) fn land_final<T>(flowing: T, mut written: Vec<T>) -> Vec<T> {
    written.push(flowing);
    written
}

/// THE RELATIONAL LANDING, SPENT. The piped relation takes the one place
/// the row leaves for it: a written `@`, or — with none written — the place
/// after everything authored, which is the final formal.
///
/// It is spent INTO THE ROW, as a landed member. Nothing is answered back
/// and nothing is recorded beside the row: the relation and the position it
/// occupies are one member from here on, so no later phase can hold one
/// without the other, and this is the only road that mints one.
pub(crate) fn land_relation(
    written: &mut Vec<HoArgument<Unresolved>>,
    flowing: crate::pipeline::asts::core::Chain<Unresolved>,
) -> Result<()> {
    let holes: Vec<usize> = written
        .iter()
        .enumerate()
        .filter(|(_, argument)| matches!(argument, HoArgument::Landing(_)))
        .map(|(index, _)| index)
        .collect();
    match holes.as_slice() {
        [] => {
            written.push(HoArgument::Landed(flowing));
            Ok(())
        }
        [only] => {
            written[*only] = HoArgument::Landed(flowing);
            Ok(())
        }
        several => Err(two_landings(several.len())),
    }
}

/// ONE pipe, ONE landing. The teaching is the law's own words, and the
/// identity is one for every position that can hold a landing — a pure
/// invocation and a directive break the same rule.
pub(crate) fn two_landings(count: usize) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "resolution/ho/pipe_landing",
        format!(
            "one pipe, one landing — this call writes {count} placeholders; \
             exactly one @ names the parameter that receives the pipe"
        ),
        "exactly one explicit @",
    )
}

/// Whether this node is the hole a landing spends.
///
/// The two-anaphor law gives `@` one meaning — what flows in — and one
/// value-level carrier. A landing spends the composition input; the
/// disregarded receives nothing and is not a slot.
fn is_hole(expression: &DomainExpression<Unresolved>) -> bool {
    matches!(
        expression,
        DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Open(
            crate::pipeline::asts::core::DomainHole::CompositionInput
        ))
    )
}

/// How many holes this form offers a landing.
///
/// A NESTED CALLABLE OWNS ITS OWN SLOT, and that is now the type's doing:
/// a callable is not a value, so one can only stand under `Callable`, and
/// the walk stops there. Every other depth is the outer form's — the hole
/// stands at any value depth, so `x /-> upper:(trim:(@))` lands inside
/// `trim`.
struct HoleCount {
    holes: usize,
}

impl AstVisit<Unresolved> for HoleCount {
    fn enter_domain(&mut self, expression: &DomainExpression<Unresolved>) -> Result<Descent> {
        if is_hole(expression) {
            self.holes += 1;
        }
        Ok(Descent::Continue)
    }

    fn enter_callable(&mut self, _: &Callable<Unresolved>) -> Result<Descent> {
        Ok(Descent::SkipSubtree)
    }
}

/// The holes a form offers, counted once.
pub(crate) fn holes_in(values: &[Domex]) -> Result<usize> {
    let mut counter = HoleCount { holes: 0 };
    for value in values {
        walk_visit_domain(&mut counter, value)?;
    }
    Ok(counter.holes)
}

/// The holes an APPLICATION offers — its whole payload, not its argument row.
///
/// An application has value positions in three places: the arguments, the
/// window it is modified by, and the guard it is filtered by. The hole stands
/// wherever a value stands, so `x /-> row_number:() <~ %(@)` writes its
/// landing in the partition and `x /-> sum:(| @ > 0)` writes it in the guard.
/// Reading only the argument row would take the implicit landing over the
/// author's head and leave the written hole unspent.
pub(crate) fn holes_in_application(application: &StandardApplication<Unresolved>) -> Result<usize> {
    let mut counter = HoleCount { holes: 0 };
    walk_visit_standard_application(&mut counter, application)?;
    Ok(counter.holes)
}

/// The landing, spent: the one hole becomes the flowing value.
struct SpendHole<'a> {
    flowing: &'a Domex,
}

impl AstTransform<Unresolved, Unresolved> for SpendHole<'_> {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

    #[stacksafe::stacksafe]
    fn transform_domain(&mut self, expression: Domex) -> Result<Domex> {
        if is_hole(&expression) {
            return Ok(self.flowing.clone());
        }
        walk_transform_domain(self, expression)
    }

    /// The same boundary the count draws: a nested callable's slot is not
    /// this landing's to spend.
    fn transform_callable(
        &mut self,
        callable: Callable<Unresolved>,
    ) -> Result<Callable<Unresolved>> {
        Ok(callable)
    }
}

/// Spend the written landing in a form's values.
pub(crate) fn spend(values: Vec<Domex>, flowing: &Domex) -> Result<Vec<Domex>> {
    let mut spender = SpendHole { flowing };
    values
        .into_iter()
        .map(|value| spender.transform_domain(value))
        .collect()
}

/// The same, over an application's whole payload. The walk and the rewrite
/// reach the same positions, so what was counted is what is spent.
pub(crate) fn spend_in_application(
    application: StandardApplication<Unresolved>,
    flowing: &Domex,
) -> Result<StandardApplication<Unresolved>> {
    let mut spender = SpendHole { flowing };
    transform_standard_application(&mut spender, application)
}

/// THE SLOT IS ONE. A second bare hole spells the flowing value twice under
/// a glyph that names nothing, so the refusal teaches the spelling that
/// does name it.
pub(crate) fn the_slot_is_one(written: &str, slots: usize) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "landing/two_holes",
        format!(
            "'{written}' writes '@' {slots} times and one value flows in: the bare hole \
             lands once, so the second one has no reading"
        ),
        "name the value and use the name as often as you like — ':(|x| …)'",
    )
}

/// A binder names the flow; `@` names it too. One form spells it once.
pub(crate) fn binder_beside_a_hole(written: &str, binder: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "landing/binder_and_hole",
        format!(
            "'{written}' binds the flowing value to '{binder}' and also writes '@' for it: \
             the binder IS the flow, so '@' inside it names nothing"
        ),
        format!("write '{binder}' where the value belongs"),
    )
}

/// A binder that stands nowhere receives nothing.
pub(crate) fn binder_receives_nothing(written: &str, binder: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "landing/discarded",
        format!(
            "'{written}' binds the flowing value to '{binder}' and never uses it, so the \
             value would be discarded"
        ),
        format!("use '{binder}' where the value belongs, or write the constant directly"),
    )
}

/// THE BINDER, SPENT. Every reference addressing the binder's name becomes
/// the slot, and the count is what the zero-use refusal reads.
///
/// A binder is a plain written name and a lambda body has no other binding
/// form, so an unqualified reference ADDRESSING that name IS a use.
/// Addressing is `SqlIdentifier`'s question, not a string's: two unstropped
/// spellings that differ only in case name the same column everywhere else
/// in the language, and a stropped one names a different column even when
/// its characters agree. Comparing display text here would answer both
/// wrongly.
pub(crate) fn bind_the_binder(
    body: Domex,
    binder: &delightql_types::SqlIdentifier,
) -> Result<(Domex, usize)> {
    struct Bind<'a> {
        binder: &'a delightql_types::SqlIdentifier,
        uses: usize,
    }

    impl AstTransform<Unresolved, Unresolved> for Bind<'_> {
        crate::pipeline::ast_transform::same_phase_payload_folds!(Unresolved);

        #[stacksafe::stacksafe]
        fn transform_domain(&mut self, expression: Domex) -> Result<Domex> {
            if let DomainExpression::Reference(crate::pipeline::asts::core::Reference::Named(
                crate::pipeline::asts::core::NamedReference(column),
            )) = &expression
            {
                if column.qualifier.is_none() && &column.name == self.binder {
                    self.uses += 1;
                    return Ok(DomainExpression::Application(FunctionApplication::Open(
                        crate::pipeline::asts::core::DomainHole::CompositionInput,
                    )));
                }
            }
            walk_transform_domain(self, expression)
        }

        /// A nested callable binds its own flow.
        fn transform_callable(
            &mut self,
            callable: Callable<Unresolved>,
        ) -> Result<Callable<Unresolved>> {
            Ok(callable)
        }
    }

    let mut bind = Bind { binder, uses: 0 };
    let body = bind.transform_domain(body)?;
    Ok((body, bind.uses))
}

/// A form with no argument row wrote no hole, so nothing receives the value.
pub(crate) fn nothing_receives_it(written: &str) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "landing/discarded",
        format!(
            "'{written}' takes the piped value and has nowhere to put it, so the value \
             would be discarded"
        ),
        "write the constant directly, or write '@' where the value belongs",
    )
}
