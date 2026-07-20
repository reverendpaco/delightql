// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Provenance population - Post-refiner fixup for CFE parameter tracking
// This walks an already-refined tree and populates provenance fields based on
// qualifier/parameter-name matching.
//
// The population rides the shared inductive spine `AstTransform<Refined, Refined>`
// (INDUCTIVE-TRAVERSAL-PLAN R-I5: owned same-phase rewrites use the existing
// recursion, not a hand-rolled walk). Overriding ONLY `transform_domain` and
// delegating every other descent to `walk_transform_*` reaches EVERY domain edge
// by construction — Filter/Join conditions, EXISTS/IN subqueries, AND the pipe
// operator expressions, Case arms, string templates, curly members, metadata
// tree groups, and arrays that the former SIMPLIFIED hand-rolled walk dropped.
// The walk is now complete over the recursive
// domain type: no `..` hides a recursive field.
//
// Behavior-preserving in expectation: the previously-dropped positions are gated at
// RESOLUTION for every parameter kind (regular/context/implicit) before this
// refined-phase fixup runs, and implicit-context CFEs never rely on provenance
// at all — so reaching those extra positions sets provenance on Lvars no
// currently-compiling query can route through them. Pinned by the sef_functions
// ball staying outcome-identical (only the documented ccafe_explicit_context--025
// error).

use crate::error::Result;
use crate::pipeline::ast_transform::{walk_transform_domain, AstTransform};
use crate::pipeline::asts::core::Refined;
use crate::pipeline::asts::refined;

/// Populate provenance in an already-refined relational tree.
///
/// Sets `LvarProvenance` on every `Lvar` in the subtree according to CFE
/// parameter-list membership, reaching every domain position via the shared
/// `AstTransform<Refined, Refined>` spine.
pub(super) fn populate_provenance_in_relational(
    expr: refined::RelationalExpression,
    curried_params: &[String],
    regular_params: &[String],
    context_params: &[String],
) -> Result<refined::RelationalExpression> {
    ProvenancePopulator {
        curried_params,
        regular_params,
        context_params,
    }
    .transform_relational(expr)
}

/// The provenance-fixer tenant: it hooks `transform_domain` to stamp Lvar
/// provenance and leans on the default walk for all structural descent.
struct ProvenancePopulator<'a> {
    curried_params: &'a [String],
    regular_params: &'a [String],
    context_params: &'a [String],
}

impl AstTransform<Refined, Refined> for ProvenancePopulator<'_> {
    fn transform_domain(
        &mut self,
        expr: refined::DomainExpression,
    ) -> Result<refined::DomainExpression> {
        use refined::{DomainExpression, LvarProvenance};

        // Only Lvars carry provenance; everything else is delegated wholesale to
        // the shared walk, which re-enters this hook at every nested Lvar.
        let DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance: _,
        } = expr
        else {
            return walk_transform_domain(self, expr);
        };

        // Determine provenance based on parameter list membership.
        // NOTE: the resolver doesn't add qualifiers even with a fake schema — it
        // only validates — so we check the parameter-name lists directly, not
        // qualifiers. Order is load-bearing: curried, then regular, then context.
        let new_provenance = if self.curried_params.iter().any(|p| name == p.as_str()) {
            log::debug!(
                "🔧 PROVENANCE FIXER: {} (qual={:?}) → CfeCurriedParameter",
                name,
                qualifier
            );
            Some(LvarProvenance::CfeCurriedParameter)
        } else if self.regular_params.iter().any(|p| name == p.as_str()) {
            log::debug!(
                "🔧 PROVENANCE FIXER: {} (qual={:?}) → CfeParameter",
                name,
                qualifier
            );
            Some(LvarProvenance::CfeParameter)
        } else if self.context_params.iter().any(|p| name == p.as_str()) {
            log::debug!(
                "🔧 PROVENANCE FIXER: {} (qual={:?}) → CfeContext",
                name,
                qualifier
            );
            Some(LvarProvenance::CfeContext)
        } else {
            log::debug!(
                "🔧 PROVENANCE FIXER: {} (qual={:?}) → None (real table)",
                name,
                qualifier
            );
            None
        };

        Ok(DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance: refined::PhaseBox::new(new_provenance),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::asts::core::expressions::helpers::QualifiedName;
    use crate::pipeline::asts::core::expressions::metadata_types::SetOperator;
    use crate::pipeline::asts::core::metadata::NamespacePath;
    use crate::pipeline::asts::core::{BooleanExpression, PhaseBox, Relation, RelationalExpression};

    fn qn(name: &str) -> QualifiedName {
        QualifiedName {
            namespace_path: NamespacePath::empty(),
            name: name.into(),
            grounding: None,
        }
    }

    fn sentinel(tag: &str) -> RelationalExpression<Refined> {
        RelationalExpression::Relation(Relation::PseudoPredicate {
            name: tag.to_string(),
            namespace: Vec::new(),
            access: crate::pipeline::asts::core::DomainSpec::Glob,
            arguments: vec![],
            alias: None,
            cpr_schema: PhaseBox::phantom(),
        })
    }

    /// RED-1 (F1) — the ProvenancePopulator-specific pin. The provenance pass is
    /// a real `AstTransform<Refined, Refined>` that runs AFTER refine, where
    /// `process_mixed_setop` has already populated `SetOperation.correlation`.
    /// Overriding only `transform_domain` and delegating structural descent to
    /// `walk_transform_relational` means the pass inherits the unconditional
    /// correlation phantom (ast_transform/mod.rs:1267): it silently erases the
    /// correlation predicate. RED today.
    #[test]
    fn provenance_populator_preserves_setoperation_correlation() {
        let correlation = BooleanExpression::<Refined>::InnerExists {
            exists: true,
            identifier: qn("q"),
            subquery: Box::new(sentinel("setop_correlation")),
            alias: None,
            using_columns: vec![],
        };
        let setop = RelationalExpression::<Refined>::SetOperation {
            operator: SetOperator::SmartUnionAll,
            operands: vec![sentinel("a"), sentinel("b")],
            correlation: <PhaseBox<Option<BooleanExpression<Refined>>, Refined>>::with_correlation(
                Some(correlation),
            ),
            cpr_schema: PhaseBox::phantom(),
        };

        // No parameter lists: the pass touches no Lvar; it should be a pure
        // structural identity — yet it drops correlation via the shared spine.
        let out = populate_provenance_in_relational(setop, &[], &[], &[])
            .expect("provenance population must succeed");

        let RelationalExpression::SetOperation { correlation, .. } = out else {
            panic!("provenance population must return a SetOperation");
        };
        assert!(
            correlation.correlation().is_some(),
            "ProvenancePopulator ERASED SetOperation.correlation via the shared \
             same-phase spine (ast_transform/mod.rs:1267 phantoms it); the populated \
             correlation predicate must survive this post-refine fixup"
        );
    }
}
