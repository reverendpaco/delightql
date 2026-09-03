// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Reference extractor — walks unresolved ASTs to find entity references
//!
//! At consult time, we parse each definition body and extract the entities
//! it references. These populate the `referenced_entity` table in bootstrap,
//! which is used by the `GroundedEntity` view and by the resolver at query time.
//!
//! ## What counts as a reference
//!
//! - **Table references**: `Relation::Ground` nodes (e.g., `users(*)` → references "users")
//! - **Function calls**: `crate::pipeline::asts::core::FunctionApplication::Standard` nodes (e.g., `double:(x)` → references "double")
//! - **EXISTS references**: `TruthExpression::InnerExists` nodes (e.g., `+orders(...)` → references "orders")
//! - **Scalar subqueries**: `FunctionApplication::Scalarized` nodes → references the table
//!
//! ## What does NOT count
//!
//! - Column references (`Lvar`) — these are resolved against table schemas, not entities
//! - Built-in calls without a registered entity — these are SQL functions like `sum`, `count`
//! - Literals, operators, globs — structural, not references
//!
//! ## Apparent type classification
//!
//! We classify each reference by how it appears syntactically:
//! - Table access (`table(*)`) → apparent type = `DbPermanentTable` (10)
//! - Function call (`func:(args)`) → apparent type = `DqlFunctionExpression` (1)
//!
//! The "apparent" type may differ from the actual type after resolution
//! (e.g., what looks like a table could be a view).

use crate::enums::EntityType;
use crate::pipeline::asts::core::operators::{EmbedMapCover, MapCover};
use crate::pipeline::asts::core::{
    Comparison, Existence, FunctionApplication, GroundForm, MemberCorrelation, Membership,
    RelationalMembership, SigmaApplication, ValueTemplatePart,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::unresolved::*;

/// A reference found in a definition body
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractedReference {
    /// Name of the referenced entity
    pub name: String,
    /// Namespace qualification (if any)
    pub namespace: Option<String>,
    /// Apparent entity type (how it looks syntactically)
    pub apparent_type: i32,
}

/// Extract all entity references from a full query (view body, may include CTEs)
pub fn extract_references_from_query(query: &Query) -> Vec<ExtractedReference> {
    let mut refs = Vec::new();
    for cfe in query.cfes() {
        walk_domain(&cfe.body, &mut refs);
    }
    for cte in query.ctes() {
        walk_relational(cte.body(), &mut refs);
    }
    walk_relational(&query.body, &mut refs);
    refs
}

/// Extract all entity references from a domain expression (function body)
/// The same census over a TRUTH body — a sigma rule's, which names relations
/// the same way a value body does.
pub fn extract_references_from_truth(expr: &TruthExpression) -> Vec<ExtractedReference> {
    let mut refs = Vec::new();
    walk_boolean(expr, &mut refs);
    refs
}

pub fn extract_references_from_domain(expr: &DomainExpression) -> Vec<ExtractedReference> {
    let mut refs = Vec::new();
    walk_domain(expr, &mut refs);
    refs
}

// --- Walkers ---

#[stacksafe::stacksafe]
fn walk_relational(expr: &Chain, refs: &mut Vec<ExtractedReference>) {
    match expr.head().form() {
        GroundForm::Reference(rel) => walk_relation(rel, refs),
        GroundForm::Literal(anon) => walk_anon_table(&anon.table, refs),
    }
    for continuation in expr.forms() {
        match continuation {
            Continuation::Access { access, .. } => walk_access(access, refs),
            Continuation::Restrict { condition, .. } => walk_boolean(condition, refs),
            // A correlation names two arms by spelling; it holds no
            // reference to a definition.
            Continuation::Bound { .. } | Continuation::Correlate { .. } => {}
            // A PATTERN REFERS TO NOTHING: its members bind names and reach
            // with paths, and neither names a declared entity.
            Continuation::Destructure { source, .. } => {
                walk_domain(source, refs);
            }
            Continuation::Member {
                rhs, correlation, ..
            } => {
                walk_relational(rhs, refs);
                // A correspondence names columns, not references: only a
                // condition has an expression to walk.
                if let Some(cond) = correlation.as_ref().and_then(MemberCorrelation::condition) {
                    walk_boolean(cond, refs);
                }
            }
            Continuation::BagOp { arm, .. } => walk_relational(arm, refs),
            Continuation::Pipe { operator, .. } => walk_unary_operator(operator, refs),
            Continuation::Structural(step) => match &step.form {
                crate::pipeline::asts::core::StructuralForm::Ordering { specs, .. } => {
                    for spec in specs {
                        walk_domain(&spec.column, refs);
                    }
                }
                // A reposition, a drill and a narrowing ADDRESS the operand's
                // columns; the fixed-heading forms name nothing at all.
                crate::pipeline::asts::core::StructuralForm::Reposition { .. }
                | crate::pipeline::asts::core::StructuralForm::Meta
                | crate::pipeline::asts::core::StructuralForm::Witness { .. }
                | crate::pipeline::asts::core::StructuralForm::SignedWitness
                | crate::pipeline::asts::core::StructuralForm::Drill { .. }
                | crate::pipeline::asts::core::StructuralForm::Narrow { .. } => {}
            },
            Continuation::ErJoin(step) => walk_relational(&step.rhs, refs),
        }
    }
}

fn walk_anon_table(anon: &AnonTable, refs: &mut Vec<ExtractedReference>) {
    if let Some(headers) = &anon.body.header {
        for h in headers.iter() {
            if let Some(term) = h.term() {
                walk_domain(&term, refs);
            }
        }
    }
    for row in &anon.body.rows {
        for datum in row.iter() {
            walk_domain(&datum.value(), refs);
        }
    }
}

fn walk_relation(rel: &Relation, refs: &mut Vec<ExtractedReference>) {
    match rel {
        Relation::Ground {
            mention: GroundMention::Named { identifier, .. },
            ..
        } => {
            let namespace = if identifier.namespace_path.is_empty() {
                None
            } else {
                Some(identifier.namespace_path.to_string())
            };
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace,
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
        }
        // A plan read names compiler-owned storage by identity: no authored
        // spelling participates, so it contributes no reference a DDL body
        // could depend on.
        Relation::Ground {
            mention:
                GroundMention::Scratch { .. }
                | GroundMention::Receipt { .. }
                | GroundMention::Structural { .. },
            ..
        } => {}
        Relation::FunctorCall { call, .. } => {
            walk_functor_call(call.call(), refs);
        }
        Relation::InnerRelation { pattern, .. } => {
            walk_inner_relation_pattern(pattern, refs);
        }
        Relation::ConsultedView { body, .. } => {
            // Recursively extract references from the consulted view body
            for cte in body.ctes() {
                walk_relational(cte.body(), refs);
            }
            walk_relational(&body.body, refs);
        }
    }
}

fn walk_inner_relation_pattern(pattern: &InnerRelationPattern, refs: &mut Vec<ExtractedReference>) {
    match pattern {
        InnerRelationPattern::Indeterminate {
            identifier,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_relational(subquery, refs);
        }
        InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_relational(subquery, refs);
        }
        InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            for filter in correlation_filters {
                walk_boolean(filter, refs);
            }
            walk_relational(subquery, refs);
        }
        InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery,
            ..
        } => {
            refs.push(ExtractedReference {
                name: identifier.name.to_string(),
                namespace: namespace_from_path(&identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            for filter in correlation_filters {
                walk_boolean(filter, refs);
            }
            for agg in aggregations {
                walk_domain(agg, refs);
            }
            walk_relational(subquery, refs);
        }
    }
}

fn walk_domain(expr: &DomainExpression, refs: &mut Vec<ExtractedReference>) {
    match expr {
        DomainExpression::Application(func) => walk_function(func, refs),
        DomainExpression::Reference(Reference::Named(NamedReference(_)))
        | DomainExpression::Reference(Reference::Ordinal(_))
        | DomainExpression::Reference(Reference::Physical(_)) => {}
    }
}

/// A RELATION MADE ONE VALUE names the relation it compresses.
fn walk_scalar_relation(
    relation: &crate::pipeline::asts::core::ScalarRelation,
    refs: &mut Vec<ExtractedReference>,
) {
    if let crate::pipeline::asts::core::ScalarRelation::Named { identifier, .. } = relation {
        refs.push(ExtractedReference {
            name: identifier.name.to_string(),
            namespace: namespace_from_path(&identifier.namespace_path),
            apparent_type: EntityType::DbPermanentTable.as_i32(),
        });
    }
    walk_relational(relation.body().body(), refs);
}

fn walk_function(func: &FunctionApplication, refs: &mut Vec<ExtractedReference>) {
    match func {
        crate::pipeline::asts::core::FunctionApplication::Ground(_)
        | crate::pipeline::asts::core::FunctionApplication::Open(_) => {}
        crate::pipeline::asts::core::FunctionApplication::Standard(application) => {
            walk_standard_application(application, refs);
        }
        crate::pipeline::asts::core::FunctionApplication::FieldSelect(select) => {
            walk_standard_application(&select.application, refs);
        }
        crate::pipeline::asts::core::FunctionApplication::Enclyph(enclyph) => {
            walk_enclyph(enclyph, refs)
        }
        crate::pipeline::asts::core::FunctionApplication::Infix(infix) => {
            walk_domain(&infix.left, refs);
            walk_domain(&infix.right, refs);
        }
        crate::pipeline::asts::core::FunctionApplication::Template(template) => {
            for part in template.parts() {
                if let ValueTemplatePart::Interpolation(expr) = part {
                    walk_domain(expr, refs);
                }
            }
        }
        crate::pipeline::asts::core::FunctionApplication::ClauseSelection(selection) => {
            for arm in &selection.arms {
                if let Some(guard) = &arm.guard {
                    walk_boolean(guard, refs);
                }
                walk_domain(&arm.result, refs);
            }
        }
        crate::pipeline::asts::core::FunctionApplication::Case(case) => walk_case(case, refs),
        crate::pipeline::asts::core::FunctionApplication::Scalarized(relation) => {
            walk_scalar_relation(relation, refs)
        }
        // The path is a spec — it names no relation and no column.
        crate::pipeline::asts::core::FunctionApplication::JsonAccess(access) => {
            walk_domain(&access.source, refs)
        }
        crate::pipeline::asts::core::FunctionApplication::Crossed(crossing) => {
            walk_boolean(crossing.truth(), refs)
        }
    }
}

fn walk_boolean(expr: &TruthExpression, refs: &mut Vec<ExtractedReference>) {
    match expr {
        TruthExpression::Comparison(Comparison { left, right, .. }) => {
            walk_domain(left, refs);
            walk_domain(right, refs);
        }
        TruthExpression::Conjunction(parts) | TruthExpression::Disjunction(parts) => {
            for part in parts.iter() {
                walk_boolean(part, refs);
            }
        }
        TruthExpression::Not { expr } => walk_boolean(expr, refs),
        TruthExpression::Existence(Existence {
            addressing,
            relation: subquery,
            ..
        }) => {
            refs.push(ExtractedReference {
                name: addressing.identifier.name.to_string(),
                namespace: namespace_from_path(&addressing.identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_relational(subquery, refs);
        }
        TruthExpression::Membership(Membership { probe, rows, .. }) => {
            for value in probe.values() {
                walk_domain(value, refs);
            }
            for row in rows {
                for value in &row.0 {
                    walk_domain(value, refs);
                }
            }
        }
        TruthExpression::RelationalMembership(RelationalMembership {
            probe,
            addressing,
            relation: subquery,
            ..
        }) => {
            for value in probe.values() {
                walk_domain(value, refs);
            }
            refs.push(ExtractedReference {
                name: addressing.identifier.name.to_string(),
                namespace: namespace_from_path(&addressing.identifier.namespace_path),
                apparent_type: EntityType::DbPermanentTable.as_i32(),
            });
            walk_relational(subquery, refs);
        }
        // An authored application observes a CALL: the body slot is
        // uninhabited before resolution, so there is no arm to write.
        TruthExpression::Sigma(SigmaApplication {
            proof: crate::pipeline::asts::core::NamedProof::Body(body),
            ..
        }) => match *body {},
        TruthExpression::Sigma(SigmaApplication {
            proof: crate::pipeline::asts::core::NamedProof::Call(call),
            ..
        }) => walk_functor_call(call.call(), refs),
    }
}

fn walk_functor_call(call: &FunctorCall, refs: &mut Vec<ExtractedReference>) {
    // A QUALIFIED callee is a reference to the entity it names — the
    // executable boundary asks the catalog which definitions reach a
    // runtime-served relation, and the callee is how a body reaches one.
    // An unqualified callee stays unrecorded: the grounding contract reads
    // unqualified rows as free data-namespace variables, which a callee is
    // not.
    let namespace = call.call().callee.namespace_texts();
    if !namespace.is_empty() {
        refs.push(ExtractedReference {
            name: call.call().callee.name_text().to_string(),
            namespace: Some(namespace.join("::")),
            apparent_type: EntityType::DbPermanentTable.as_i32(),
        });
    }
    for rel in call.call().relations() {
        walk_relational(rel, refs);
    }
    // A spread addresses columns of the operand and a star names the whole
    // of it; neither refers to anything.
    for expr in call.call().arguments.value_domains() {
        walk_domain(expr, refs);
    }
}

/// A COVER'S CALLABLE, walked as the form it is.
fn walk_callable(
    callable: &crate::pipeline::asts::core::Callable,
    refs: &mut Vec<ExtractedReference>,
) {
    match callable {
        crate::pipeline::asts::core::Callable::Functor(application) => {
            walk_standard_application(application, refs)
        }
        crate::pipeline::asts::core::Callable::String(template) => {
            for part in template.parts() {
                if let ValueTemplatePart::Interpolation(expr) = part {
                    walk_domain(expr, refs);
                }
            }
        }
        crate::pipeline::asts::core::Callable::Lambda(lambda) => walk_domain(&lambda.body, refs),
    }
}

/// The whole application: the call's arguments, the window it is modified by
/// and the guard it is filtered by.
fn walk_standard_application(
    application: &crate::pipeline::asts::core::StandardApplication,
    refs: &mut Vec<ExtractedReference>,
) {
    walk_functor_call(application.call(), refs);
    if let Some(window) = &application.window {
        for expr in &window.partition {
            walk_domain(expr, refs);
        }
        for ordering in &window.ordering {
            walk_domain(&ordering.column, refs);
        }
        if let Some(frame) = &window.frame {
            walk_window_frame(frame, refs);
        }
    }
    if let Some(guard) = &application.guard {
        walk_boolean(guard, refs);
    }
}

fn walk_window_frame(frame: &WindowFrame, refs: &mut Vec<ExtractedReference>) {
    let mut walk_bound = |bound: &FrameBound| match bound {
        FrameBound::Preceding(expr) | FrameBound::Following(expr) => walk_domain(expr, refs),
        FrameBound::Unbounded | FrameBound::CurrentRow => {}
    };
    walk_bound(&frame.start);
    walk_bound(&frame.end);
}

fn walk_case(case: &CaseExpression, refs: &mut Vec<ExtractedReference>) {
    let default = match case {
        CaseExpression::Anchored {
            anchor,
            arms,
            default,
        } => {
            walk_domain(anchor, refs);
            for arm in arms.iter() {
                walk_domain(&arm.result, refs);
            }
            default
        }
        CaseExpression::Searched { arms, default } => {
            for arm in arms.iter() {
                walk_boolean(&arm.condition, refs);
                walk_domain(&arm.result, refs);
            }
            default
        }
    };
    if let Some(result) = default {
        walk_domain(result, refs);
    }
}

fn walk_enclyph(
    enclyph: &crate::pipeline::asts::core::Enclyph,
    refs: &mut Vec<ExtractedReference>,
) {
    use crate::pipeline::asts::core::{Enclyph, RecordMember};
    match enclyph {
        Enclyph::Record(record) => {
            for member in record.members.iter() {
                match member {
                    RecordMember::Keyed { value, .. } => walk_domain(value, refs),
                    RecordMember::Induced { value, .. } => walk_enclyph(value, refs),
                    // A self-keyed member and a spread both address columns
                    // of the operand, not a declared entity; a metadata
                    // member's keys and targets do too.
                    RecordMember::SelfKeyed(_)
                    | RecordMember::Spread(_)
                    | RecordMember::Metadata { .. } => {}
                }
            }
        }
        Enclyph::EmptyRecord(empty) => match *empty {},
        Enclyph::Tuple(tuple) => {
            for element in tuple.elements.iter() {
                match element {
                    crate::pipeline::asts::core::TupleElement::Value(element) => {
                        walk_domain(element, refs)
                    }
                    crate::pipeline::asts::core::TupleElement::Spread(_) => {}
                }
            }
        }
    }
}

fn walk_metadata_group(
    group: &crate::pipeline::asts::core::MetadataGroup,
    refs: &mut Vec<ExtractedReference>,
) {
    use crate::pipeline::asts::core::MetadataTarget;
    match &group.target {
        MetadataTarget::Enclyph(enclyph) => walk_enclyph(enclyph, refs),
        MetadataTarget::Group(nested) => walk_metadata_group(nested, refs),
    }
}

/// A publication item references what its value references. A spread names
/// columns of the operand rather than referring to a declared entity.
fn walk_out_item(item: &crate::pipeline::asts::core::OutItem, refs: &mut Vec<ExtractedReference>) {
    if let Some(expr) = item.value() {
        walk_domain(expr, refs);
    }
}

/// A reduction publishes one column: a value, or a metadata level whose
/// target holds the references.
fn walk_reduction_item(
    item: &crate::pipeline::asts::core::ReductionItem,
    refs: &mut Vec<ExtractedReference>,
) {
    use crate::pipeline::asts::core::ReductionItem;
    match item {
        ReductionItem::Out(item) => walk_out_item(item, refs),
        ReductionItem::Metadata(metadata) => walk_metadata_group(&metadata.group, refs),
        ReductionItem::Pivot(pivot) => {
            walk_domain(&pivot.value_column, refs);
            walk_domain(&pivot.pivot_key, refs);
        }
        ReductionItem::Delegate(delegate) => {
            for item in &delegate.payload {
                walk_out_item(item, refs);
            }
            for o in &delegate.order {
                walk_domain(&o.column, refs);
            }
        }
    }
}

fn walk_group_spec(spec: &GroupSpec, refs: &mut Vec<ExtractedReference>) {
    match spec {
        GroupSpec::Distinct { keys } => {
            for col in keys.iter() {
                walk_out_item(col, refs);
            }
        }
        GroupSpec::Reduce {
            keys,
            reductions,
            plan: _,
        } => {
            for item in keys {
                walk_out_item(item, refs);
            }
            for item in reductions.iter() {
                walk_reduction_item(item, refs);
            }
        }
    }
}

fn walk_access(spec: &Access, refs: &mut Vec<ExtractedReference>) {
    match spec {
        Access::All => {}
        Access::Dequalify(_) => {}
        Access::DequalifyAll => {}
        Access::Slots(slots) => {
            for slot in slots {
                if let Some(term) = slot.constraint() {
                    walk_domain(term, refs);
                }
            }
        }
        Access::Unasked => {}
    }
}

fn walk_unary_operator(op: &PipeOp, refs: &mut Vec<ExtractedReference>) {
    match op {
        PipeOp::Project(items) | PipeOp::Embed(items) => {
            for item in items {
                walk_out_item(item, refs);
            }
        }
        PipeOp::Group(spec) => {
            walk_group_spec(spec, refs);
        }
        // A selector and a rename source ADDRESS columns of the operand:
        // neither names a relation nor reaches a definition.
        PipeOp::MapCover(MapCover {
            callable: function, ..
        }) => walk_callable(function, refs),
        PipeOp::ProjectOut(_) | PipeOp::Rename(_) => {}
        PipeOp::Transform {
            items: transformations,
            ..
        } => {
            for item in transformations {
                walk_domain(&item.expr, refs);
            }
        }
        PipeOp::EmbedMapCover(EmbedMapCover {
            callable: function, ..
        }) => {
            walk_callable(function, refs);
        }
    }
}

fn namespace_from_path(path: &NamespacePath) -> Option<String> {
    if path.is_empty() {
        None
    } else {
        Some(path.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::reconstruct;
    use crate::pipeline::asts::ddl::DdlBody;

    /// The clause a stored definition reconstructs to, by body position.
    fn scalar(source: &str) -> crate::pipeline::asts::unresolved::DomainExpression {
        match reconstruct::clauses(source).unwrap().remove(0).body {
            DdlBody::Scalar(expr) => expr,
            other => panic!("expected a value body, got {other:?}"),
        }
    }

    fn relational(source: &str) -> crate::pipeline::asts::unresolved::Query {
        match reconstruct::clauses(source).unwrap().remove(0).body {
            DdlBody::Relational(query) => query,
            other => panic!("expected a relational body, got {other:?}"),
        }
    }

    #[test]
    fn test_function_body_no_references() {
        // x * 2 has no entity references (just parameter lvars and literals)
        let expr = scalar("f:(x) :- x * 2");
        let refs = extract_references_from_domain(&expr);
        assert!(
            refs.is_empty(),
            "Function body 'x * 2' should have no references, got: {:?}",
            refs
        );
    }

    #[test]
    fn test_view_body_table_reference() {
        // users(*), balance > 1000 references the "users" table
        let query = relational("v(*) :- users(*), balance > 1000");
        let refs = extract_references_from_query(&query);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
        assert_eq!(refs[0].apparent_type, EntityType::DbPermanentTable.as_i32());
        assert_eq!(refs[0].namespace, None);
    }

    #[test]
    fn test_view_body_multiple_references() {
        // users(*), orders(*) references both tables
        let query = relational("v(*) :- users(*), orders(*)");
        let refs = extract_references_from_query(&query);
        assert_eq!(refs.len(), 2);
        let names: Vec<&str> = refs.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"orders"));
    }

    #[test]
    fn test_view_body_with_pipe_preserves_table_ref() {
        // users(*) |> (first_name, last_name) still references "users"
        let query = relational("v(*) :- users(*) |> (first_name, last_name)");
        let refs = extract_references_from_query(&query);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
    }

    #[test]
    fn test_function_body_with_nested_function_call() {
        // `round:(x)` is a curried call and IS a reference; `round(x)` is a
        // regular SQL function and is not. Neither stands here: `x + 10` is
        // two leaves.
        let expr = scalar("f:(x) :- x + 10");
        let refs = extract_references_from_domain(&expr);
        assert!(refs.is_empty());
    }
}
