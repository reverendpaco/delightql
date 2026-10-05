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
use crate::pipeline::asts::core::operators::{EmbedMapCover, HoArgument, MapCover};
use crate::pipeline::asts::core::{
    Comparison, Existence, FunctionApplication, GroundForm, MemberCorrelation, Membership,
    RelationalMembership, SigmaApplication, ValueTemplatePart,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::unresolved::*;

/// A reference found in a definition body
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExtractedReference {
    /// Name of the referenced entity
    pub name: String,
    /// Namespace qualification (if any)
    pub namespace: Option<String>,
    /// Apparent entity type (how it looks syntactically)
    pub apparent_type: i32,
}

/// ONE SET OF NAMES standing over the walk.
enum Declarations {
    /// One clause's formals that a bare name can mention: its relation and
    /// rule formals, which a relation mention reads, and its code formals,
    /// which a call invokes. A value formal occupies no bare name — the body
    /// reads it by position — so a relation spelled like one is a relation.
    Parameters {
        relations: Vec<delightql_types::SqlIdentifier>,
        callees: Vec<delightql_types::SqlIdentifier>,
    },
    /// A query block's own claims, in the order the block minted them.
    Block(crate::pipeline::asts::core::QueryLocalNames),
}

/// Where a bare name is mentioned.
#[derive(Clone, Copy)]
enum Mentioned {
    AsRelation,
    AsCallee,
}

impl Declarations {
    /// The formals of one clause's head that a bare name can mention.
    fn parameters(head: &crate::pipeline::asts::ddl::Head) -> Self {
        use crate::pipeline::asts::core::definitions::HoParam;
        let mut relations = Vec::new();
        let mut callees = Vec::new();
        for param in head.ho_params.as_deref().unwrap_or_default() {
            match param {
                HoParam::Relation { name, .. } | HoParam::Rule { name, .. } => {
                    relations.push(name.clone())
                }
                HoParam::Scalar {
                    name,
                    callable: true,
                    ..
                } => callees.push(name.clone()),
                HoParam::Scalar {
                    callable: false, ..
                }
                | HoParam::Ground { .. } => {}
            }
        }
        Declarations::Parameters { relations, callees }
    }

    fn declares(&self, name: &delightql_types::SqlIdentifier, mentioned: Mentioned) -> bool {
        match (self, mentioned) {
            (Declarations::Parameters { relations, .. }, Mentioned::AsRelation) => {
                relations.contains(name)
            }
            (Declarations::Parameters { callees, .. }, Mentioned::AsCallee) => {
                callees.contains(name)
            }
            (Declarations::Block(names), _) => names.declares(name),
        }
    }
}

/// THE CENSUS OF ONE DEFINITION BODY, and the lexical declarations standing
/// over the walk.
///
/// A body's own parameters, every query block it contains, and every
/// query-local parameterized definition inside those blocks DECLARE names.
/// A mention of such a name reaches that declaration — it is not a
/// reference to an entity, and it must never become a `referenced_entity`
/// row, because grounding admission judges every unqualified row against a
/// data world and a lexical name has no answer there.
///
/// The declarations are pushed and popped as the walk enters and leaves the
/// thing that owns them, so this judgment is made where the binding
/// structure is still in hand. It is not a list of spellings filtered out
/// afterwards: a name the walk never saw declared cannot be excused by one.
pub(crate) struct Census {
    refs: Vec<ExtractedReference>,
    /// Every name the walked bodies MENTION, as written: each ground
    /// relation reference and each callee, qualified or not. An unqualified
    /// callee is no `referenced_entity` row (the grounding contract reads
    /// unqualified rows as data-namespace variables), but it is a mention,
    /// and the recursion authority selects every mention through the
    /// definition-use authority to learn, by identity, whether a clause
    /// reads the instance being opened. Nothing here compares spellings.
    mentions: Vec<Mention>,
    /// Outermost first.
    scopes: Vec<Declarations>,
    /// A clause body that is BROKEN rather than merely unsubstituted. The
    /// walkers answer nothing, so the refusal is held here and spent by
    /// `finish` — the census never turns an unreadable body into "no
    /// dependencies".
    refusal: Option<crate::error::DelightQLError>,
    /// The marked scope of the clause the walk began at, as its reading
    /// opened it — known once that clause's body is in hand.
    root: Root,
    /// The argument positions of the clause the walk began at that a `$.x`
    /// selects, wherever below it the reference is written — a nested
    /// definition's capture included, whether or not anything invokes it.
    selected:
        std::collections::BTreeSet<crate::pipeline::asts::core::definitions::ArgumentPosition>,
}

impl Census {
    fn new() -> Self {
        Census {
            refs: Vec::new(),
            mentions: Vec::new(),
            scopes: Vec::new(),
            refusal: None,
            root: Root::Unread,
            selected: std::collections::BTreeSet::new(),
        }
    }

    /// A `$.x` below the walk's clause: a use of that clause's position when
    /// it selects that clause's scope.
    fn selects(&mut self, selector: crate::pipeline::asts::core::definitions::FormalSelector) {
        if matches!(self.root, Root::Scope(scope) if scope == selector.scope()) {
            self.selected.insert(selector.position());
        }
    }

    /// The first body the walk reads is its own clause's: its reading
    /// recorded the scope it opened.
    fn read_root(&mut self, query: &Query) {
        if matches!(self.root, Root::Unread) {
            self.root = match query.locals.clause_formals.marked_scope() {
                Some(scope) => Root::Scope(scope),
                None => Root::Unmarked,
            };
        }
    }

    /// Every `$.x` a body holds, by the generated exhaustive walk.
    fn selections_in(&mut self, body: Body<'_>) {
        use crate::pipeline::ast_visit::{AstVisit, Descent};
        struct Selections(Vec<crate::pipeline::asts::core::definitions::FormalSelector>);
        impl AstVisit<crate::pipeline::asts::core::Unresolved> for Selections {
            fn enter_domain(&mut self, e: &DomainExpression) -> crate::error::Result<Descent> {
                match e {
                    DomainExpression::Reference(Reference::Argument(selector)) => self.0.push(*selector),
                    DomainExpression::Reference(Reference::Ordinal(ordinal)) => self.0.extend(ordinal.position.formal()),
                    _ => {}
                }
                Ok(Descent::Continue)
            }
            fn enter_continuation(
                &mut self,
                c: &crate::pipeline::asts::core::Continuation<crate::pipeline::asts::core::Unresolved>,
            ) -> crate::error::Result<Descent> {
                self.0.extend(c.bound_formals());
                Ok(Descent::Continue)
            }
        }
        let mut found = Selections(Vec::new());
        let walked = match body {
            Body::Query(query) => crate::pipeline::ast_visit::walk_visit_query(&mut found, query),
            Body::Domain(value) => crate::pipeline::ast_visit::walk_visit_domain(&mut found, value),
            Body::Truth(truth) => crate::pipeline::ast_visit::walk_visit_boolean(&mut found, truth),
        };
        walked.expect("a selection census over authored syntax is infallible");
        for selector in found.0 {
            self.selects(selector);
        }
    }

    /// Walk one query block's bodies under its own claims: its value
    /// definitions, its parameterized definitions, its truth-rule
    /// definitions, its relation bindings, and then the query body itself.
    /// The block's claims stand over all four, which is why a clause-local
    /// binding is lexical even where a sibling binding mentions it.
    fn in_query(&mut self, query: &Query) {
        self.scopes
            .push(Declarations::Block(query.local_names().clone()));
        self.selections_in(Body::Query(query));
        for cfe in query.cfes() {
            self.selections_in(Body::Domain(&cfe.body));
            walk_domain(&cfe.body, self);
        }
        for ho in query.hos() {
            self.in_scoped_parameterized(ho);
        }
        for sigma in query.sigmas() {
            self.in_scoped_sigma(sigma);
        }
        for cte in query.ctes() {
            walk_relational(cte.body(), self);
        }
        walk_relational(&query.body, self);
        self.scopes.pop();
    }

    /// A QUERY-LOCAL PARAMETERIZED DEFINITION'S CLAUSES, under the block's
    /// claims and this definition's OWN declared parameters.
    ///
    /// Its body is a definition body like any other, so it crosses the same
    /// per-clause road every consulted clause crosses — including the
    /// deferred one, whose text the analysis reading reads back. A
    /// dependency named only here is therefore a recorded reference, not one
    /// that surfaces when the definition is finally invoked.
    fn in_scoped_parameterized(&mut self, definition: &crate::pipeline::asts::core::HoDefinition) {
        for clause in definition.group().clauses() {
            self.in_clause(clause);
        }
    }

    fn in_scoped_sigma(&mut self, definition: &crate::pipeline::asts::core::SigmaDefinition) {
        for clause in definition.group().clauses() {
            self.in_clause(clause);
        }
    }

    /// ONE CLAUSE BODY, whatever kind it is. The one road every definition
    /// clause's references are read on — a consulted clause and a
    /// query-local parameterized clause alike.
    fn in_clause(&mut self, clause: &crate::pipeline::asts::ddl::Clause) {
        // THIS CLAUSE'S OWN PARAMETERS, over this clause's own body. The
        // assembler makes sibling clauses agree on parameter COUNT and
        // deliberately lets each ground different positions, so a name
        // bound in one clause's head is not a declaration in another's:
        // `f(T(*))(*) : T(*)` declares `T`, and `f("k")(*) : T(*)` reads the
        // relation `T`. Taking the family's names from the first clause
        // would make the recorded dependencies depend on authored order.
        self.scopes.push(Declarations::parameters(&clause.head));
        self.in_clause_body(clause);
        self.scopes.pop();
    }

    fn in_clause_body(&mut self, clause: &crate::pipeline::asts::ddl::Clause) {
        use crate::pipeline::asts::ddl::DdlBody;
        match &clause.body {
            DdlBody::Scalar(expr) => {
                self.selections_in(Body::Domain(expr));
                walk_domain(expr, self)
            }
            DdlBody::Truth(expr) => {
                self.selections_in(Body::Truth(expr));
                walk_boolean(expr, self)
            }
            DdlBody::Relational(query) => {
                self.read_root(query);
                self.in_query(query)
            }
            // A mode's references live in every authored output cell,
            // including the default. Read them directly: a default-bearing
            // mode has no relational body to synthesize merely for analysis.
            DdlBody::FactFunction(definition) => {
                let mode = definition.mode();
                for arm in mode.arms.iter() {
                    for output in arm.outputs.iter() {
                        self.selections_in(Body::Domain(output));
                        walk_domain(output, self);
                    }
                }
                if let Some(default) = &mode.default {
                    for output in default.iter() {
                        self.selections_in(Body::Domain(output));
                        walk_domain(output, self);
                    }
                }
            }
            // A DEFERRED BODY is read from its text, which is enough to reach
            // the references and to catch a body that is broken rather than
            // merely waiting for a use. A body that cannot be read refuses
            // HERE, at declaration, rather than being recorded as a
            // definition with no dependencies.
            DdlBody::Deferred => match crate::ddl::reconstruct::analysis_clause_body(clause) {
                Ok(query) => {
                    self.read_root(&query);
                    self.in_query(&query)
                }
                Err(error) => self.refuse(error),
            },
        }
    }

    /// Hold the first refusal a clause body produced.
    fn refuse(&mut self, error: crate::error::DelightQLError) {
        if self.refusal.is_none() {
            self.refusal = Some(error);
        }
    }

    /// Whether a BARE name, mentioned as `mentioned`, reaches a declaration
    /// standing over this walk.
    fn declared(&self, name: &delightql_types::SqlIdentifier, mentioned: Mentioned) -> bool {
        self.scopes
            .iter()
            .any(|scope| scope.declares(name, mentioned))
    }

    /// THE ONE ROAD A REFERENCE ROW IS BORN ON. A qualified name always
    /// names an entity in the namespace it spells; a bare name does so only
    /// when no enclosing declaration owns it.
    fn record(&mut self, identifier: &QualifiedName, apparent_type: i32) {
        let namespace = namespace_from_path(&identifier.namespace_path);
        if namespace.is_none() && self.declared(&identifier.name, Mentioned::AsRelation) {
            return;
        }
        self.mentions.push(Mention {
            name: identifier.name.clone(),
            namespace: (!identifier.namespace_path.is_empty())
                .then(|| identifier.namespace_path.qualifier()),
        });
        self.refs.push(ExtractedReference {
            name: identifier.name.to_string(),
            namespace,
            apparent_type,
        });
    }

    /// A callee spelled with its namespace: never a query-local name.
    fn record_qualified(&mut self, name: String, namespace: String, apparent_type: i32) {
        self.refs.push(ExtractedReference {
            name,
            namespace: Some(namespace),
            apparent_type,
        });
    }

    /// A callee, however spelled. A declared query-local name is a
    /// declaration, not a mention of an entity.
    fn record_callee(
        &mut self,
        name: &str,
        namespace: Option<crate::pipeline::asts::vocabulary::Qualifier>,
    ) {
        let name = delightql_types::SqlIdentifier::new(name);
        if namespace.is_none() && self.declared(&name, Mentioned::AsCallee) {
            return;
        }
        self.mentions.push(Mention { name, namespace });
    }

    /// The rows this census recorded — or the refusal of a body it could
    /// not read. A caller that must judge every dependency at ONE boundary
    /// never receives a silently empty answer for an unreadable body.
    pub(crate) fn finish(self) -> crate::error::Result<Vec<ExtractedReference>> {
        match self.refusal {
            Some(error) => Err(error),
            None => Ok(self.refs),
        }
    }
}

/// THE CENSUS OF ONE DEFINITION GROUP: every clause, each under ITS OWN
/// declared parameters.
pub(crate) fn census_of_group(group: &crate::pipeline::asts::ddl::DefinitionGroup) -> Census {
    let mut census = Census::new();
    for clause in group.clauses() {
        census.in_clause(clause);
    }
    census
}

/// The marked scope of the clause a census began at.
enum Root {
    /// Its body is not yet read.
    Unread,
    /// Its reading opened this scope.
    Scope(crate::pipeline::asts::core::definitions::MarkedScopeId),
    /// It opened none: a value or truth clause, which no `$.x` selects.
    Unmarked,
}

/// One body the selection census walks.
enum Body<'a> {
    Query(&'a Query),
    Domain(&'a DomainExpression),
    Truth(&'a TruthExpression),
}

/// THE ARGUMENT POSITIONS ONE RELATIONAL OR EFFECT HIGHER-ORDER CLAUSE'S
/// TEXT SELECTS: every `$.x` below the clause that selects its own scope,
/// read on the same census road its mentions are read on — a nested
/// definition's body included. A body that is broken rather than merely
/// unsubstituted refuses here.
pub(crate) fn clause_selections(
    clause: &crate::pipeline::asts::ddl::Clause,
) -> crate::error::Result<
    std::collections::BTreeSet<crate::pipeline::asts::core::definitions::ArgumentPosition>,
> {
    let mut census = Census::new();
    census.in_clause(clause);
    if let Some(refusal) = census.refusal.take() {
        return Err(refusal);
    }
    Ok(census.selected)
}

/// ONE NAME A BODY MENTIONS, AS WRITTEN: the identifier with its strop and
/// the namespace path it was qualified with, if any. A mention names
/// nothing by itself — the definition-use authority selects what it names.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Mention {
    pub(crate) name: delightql_types::SqlIdentifier,
    /// The written qualifier, when one was written; the world the clause
    /// is judged in resolves its route.
    pub(crate) namespace: Option<crate::pipeline::asts::vocabulary::Qualifier>,
}

// --- Walkers ---

#[stacksafe::stacksafe]
fn walk_relational(expr: &Chain, refs: &mut Census) {
    match expr.head().form() {
        GroundForm::Reference(rel) => walk_relation(rel, refs),
        GroundForm::Literal(anon) => {
            if let Some(table) = anon.table() {
                walk_anon_table(table, refs)
            }
        }
    }
    for continuation in expr.forms() {
        match continuation {
            Continuation::Access { access, .. } => walk_access(access, refs),
            Continuation::Restrict { condition, .. } => walk_boolean(condition, refs),
            // Nothing is correlated before names resolve.
            Continuation::Correlated(never) => match *never {},
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

fn walk_anon_table(anon: &AnonTable, refs: &mut Census) {
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

fn walk_relation(rel: &Relation, refs: &mut Census) {
    match rel {
        Relation::Ground {
            mention: GroundMention::Named { identifier, .. },
            ..
        } => {
            refs.record(identifier, EntityType::DbPermanentTable.as_i32());
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
            // The nested body is its own query block: its claims stand over
            // its bodies exactly as the outer block's stand over this one.
            refs.in_query(body);
        }
    }
}

fn walk_inner_relation_pattern(pattern: &InnerRelationPattern, refs: &mut Census) {
    match pattern {
        InnerRelationPattern::Indeterminate {
            identifier,
            subquery,
            ..
        } => {
            refs.record(&identifier, EntityType::DbPermanentTable.as_i32());
            walk_relational(subquery, refs);
        }
        InnerRelationPattern::DerivedTable {
            identifier,
            subquery,
            ..
        } => {
            refs.record(&identifier, EntityType::DbPermanentTable.as_i32());
            walk_relational(subquery, refs);
        }
        InnerRelationPattern::Correlated(never) => match *never {},
    }
}

fn walk_domain(expr: &DomainExpression, refs: &mut Census) {
    match expr {
        DomainExpression::Application(func) => walk_function(func, refs),
        DomainExpression::Reference(Reference::Named(NamedReference(_)))
        | DomainExpression::Reference(Reference::Ordinal(_))
        | DomainExpression::Reference(Reference::Argument(_))
        | DomainExpression::Reference(Reference::Physical(_)) => {}
    }
}

/// A RELATION MADE ONE VALUE names the relation it compresses.
fn walk_scalar_relation(relation: &crate::pipeline::asts::core::ScalarRelation, refs: &mut Census) {
    if let crate::pipeline::asts::core::ScalarRelation::Named { identifier, .. } = relation {
        refs.record(&identifier, EntityType::DbPermanentTable.as_i32());
    }
    walk_relational(relation.body().body(), refs);
}

fn walk_function(func: &FunctionApplication, refs: &mut Census) {
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
            for arm in selection.arms().iter() {
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

fn walk_boolean(expr: &TruthExpression, refs: &mut Census) {
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
            refs.record(
                &addressing.identifier,
                EntityType::DbPermanentTable.as_i32(),
            );
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
            refs.record(
                &addressing.identifier,
                EntityType::DbPermanentTable.as_i32(),
            );
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

fn walk_functor_call(call: &FunctorCall, refs: &mut Census) {
    // A QUALIFIED callee is a reference to the entity it names — the
    // executable boundary asks the catalog which definitions reach a
    // runtime-served relation, and the callee is how a body reaches one.
    // An unqualified callee stays unrecorded: the grounding contract reads
    // unqualified rows as free data-namespace variables, which a callee is
    // not.
    if let Some(qualifier) = call.call().callee.qualifier() {
        refs.record_qualified(
            call.call().callee.name_text().to_string(),
            qualifier.spelled(),
            EntityType::DbPermanentTable.as_i32(),
        );
    }
    refs.record_callee(
        &call.call().callee.name_text(),
        call.call().callee.qualifier(),
    );
    let descriptor = crate::pipeline::asts::effects::descriptor_for_reference(&call.call().callee);
    for (position, member) in call.call().arguments.ho_members().enumerate() {
        match member {
            // A target designator names where a directive writes or creates;
            // it is not an input dependency of the defining family. The
            // source relation, including a landed pipe member, remains a
            // dependency and is walked on this same census.
            HoArgument::Relation(rel) | HoArgument::Rule(rel) => {
                let is_target = descriptor
                    .and_then(|descriptor| descriptor.params.get(position))
                    .is_some_and(|param| {
                        param.kind
                            == crate::pipeline::asts::effects::DirectiveParamKind::RelationTarget
                    });
                if !is_target {
                    walk_relational(rel, refs);
                }
            }
            HoArgument::Landed(rel) => walk_relational(rel, refs),
            HoArgument::Value(_) | HoArgument::Landing(_) | HoArgument::Skip => {}
        }
    }
    // A spread addresses columns of the operand and a star names the whole
    // of it; neither refers to anything.
    for expr in call.call().arguments.value_domains() {
        walk_domain(expr, refs);
    }
}

/// A COVER'S CALLABLE, walked as the form it is.
fn walk_callable(callable: &crate::pipeline::asts::core::Callable, refs: &mut Census) {
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
    refs: &mut Census,
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

fn walk_window_frame(frame: &WindowFrame, refs: &mut Census) {
    let mut walk_bound = |bound: &FrameBound| match bound {
        FrameBound::Preceding(expr) | FrameBound::Following(expr) => walk_domain(expr, refs),
        FrameBound::Unbounded | FrameBound::CurrentRow => {}
    };
    walk_bound(&frame.start);
    walk_bound(&frame.end);
}

fn walk_case(case: &CaseExpression, refs: &mut Census) {
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

fn walk_enclyph(enclyph: &crate::pipeline::asts::core::Enclyph, refs: &mut Census) {
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

fn walk_metadata_group(group: &crate::pipeline::asts::core::MetadataGroup, refs: &mut Census) {
    use crate::pipeline::asts::core::MetadataTarget;
    match &group.target {
        MetadataTarget::Enclyph(enclyph) => walk_enclyph(enclyph, refs),
        MetadataTarget::Group(nested) => walk_metadata_group(nested, refs),
    }
}

/// A publication item references what its value references. A spread names
/// columns of the operand rather than referring to a declared entity.
fn walk_out_item(item: &crate::pipeline::asts::core::OutItem, refs: &mut Census) {
    if let Some(expr) = item.value() {
        walk_domain(expr, refs);
    }
}

/// A reduction publishes one column: a value, or a metadata level whose
/// target holds the references.
fn walk_reduction_item(item: &crate::pipeline::asts::core::ReductionItem, refs: &mut Census) {
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

fn walk_group_spec(spec: &GroupSpec, refs: &mut Census) {
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

fn walk_access(spec: &Access, refs: &mut Census) {
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

fn walk_unary_operator(op: &PipeOp, refs: &mut Census) {
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

/// The qualifier a reference row records: the written spelling of its
/// route — an exact path as `a::b`, the self-relative child route as
/// `.::child` — which the reader resolves in the declaring world.
fn namespace_from_path(path: &NamespacePath) -> Option<String> {
    if path.is_empty() {
        None
    } else {
        Some(path.qualifier().spelled())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl::reconstruct;

    /// THE PRODUCTION CENSUS over one authored definition — the same road
    /// consultation takes, so a test cannot pass through a walk the catalog
    /// does not use.
    fn census(source: &str) -> Vec<ExtractedReference> {
        let group = reconstruct::group(source).expect("the source is one definition");
        census_of_group(&group)
            .finish()
            .expect("every clause body reads")
    }

    fn names(refs: &[ExtractedReference]) -> Vec<&str> {
        refs.iter().map(|r| r.name.as_str()).collect()
    }

    #[test]
    fn test_function_body_no_references() {
        // x * 2 has no entity references (just parameter lvars and literals)
        let refs = census("f:(x) :- x * 2");
        assert!(
            refs.is_empty(),
            "Function body 'x * 2' should have no references, got: {refs:?}"
        );
    }

    #[test]
    fn test_view_body_table_reference() {
        // users(*), balance > 1000 references the "users" table
        let refs = census("v(*) :- users(*), balance > 1000");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
        assert_eq!(refs[0].apparent_type, EntityType::DbPermanentTable.as_i32());
        assert_eq!(refs[0].namespace, None);
    }

    #[test]
    fn test_view_body_multiple_references() {
        let refs = census("v(*) :- users(*), orders(*)");
        assert_eq!(refs.len(), 2);
        assert!(names(&refs).contains(&"users"));
        assert!(names(&refs).contains(&"orders"));
    }

    #[test]
    fn test_view_body_with_pipe_preserves_table_ref() {
        let refs = census("v(*) :- users(*) |> (first_name, last_name)");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "users");
    }

    #[test]
    fn test_function_body_with_nested_function_call() {
        // `round:(x)` is a curried call and IS a reference; `round(x)` is a
        // regular SQL function and is not. Neither stands here: `x + 10` is
        // two leaves.
        assert!(census("f:(x) :- x + 10").is_empty());
    }

    /// A CLAUSE-LOCAL BINDING IS NOT A REFERENCE. Its spelling reaches
    /// the declaration in the same clause, so grounding admission — which
    /// judges every unqualified row against a data world — must never be
    /// handed one.
    #[test]
    fn a_clause_local_relation_binding_is_not_a_reference() {
        let refs = census("v(id) :- low(id) : users(*) |> (id)\n  low(id), orders(*) |> (id)");
        let names = names(&refs);
        assert!(
            names.contains(&"users"),
            "the body's own reads stand: {names:?}"
        );
        assert!(
            names.contains(&"orders"),
            "the body's own reads stand: {names:?}"
        );
        assert!(
            !names.contains(&"low"),
            "a clause-local binding is lexical, not a reference: {names:?}"
        );
    }

    /// The same law for a clause-local PARAMETERIZED binding, mentioned as
    /// a relation actual — the shape a post-hoc spelling filter would have
    /// had to be told about separately.
    #[test]
    fn a_clause_local_parameterized_binding_is_not_a_reference() {
        let refs = census("v(id) :- pass(T(*))(*) : T(*)\n  pass(users(*))(*) |> (id)");
        let names = names(&refs);
        assert!(
            names.contains(&"users"),
            "the body's own reads stand: {names:?}"
        );
        assert!(
            !names.contains(&"pass"),
            "a clause-local parameterized binding is lexical: {names:?}"
        );
    }

    /// A DEPENDENCY NAMED ONLY INSIDE A QUERY-LOCAL PARAMETERIZED BODY is
    /// still this definition's dependency. The census walks that body, so
    /// the row exists at the one boundary grounding judges — it does not
    /// surface when the local definition is finally invoked.
    #[test]
    fn a_dependency_named_only_in_a_scoped_parameterized_body_is_a_reference() {
        let refs = census(
            "v(id) :- wrap(T(*))(*) : T(*), missing(id) |> (id)\n  wrap(users(*))(*) |> (id)",
        );
        let names = names(&refs);
        assert!(
            names.contains(&"missing"),
            "the local body's own external read is recorded: {names:?}"
        );
        assert!(names.contains(&"users"), "so is the actual: {names:?}");
    }

    /// THE CONTROL: inside that same body, neither the local definition's
    /// own name nor its relation formal is a catalog hole.
    #[test]
    fn a_scoped_parameterized_definitions_name_and_formal_are_not_references() {
        let refs = census(
            "v(id) :- wrap(T(*))(*) : T(*), missing(id) |> (id)\n  wrap(users(*))(*) |> (id)",
        );
        let names = names(&refs);
        assert!(
            !names.contains(&"wrap"),
            "the definition's own name is lexical: {names:?}"
        );
        assert!(
            !names.contains(&"T"),
            "its relation formal is bound, not free: {names:?}"
        );
    }

    /// A SIBLING BINDING OF THE ENCLOSING BLOCK is lexical inside a
    /// query-local parameterized body too: the block's claims stand over
    /// the body, which is why the walk pushes rather than replaces.
    #[test]
    fn an_enclosing_blocks_binding_is_lexical_inside_a_scoped_body() {
        let refs = census(
            "v(id) :- low(id) : users(*) |> (id)\n  wrap(T(*))(*) : T(*), low(id) |> (id)\n  wrap(orders(*))(*) |> (id)",
        );
        let names = names(&refs);
        assert!(
            !names.contains(&"low"),
            "the enclosing block's binding is lexical here: {names:?}"
        );
        assert!(names.contains(&"users"));
        assert!(names.contains(&"orders"));
    }

    /// A DEFINITION'S OWN PARAMETER is a declaration standing over the
    /// whole body — judged where the binding structure is, not filtered
    /// out of the finished list afterwards.
    #[test]
    fn a_declared_parameter_is_not_a_reference() {
        let refs = census("v(T(*))(*) :- T(*), users(*) |> (id)");
        assert_eq!(names(&refs), vec!["users"], "only the catalog read stands");
    }

    /// A CREATION TARGET IS AN OUTPUT ROLE, not an input dependency. The
    /// landed source still enters the census, while the target is admitted
    /// by the directive's descriptor and therefore cannot make grounding
    /// reject an otherwise complete definition.
    #[test]
    fn a_creation_target_is_not_a_definition_dependency() {
        let refs = census(
            "stage!(*) :- users(*) |> temp_table!(made(*))(*)\n\
             stage!(*) :- absent_users(*) |> temp_table!(made(*))(*)",
        );
        assert_eq!(
            names(&refs),
            vec!["users", "absent_users"],
            "only landed sources enter the dependency census: {refs:?}"
        );
    }
}
