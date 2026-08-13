// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! ResolverFold — the resolver as an AstTransform<Unresolved, Resolved>.
//!
//! All recursive calls within resolve_relational_impl go through
//! `self.resolve_relational()` / `self.resolve_child()` — never a
//! temporary fold via the free function wrapper.
//!
//! Scope stack (`push_scope`/`pop_scope`) manages outer_context and grounding
//! at recursion boundaries. Expression-level hooks (transform_sigma,
//! transform_operator) read from `self.available` / `self.in_correlation`.
//!
//! The free function in mod.rs remains for callers outside this file
//! (relation_resolver, predicates, subqueries, etc.).
use crate::pipeline::asts::core::literals::column_ordinal_text;
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};
use delightql_types::SqlIdentifier;

use super::unification::ColumnReference;
use super::{BubbledState, DmlPipeKind, ResolutionConfig};
use crate::names::DmlVerb;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::operators::JoinType;
use crate::pipeline::asts::core::phases::{Resolved, Unresolved};
use crate::pipeline::asts::core::Chain;
use crate::pipeline::asts::core::ProbeAddressing;
use crate::pipeline::asts::core::ValueRow;
use crate::pipeline::asts::core::{
    Existence, Membership, MembershipSource, RelationalMembership, SigmaApplication,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::{ast_resolved, ast_unresolved};
use delightql_types::error::{DelightQLError, Result};

/// Scope frame — tracks context at recursion boundaries.
struct ResolverScope {
    outer_context: Option<Vec<crate::names::ColId>>,
    grounding: Option<ast_unresolved::GroundedPath>,
}

/// The resolver as an AstTransform<Unresolved, Resolved>.
///
/// Holds the EntityRegistry, config, and a scope stack. The `last_bubbled`
/// sidecar carries BubbledState out of transform_relational since the trait
/// return type is just `Result<Node<Q>>`.
pub(super) struct ResolverFold<'reg, 'db> {
    pub registry: &'reg mut crate::resolution::EntityRegistry<'db>,
    pub config: ResolutionConfig,
    scope: Vec<ResolverScope>,
    /// Populated by transform_relational, consumed by callers via take_bubbled().
    pub last_bubbled: Option<BubbledState>,
    /// Available columns for expression-level resolution. Set before calling
    /// transform_sigma / transform_operator / transform_domain / transform_boolean.
    pub(super) available: Vec<crate::names::ColId>,
    /// Columns belonging to the current lexical relation. In a correlated
    /// expression an unqualified reference binds here before considering the
    /// enclosing context.
    pub(super) local_available: Vec<crate::names::ColId>,
    /// What the correlated condition being resolved did with its names.
    /// Accumulated across the whole condition, because whether one reference
    /// reaching outward is a correlation or a mistake depends on what the
    /// OTHER references did.
    pub(super) correlation_witness: super::resolving::domain_expressions::simple::Witness,
    /// THE CELL THE COVER IS APPLYING. Set only while a cover (or a
    /// deferred callable-argument body) resolves its body for one cell:
    /// an open leaf met during that resolution becomes this expression,
    /// which is how the applying position spends the leaf BEFORE any
    /// closed resolved tree is minted. `None` everywhere else, where a
    /// leaf refuses instead.
    pub(super) cover_cell: Option<crate::pipeline::asts::resolved::DomainExpression>,
    /// Lexically visible qualified bindings for expression-level resolution.
    /// Kept separate from `available` because relational output identity and
    /// qualifier scope have different lifetime laws (notably set operations
    /// and pipe barriers).
    pub(super) qualifier_scope: Vec<crate::names::ScopeId>,
    /// Whether we're in a correlation context (for deferred validation).
    pub(super) in_correlation: bool,
    /// Pivot IN values for operator resolution.
    pivot_in_values: std::collections::HashMap<crate::names::Sym, Vec<String>>,
    /// The self-name of the access whose INTERIOR this fold resolves —
    /// the alias if authored, the access name otherwise. Inside the
    /// parens the access is one relation under that name whatever stage
    /// its interior has reached, so spine stages keep it answering.
    pub(super) interior_self: Option<crate::names::Sym>,
    /// Output columns from the last operator resolution (sidecar like last_bubbled).
    last_operator_output: Option<Vec<crate::names::ColId>>,
    /// Pending join input for higher-order caller carrying.
    /// Set by the Join handler when the right side is an HO TVF; consumed by resolve_tvf
    /// if the HO view has free scalar params.
    pub(super) pending_ho_join_input: Option<ast_unresolved::Chain>,
    /// Columns published by the pending input occurrence. An absorbed input is
    /// represented by its carrier inside the higher-order body, so these exact
    /// columns must not remain visible there as a second outer occurrence.
    pub(super) pending_ho_join_columns: Option<Vec<crate::names::ColId>>,
    /// Set to true when resolve_tvf absorbed the pending join input into its clauses.
    pub(super) ho_join_input_absorbed: bool,
    /// Nonzero while folding a curried callee's CODE positions: `id:()`
    /// standing there is a callable handed to a formal, not an invocation,
    /// so the instantiation road must not fire on it.
    pub(super) cfe_code_suppression: usize,
}

fn validate_witness_membership(
    probe: &ast_unresolved::Probe,
    rows: &crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::ValueRow>,
) -> Result<()> {
    for header in probe.values() {
        let name = match header {
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(_),
            ) => continue,
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn { name, .. },
            ))) => name,
            _ => {
                return Err(DelightQLError::validation_error_categorized(
                    "resolution/anon/witness_shape",
                    "every witness header must be a ground value or a unifying lvar",
                    "drop the witness marker for relational function headers",
                ))
            }
        };
        let repeated = rows.iter().any(|row| {
            row.values().any(|candidate| {
                matches!(candidate,
                    ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn { name: candidate_name, .. })))
                        if SqlIdentifier::str_eq(candidate_name.as_str(), name)
                )
            })
        });
        if repeated {
            return Err(DelightQLError::validation_error_categorized(
                "resolution/anon/header_row_lvar",
                format!(
                    "lvar '{}' appears both as a header and in the data rows of the same anonymous table",
                    name
                ),
                "the header is the probe and a row lvar is a candidate — probing a column against itself is vacuously true; drop the self-candidate or rename the header",
            ));
        }
    }
    Ok(())
}

impl<'reg, 'db> ResolverFold<'reg, 'db> {
    pub fn new(
        registry: &'reg mut crate::resolution::EntityRegistry<'db>,
        config: ResolutionConfig,
        outer_context: Option<Vec<crate::names::ColId>>,
        grounding: Option<ast_unresolved::GroundedPath>,
    ) -> Self {
        Self {
            registry,
            config,
            scope: vec![ResolverScope {
                outer_context,
                grounding,
            }],
            last_bubbled: None,
            available: vec![],
            local_available: vec![],
            correlation_witness: Default::default(),
            cover_cell: None,
            qualifier_scope: vec![],
            in_correlation: false,
            pivot_in_values: std::collections::HashMap::new(),
            interior_self: None,
            last_operator_output: None,
            pending_ho_join_input: None,
            pending_ho_join_columns: None,
            ho_join_input_absorbed: false,
            cfe_code_suppression: 0,
        }
    }

    pub fn current_outer_context(&self) -> Option<&[crate::names::ColId]> {
        self.scope.last().and_then(|s| s.outer_context.as_deref())
    }

    pub fn current_grounding(&self) -> Option<&ast_unresolved::GroundedPath> {
        self.scope.last().and_then(|s| s.grounding.as_ref())
    }

    fn resolve_glob_scope(
        &self,
        qualifier: &delightql_types::SqlIdentifier,
    ) -> Result<crate::names::ScopeId> {
        let spelling = self
            .registry
            .identities
            .intern(qualifier.as_str(), qualifier.is_stropped());
        let qualifier_sym = self.registry.identities.canonical(spelling);
        let dedup = |scopes: Vec<crate::names::ScopeId>| {
            scopes.into_iter().fold(Vec::new(), |mut unique, scope| {
                if !unique.contains(&scope) {
                    unique.push(scope);
                }
                unique
            })
        };
        // A whole-heading correlation addresses an OPERAND's own heading, so
        // the scopes the statement still names answer first. A corresponding
        // union republishes each arm's columns into the merged heading, and
        // those republished columns keep the arm's answering address — asking
        // the glob road first would find the merge as well as the arm and
        // read one written name as an ambiguity.
        let named = dedup(
            self.qualifier_scope
                .iter()
                .copied()
                .filter(|scope| self.registry.identities.answers_to(*scope) == Some(qualifier_sym))
                .collect(),
        );
        if let [scope] = named.as_slice() {
            return Ok(*scope);
        }
        let mut scopes = self
            .registry
            .identities
            .qualified_glob(qualifier_sym, &self.available)
            .into_iter()
            .map(|column| self.registry.identities.scope_of(column))
            .collect::<Vec<_>>();
        scopes.extend(named);
        let scopes = dedup(scopes);
        match scopes.as_slice() {
            [scope] => Ok(*scope),
            [] => Err(DelightQLError::validation_error_categorized(
                "resolution/setop/correlation_owner",
                format!(
                    "set-operation correlation qualifier '{}' does not name a visible operand",
                    qualifier
                ),
                "qualify each whole-heading reference by an operand name or alias",
            )),
            _ => Err(DelightQLError::validation_error_categorized(
                "resolution/setop/correlation_owner",
                format!(
                    "set-operation correlation qualifier '{}' names more than one visible operand",
                    qualifier
                ),
                "use distinct operand aliases",
            )),
        }
    }

    fn push_scope(
        &mut self,
        outer: Option<Vec<crate::names::ColId>>,
        grounding: Option<ast_unresolved::GroundedPath>,
    ) {
        self.scope.push(ResolverScope {
            outer_context: outer,
            grounding,
        });
    }

    fn pop_scope(&mut self) {
        self.scope.pop();
    }

    /// Open a scope with NO outer context, for a body whose binders are
    /// declared rather than inherited. A declared mode's output cells are
    /// that: their only names are the head's inputs, so a name that misses
    /// must refuse rather than reach the enclosing row.
    pub(super) fn push_declared_scope(&mut self) {
        self.push_scope(None, None);
    }

    pub(super) fn pop_declared_scope(&mut self) {
        self.pop_scope();
    }

    /// Namespace-aware fallback for sigma classification: does a bare guard
    /// functor name a relation? Asks the SAME resolution authority the
    /// relation path uses (`resolve_entity_with_alias`, which honors
    /// `config.resolution_namespace` and enlistment edges) and accepts the
    /// relation-shaped answers: physical/mounted tables (`DatabaseEntity`),
    /// plan-materialized relations, consulted views, consulted facts.
    /// Query-local CTEs and built-in functions are deliberately NOT accepted
    /// — guards on those keep today's behavior.
    fn functor_is_relation_entity(
        &mut self,
        functor: &delightql_types::SqlIdentifier,
    ) -> Result<bool> {
        use crate::resolution::{resolve_entity_with_alias, ResolutionResult};
        Ok(matches!(
            resolve_entity_with_alias(
                functor,
                None,
                self.registry,
                self.config.resolution_namespace.as_deref(),
            )?,
            ResolutionResult::DatabaseEntity(_)
                | ResolutionResult::MaterializedRelation(_)
                | ResolutionResult::ConsultedView { .. }
        ))
    }

    /// Push scope, resolve child through self.resolve_relational(), pop scope.
    /// Use for recursive calls that need DIFFERENT context than the current scope.
    fn resolve_child(
        &mut self,
        child: ast_unresolved::Chain,
        outer: Option<Vec<crate::names::ColId>>,
        grounding: Option<ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        self.push_scope(outer, grounding);
        let result = self.resolve_relational(child);
        self.pop_scope();
        result
    }

    /// Convenience: transform_relational + extract BubbledState.
    pub fn resolve_relational(
        &mut self,
        expr: ast_unresolved::Chain,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        let resolved = self.transform_relational(expr)?;
        let bubbled = self
            .last_bubbled
            .take()
            .expect("BubbledState must be set by transform_relational");
        Ok((resolved, bubbled))
    }

    /// Core relational resolution logic.
    ///
    /// Reads outer_context and grounding from the scope stack — no parameters needed.
    #[stacksafe::stacksafe]
    pub(super) fn resolve_relational_impl(
        &mut self,
        expr: ast_unresolved::Chain,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        let outer_context: Option<Vec<crate::names::ColId>> =
            self.current_outer_context().map(|s| s.to_vec());
        let grounding: Option<ast_unresolved::GroundedPath> = self.current_grounding().cloned();

        // Borrow as refs for compatibility with existing code
        let outer_context = outer_context.as_deref();
        let grounding = grounding.as_ref();

        // The fold reads the chain from the OUTSIDE in: the last
        // continuation is the one whose operand is everything before it,
        // which IS the chain minus that continuation.
        // The base of the fold is the chain's READ: the head together with
        // the access its own parens asked for. Resolving a mention needs
        // both — which dimensions the read publishes is what the access
        // says — so the two are never handed to this authority apart.
        // The base of the fold is the chain's READ: the head together with
        // the access its own parens asked for. Both reach one authority.
        let mut expr = expr;
        let Some(last) = expr.pop_step() else {
            let (head, access, _) = expr.split_head_access();
            return match head {
                ast_unresolved::Grelex::Reference(rel) => {
                    self.resolve_relation_impl(rel, access, outer_context, grounding)
                }
                ast_unresolved::Grelex::Literal(anon) => {
                    self.resolve_anon_table_impl(anon, outer_context, grounding)
                }
            };
        };
        match last {
            // A dimension access standing on the relation the chain has
            // built. It resolves in the SAME run the pipes do — the
            // available columns, qualifier scope, liminal classification and
            // DML shape a pipe segment sees are what an access sees, and
            // splitting the run is what would give them two contexts.
            step @ ast_unresolved::Continuation::Access { .. } => {
                expr.continuations.push(step);
                self.r_resolve_pipe(expr, outer_context)
            }
            ast_unresolved::Continuation::Restrict {
                condition, origin, ..
            } => self.r_resolve_filter(expr, condition, origin, outer_context, grounding),

            // A whole-heading correlation names two ARMS. Resolution answers
            // each spelling with the scope that arm published; which PAIR of
            // the run they are is the refiner's question, not this one's.
            ast_unresolved::Continuation::Correlate { whole, .. } => {
                let (resolved, bubbled) = self.resolve_relational(expr)?;
                let cpr_schema = super::extract_cpr_schema(&resolved);
                // A correlation arm is answered against the SOURCE's
                // qualifier scopes — the arms of the run it stands on —
                // exactly as a predicate's qualified reference is.
                self.qualifier_scope = bubbled.qualifier_scope.clone();
                if let Some(outer) = outer_context {
                    for column in outer {
                        let scope = self.registry.identities.scope_of(*column);
                        if !self.qualifier_scope.contains(&scope) {
                            self.qualifier_scope.push(scope);
                        }
                    }
                }
                let whole = match whole {
                    ast_unresolved::WholeHeading::ByName { left, right } => {
                        ast_resolved::WholeHeading::ByName {
                            left: self.resolve_glob_scope(&left)?,
                            right: self.resolve_glob_scope(&right)?,
                        }
                    }
                    ast_unresolved::WholeHeading::ByPosition { left, right } => {
                        ast_resolved::WholeHeading::ByPosition {
                            left: self.resolve_glob_scope(&left)?,
                            right: self.resolve_glob_scope(&right)?,
                        }
                    }
                };
                Ok((
                    resolved.then(ast_resolved::Continuation::Correlate { whole, cpr_schema }),
                    bubbled,
                ))
            }

            ast_unresolved::Continuation::Bound { bound, .. } => {
                self.r_resolve_bound(expr, bound, outer_context, grounding)
            }

            ast_unresolved::Continuation::Destructure {
                source,
                pattern,
                mode,
                ..
            } => self.r_resolve_destructure(expr, *source, pattern, mode, outer_context, grounding),

            ast_unresolved::Continuation::Member {
                rhs,
                correlation,
                join_type,
                ..
            } => {
                if direct_dml_terminal(&expr)? || direct_dml_terminal(&rhs)? {
                    return Err(dml_multi_terminal_error());
                }
                self.r_resolve_join(expr, rhs, correlation, join_type, outer_context, grounding)
            }

            // The trailing pipes resolve as one run: the chain already holds
            // them flat, so there is no pipe-spine recursion to eliminate.
            // The structural forms — ordering, reposition, meta, the
            // witnesses, drill and narrowing — are steps of the same run.
            operator @ (ast_unresolved::Continuation::Pipe { .. }
            | ast_unresolved::Continuation::Structural(_)) => {
                expr.continuations.push(operator);
                self.r_resolve_pipe(expr, outer_context)
            }

            ast_unresolved::Continuation::BagOp { operator, arm, .. } => {
                if direct_dml_terminal(&expr)? || direct_dml_terminal(&arm)? {
                    return Err(dml_multi_terminal_error());
                }
                self.r_resolve_set_op(operator, expr, arm, outer_context, grounding)
            }

            ast_unresolved::Continuation::ErJoin(step) if step.transitive => self
                .r_resolve_er_transitive(
                    expr,
                    step.rhs,
                    step.left_spelling,
                    step.right_spelling,
                    step.context,
                    outer_context,
                    grounding,
                ),

            ast_unresolved::Continuation::ErJoin(step) => {
                // A direct edge run: the head plus every `&` step, read as
                // the pair sequence the resolver expands.
                expr.continuations
                    .push(ast_unresolved::Continuation::ErJoin(step));
                if !matches!(expr.head, ast_unresolved::Grelex::Reference(_)) {
                    return Err(er_operand_error());
                }
                let (head, steps) = expr.split_read();
                let mut relations = vec![head];
                let mut term_spellings = Vec::new();
                let mut contexts = Vec::new();
                for continuation in steps {
                    let ast_unresolved::Continuation::ErJoin(step) = continuation else {
                        return Err(er_operand_error());
                    };
                    if term_spellings.is_empty() {
                        term_spellings.push(step.left_spelling);
                    }
                    term_spellings.push(step.right_spelling);
                    contexts.push(step.context);
                    relations.push(step.rhs);
                }
                self.r_resolve_er_join_chain(
                    relations,
                    term_spellings,
                    contexts,
                    outer_context,
                    grounding,
                )
            }
        }
    }

    // ── Extracted match-arm methods ─────────────────────────────────────

    fn r_resolve_er_join_chain(
        &mut self,
        relations: Vec<ast_unresolved::Chain>,
        term_spellings: Vec<String>,
        contexts: Vec<Option<String>>,
        outer_context: Option<&[crate::names::ColId]>,
        grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        let context = super::er_chain_context(&contexts)?;

        // The published schema is the pair schema (GROUNDING-AND-MENTION):
        // a direct call publishes its WRITTEN terms' exports — helpers and
        // computed body columns never cross the entity boundary.
        let published: Vec<String> = relations
            .iter()
            .map(|rel| super::er_endpoint(rel).0)
            .collect();

        Ok(super::expand_er_join_chain(
            relations,
            &term_spellings,
            &context,
            self.registry,
            outer_context,
            &self.config,
            grounding,
            Some(published),
        )?)
    }

    fn r_resolve_er_transitive(
        &mut self,
        left: ast_unresolved::Chain,
        right: ast_unresolved::Chain,
        left_spelling: String,
        right_spelling: String,
        context: Option<String>,
        outer_context: Option<&[crate::names::ColId]>,
        grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        let context = super::er_chain_context(std::slice::from_ref(&context))?;

        Ok(super::expand_er_transitive_join(
            left,
            right,
            &left_spelling,
            &right_spelling,
            &context,
            self.registry,
            outer_context,
            &self.config,
            grounding,
        )?)
    }

    /// One bag STEP: the chain-so-far against one arm. A three-arm
    /// expression reaches here twice, and each visit settles exactly the
    /// pair it sees — the heading a step publishes is the one its own two
    /// operands make.
    fn r_resolve_set_op(
        &mut self,
        operator: ast_unresolved::SetOperator,
        left: ast_unresolved::Chain,
        arm: ast_unresolved::Chain,
        _outer_context: Option<&[crate::names::ColId]>,
        _grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        let (left, left_bubbled) = self.resolve_relational(left)?;
        let (arm, arm_bubbled) = self.resolve_relational(arm)?;

        let left_schema = super::extract_cpr_schema(&left);
        let arm_schema = super::extract_cpr_schema(&arm);

        let final_schema = match operator {
            // Corresponding is the only operator that widens: its output is
            // the ordered union of both headings. Chained steps merge left
            // to right, which is the same heading a written group would
            // publish because the merge keeps first-appearance order.
            ast_unresolved::SetOperator::UnionCorresponding => super::build_corresponding_schema(
                &[left_schema, arm_schema],
                &self.registry.identities,
            )?,
            // Positional, smart, and minus all publish the LEFT heading;
            // the arm only has to be shaped to fit it.
            ast_unresolved::SetOperator::UnionAllPositional
            | ast_unresolved::SetOperator::SmartUnionAll
            | ast_unresolved::SetOperator::MinusCorresponding => {
                super::validate_set_operation_schemas(
                    &operator,
                    left_schema,
                    arm_schema,
                    &self.registry.identities,
                )?;
                left_schema
            }
        };

        let mut bubbled = BubbledState::resolved(
            self.registry
                .identities
                .known_heading(final_schema)?
                .to_vec(),
            &self.registry.identities,
        );
        // A set operation has a merged output schema, but its immediately
        // attached correlation predicate is resolved in the lexical scope of
        // the operands. Preserve those bindings independently of i_provide.
        bubbled.qualifier_scope = left_bubbled
            .qualifier_scope
            .iter()
            .chain(arm_bubbled.qualifier_scope.iter())
            .cloned()
            .collect();
        Ok((left.bag_op(operator, arm, None, final_schema), bubbled))
    }

    fn r_resolve_filter(
        &mut self,
        source: ast_unresolved::Chain,
        condition: ast_unresolved::TruthExpression,
        origin: ast_resolved::FilterOrigin,
        outer_context: Option<&[crate::names::ColId]>,
        grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        // Check for EXISTS in the condition and handle through registry
        {
            if let ast_unresolved::TruthExpression::Existence(Existence {
                relation: subquery,
                polarity,
                addressing:
                    ProbeAddressing {
                        identifier: _,
                        using_columns,
                    },
            }) = &condition
            {
                // The correlation context and the source that survives are one
                // resolution. Resolving the source a second time to build the
                // context mints a parallel set of occurrences, and the
                // subquery's outer references bind to the set that is then
                // thrown away — leaving them owned by a scope no FROM entry
                // establishes.
                let (resolved_source, source_bubbled) = self.resolve_relational(source.clone())?;
                let source_schema = super::extract_cpr_schema(&resolved_source);
                let source_scope = source_schema;
                let available_columns = self.registry.identities.known_heading(source_scope)?;

                // === INLINED handle_exists_subquery START ===
                let resolved_subquery = {
                    let subquery_expr = *subquery.clone();
                    let mut enriched_context = match outer_context {
                        Some(outer) => {
                            let mut combined = outer.to_vec();
                            combined.extend(available_columns.iter().copied());
                            combined
                        }
                        None => available_columns.to_vec(),
                    };

                    // Interdependent EXISTS (e.g.
                    // `+orders(...), +order_items(...), +products(, order_items.x = products.y)`)
                    // reference tables from sibling EXISTS scopes. Those columns
                    // are read off the source resolved just above — the one the
                    // statement will contain — for the same reason the source is
                    // resolved once.
                    super::collect_exists_table_columns_in_scope(
                        &resolved_source,
                        &self.registry.identities,
                        &mut enriched_context,
                    )?;

                    // Config swap for EXISTS: validate_in_correlation = true
                    let exists_config = ResolutionConfig {
                        validate_in_correlation: true,
                        ..self.config.clone()
                    };
                    let saved_config = std::mem::replace(&mut self.config, exists_config);
                    let grounding_for_exists = grounding.cloned();
                    let result = self.resolve_child(
                        subquery_expr,
                        Some(enriched_context),
                        grounding_for_exists,
                    );
                    self.config = saved_config;
                    let (resolved_subquery, _) = result?;

                    resolved_subquery
                };
                // === INLINED handle_exists_subquery END ===

                // Synthesize correlation predicates from USING columns
                let final_subquery = super::resolving::synthesize_using_correlation(
                    resolved_subquery,
                    using_columns,
                    &available_columns.to_vec(),
                    &self.registry.identities,
                )?;

                // Create resolved EXISTS condition
                let resolved_condition = ast_resolved::TruthExpression::Existence(Existence {
                    polarity: *polarity,
                    relation: Box::new(final_subquery),
                    addressing: (),
                });

                return Ok((
                    resolved_source.then(ast_resolved::Continuation::Restrict {
                        condition: resolved_condition,
                        origin: origin,
                        cpr_schema: source_schema,
                    }),
                    source_bubbled,
                ));
            }
        }

        let (resolved_source, source_bubbled) = self.resolve_relational(source)?;

        let source_schema = super::extract_cpr_schema(&resolved_source);

        // Get columns for condition resolution.
        // Prefer source_bubbled.i_provide — it carries the user alias (e.g., `as a`)
        // so qualified refs like `a.first_name` can match. The cpr_schema on the
        // AST node may have internal body names (e.g., from ConsultedView expansion)
        // that don't reflect the alias.
        let source_columns = if !source_bubbled.i_provide.is_empty() {
            source_bubbled.i_provide.clone()
        } else {
            // An opaque source enumerates nothing here. That is not the
            // claim that it publishes nothing: an operation that does not
            // inspect columns carries it, and one that names a column
            // refuses where the name is resolved.
            self.registry
                .identities
                .heading(source_schema)
                .columns_seen()
        };

        // Combine outer context with source columns for correlation support
        // This allows correlated predicates to reference both:
        // - Columns from the current source (e.g., orders.user_id)
        // - Columns from outer context (e.g., CFE parameters like buyer_id)
        let available_columns = if let Some(outer) = outer_context {
            let mut combined = outer.to_vec();
            combined.extend(source_columns.iter().copied());
            combined
        } else {
            source_columns.clone()
        };

        // Resolve condition using combined schema (source + outer context)
        // Use outer_context presence as heuristic for correlation contexts,
        // unless validate_in_correlation is set (EXISTS subqueries where
        // the full column set is known and validation is safe)
        self.in_correlation = outer_context.is_some() && !self.config.validate_in_correlation;
        self.local_available = source_columns;
        self.available = available_columns;
        self.qualifier_scope = source_bubbled.qualifier_scope.clone();
        if let Some(outer) = outer_context {
            for column in outer {
                let scope = self.registry.identities.scope_of(*column);
                if !self.qualifier_scope.contains(&scope) {
                    self.qualifier_scope.push(scope);
                }
            }
        }
        let saved_witness = std::mem::take(&mut self.correlation_witness);
        let resolved_condition = self.transform_boolean(condition)?;
        let witness = std::mem::replace(&mut self.correlation_witness, saved_witness);
        // A condition attached to an interior relation whose every name was
        // answered by the ENCLOSING row constrains nothing about the relation
        // it is attached to: the subquery is not correlated, and the predicate
        // it appears to carry is decided outside it. Silently that reads as a
        // plausible number — `country = users.country` over a relation
        // publishing only `n` becomes `users.country = users.country`.
        //
        // PROVISIONAL: whether this refuses, and behind which danger gate,
        // is still an open question; this keeps the shape loud until it is
        // answered rather than letting a wrong answer become a baseline.
        if witness.escaped && !witness.anchored {
            return Err(DelightQLError::validation_error_categorized(
                "resolution/correlation/uncorrelated_predicate",
                "every name in this condition was answered by the enclosing relation,                  so it constrains nothing about the relation it is attached to — an                  interior relation reads the heading its source PUBLISHES, and a name                  absent from that heading reaches outward",
                "attach the condition where the columns it names are published — inside                  the argument rather than on the call's result — or move it out of the                  interior relation, which is where a condition about the enclosing row                  belongs",
            ));
        }

        // A restriction drops rows and touches no heading, so what it
        // publishes is what its source published.
        Ok((
            resolved_source.then(ast_resolved::Continuation::Restrict {
                condition: resolved_condition,
                origin,
                cpr_schema: source_schema,
            }),
            source_bubbled,
        ))
    }

    fn r_resolve_join(
        &mut self,
        left: ast_unresolved::Chain,
        right: ast_unresolved::Chain,
        correlation: Option<ast_unresolved::MemberCorrelation>,
        join_type: Option<JoinType>,
        outer_context: Option<&[crate::names::ColId]>,
        grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        // Inverted CTE strategy: a relational call may be a higher-order view;
        // stash the unresolved left so call resolution can absorb it if needed.
        let right_is_tvf = matches!(
            right.as_read_relation(),
            Some(ast_unresolved::Relation::FunctorCall { call: _, .. })
        );
        let pending_join_input = right_is_tvf.then(|| left.clone());
        let (resolved_left, left_bubbled) = self.resolve_relational(left)?;

        let left_schema = super::extract_cpr_schema(&resolved_left);
        let left_scope = left_schema;
        let left_columns = self.registry.identities.known_heading(left_scope)?;

        // For EXISTS joins, we need to combine outer context with left columns
        let right_context: Vec<crate::names::ColId> = if let Some(outer) = outer_context {
            let mut combined = outer.to_vec();
            combined.extend(left_columns.clone());
            combined
        } else {
            left_columns.to_vec()
        };

        if right_is_tvf {
            self.pending_ho_join_input = pending_join_input;
            self.pending_ho_join_columns = Some(left_columns.to_vec());
            self.ho_join_input_absorbed = false;
        }

        // Check if right side uses positional patterns and needs unification
        let right_anon = match (&right.head, right.continuations.is_empty()) {
            (ast_unresolved::Grelex::Literal(anon), true) => Some(anon.clone()),
            _ => None,
        };
        let (resolved_right, right_bubbled, positional_correlation) = if let Some(
            ast_unresolved::AnonRelation {
                table,
                alias: anon_alias,
                ..
            },
        ) = right_anon
        {
            let column_headers = table.body.header.as_ref().map(|header| {
                header
                    .iter()
                    .map(|item| {
                        item.term()
                            .expect("a tabular header slot has a domain term")
                    })
                    .collect::<Vec<_>>()
            });
            // Handle anonymous table unification
            let (resolved, bubbled) = self.resolve_child(
                right.clone(),
                Some(right_context.clone()),
                grounding.cloned(),
            )?;

            // Extract right-side columns from resolved anonymous table
            let right_cpr_schema = super::helpers::extraction::extract_cpr_schema(&resolved);
            let right_scope = right_cpr_schema;
            let right_columns = self.registry.identities.known_heading(right_scope)?;

            // Check for unification opportunities based on column
            // names. An ALIASED anon table is a closed relation —
            // its headers declare under the alias, so they neither
            // unify bare nor collide; the refusal-free probe only
            // detects membership shape (which refuses the alias).
            // Witness markers keep the full scan: they demand
            // membership shape outright.
            let anon_correlation = if let Some(headers) = column_headers.as_ref() {
                if anon_alias.is_some() {
                    super::join_resolver::aliased_anon_would_unify(
                        headers,
                        &left_columns.to_vec(),
                        &self.registry.identities,
                    )
                } else {
                    super::detect_anonymous_table_unification(
                        headers,
                        &left_columns.to_vec(),
                        &right_columns.to_vec(),
                        &self.registry.identities,
                    )?
                }
            } else {
                None
            };

            // Membership routing. An anonymous table whose every
            // header is a probe — a ground literal, or an lvar
            // that unifies with a column in scope — is not a
            // relation but a membership test: the probe tuple
            // must (or, under \+, must not) match one of the
            // rows. The plain comma form takes it whenever every column
            // unifies, because multi-row
            // unification is membership — a duplicate row
            // cannot multiply outer rows, and a null component
            // is a value the probe can match.
            if let Some(membership) = super::join_resolver::build_anon_membership(
                column_headers.as_deref(),
                &anon_correlation,
                &left_columns.to_vec(),
                &resolved,
                anon_alias.as_ref(),
                &self.registry.identities,
            )? {
                let filter = resolved_left.then(ast_resolved::Continuation::Restrict {
                    condition: membership,
                    origin: ast_resolved::FilterOrigin::UserWritten,
                    cpr_schema: left_schema,
                });
                return Ok((filter, left_bubbled));
            }

            (resolved, bubbled, anon_correlation)
        } else if let Some(rel) = right.as_read_relation().cloned() {
            let right_access = right.head_access().cloned();
            match (&rel, right_access.as_ref()) {
                (
                    ast_unresolved::Relation::Ground {
                        mention:
                            ast_unresolved::GroundMention::Named {
                                identifier, alias, ..
                            },
                        outer,
                        cpr_schema: _,
                    },
                    Some(ast_unresolved::Access::Slots(patterns)),
                ) => {
                    // Use the SAME pattern resolver that single tables use!
                    let table_name = &identifier.name;
                    // Get table schema — check CTEs first, then database.
                    // BOTH branches yield rich ColumnMetadata: squeezing a
                    // CTE's columns through ColumnInfo (the narrow
                    // database-boundary type) would strip every value fact
                    // the thin type cannot hold — the interior heading of a
                    // staged tree dying BY TYPE, with nullability
                    // hardcoded true. Value facts are conserved (the
                    // carrying law); only identity is rebuilt below.
                    let maybe_table_columns: Option<Vec<crate::names::ColId>> = if let Some(
                        cte_schema,
                    ) =
                        self.registry.query_local.lookup_cte(table_name)
                    {
                        Some(
                            self.registry
                                .identities
                                .known_heading(*cte_schema)?
                                .to_vec(),
                        )
                    } else {
                        let resolved_database = if !identifier.namespace_path.is_empty() {
                            self.registry
                                .database
                                .lookup_table_with_namespace_qualified(
                                    &identifier.namespace_path,
                                    table_name,
                                )?
                                .map(|(schema, connection, canonical, backend_schema)| {
                                    self.registry.track_connection_id(connection);
                                    (schema, Some(canonical), backend_schema)
                                })
                        } else {
                            match crate::resolution::resolve_entity_with_alias(
                                table_name,
                                alias.as_ref(),
                                self.registry,
                                self.config.resolution_namespace.as_deref(),
                            )? {
                                crate::resolution::ResolutionResult::DatabaseEntity(entity)
                                | crate::resolution::ResolutionResult::MaterializedRelation(
                                    entity,
                                ) => {
                                    let crate::resolution::EntityDefinition::RelationSchema(schema) =
                                        entity.definition;
                                    Some((schema, entity.canonical_name, entity.backend_schema))
                                }
                                _ => None,
                            }
                        };

                        if let Some((schema, canonical, backend_schema)) = resolved_database {
                            super::relation_resolver::bind_physical_relation(
                                schema,
                                canonical.as_ref(),
                                backend_schema.as_deref(),
                                &self.registry.identities,
                            )?;
                            Some(self.registry.identities.known_heading(schema)?.to_vec())
                        } else {
                            None
                        }
                    };

                    if let Some(table_columns) = maybe_table_columns {
                        // CTE or database table — use existing mini-pipeline

                        // VALIDATE: slot pattern length must match table columns
                        if patterns.len() != table_columns.len() {
                            return Err(DelightQLError::validation_error(
                                format!(
                                    "Positional pattern incomplete - table '{}' has {} columns but pattern specifies {} elements",
                                    table_name, table_columns.len(), patterns.len()
                                ),
                                "Pattern references unknown table".to_string()
                            ));
                        }

                        // Rebuild identity for the pattern resolver (alias
                        // as table_name when present — the SQL-visible name,
                        // so qualified refs like `t.val` match) while
                        // CARRYING each column's value facts.
                        let visible_name = alias.as_deref().unwrap_or(table_name);
                        let source_scope = self.registry.identities.common_scope(&table_columns);
                        let visible_spelling = self.registry.identities.intern(visible_name, false);
                        let scope = source_scope.map_or_else(
                            || {
                                self.registry.identities.mint_scope(
                                    crate::names::ScopeOrigin::AnonRelation,
                                    crate::names::Hint::User(visible_spelling),
                                    None,
                                )
                            },
                            |of| {
                                self.registry.identities.mint_derived_scope(
                                    crate::names::ScopeOrigin::UserAlias { of },
                                    crate::names::Hint::User(visible_spelling),
                                )
                            },
                        );
                        for column in table_columns {
                            let (published, addressing, facts) = {
                                let published = self.registry.identities.published(column);
                                let addressing = self.registry.identities.addressing(column);
                                let facts = self.registry.identities.facts(column);
                                (published, addressing, facts)
                            };
                            self.registry.identities.mint_column(
                                scope,
                                crate::names::ColumnOrigin::Republished {
                                    from: column,
                                    how: crate::names::Republish::BoundaryExport,
                                },
                                published,
                                addressing,
                                facts,
                            );
                        }
                        let table_schema = self.registry.identities.known_heading(scope)?;

                        // Create join context with left columns
                        let join_ctx = super::JoinContext {
                            left_columns: left_columns.to_vec(),
                        };

                        // Use the SAME pattern resolver!
                        let frame = self.config.cfe_formal_frame.clone();
                        let scope = self.config.resolution_namespace.clone();
                        let depth = self.config.instantiation_depth.clone();
                        let pattern_resolver = super::PatternResolver::with_formals(
                            frame.as_deref(),
                            Some(super::SlotInstantiation {
                                scoped_cfes: &self.registry.query_local.scoped_cfes,
                                consult: &self.registry.consult,
                                lookup_scope: scope.as_deref(),
                                depth: &depth,
                            }),
                        );
                        let authored_spec = ast_unresolved::Access::Slots(patterns.clone());
                        let pattern_result = pattern_resolver.resolve_pattern(
                            &authored_spec,
                            &table_schema.to_vec(),
                            visible_name,
                            Some(&join_ctx),
                            &self.registry.identities,
                        )?;

                        let output_scope = pattern_result.output_scope;
                        let resolved_spec = pattern_result.resolved_spec(&authored_spec)?;

                        let resolved_expr = ast_resolved::Relation::ground_read(
                            resolved_spec,
                            *outer,
                            output_scope,
                        );

                        // Get bubbled state. As on the single-relation
                        // road: a pattern controls what the relation
                        // CONTRIBUTES, but its source columns remain
                        // addressable while the surrounding expression is
                        // formed — `employees.id` after
                        // `employees(zid, _, zmid)` resolves through the
                        // source scope (a following pipe stays the
                        // barrier). Without this, the history tier is
                        // dead in join scopes while alive everywhere else.
                        let mut bubbled = BubbledState::resolved(
                            pattern_result.output_columns.columns().to_vec(),
                            &self.registry.identities,
                        );
                        for column in table_schema.iter() {
                            let scope = self.registry.identities.scope_of(*column);
                            if !bubbled.qualifier_scope.contains(&scope) {
                                bubbled.qualifier_scope.push(scope);
                            }
                        }

                        // Generate USING condition if there are unification columns
                        let join_cond = if let Some(using_cols) = pattern_result.using_columns {
                            if !using_cols.is_empty() {
                                Some(super::join_resolver::create_using_condition(
                                    &using_cols,
                                    &self.registry.identities,
                                )?)
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        let resolved_expr = super::pattern_resolver::apply_local_constraints(
                            resolved_expr,
                            pattern_result.where_constraints,
                            output_scope,
                        );

                        (resolved_expr, bubbled, join_cond)
                    } else {
                        // Not CTE or database — likely a consulted entity.
                        // Route through the full resolver which handles consulted
                        // entities (views, facts) and applies positional patterns.
                        // The READ goes whole: rebuilding the relation without
                        // the access it was read under hands the resolver a
                        // mention nobody parameterized.
                        let right_expr = right.clone();
                        let (resolved, bubbled) = self.resolve_child(
                            right_expr,
                            Some(right_context.clone()),
                            grounding.cloned(),
                        )?;

                        // Derive join conditions: check which lvar names in the
                        // positional pattern match left-side column names.
                        let mut using_cols: Vec<SqlIdentifier> = Vec::new();
                        let mut seen = Vec::new();
                        for pattern in patterns {
                            if let Some(name) = pattern.binder().map(|binder| &binder.name) {
                                let spelling = self
                                    .registry
                                    .identities
                                    .intern(name.as_str(), name.is_stropped());
                                let symbol = self.registry.identities.canonical(spelling);
                                let matches_left = left_columns.iter().any(|column| {
                                    self.registry.identities.published_sym(*column) == Some(symbol)
                                });
                                if matches_left && !seen.contains(&symbol) {
                                    seen.push(symbol);
                                    using_cols.push(name.clone());
                                }
                            }
                        }
                        let join_cond = if using_cols.is_empty() {
                            None
                        } else {
                            Some(super::join_resolver::create_using_condition(
                                &using_cols,
                                &self.registry.identities,
                            )?)
                        };

                        (resolved, bubbled, join_cond)
                    }
                }
                (
                    ast_unresolved::Relation::Ground { .. },
                    Some(ast_unresolved::Access::Dequalify(using_cols)),
                ) => {
                    // Dequalify on consulted views (or any non-positional entity):
                    // resolve the entity, then create USING join condition from the
                    // specified columns.
                    let using_cols = using_cols.clone();
                    let (resolved, bubbled) =
                        self.resolve_child(right, Some(right_context.clone()), grounding.cloned())?;
                    let join_cond = if !using_cols.is_empty() {
                        Some(super::join_resolver::create_using_condition(
                            &using_cols,
                            &self.registry.identities,
                        )?)
                    } else {
                        None
                    };
                    (resolved, bubbled, join_cond)
                }
                (
                    ast_unresolved::Relation::Ground { .. },
                    Some(ast_unresolved::Access::DequalifyAll),
                ) => {
                    // DequalifyAll: resolve the right side, then compute
                    // shared columns between left and right as USING columns.
                    let (resolved, bubbled) =
                        self.resolve_child(right, Some(right_context.clone()), grounding.cloned())?;
                    let join_cond = super::join_resolver::create_using_all_condition(
                        &left_columns.to_vec(),
                        &bubbled.i_provide,
                        &self.registry.identities,
                    )?;
                    (resolved, bubbled, Some(join_cond))
                }
                _ => {
                    let (resolved, bubbled) =
                        self.resolve_child(right, Some(right_context.clone()), grounding.cloned())?;
                    (resolved, bubbled, None)
                }
            }
        } else {
            let (resolved, bubbled) =
                self.resolve_child(right, Some(right_context.clone()), grounding.cloned())?;
            (resolved, bubbled, None)
        };

        // Inverted CTE: if the right side absorbed the left, skip join assembly.
        // The right side's ConsultedView already contains the left as an internal CTE.
        if self.ho_join_input_absorbed {
            self.ho_join_input_absorbed = false;
            self.pending_ho_join_input = None;
            self.pending_ho_join_columns = None;
            return Ok((resolved_right, right_bubbled));
        }
        // Clean up pending state if not absorbed
        if right_is_tvf {
            self.pending_ho_join_input = None;
            self.pending_ho_join_columns = None;
            self.ho_join_input_absorbed = false;
        }

        // Join conditions need to be preserved and bubbled
        let mut join_bubbled = BubbledState::empty();
        // An authored member correlation is a CONDITION — a correspondence is
        // read off the access that directs it, so `Correspond` has no
        // inhabitant here and no arm to write.
        let resolved_condition =
            if let Some(ast_unresolved::MemberCorrelation::Condition(cond)) = correlation {
                // For now, keep the condition as None but bubble the needs.
                // The condition will be resolved later when filters are processed.
                let schema = self.registry.database.schema();
                let system = self.registry.database.system;
                let identities = std::rc::Rc::clone(&self.registry.identities);
                let cte_context = &mut self.registry.query_local.ctes;
                let (_unresolved_cond, cond_bubbled) = super::bubble_predicate_expression(
                    cond,
                    schema,
                    system,
                    cte_context,
                    Some(&left_columns.to_vec()),
                    &identities,
                )?;
                join_bubbled = cond_bubbled;
                None // Will be attached later via filter-to-join transformation
            } else {
                positional_correlation
            };

        // Handle USING deduplication if present
        let using_columns = super::extract_inline_using_columns(&resolved_right)
            .map(|columns| {
                columns
                    .into_iter()
                    .map(|name| {
                        let spelling = self.registry.identities.intern(&name, false);
                        self.registry.identities.canonical(spelling)
                    })
                    .collect::<Vec<crate::names::Sym>>()
            })
            .or_else(|| {
                // For positional patterns, the correspondence names them.
                resolved_condition
                    .as_ref()
                    .and_then(ast_resolved::MemberCorrelation::correspondence)
                    .map(|correspondence| correspondence.columns.clone())
            });

        // Which road named the join's USING columns, and whether one did.
        // A join that merges its key and one that repeats it differ only
        // here, and the difference is invisible in the rows.
        crate::probe::probe!(using, "dedup columns={using_columns:?}");
        // Combine schemas with USING deduplication.
        // Use i_provide (which carries user aliases like "a", "s") rather than
        // extract_cpr_schema (which may have internal body names from ConsultedView).
        // This ensures the join's cpr_schema reflects the external interface.
        let (combined_schema, combined_output) = {
            let left_cols = &left_bubbled.i_provide;
            let right_cols = &right_bubbled.i_provide;
            if left_cols.is_empty() && right_cols.is_empty() {
                // Neither operand offered columns to combine, so the join
                // publishes whatever they do. Its identity is still a join
                // OF THEM: taking the operands' own scopes keeps both
                // reachable and lets the heading capability follow, where a
                // fresh anonymous scope would lose both and claim a heading
                // of its own. An empty `i_provide` cannot tell a known
                // zero-column heading from one nobody enumerated; the
                // operands' scopes can.
                let left_scope = super::extract_cpr_schema(&resolved_left);
                let right_scope = super::extract_cpr_schema(&resolved_right);
                let output_scope = self.registry.identities.mint_scope(
                    crate::names::ScopeOrigin::Join {
                        left: left_scope,
                        right: right_scope,
                    },
                    crate::names::Hint::None,
                    None,
                );
                if self
                    .registry
                    .identities
                    .any_heading_opaque(&[left_scope, right_scope])
                {
                    self.registry.identities.mark_heading_opaque(output_scope);
                }
                (output_scope, Vec::new())
            } else {
                let left_scope = self
                    .registry
                    .identities
                    .common_scope(left_cols)
                    .unwrap_or_else(|| {
                        self.registry.identities.mint_scope(
                            crate::names::ScopeOrigin::AnonRelation,
                            crate::names::Hint::None,
                            None,
                        )
                    });
                let right_scope = self
                    .registry
                    .identities
                    .common_scope(right_cols)
                    .unwrap_or_else(|| {
                        self.registry.identities.mint_scope(
                            crate::names::ScopeOrigin::AnonRelation,
                            crate::names::Hint::None,
                            None,
                        )
                    });
                let output_scope = self.registry.identities.mint_scope(
                    crate::names::ScopeOrigin::Join {
                        left: left_scope,
                        right: right_scope,
                    },
                    crate::names::Hint::None,
                    None,
                );
                self.registry
                    .identities
                    .carry_qualified_from(left_scope, output_scope);
                self.registry
                    .identities
                    .carry_qualified_from(right_scope, output_scope);
                let using_names: Vec<_> = using_columns
                    .as_ref()
                    .into_iter()
                    .flatten()
                    .copied()
                    .collect();
                for column in left_cols {
                    self.registry.identities.republish_column(
                        *column,
                        output_scope,
                        crate::names::Republish::JoinArm,
                        self.registry.identities.published(*column),
                        self.registry.identities.addressing(*column),
                        |_| {},
                    );
                }
                for column in right_cols {
                    if self
                        .registry
                        .identities
                        .published_sym(*column)
                        .is_some_and(|name| using_names.contains(&name))
                    {
                        // The USING slot contributes no second heading column,
                        // but the right relation remains a live SQL arm.
                        self.registry
                            .identities
                            .carry_qualified(*column, output_scope);
                        continue;
                    }
                    self.registry.identities.republish_column(
                        *column,
                        output_scope,
                        crate::names::Republish::JoinArm,
                        self.registry.identities.published(*column),
                        self.registry.identities.addressing(*column),
                        |_| {},
                    );
                }
                (
                    output_scope,
                    self.registry
                        .identities
                        .known_heading(output_scope)?
                        .to_vec(),
                )
            }
        };

        // Create the join
        let result_expr = resolved_left.then(ast_resolved::Continuation::Member {
            rhs: resolved_right,
            correlation: resolved_condition,
            join_type: join_type,
            cpr_schema: combined_schema,
        });

        let mut state = BubbledState::combine(
            BubbledState::combine(left_bubbled, right_bubbled),
            join_bubbled,
        );
        state.i_provide = combined_output;
        Ok((result_expr, state))
    }

    /// Detect `ns::(*).liminal(…)` — a `liminal` interior drill whose source
    /// is a catalog functor (a Ground in sys::meta whose name ends in `::`,
    /// the wrapper naming convention of `register_catalog_wrapper`) — and
    /// build the wrapper query that carries the namespace's liminal
    /// ledger as a fourth tree-group column beside entities/namespaces/name
    /// (EFFECT-ALGEBRA §8, THE LIMINAL RELATION). Returns the pieces
    /// r_resolve_view_query needs; None falls through to the ordinary
    /// path (not a liminal drill, not a catalog functor, or no such
    /// namespace — the stock "Table not found" then reports as today).
    fn liminal_catalog_expansion(
        &self,
        base: &ast_unresolved::Chain,
        first_drill: Option<&crate::pipeline::asts::core::operators::AuthoredDrill>,
    ) -> Result<
        Option<(
            delightql_types::SqlIdentifier,
            ast_unresolved::Query,
            ast_unresolved::Access,
            Option<delightql_types::SqlIdentifier>,
            bool,
        )>,
    > {
        let Some(drill) = first_drill else {
            return Ok(None);
        };
        if drill.column != "liminal" {
            return Ok(None);
        }
        let (
            Some(ast_unresolved::Relation::Ground {
                mention:
                    ast_unresolved::GroundMention::Named {
                        identifier, alias, ..
                    },
                outer,
                ..
            }),
            Some(access),
        ) = (base.as_read_relation(), base.head_access())
        else {
            return Ok(None);
        };
        let ns_parts: Vec<&str> = identifier
            .namespace_path
            .items()
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        if ns_parts != ["sys", "meta"] {
            return Ok(None);
        }
        let wrapper_name = identifier.name.as_str();
        let Some(ns_typed) = wrapper_name.strip_suffix("::") else {
            return Ok(None);
        };
        let Some(system) = self.registry.database.system else {
            return Ok(None);
        };
        let Some((ns_fq, echo_columns)) = system.liminal_echo_columns(ns_typed)? else {
            return Ok(None);
        };
        let query = liminal_wrapper_query(&ns_fq, &echo_columns, &self.registry.identities);
        Ok(Some((
            identifier.name.clone(),
            query,
            access.clone(),
            alias.clone(),
            *outer,
        )))
    }

    fn r_resolve_pipe(
        &mut self,
        expr: ast_unresolved::Chain,
        outer_context: Option<&[crate::names::ColId]>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        // The trailing run, in source order, and the relation it shapes.
        //
        // ONE RUN. An access and a pipe operator are different steps but the
        // same walk: the available columns, qualifier scope, liminal
        // classification, DML shape and descriptor roles a pipe segment sees
        // are what an access sees, and collecting them separately would give
        // one of them a context the other never had.
        let mut base = expr;
        let mut segments: Vec<RunStep> = Vec::new();
        // THE PARTITION IS THE MEMBERSHIP: each pop either returns the
        // run-step family or restores the step and ends the run — no
        // second list, no reachable panic. `pop_run_step` never crosses
        // the head span: the leading continuations inside it are the
        // HEAD'S OWN READ, never run steps.
        while let Some(step) = base.pop_run_step() {
            segments.push(step);
        }
        segments.reverse();

        let pivot_in_values;
        let mut resolved_source;
        let mut source_bubbled;

        // THE LIMINAL RELATION (EFFECT-ALGEBRA §8): `ns::(*).liminal(*)`
        // drills into the namespace's liminal ledger beside `entities` and
        // `namespaces`. The stored catalog-wrapper definition carries only
        // (entities, namespaces, name) — the ledger's presented schema is the
        // corresponding union of THAT NAMESPACE's receipts, knowable only at
        // resolve time — so when the drill's column is `liminal` and its
        // source is a catalog functor, the source expands from a SYNTHESIZED
        // wrapper body (generator ⨯ ledger tree group carrying this
        // namespace's echo columns) through the ordinary consulted-view road.
        // Bare `ns::(*)` reads and entities/namespaces drills never enter
        // here and keep the stored definition byte-for-byte (pinned by
        // scratch_home--02, home_namespace--01, nesting--10). Pinned by
        // effects/liminal--43, --45,
        // `liminal_drill_presents_the_corresponding_union`.
        let first_drill = segments.first().and_then(|step| match step {
            RunStep::Structural(ast_unresolved::StructuralStep {
                form: ast_unresolved::StructuralForm::Drill { drill },
                ..
            }) => Some(drill),
            _ => None,
        });
        let liminal_expansion = self.liminal_catalog_expansion(&base, first_drill)?;

        {
            // Resolve the base expression through registry.
            // If base is Pipe(HoView, ...), recursion handles the expansion.
            let (rs, sb) = match liminal_expansion {
                Some((view_name, query, access, alias, outer)) => {
                    super::relation_resolver::r_resolve_view_query(
                        view_name,
                        query,
                        "sys::meta".to_string(),
                        access,
                        alias,
                        outer,
                        self.registry,
                        outer_context,
                        &self.config,
                        None,
                    )?
                }
                None => self.resolve_relational(base)?,
            };
            resolved_source = rs;
            source_bubbled = sb;

            // THE PIVOT'S KEYS ARE READ AFTER RESOLUTION, AND ONLY THERE. A
            // name and an ordinal are the same column occurrence by now, which
            // is what lets one answer serve both addressings; before
            // resolution an ordinal has no name and the question cannot be
            // asked at all. Reading it twice and merging gave the two
            // addressings two different laws — the earlier answer entered
            // `Or` and `Not`, where an `IN` constrains nothing exhaustively,
            // so a named key produced pivot columns that the other arm of the
            // `or` could add rows outside of, while the same query written
            // positionally refused.
            pivot_in_values = super::extract_in_predicate_values_from_resolved(
                &resolved_source,
                &self.registry.identities,
            );
        }

        // Iterate the run bottom-up (innermost step first)
        for step in segments {
            // Narrowing a knowable object literal is a provable mistake —
            // refuse while the anon source is still in hand.
            if let RunStep::Structural(ast_unresolved::StructuralStep {
                form: ast_unresolved::StructuralForm::Narrow { nest, .. },
                ..
            }) = &step
            {
                if let crate::pipeline::asts::core::Reference::Named(
                    crate::pipeline::asts::core::NamedReference(authored),
                ) = nest
                {
                    super::relation_resolver::refuse_knowable_object_narrowing(
                        authored.name.as_str(),
                        &resolved_source,
                        &self.registry.identities,
                    )?;
                }
            }

            // Check for unresolved columns before pipe (scope barrier)
            if !source_bubbled.i_need.is_empty() {
                let first_unresolved = &source_bubbled.i_need[0];
                let qual_str = match first_unresolved {
                    ColumnReference::Named {
                        name, qualifier, ..
                    } => qualifier
                        .as_ref()
                        .map(|q| format!("{}.{}", q, name))
                        .unwrap_or_else(|| name.to_string()),
                    ColumnReference::Ordinal {
                        position, reverse, ..
                    } => column_ordinal_text(*position, *reverse),
                };

                return Err(DelightQLError::ColumnNotFoundError {
                    column: qual_str,
                    context:
                        "Column reference before pipe operator cannot be resolved (scope barrier)"
                            .to_string(),
                });
            }

            // Get available columns from source
            let available_columns = if source_bubbled.i_provide.is_empty() {
                let source_schema = super::extract_cpr_schema(&resolved_source);
                let columns = self.registry.identities.known_heading(source_schema)?;
                if std::env::var("DQL_DEBUG").is_ok() {
                    eprintln!("PIPE: Source has {} columns", columns.len());
                }
                columns.to_vec()
            } else {
                source_bubbled.i_provide.clone()
            };

            // A relational source may carry qualifiers licensed only at its
            // own lexical position. Set-operation arms, for example, remain
            // visible to an attached correlation condition but do not name
            // the one relation a following pipe receives. Keep a qualifier
            // at the pipe boundary only when it owns a current occurrence or
            // the addressing authority can still reach one through it. The
            // latter retains join arms, which remain live FROM entries.
            let pipe_qualifier_scope: Vec<_> = source_bubbled
                .qualifier_scope
                .iter()
                .copied()
                .filter(|scope| {
                    available_columns
                        .iter()
                        .any(|column| self.registry.identities.scope_of(*column) == *scope)
                        || self
                            .registry
                            .identities
                            .answers_to(*scope)
                            .is_some_and(|answer| {
                                !self
                                    .registry
                                    .identities
                                    .qualified_glob(answer, &available_columns)
                                    .is_empty()
                            })
                })
                .collect();

            // USING→correlation intercept
            if let RunStep::Access {
                access: ast_unresolved::Access::Dequalify(ref columns),
                ..
            } = step
            {
                if let Some(outer) = outer_context {
                    resolved_source = self.correlate_using(
                        resolved_source,
                        super::CorrelatingRun::Named(columns),
                        outer,
                    )?;
                    continue;
                }
            }

            // The step's own resolution — the one place the two kinds part.
            // Everything above and below is the run's, shared.
            let (resolved_step, mut output_columns, stage_name) = match step {
                RunStep::Access { access, .. } => {
                    let (access, output) = super::resolving::operators::schema_ops::resolve_access(
                        super::relation_resolver::resolve_schema_free_access(&access)?,
                        &available_columns,
                        &self.registry.identities,
                    )?;
                    (ResolvedRunStep::Access(access), output, None)
                }
                RunStep::Pipe {
                    operator,
                    named: stage_name,
                    ..
                } => {
                    // Bubble the operator to collect column needs
                    let schema = self.registry.database.schema();
                    let system = self.registry.database.system;
                    let identities = std::rc::Rc::clone(&self.registry.identities);
                    let cte_context = &mut self.registry.query_local.ctes;
                    let (unresolved_operator, operator_bubbled) =
                        super::bubbling::bubble_unary_operator(
                            operator,
                            schema,
                            system,
                            cte_context,
                            &identities,
                        )?;

                    // Validate that all operator needs can be satisfied
                    if !operator_bubbled.i_need.is_empty() {
                        super::validate_and_get_resolved(
                            operator_bubbled.i_need.clone(),
                            &available_columns,
                            &pipe_qualifier_scope,
                            &self.registry.identities,
                            "in pipe operator",
                        )?;
                    }

                    // Resolve the operator at the pipe boundary with the
                    // source schema
                    self.local_available = available_columns.clone();
                    self.available = available_columns.clone();
                    self.qualifier_scope = pipe_qualifier_scope;
                    self.pivot_in_values = pivot_in_values.clone();
                    let resolved_operator = self.transform_operator(unresolved_operator)?;
                    let output = self
                        .last_operator_output
                        .take()
                        .expect("transform_operator must populate last_operator_output");
                    (
                        ResolvedRunStep::Operator(resolved_operator),
                        output,
                        stage_name,
                    )
                }
                RunStep::Structural(mut step) => {
                    // The stage name is the POSITION's, not the form's:
                    // it is taken off the step here and spent on the stage
                    // scope below, exactly as an operator's is.
                    let structural_stage_name = step.named.take();
                    // Only an ordering carries expressions whose needs are
                    // validated against the source; the other structural
                    // steps address by name at their own resolution.
                    if let ast_unresolved::StructuralForm::Ordering { specs } = &step.form {
                        let schema = self.registry.database.schema();
                        let system = self.registry.database.system;
                        let identities = std::rc::Rc::clone(&self.registry.identities);
                        let cte_context = &mut self.registry.query_local.ctes;
                        let bubbled = super::bubbling::bubble_ordering_specs(
                            specs,
                            schema,
                            system,
                            cte_context,
                            &identities,
                        )?;
                        if !bubbled.i_need.is_empty() {
                            super::validate_and_get_resolved(
                                bubbled.i_need.clone(),
                                &available_columns,
                                &pipe_qualifier_scope,
                                &self.registry.identities,
                                "in pipe operator",
                            )?;
                        }
                    }
                    self.local_available = available_columns.clone();
                    self.available = available_columns.clone();
                    self.qualifier_scope = pipe_qualifier_scope;
                    self.pivot_in_values = pivot_in_values.clone();
                    let (resolved, output) =
                        self.resolve_structural_step(step, &available_columns)?;
                    (
                        ResolvedRunStep::Structural(resolved),
                        output,
                        structural_stage_name,
                    )
                }
            };

            // After a pipe, columns become Fresh (scope barrier).
            // Exception: a drill-down preserves table provenance — the
            // interior answers to the drilled column's name, and `R |> .t(*)`
            // normalizes to `R.t(*) |> (t.*)`, so the qualifier must cross the
            // pipe the normalization itself inserts.
            // Transform ($$) is not an exception: qualified refs must not
            // leak past the pipe — the transform resolves its own targets
            // against the pre-pipe columns; output must be Fresh.
            //
            // Both spellings of the operator, because this runs on the
            // RESOLVED one: naming only the pre-resolution variant here is a
            // barrier that never lifts.
            let preserves_scope = matches!(
                &resolved_step,
                ResolvedRunStep::Structural(ast_resolved::StructuralForm::Drill { .. })
            );
            // A qualify stage is a pipe the access desugar itself inserts:
            // `t(*, cond)` is ONE authored access, so `t` must still reach
            // its columns where the condition resolves, past the stage the
            // compiler wrote for itself. The answer rides the column, not a
            // scope of its own — the same road a drill-down's interior takes.
            let is_qualify = matches!(
                &resolved_step,
                ResolvedRunStep::Access(ast_resolved::Access::All)
            );
            // Inside a named access, the access's own name survives every
            // interior stage: the access IS that name whatever stage its
            // interior has reached, and the nested-correlation road
            // (`oi.order_id = o.oid`) addresses the CURRENT heading through
            // it. Licensed only where the stage's input already carries the
            // name, so a sibling relation piped inside the interior never
            // inherits it. The outer-query law is untouched: no interior,
            // no ride, and a projection still consumes its input's OTHER
            // names.
            let interior_ride = self.interior_self.filter(|answer| {
                available_columns
                    .iter()
                    .any(|input| self.registry.identities.answering_reach(*input) == Some(*answer))
            });
            let pipe_input = self
                .registry
                .identities
                .common_scope(&available_columns)
                .or_else(|| self.registry.identities.common_scope(&output_columns))
                .unwrap_or_else(|| {
                    self.registry.identities.mint_scope(
                        crate::names::ScopeOrigin::AnonRelation,
                        crate::names::Hint::None,
                        None,
                    )
                });
            // `|> (id) as f` — the stage answers to `f` from here on. An
            // alias REPLACES the anonymous form: a named stage is
            // reached by its name and is no longer one of the unnamed pipes
            // the deictic `_` enumerates.
            let stage_spelling = stage_name.as_ref().map(|name| {
                self.registry
                    .identities
                    .intern(name.as_str(), name.is_stropped())
            });
            let stage_hint = match stage_spelling {
                Some(spelling) => crate::names::Hint::User(spelling),
                None => crate::names::Hint::None,
            };
            let stage_answer =
                stage_spelling.map(|spelling| self.registry.identities.canonical(spelling));
            let pipe_scope = self.registry.identities.mint_derived_scope(
                crate::names::ScopeOrigin::PipeStage { input: pipe_input },
                stage_hint,
            );

            output_columns = output_columns
                .into_iter()
                .map(|column| {
                    let addressing = if preserves_scope {
                        self.registry.identities.addressing(column)
                    } else if self.registry.identities.addressing(column)
                        == crate::names::Addressing::Hygienic
                    {
                        crate::names::Addressing::Hygienic
                    } else if is_qualify {
                        match self.registry.identities.addressing(column) {
                            crate::names::Addressing::Published => {
                                let scope = self.registry.identities.scope_of(column);
                                match self.registry.identities.answers_to(scope) {
                                    Some(answer) => crate::names::Addressing::BareAnswering(answer),
                                    None => crate::names::Addressing::Published,
                                }
                            }
                            other => other,
                        }
                    } else if let Some(answer) = stage_answer {
                        // A named stage's columns carry its name the way an
                        // aliased relation's do, so `f.id` still reaches
                        // them after a join has republished the heading.
                        crate::names::Addressing::BareAnswering(answer)
                    } else if let Some(answer) = interior_ride {
                        crate::names::Addressing::BareAnswering(answer)
                    } else {
                        crate::names::Addressing::Published
                    };
                    self.registry.identities.republish_column(
                        column,
                        pipe_scope,
                        crate::names::Republish::Passthrough,
                        self.registry.identities.published(column),
                        addressing,
                        |_| {},
                    )
                })
                .collect();

            // Extend the resolved chain with this pipe.
            // The authored name is SPENT above, on the scope this stage
            // publishes and on the addressing its columns carry. The
            // resolved pipe holds no spelling: `()` is not "no name was
            // written", it is the absence of a second place to keep one.
            resolved_source = resolved_source.then(match resolved_step {
                ResolvedRunStep::Access(access) => ast_resolved::Continuation::Access {
                    access,
                    cpr_schema: pipe_scope,
                },
                ResolvedRunStep::Operator(operator) => ast_resolved::Continuation::Pipe {
                    operator,
                    named: (),
                    cpr_schema: pipe_scope,
                },
                ResolvedRunStep::Structural(form) => {
                    ast_resolved::Continuation::Structural(ast_resolved::StructuralStep {
                        form,
                        named: (),
                        cpr_schema: pipe_scope,
                    })
                }
            });
            source_bubbled = BubbledState::resolved(output_columns, &self.registry.identities);
        }

        Ok((resolved_source, source_bubbled))
    }

    /// One structural step of the run, resolved with the same available
    /// columns the operators see. Each kind's judgment lives in its own
    /// resolving function; this match routes the EXACT structural family
    /// exhaustively — there is no other continuation to receive.
    fn resolve_structural_step(
        &mut self,
        step: ast_unresolved::StructuralStep,
        available: &[crate::names::ColId],
    ) -> Result<(ast_resolved::StructuralForm, Vec<crate::names::ColId>)> {
        use super::resolving::operators::{ordering, schema_ops};
        match step.form {
            ast_unresolved::StructuralForm::Ordering { specs } => {
                let (specs, output) =
                    ordering::resolve_tuple_ordering_via_fold(self, specs, available)?;
                Ok((ast_resolved::StructuralForm::Ordering { specs }, output))
            }
            ast_unresolved::StructuralForm::Reposition { moves } => {
                let (moves, output) = schema_ops::resolve_reposition(self, moves, available)?;
                Ok((ast_resolved::StructuralForm::Reposition { moves }, output))
            }
            ast_unresolved::StructuralForm::Meta => {
                let output = schema_ops::resolve_meta_ize(available, &self.registry.identities)?;
                Ok((ast_resolved::StructuralForm::Meta, output))
            }
            ast_unresolved::StructuralForm::Witness { polarity } => {
                let output = schema_ops::resolve_witness(available, &self.registry.identities)?;
                Ok((ast_resolved::StructuralForm::Witness { polarity }, output))
            }
            ast_unresolved::StructuralForm::SignedWitness => {
                let output =
                    schema_ops::resolve_signed_witness(available, &self.registry.identities)?;
                Ok((ast_resolved::StructuralForm::SignedWitness, output))
            }
            ast_unresolved::StructuralForm::Drill { drill } => {
                let (drill, output) = schema_ops::resolve_interior_drill_down(
                    drill.column,
                    drill.glob,
                    drill.columns,
                    drill.groundings,
                    available,
                    &self.registry.identities,
                )?;
                Ok((ast_resolved::StructuralForm::Drill { drill }, output))
            }
            ast_unresolved::StructuralForm::Narrow { nest, pattern, .. } => {
                let (narrowing, output) = schema_ops::resolve_narrowing_destructure(
                    nest,
                    pattern,
                    available,
                    &self.registry.identities,
                )?;
                Ok((
                    ast_resolved::StructuralForm::Narrow {
                        nest: narrowing.nest,
                        pattern: narrowing.pattern,
                        schema: narrowing.schema,
                    },
                    output,
                ))
            }
        }
    }

    /// The mutation contract: who may be mutated, and what shapes may stand
    /// between the source and the mutation.
    ///
    /// ONE road. A mutation is written two ways and reaches this function by
    /// one of them, because the other becomes it before resolution starts.
    /// The `!!` evidence, the target agreement, and the shaping laws are read
    /// here and nowhere else — a second copy is how a source once reached SQL
    /// with its marker unexamined.
    fn enforce_mutation_contract(
        &mut self,
        kind: DmlVerb,
        target: &str,
        marks: &[(crate::names::ScopeId, crate::names::Spelling)],
        pipe_ops: &[DmlPipeKind],
        available_columns: &[crate::names::ColId],
    ) -> Result<()> {
        let marked_names = || {
            marks
                .iter()
                .map(|(_, relation)| {
                    let mut text = String::new();
                    self.registry
                        .identities
                        .write(*relation, &mut crate::names::Teaching(&mut text));
                    text
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        if marks.len() > 1 {
            return Err(DelightQLError::validation_error_categorized(
                "dml/marker/multiple",
                format!(
                    "DML source has !! on multiple relations: {}",
                    marked_names()
                ),
                "Only one relation can be marked with !! — the mutation target must be unambiguous",
            ));
        }

        match kind {
            DmlVerb::Insert => {
                if !marks.is_empty() {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/marker/forbidden",
                            format!("insert! source must not have !! marker (found on: {})", marked_names()),
                            "Remove !! from the source relation — insert reads from source, it does not mutate it".to_string(),
                        ));
                }
            }
            DmlVerb::Update | DmlVerb::Delete => {
                let kind_name = match kind {
                    DmlVerb::Update => "update!",
                    DmlVerb::Delete => "delete!",
                    _ => unreachable!(),
                };
                let Some((_, marked)) = marks.first() else {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/marker/missing",
                            format!("{} requires !! on the source relation that will be mutated", kind_name),
                            format!("Mark the source with !!: {}!!(*)  — this makes the mutation target explicit", target),
                        ));
                };
                // Both sides name a relation, and the identifier law folds
                // them the same way — so the comparison is of names as this
                // language means them, not of characters.
                let written = self.registry.identities.intern(target, false);
                if self.registry.identities.canonical(*marked)
                    != self.registry.identities.canonical(written)
                {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/marker/mismatch",
                            format!("!! source table '{}' does not match {} target '{}'", marked_names(), kind_name, target),
                            format!("The !! marker must be on the same table as the DML target: {}!!(*)  |> {}({}(*))", target, kind_name, target),
                        ));
                }
            }
        }

        match kind {
            DmlVerb::Update => {
                let has_transform = pipe_ops
                    .iter()
                    .any(|op| matches!(op, DmlPipeKind::Transform));
                if !has_transform {
                    let has_non_filter_ops = pipe_ops.iter().any(|op| {
                        matches!(
                            op,
                            DmlPipeKind::ProjectOut
                                | DmlPipeKind::Rename
                                | DmlPipeKind::TupleOrdering
                                | DmlPipeKind::General
                                | DmlPipeKind::Group
                        )
                    });
                    if has_non_filter_ops {
                        return Err(DelightQLError::validation_error_categorized(
                                "dml/shape/update_no_transform",
                                "update! requires a Transform ($$) to specify column assignments — embed (+), project-out (-), rename (*), ordering (#), and projection do not produce SET clauses",
                                "Use $$(new_value as column_name) before update! to specify what to change",
                            ));
                    }
                } else {
                    let has_aggregate = pipe_ops.iter().any(|op| matches!(op, DmlPipeKind::Group));
                    if has_aggregate {
                        return Err(DelightQLError::validation_error_categorized(
                                "dml/source/aggregate",
                                "Cannot aggregate/group data before update! — aggregation changes the row identity, making it impossible to map results back to source rows",
                                "Remove the aggregate/group-by pipe before the DML operation",
                            ));
                    }
                    let transform_count = pipe_ops
                        .iter()
                        .filter(|op| matches!(op, DmlPipeKind::Transform))
                        .count();
                    if transform_count > 1 {
                        return Err(DelightQLError::validation_error_categorized(
                                "dml/shape/update_no_transform",
                                "update! requires exactly one Transform ($$) — multiple covers produce ambiguous SET clauses",
                                "Combine the transforms into a single $$(expr1 as col1, expr2 as col2) before update!",
                            ));
                    }
                    let has_ordering = pipe_ops
                        .iter()
                        .any(|op| matches!(op, DmlPipeKind::TupleOrdering));
                    if has_ordering {
                        return Err(DelightQLError::validation_error_categorized(
                                "dml/shape/update_no_transform",
                                "Ordering (#) before update! is meaningless — UPDATE does not preserve row order",
                                "Remove the ordering pipe from the DML pipeline",
                            ));
                    }
                }
            }
            DmlVerb::Delete => {
                let has_transform = pipe_ops
                    .iter()
                    .any(|op| matches!(op, DmlPipeKind::Transform));
                if has_transform {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/shape/delete_with_cover",
                            "delete! discards column data — a Transform ($$) before it is wasted",
                            "Remove the Transform before delete! — only filters affect which rows are deleted",
                        ));
                }
                // A shape operator before delete! is not waste by
                // construction: the rows that die are the ones the
                // source still identifies, and a projection DOWN to
                // the target's own columns is what makes that match
                // well-formed over a join. What is wasted — and
                // refused — is a shape that stops the source
                // publishing a column the target has, because the
                // match is then over a heading the target does not
                // share.
                let has_shape_ops = pipe_ops.iter().any(|op| {
                    matches!(
                        op,
                        DmlPipeKind::ProjectOut | DmlPipeKind::Rename | DmlPipeKind::General
                    )
                });
                if has_shape_ops {
                    let carried: Vec<_> = available_columns
                        .iter()
                        .filter_map(|column| self.registry.identities.published_sym(*column))
                        .collect();
                    let dropped: Vec<String> = self
                        .registry
                        .database
                        .schema()
                        .get_table_columns(None, target)?
                        .into_iter()
                        .flatten()
                        .filter_map(|column| {
                            let spelling = self.registry.identities.intern(&column.name, false);
                            let name = self.registry.identities.canonical(spelling);
                            (!carried.contains(&name)).then_some(column.name.to_string())
                        })
                        .collect();
                    if !dropped.is_empty() {
                        return Err(DelightQLError::validation_error_categorized(
                            "dml/shape/delete_with_cover",
                            format!(
                                "the source of this delete! no longer publishes \
                                     [{}], columns of '{target}' — the rows to delete \
                                     are identified by the target's whole heading, so a \
                                     shape that drops one of its columns identifies no rows",
                                dropped.join(", ")
                            ),
                            "keep the target's columns through the shaping pipes, or \
                                 drop the shaping and filter instead",
                        ));
                    }
                }
                let has_aggregate = pipe_ops.iter().any(|op| matches!(op, DmlPipeKind::Group));
                if has_aggregate {
                    return Err(DelightQLError::validation_error_categorized(
                            "dml/source/aggregate",
                            "Cannot aggregate/group data before delete! — aggregation changes the row identity",
                            "Remove the aggregate/group-by pipe before the DML operation",
                        ));
                }
            }
            DmlVerb::Insert => {
                // Insert is more permissive — projections, transforms, etc. are valid
                // for shaping the data before insertion. Aggregates are suspicious but
                // not necessarily wrong (e.g., insert aggregated results into a summary table).
            }
        }
        Ok(())
    }

    /// Operator resolution — delegates to `resolving::resolve_operator_via_fold`.
    pub(super) fn resolve_operator_impl(
        &mut self,
        operator: ast_unresolved::PipeOp,
        available: &[crate::names::ColId],
        pivot_in_values: &std::collections::HashMap<crate::names::Sym, Vec<String>>,
    ) -> Result<(ast_resolved::PipeOp, Vec<crate::names::ColId>)> {
        super::resolving::resolve_operator_via_fold(self, operator, available, pivot_in_values)
    }

    /// Resolve a DML call IN RELATION POSITION — the one invocation shape a
    /// mutation has. The call carries its target first and its source second
    /// (the descriptor's layout); the source resolves as the ordinary chain
    /// it is, THE MUTATION CONTRACT is enforced against that resolved source,
    /// and the target resolves through the physical mutation lookup — a
    /// normal relation access mints an output occurrence for its visible
    /// heading; a mutation target must instead name the catalog-owned scope
    /// that the SQL statement will delete, update, or insert into. The
    /// resolved chain is the call at the head with its receipt access beside
    /// it; no operator carrier stands between the source and the terminal.
    pub(super) fn resolve_dml_call(
        &mut self,
        call: ast_unresolved::SealedCall,
        access: Option<ast_unresolved::Access>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        let effect = call.is_effect();
        let (call, source) = split_dml_source(call)?;
        let call = call.into_inner();
        let reference = Some(&call.call().callee).ok_or_else(|| {
            DelightQLError::parse_error("a DML call has no written operation identity")
        })?;
        let operation = reference.name_text();
        let verb = match crate::pipeline::asts::effects::directive_category(&operation) {
            crate::pipeline::asts::effects::DirectiveCategory::Dml(verb) => verb,
            _ => unreachable!("non-DML call reached DML resolver"),
        };

        // THE SHAPES between the source read and the terminal — every
        // trailing run step of the source chain, outermost first, so an
        // ordering or a reshaping before a mutation is judged whatever
        // carrier it rides.
        let contract_target = call
            .call()
            .relations()
            .next()
            .and_then(extract_unresolved_base_ground_name)
            .unwrap_or_else(|| "target".to_string());
        let dml_pipe_ops = classify_dml_source_shapes(&source);

        // The source resolves as the ordinary relational chain it is; its
        // own trailing run resolves on the shared run road.
        let (resolved_source, source_bubbled) = self.resolve_relational(source)?;
        if let Some(first_unresolved) = source_bubbled.i_need.first() {
            let qual_str = match first_unresolved {
                ColumnReference::Named {
                    name, qualifier, ..
                } => qualifier
                    .as_ref()
                    .map(|q| format!("{}.{}", q, name))
                    .unwrap_or_else(|| name.to_string()),
                ColumnReference::Ordinal {
                    position, reverse, ..
                } => column_ordinal_text(*position, *reverse),
            };
            return Err(DelightQLError::ColumnNotFoundError {
                column: qual_str,
                context: "Column reference before pipe operator cannot be resolved (scope barrier)"
                    .to_string(),
            });
        }
        let available_columns = if source_bubbled.i_provide.is_empty() {
            let source_schema = super::extract_cpr_schema(&resolved_source);
            self.registry
                .identities
                .known_heading(source_schema)?
                .to_vec()
        } else {
            source_bubbled.i_provide.clone()
        };

        // THE MUTATION CONTRACT, in the one place it is enforced: the `!!`
        // evidence is read off the relation the mutation is about to receive.
        let marks = self
            .registry
            .identities
            .mutation_marks(super::extract_cpr_schema(&resolved_source));
        self.enforce_mutation_contract(
            verb,
            &contract_target,
            &marks,
            &dml_pipe_ops,
            &available_columns,
        )?;
        let callable_name = self.registry.identities.intern(&operation, false);
        let callable_namespace = reference
            .namespace_texts()
            .into_iter()
            .map(|part| self.registry.identities.intern(&part, false))
            .collect();
        // DML classification is minted once into the registry and carried by
        // the callable identity through resolution and lowering. It is applied
        // to the RESOLVED call below: a classification written onto the
        // authored call would be a resolution decision sitting in a tree that
        // has not been resolved.
        let dml_callee = self.registry.identities.mint_callable(
            callable_name,
            callable_namespace,
            crate::names::CallableCategory::Dml(verb),
        );
        // THE TARGET IS THE FIRST RELATION FORMAL: the descriptor's layout
        // for every mutation verb puts the destination first and the source
        // after it, so the position answers what the deleted role mark used
        // to say.
        let target_relation = call
            .call()
            .relations()
            .next()
            .cloned()
            .ok_or_else(|| DelightQLError::parse_error("DML call has no target relation"))?;
        let bare = operation.strip_suffix('!').unwrap_or(operation.as_ref());
        let (target, target_namespace) = crate::pipeline::asts::effects::target_designator(
            bare,
            "effect/dml/target_designator",
            "naming where to write",
            &target_relation,
        )?;

        if let Some(system) = self.registry.database.system {
            let scope = self
                .config
                .resolution_namespace
                .as_deref()
                .unwrap_or("main");
            if let Some((owner, kind)) =
                system.effect_target_owner(&target, target_namespace.as_deref(), scope)?
            {
                if kind == "system" {
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/target/engine_owned",
                        format!(
                            "DML target '{target}' resolves into the engine-owned namespace \
                             '{owner}': programs cannot mutate system relations — query it, \
                             never write it"
                        ),
                        "engine-owned namespace",
                    ));
                }
            }
        }

        let (target_schema, canonical, backend_schema) = if let Some(namespace) = target_namespace {
            let path =
                ast_unresolved::NamespacePath::from_fq_string(&namespace).map_err(|error| {
                    DelightQLError::validation_error(
                        format!("Invalid DML target namespace: {error}"),
                        "Use a valid namespace path",
                    )
                })?;
            let Some((schema, connection, canonical, backend_schema)) = self
                .registry
                .database
                .lookup_table_with_namespace_qualified(&path, &target)?
            else {
                return Err(DelightQLError::TableNotFoundError {
                    table_name: target,
                    context: "DML target was not found in its namespace".to_string(),
                });
            };
            self.registry.track_connection_id(connection);
            (schema, Some(canonical), backend_schema)
        } else {
            use crate::resolution::{EntityDefinition, ResolutionResult};
            let resolved = crate::resolution::resolve_entity_with_alias(
                &delightql_types::SqlIdentifier::new(target.clone()),
                None,
                self.registry,
                self.config.resolution_namespace.as_deref(),
            )?;
            let info = match resolved {
                ResolutionResult::DatabaseEntity(info)
                | ResolutionResult::MaterializedRelation(info) => info,
                _ => {
                    return Err(DelightQLError::validation_error(
                        format!("DML target '{target}' is not a physical table"),
                        "DML targets must resolve to database tables",
                    ))
                }
            };
            let EntityDefinition::RelationSchema(schema) = info.definition;
            (schema, info.canonical_name, info.backend_schema)
        };
        let target_scope = target_schema;
        let target_spelling = self.registry.identities.intern(&target, false);
        let target_entity = self
            .registry
            .identities
            .entity_of_scope(target_scope)
            .unwrap_or_else(|| self.registry.identities.mint_entity(target_spelling));
        let canonical = canonical.as_ref().map(|name| {
            self.registry
                .identities
                .intern(name.as_str(), name.is_stropped())
        });
        let backend_schema = backend_schema
            .as_deref()
            .map(|name| self.registry.identities.intern(name, false));
        self.registry
            .identities
            .bind_entity_physical(target_entity, canonical, backend_schema);

        self.local_available = available_columns.clone();
        self.available = available_columns.clone();
        let mut resolved_call = self.resolve_functor_call(call)?;
        resolved_call.callee = dml_callee;
        let target =
            ast_resolved::Relation::ground_read(ast_resolved::Access::All, false, target_scope);
        let mut replaced = false;
        for argument in resolved_call.call_mut().arguments.ho_members_mut() {
            if !replaced {
                if let ast_resolved::HoArgument::Relation(relation) = argument {
                    *relation = target.clone();
                    replaced = true;
                }
            }
        }
        // The source rides the call in its own formal position — after the
        // target, per the descriptor's layout — so the lowering reads
        // [target, source] off the one call.
        insert_dml_source_argument(&mut resolved_call, resolved_source);

        // The terminal publishes the source's heading republished into its
        // own stage scope, exactly as the shared run tail publishes a pipe
        // stage's.
        let terminal_input = self
            .registry
            .identities
            .common_scope(&available_columns)
            .unwrap_or_else(|| {
                self.registry.identities.mint_scope(
                    crate::names::ScopeOrigin::AnonRelation,
                    crate::names::Hint::None,
                    None,
                )
            });
        let terminal_scope = self.registry.identities.mint_derived_scope(
            crate::names::ScopeOrigin::PipeStage {
                input: terminal_input,
            },
            crate::names::Hint::None,
        );
        let republish_into = |registry: &crate::names::Registry,
                              columns: &[crate::names::ColId],
                              scope: crate::names::ScopeId,
                              qualify: bool| {
            columns
                .iter()
                .map(|column| {
                    let addressing = if registry.addressing(*column)
                        == crate::names::Addressing::Hygienic
                    {
                        crate::names::Addressing::Hygienic
                    } else if qualify {
                        match registry.addressing(*column) {
                            crate::names::Addressing::Published => {
                                let owner = registry.scope_of(*column);
                                match registry.answers_to(owner) {
                                    Some(answer) => crate::names::Addressing::BareAnswering(answer),
                                    None => crate::names::Addressing::Published,
                                }
                            }
                            other => other,
                        }
                    } else {
                        crate::names::Addressing::Published
                    };
                    registry.republish_column(
                        *column,
                        scope,
                        crate::names::Republish::Passthrough,
                        registry.published(*column),
                        addressing,
                        |_| {},
                    )
                })
                .collect::<Vec<_>>()
        };
        let terminal_columns = republish_into(
            &self.registry.identities,
            &available_columns,
            terminal_scope,
            false,
        );

        let mut resolved_chain =
            ast_resolved::Chain::relation(ast_resolved::Relation::FunctorCall {
                call: ast_resolved::SealedCall::from_inner(resolved_call, effect),
                alias: (),
                cpr_schema: terminal_scope,
            });

        // THE RECEIPT IS THE MUTATION'S OWN: the access on what the mutation
        // publishes, standing beside the call exactly as it was written.
        let mut output_columns = terminal_columns;
        if let Some(access) = access {
            let (resolved_access, access_output) =
                super::resolving::operators::schema_ops::resolve_access(
                    super::relation_resolver::resolve_schema_free_access(&access)?,
                    &output_columns,
                    &self.registry.identities,
                )?;
            let access_input = self
                .registry
                .identities
                .common_scope(&access_output)
                .unwrap_or(terminal_scope);
            let access_scope = self.registry.identities.mint_derived_scope(
                crate::names::ScopeOrigin::PipeStage {
                    input: access_input,
                },
                crate::names::Hint::None,
            );
            let is_qualify = matches!(resolved_access, ast_resolved::Access::All);
            output_columns = republish_into(
                &self.registry.identities,
                &access_output,
                access_scope,
                is_qualify,
            );
            resolved_chain = resolved_chain.then(ast_resolved::Continuation::Access {
                access: resolved_access,
                cpr_schema: access_scope,
            });
        }

        Ok((
            resolved_chain,
            BubbledState::resolved(output_columns, &self.registry.identities),
        ))
    }

    /// Relation dispatch.
    /// Matches on the Relation variant and delegates to the appropriate helper in
    /// `relation_resolver`. The helpers remain as free functions; only the
    /// dispatch is absorbed so `self.registry` / `self.config` are threaded
    /// implicitly.
    #[stacksafe::stacksafe]
    fn resolve_anon_table_impl(
        &mut self,
        anon: ast_unresolved::AnonRelation,
        outer_context: Option<&[crate::names::ColId]>,
        _grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        super::relation_resolver::resolve_anonymous(anon, self, outer_context)
    }

    /// WHAT LOOKING LEFT MEANS IN A CORRELATED INTERIOR.
    ///
    /// `.(name)` renames its own argument to the first lvar of that name to
    /// its left. Inside a correlated interior the nearest such lvar is the
    /// enclosing row's, so the rename ties the two together — the same
    /// unification that becomes SQL `USING` at a join, written where there is
    /// no join to carry it.
    ///
    /// ONE ROAD FOR BOTH CARRIERS. A dequalifying run reaches the resolver as
    /// the mention's own access or as a pipe operator, depending on what it
    /// stood after; it means one thing, so it correlates through one function.
    pub(super) fn correlate_using(
        &mut self,
        source: ast_resolved::Chain,
        run: super::CorrelatingRun<'_>,
        outer: &[crate::names::ColId],
    ) -> Result<ast_resolved::Chain> {
        // THE BASE IS WHAT THE STEP NAMED. A pipe publishes its own heading,
        // so the column the run named may be gone by the end of the chain —
        // `addresses:(*.(country) ~> count:(*))` publishes a count and no
        // `country`. Read the heading where the filter will stand, which is
        // the same place `insert_filters_at_base` puts it.
        let mut base = source;
        let mut trailing = Vec::new();
        while matches!(
            base.continuations.last(),
            Some(ast_resolved::Continuation::Pipe { .. })
        ) {
            trailing.push(base.continuations.pop().expect("just matched"));
        }
        let filters = match run {
            super::CorrelatingRun::Named(columns) => {
                super::resolving::build_using_correlation_filters(
                    columns,
                    outer,
                    &base,
                    &self.registry.identities,
                )?
            }
            // `.*` names no column: the shared ones are computed where the
            // step stands, exactly as the join spelling computes them at the
            // join. The names never become characters on the way — the
            // published symbol IS the identity both sides are compared on.
            super::CorrelatingRun::All => super::resolving::build_using_all_correlation_filters(
                outer,
                &base,
                &self.registry.identities,
            )?,
        };
        let mut correlated = super::insert_filters_at_base(base, filters)?;
        correlated.continuations.extend(trailing.into_iter().rev());
        Ok(correlated)
    }

    /// Resolve the chain's READ: the head relation, and the access its own
    /// parens asked for.
    ///
    /// The two arrive together because a mention's published heading is what
    /// the access decides — the caller pattern's binders, the dequalifying
    /// merge, the activation of an inchoate source. Handing the relation on
    /// without it would leave a read nobody parameterized.
    fn resolve_relation_impl(
        &mut self,
        rel: ast_unresolved::Relation,
        access: Option<ast_unresolved::Access>,
        outer_context: Option<&[crate::names::ColId]>,
        grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        match &rel {
            // Which road a ground read takes is the MENTION's question: a
            // plan read is already addressed, so no spelling lookup runs.
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Plan { .. },
                ..
            } => super::relation_resolver::resolve_plan_scope(
                rel,
                read_access(access)?,
                self.registry,
                outer_context,
                self.config.cfe_formal_frame.as_deref(),
                &self.config,
            ),
            ast_unresolved::Relation::Ground { .. } => super::relation_resolver::resolve_ground(
                rel,
                read_access(access)?,
                self.registry,
                outer_context,
                &self.config,
                grounding,
            ),
            ast_unresolved::Relation::FunctorCall { call, alias, .. } => {
                // The builder has already substituted a piped relation into
                // the call's table arguments. Resolve that call in place:
                // application provenance is diagnostic data only, so no
                // placeholder or synthetic application pipe is rebuilt here.
                let is_dml = Some(&call.call().callee).is_some_and(|reference| {
                    matches!(
                        crate::pipeline::asts::effects::directive_category(&reference.name_text()),
                        crate::pipeline::asts::effects::DirectiveCategory::Dml(_)
                    )
                });
                if is_dml && call.call().relations().nth(1).is_some() {
                    // ONE INVOCATION SHAPE. A mutation is the relation-
                    // position call the builder and the effect machinery
                    // both construct — [target, source] in the descriptor's
                    // layout — and it resolves IN PLACE: the mutation
                    // contract has one place to be enforced and the lowerer
                    // one shape to consume. A second road is what let a
                    // source reach SQL with its `!!` evidence and its shape
                    // unexamined.
                    return self.resolve_dml_call(call.clone(), access);
                }
                let join_input = self.pending_ho_join_input.take();
                let join_columns = self.pending_ho_join_columns.take();
                let (expr, bubbled, absorbed) = super::relation_resolver::resolve_functor_call(
                    call.clone().into_inner(),
                    alias.clone(),
                    read_access(access)?,
                    self.registry,
                    outer_context,
                    &self.config,
                    join_input,
                    join_columns.as_deref(),
                    None,
                )?;
                self.ho_join_input_absorbed = absorbed;
                let read = expr.clone().split_head_access();
                let (
                    ast_resolved::Grelex::Reference(ast_resolved::Relation::FunctorCall {
                        alias: (),
                        call: resolved_call,
                        cpr_schema,
                    }),
                    resolved_access,
                    steps,
                ) = read
                else {
                    // Higher-order expansion consumes the call carrier and
                    // returns the expanded relation directly. It is still
                    // one invocation road; only an ordinary unresolved TVF
                    // remains a callable relation after resolution.
                    return Ok((expr, bubbled));
                };
                // The access stands where the call road put it: after the
                // call, on what the call publishes.
                let mut rebuilt =
                    ast_resolved::Chain::relation(ast_resolved::Relation::FunctorCall {
                        alias: (),
                        call: resolved_call,
                        cpr_schema,
                    });
                if let Some(access) = resolved_access {
                    rebuilt =
                        rebuilt.then(ast_resolved::Continuation::Access { access, cpr_schema });
                }
                rebuilt.continuations.extend(steps);
                Ok((rebuilt, bubbled))
            }
            ast_unresolved::Relation::InnerRelation { .. } => {
                super::relation_resolver::resolve_inner_relation(
                    rel,
                    self.registry,
                    outer_context,
                    &self.config,
                    grounding,
                )
            }
            // Matched on a REFERENCE: rustc's uninhabited-variant elision is
            // by value only, so the consulted form still needs an arm even
            // though `Unresolved::Consulted` cannot be built.
            ast_unresolved::Relation::ConsultedView { .. } => {
                unreachable!("resolved relation reached relation resolution")
            }
        }
    }

    pub(super) fn resolve_functor_call(
        &mut self,
        call: ast_unresolved::FunctorCall,
    ) -> Result<ast_resolved::FunctorCall> {
        let is_cast = call.callee.namespace_texts().is_empty() && call.callee.name_text() == "cast";
        let expected_cast_type = |type_name: &str| {
            const CAST_TYPES: &[&str] = &["integer", "real", "text", "numeric", "boolean"];
            if CAST_TYPES.contains(&type_name) {
                Ok(())
            } else {
                Err(DelightQLError::validation_error_categorized(
                    "cast",
                    format!(
                        "cast: unknown type '{}'. Types: {} (date/timestamp and parameterized types are not yet supported)",
                        type_name,
                        CAST_TYPES.join("|")
                    ),
                    "cast resolution",
                ))
            }
        };
        let callee = call.callee.written_call_identity(&self.registry.identities);
        use crate::pipeline::asts::core::operators::{CallArguments, HoArgument, ScalarArgument};
        if is_cast && call.arguments.scalar_members().len() != 2 {
            return Err(DelightQLError::validation_error_categorized(
                "cast",
                format!(
                    "cast: expects exactly 2 arguments: cast:(expr, type), got {}",
                    call.arguments.scalar_members().len()
                ),
                "cast resolution",
            ));
        }
        let arguments = match call.arguments {
            CallArguments::None => CallArguments::None,
            CallArguments::HigherOrder(part) => {
                CallArguments::HigherOrder(crate::pipeline::asts::core::operators::HoPart {
                    // The landing was judged on the higher-order road before
                    // this fold; a resolved group carries none.
                    landing: (),
                    members: Box::new((*part.members).try_map(|argument| {
                        Ok::<_, DelightQLError>(match argument {
                            HoArgument::Relation(relation) => {
                                HoArgument::Relation(self.transform_relational(relation)?)
                            }
                            HoArgument::Value(value) => HoArgument::Value(
                                crate::pipeline::ast_transform::transform_argument_value(
                                    self, value,
                                )?,
                            ),
                            // A landing reaching this fold is one no pipe
                            // applied an invocation to; the fold refuses it.
                            HoArgument::Landing(landing) => {
                                HoArgument::Landing(self.fold_placeholder(landing)?)
                            }
                            HoArgument::Skip => HoArgument::Skip,
                        })
                    })?),
                })
            }
            CallArguments::Scalar(members) => {
                // A curried callee's leading positions take CODE: what
                // stands there is handed to the formal, not invoked here.
                let code_positions = super::grounding::curried_code_positions(
                    &call.callee,
                    self.registry,
                    self.config.resolution_namespace.as_deref(),
                );
                let mut resolved_members = Vec::with_capacity(members.len());
                for (index, member) in members.into_iter().enumerate() {
                    let code_position = index < code_positions;
                    if code_position {
                        self.cfe_code_suppression += 1;
                    }
                    let resolved = match member {
                        // The star is the RESOLVED reading of a bare glob; an
                        // authored argument row has none to carry across.
                        ScalarArgument::Star => {
                            return Err(DelightQLError::transformation_error(
                                "a whole-operand star reached resolution already resolved: the \
                                 bare glob it reads is spent here, not before",
                                "ho_argument",
                            ))
                        }
                        // A crossed argument carries a truth; the cast road reads a
                        // type TAG, which a truth is not, so only a domain value
                        // reaches the cast judgment.
                        ScalarArgument::Value(ast_unresolved::ArgumentValue::Domain {
                            distinct,
                            value: domain,
                        }) => {
                            if is_cast && index == 1 {
                                let type_name = match &domain {
                                    ast_unresolved::DomainExpression::Application(ast_unresolved::FunctionApplication::Ground(crate::pipeline::asts::core::literals::LiteralValue::Symbol(
                                            symbol,
                                        ))) => symbol.clone(),
                                    ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(AuthoredColumn {
                                        qualifier: None, ..
                                    }))) => {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "cast",
                                            "cast: a bare name is use; the type is a tag — write cast:(x, ::integer). Types: integer|real|text|numeric|boolean",
                                            "cast resolution",
                                        ));
                                    }
                                    ast_unresolved::DomainExpression::Application(ast_unresolved::FunctionApplication::Ground(_)) => {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "cast",
                                            "cast: takes a type symbol, not a string — write cast:(x, ::integer). Types: integer|real|text|numeric|boolean",
                                            "cast resolution",
                                        ));
                                    }
                                    _ => {
                                        return Err(DelightQLError::validation_error_categorized(
                                            "cast",
                                            "cast: second argument must be a type symbol. Types: integer|real|text|numeric|boolean",
                                            "cast resolution",
                                        ));
                                    }
                                };
                                expected_cast_type(&type_name)?;
                                ScalarArgument::plain(ast_resolved::DomainExpression::Application(
                                    ast_resolved::FunctionApplication::Ground(
                                        crate::pipeline::asts::core::literals::LiteralValue::String(
                                            type_name,
                                        ),
                                    ),
                                ))
                            } else {
                                ScalarArgument::Value(ast_resolved::ArgumentValue::Domain {
                                    distinct,
                                    value: self.transform_domain(domain)?,
                                })
                            }
                        }
                        ScalarArgument::Value(crossing) => ScalarArgument::Value(
                            crate::pipeline::ast_transform::transform_argument_value(
                                self, crossing,
                            )?,
                        ),
                        // A CALLABLE RESOLVES AS THE FORM IT IS. Its slot is the
                        // callee's to supply, so resolution reaches its body and
                        // leaves the slot standing.
                        ScalarArgument::Callable(callable) => {
                            ScalarArgument::Callable(self.transform_callable(callable)?)
                        }
                        // AN ENUMERATION IS SPENT WHERE ITS CONTAINER RESOLVES IT,
                        // and an argument row is such a container: FN.35 lists it
                        // among the enumerating positions, and an addressing spread
                        // expands there into the columns it addresses — the
                        // ENCLOSING relation's, which is the same heading every
                        // other container in this fold reads.
                        //
                        // The bare glob is the one that does not address. It NAMES
                        // the whole of what the position offers — the mark `t(*)`
                        // writes — so it has no several columns to become, and
                        // resolves to the star instead.
                        ScalarArgument::Spread(crate::pipeline::asts::core::Spread::Glob(
                            crate::pipeline::asts::core::Glob {
                                qualifier: None, ..
                            },
                        )) => ScalarArgument::Star,
                        ScalarArgument::Spread(spread) => {
                            let available = self.available.clone();
                            // Width and arity are judged AFTER the position
                            // expands — by the callee's own authority — so nothing
                            // here refuses on a count. What can refuse is the
                            // addressing itself: a qualifier naming no live scope,
                            // a pattern matching nothing, a span outside the
                            // heading, each at the authority that owns it.
                            let expanded =
                                super::resolving::domain_expressions::projection::expand_spread(
                                    self, &spread, &available, false,
                                )?;
                            for value in expanded {
                                resolved_members.push(ScalarArgument::plain(value));
                            }
                            if code_position {
                                self.cfe_code_suppression -= 1;
                            }
                            continue;
                        }
                        // A `..` reaching this fold selects the context mode
                        // of a callee that instantiates none; the fold
                        // refuses it.
                        ScalarArgument::Context(marker) => {
                            ScalarArgument::Context(self.fold_context_marker(marker)?)
                        }
                    };
                    if code_position {
                        self.cfe_code_suppression -= 1;
                    }
                    resolved_members.push(resolved);
                }
                CallArguments::Scalar(resolved_members)
            }
        };
        Ok(ast_resolved::FunctorCall {
            callee,
            arguments,
            marks: call.marks,
        })
    }

    /// THE WINDOW RIDES THE WINDOW FUNCTION, both directions, judged from the
    /// FINAL argument row.
    ///
    /// A window builtin computes over a window, so one standing bare has
    /// nothing to compute over; a scalar function computes per row and takes
    /// no window. Which function a name means can depend on the row —
    /// `max:(x)` is the aggregate and `max:(x, 2)` is the scalar overload —
    /// and the overload is read from the one authority the lowering picks its
    /// render form from, so the two cannot disagree.
    ///
    /// An unknown callee passes through unmapped: engine semantics apply.
    fn judge_window(
        &self,
        name: &str,
        windowed: bool,
        call: &ast_resolved::FunctorCall,
    ) -> Result<()> {
        let builtin = &self.registry.built_in;
        if !windowed {
            if builtin.window_signature(name).is_some() {
                return Err(DelightQLError::validation_error_categorized(
                    "window/needs_window",
                    format!(
                        "'{name}' is a window function and computes over a window; \
                         standing bare it has nothing to compute over"
                    ),
                    format!("attach the spec to the call itself: `{name}:(…) <~ #(…)`"),
                ));
            }
            return Ok(());
        }
        // THE FINAL ROW: what the call actually hands the function. The
        // window builtins' argument bounds read it too — one row, one
        // reading, for both questions.
        let supplied = call.arguments.scalar_members().len();
        super::grounding::judge_window_row(self, name, supplied)?;
        if builtin.window_signature(name).is_some() {
            return Ok(());
        }
        let scalar_overload = crate::names::Intrinsic::scalar_overload(name, supplied).is_some();
        let aggregates = builtin.is_aggregate(name) && !scalar_overload;
        if !aggregates && (builtin.is_known_function(name) || scalar_overload) {
            return Err(DelightQLError::validation_error_categorized(
                "window/not_a_window",
                format!(
                    "the window rides the window function itself, and '{name}' is a \
                     scalar function — it computes per row and takes no window"
                ),
                format!(
                    "spell the windowed call inside the argument: \
                     `{name}:(lag:(x) <~ #(…), …)`"
                ),
            ));
        }
        Ok(())
    }

    /// The application, resolved whole: the call plus the scalar context the
    /// value position gave it.
    ///
    /// THE WINDOW IS JUDGED HERE because this is where the FINAL argument row
    /// exists. An authored row is not that row — one addressing spread is a
    /// single authored member and several resolved arguments — so a judgment
    /// made before this point reads a width the call does not have. Every
    /// resolved standard application is built here, authored or rebuilt, so
    /// the judgment cannot be reached around.
    pub(super) fn resolve_standard_application(
        &mut self,
        application: ast_unresolved::StandardApplication,
    ) -> Result<ast_resolved::StandardApplication> {
        let windowed = application.window.is_some();
        let callee = application.call().callee.clone();
        let call = self.resolve_functor_call(application.call.into_inner())?;
        let window = application
            .window
            .map(|window| {
                Ok::<_, DelightQLError>(ast_resolved::WindowSpec {
                    partition: window
                        .partition
                        .into_iter()
                        .map(|expr| self.transform_domain(expr))
                        .collect::<Result<Vec<_>>>()?,
                    ordering: window
                        .ordering
                        .into_iter()
                        .map(|spec| {
                            Ok(ast_resolved::OrderingSpec {
                                column: self.transform_domain(spec.column)?,
                                direction: spec.direction,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    frame: window
                        .frame
                        .map(|frame| {
                            super::resolving::functions::resolve_window_frame_via_fold(self, frame)
                        })
                        .transpose()?,
                })
            })
            .transpose()?;
        let guard = application
            .guard
            .map(|condition| self.transform_boolean(*condition).map(Box::new))
            .transpose()?;
        // JUDGED LAST, so every refusal the application's own parts can make
        // is made first: an argument's, the window's own expressions', the
        // guard's. A mention probe is not an invocation and keeps its window
        // question for the invocation that spends it; a qualified callee is
        // not a built-in.
        if callee.namespace_fq().is_none() && self.cfe_code_suppression == 0 {
            self.judge_window(&callee.name_text(), windowed, &call)?;
        }
        Ok(ast_resolved::StandardApplication {
            call: crate::pipeline::asts::core::PureCall::from_inner(call),
            guard,
            window,
        })
    }
}

impl<'reg, 'db> AstTransform<Unresolved, Resolved> for ResolverFold<'reg, 'db> {
    crate::pipeline::ast_transform::position_is_resolved_against_a_heading!();
    fn fold_entity(
        &mut self,
        entity: crate::pipeline::asts::vocabulary::Ref,
    ) -> crate::error::Result<crate::names::CallableId> {
        Ok(entity.written_call_identity(&self.registry.identities))
    }
    crate::pipeline::ast_transform::column_is_bound_where_it_is_resolved!();
    crate::pipeline::ast_transform::binder_is_bound_where_the_pattern_is_resolved!();
    crate::pipeline::ast_transform::a_landing_is_consumed_where_the_pipe_is_applied!();
    crate::pipeline::ast_transform::a_context_marker_is_consumed_where_the_call_instantiates!();
    crate::pipeline::ast_transform::scope_is_minted_where_it_is_resolved!();
    crate::pipeline::ast_transform::minted_where_it_is_decided!(
        fold_recursion -> crate::pipeline::asts::vocabulary::RecursionState: "a binding's recursion",
    );
    fn fold_cte_subject(
        &mut self,
        _: crate::pipeline::asts::core::CteSubject,
    ) -> crate::error::Result<crate::names::ScopeId> {
        Err(crate::error::DelightQLError::transformation_error(
            "a binding's subject is spent where the resolver's CTE road mints its scope, \
             and this fold is not that place",
            "phase_payload",
        ))
    }
    fn fold_cte_authority(
        &mut self,
        _: crate::pipeline::asts::core::CteAuthority,
    ) -> crate::error::Result<()> {
        Err(crate::error::DelightQLError::transformation_error(
            "a binding's head and provenance are spent where the resolver's CTE road \
             mints its scope, and this fold is not that place",
            "phase_payload",
        ))
    }
    crate::pipeline::ast_transform::minted_where_it_is_decided!(
        fold_output -> Option<crate::names::ColId>: "an expression's output occurrence",
        fold_scalar_output -> crate::names::ColId: "a scalarized relation's column",
        fold_destructure -> Vec<crate::pipeline::asts::core::DestructureMapping>: "a destructuring pattern's columns",
    );
    fn fold_open_leaf(
        &mut self,
        _: crate::pipeline::asts::core::DomainHole,
    ) -> crate::error::Result<crate::pipeline::asts::vocabulary::Never> {
        Err(crate::error::DelightQLError::validation_error_categorized(
            "value/open/unapplied",
            "a composition input stands outside any callable applying it",
            "the position that applies an open body spends its slot",
        ))
    }

    fn fold_cover_callable(
        &mut self,
        _: crate::pipeline::asts::core::Callable<crate::pipeline::asts::core::Unresolved>,
    ) -> crate::error::Result<()> {
        Err(crate::error::DelightQLError::transformation_error(
            "a cover's callable is applied where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    fn fold_rename_target(
        &mut self,
        _: crate::pipeline::asts::core::NameTarget,
    ) -> crate::error::Result<crate::names::Spelling> {
        Err(crate::error::DelightQLError::transformation_error(
            "a rename target is expanded where the rename resolves, and this fold is not that place",
            "phase_payload",
        ))
    }
    fn fold_drill(
        &mut self,
        _: crate::pipeline::asts::core::operators::AuthoredDrill,
    ) -> crate::error::Result<crate::pipeline::asts::core::operators::BoundDrill> {
        Err(crate::error::DelightQLError::transformation_error(
            "an interior drill binds where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    fn transform_relational(&mut self, e: Chain<Unresolved>) -> Result<Chain<Resolved>> {
        let (resolved, bubbled) = self.resolve_relational_impl(e)?;
        self.last_bubbled = Some(bubbled);
        Ok(resolved)
    }

    /// Stack-safe: this is the walk that a nested expression descends once
    /// per level, and it is the one that overflowed. Its relational sibling
    /// and the anonymous-table road already carry the same guard; a
    /// parenthesis ladder reaches none of them and reached this.
    #[stacksafe::stacksafe]
    fn transform_domain(
        &mut self,
        expr: ast_unresolved::DomainExpression,
    ) -> Result<ast_resolved::DomainExpression> {
        use crate::pipeline::ast_transform::walk_transform_domain;

        // THE APPLYING POSITION SPENDS THE LEAF. While a cover applies its
        // body for one cell, the leaf becomes that cell — here, before any
        // resolved tree is minted — so a closed phase never carries one.
        // Outside an applying position the leaf refuses instead.
        if let ast_unresolved::DomainExpression::Application(
            ast_unresolved::FunctionApplication::Open(hole),
        ) = &expr
        {
            return match (hole, &self.cover_cell) {
                (crate::pipeline::asts::core::DomainHole::CompositionInput, Some(cell)) => {
                    Ok(cell.clone())
                }
                _ => Err(crate::error::DelightQLError::validation_error_categorized(
                    "value/open/unapplied",
                    "a composition input stands outside any callable applying it",
                    "the position that applies an open body spends its slot",
                )),
            };
        }

        // A FORMAL IS SPENT, NOT LOOKED UP: inside an open instantiation, a
        // bare name that is one of the definition's formals stands for the
        // argument as the CALLER resolved it. No scope is consulted, so a
        // probe or relation the body opens cannot capture the name.
        if let ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
            authored,
        ))) = &expr
        {
            if authored.qualifier.is_none() && authored.namespace_path.is_empty() {
                if let Some(frame) = self.config.cfe_formal_frame.as_deref() {
                    if let Some(resolved) = frame.values.get(&authored.name) {
                        return Ok(resolved.clone());
                    }
                }
            }
        }

        match expr {
            // A CONSULTED VALUE DEFINITION IS SPENT AT ITS CALL SITE, before
            // ordinary closed resolution: the formals substitute and what
            // remains is closed code in the caller's position.
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Standard(application),
            ) => {
                if let Some(inlined) = super::grounding::inline_cfe_call(self, &application)? {
                    return Ok(inlined);
                }
                // The window judgment is `resolve_standard_application`'s,
                // where the final argument row exists; this arm only spends
                // the consulted-definition road before it.
                walk_transform_domain(
                    self,
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Standard(application),
                    ),
                )
            }
            // StringTemplate at domain level → concat chain (returns DomainExpression, not Function)
            ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Template(template),
            ) => {
                let mut resolved_parts: Vec<ast_resolved::ValueTemplatePart> = Vec::new();
                for part in template.into_parts() {
                    match part {
                        ast_unresolved::ValueTemplatePart::Text(text) => {
                            resolved_parts.push(ast_resolved::ValueTemplatePart::Text(text));
                        }
                        ast_unresolved::ValueTemplatePart::Interpolation(expr) => {
                            let resolved_expr = self.transform_domain(*expr)?;
                            resolved_parts.push(ast_resolved::ValueTemplatePart::Interpolation(
                                Box::new(resolved_expr),
                            ));
                        }
                    }
                }
                Ok(super::string_templates::build_concat_chain(resolved_parts))
            }

            // Simple expressions — column validation, ordinal resolution, literal conversion
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn { .. },
            )))
            | ast_unresolved::DomainExpression::Reference(Reference::Ordinal(_))
            | ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Ground(_),
            )
            | ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Open(_),
            ) => {
                let available = self.available.clone();
                let local_available = self.local_available.clone();
                let qualifier_scope = self.qualifier_scope.clone();
                super::resolving::domain_expressions::simple::resolve_simple_expr(
                    expr,
                    &available,
                    &local_available,
                    &qualifier_scope,
                    self.in_correlation,
                    &mut self.correlation_witness,
                    &self.registry.identities,
                )
            }

            // Everything else: walk handles structural descent
            // Function(non-StringTemplate) → transform_function
            // Predicate → transform_boolean
            // Nested value forms → recursive transform_domain
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        pred: ast_unresolved::TruthExpression,
    ) -> Result<ast_resolved::TruthExpression> {
        use crate::pipeline::ast_transform::walk_transform_boolean;

        match pred {
            // Literal membership stays a membership; lowering owns the
            // doctrine.
            ast_unresolved::TruthExpression::Membership(Membership {
                probe,
                rows,
                negated,
                source,
            }) => {
                if source == MembershipSource::WitnessAnon {
                    validate_witness_membership(&probe, &rows)?;
                }
                let probe = crate::pipeline::ast_transform::transform_probe(self, probe).map_err(
                    |error| {
                        if source == MembershipSource::WitnessAnon {
                            DelightQLError::validation_error_categorized(
                                "resolution/anon/witness_shape",
                                "a witness anonymous table (+_ or \\+_) is a membership test: every header must be a ground value or an lvar that unifies with a column in scope",
                                "a header that unifies with nothing would declare a fresh column, and a membership test has no columns to declare",
                            )
                        } else {
                            error
                        }
                    },
                )?;
                let rows = rows.try_map(|row| -> Result<_> {
                    Ok(ValueRow(row.0.try_map(|e| self.transform_domain(e))?))
                })?;
                Ok(ast_resolved::TruthExpression::Membership(Membership {
                    probe,
                    rows,
                    negated,
                    source,
                }))
            }

            // InRelational → fresh subquery resolution via registry
            ast_unresolved::TruthExpression::RelationalMembership(RelationalMembership {
                probe,
                relation: subquery,
                addressing: ProbeAddressing { identifier, .. },
                negated,
            }) => {
                let resolved_value = crate::pipeline::ast_transform::transform_probe(self, probe)?;
                let available = self.available.clone();
                let config = self.config.clone();
                let (resolved_subquery, _) = super::resolve_relational_expression_with_registry(
                    *subquery,
                    self.registry,
                    Some(&available),
                    &config,
                    None,
                )?;
                // Arity law: N tested expressions require exactly N produced
                // columns — a mismatch is a compile-time refusal, never a
                // backend "sub-select returns N columns" surprise.
                // The probe SAYS its width; nothing re-derives it from a
                // tuple value's shape.
                let left_arity = resolved_value.width();
                {
                    let scope = super::extract_cpr_schema(&resolved_subquery);
                    let right_arity = self.registry.identities.known_heading(scope)?.len();
                    if right_arity != left_arity {
                        return Err(crate::error::DelightQLError::validation_error_categorized(
                            "membership/arity",
                            format!(
                                "relational '{}' arity mismatch: the left side tests {} expression(s) but '{}' produces {} column(s)",
                                if negated { "not in" } else { "in" },
                                left_arity,
                                identifier.name,
                                right_arity
                            ),
                            "project the relation to the tested width, e.g. R(|> (col))",
                        ));
                    }
                }
                Ok(ast_resolved::TruthExpression::RelationalMembership(
                    RelationalMembership {
                        probe: resolved_value,
                        relation: Box::new(resolved_subquery),
                        negated,
                        addressing: (),
                    },
                ))
            }

            // InnerExists → fresh subquery resolution + USING correlation
            ast_unresolved::TruthExpression::Existence(Existence {
                polarity,
                relation: subquery,
                addressing:
                    ProbeAddressing {
                        identifier: _,
                        using_columns,
                    },
            }) => {
                let available = self.available.clone();
                let config = self.config.clone();
                let (resolved_subquery, _) = super::resolve_relational_expression_with_registry(
                    *subquery,
                    self.registry,
                    Some(&available),
                    &config,
                    None,
                )?;
                let final_subquery = super::resolving::predicates::synthesize_using_correlation(
                    resolved_subquery,
                    &using_columns,
                    &available,
                    &self.registry.identities,
                )?;
                Ok(ast_resolved::TruthExpression::Existence(Existence {
                    polarity,
                    relation: Box::new(final_subquery),
                    addressing: (),
                }))
            }

            ast_unresolved::TruthExpression::Sigma(SigmaApplication {
                proof: crate::pipeline::asts::core::NamedProof::Call(call),
                polarity,
            }) => self.resolve_sigma_application(call, polarity),

            // Everything else: the walk handles structural descent.
            other => walk_transform_boolean(self, other),
        }
    }

    fn transform_function(
        &mut self,
        func: ast_unresolved::FunctionApplication,
    ) -> Result<ast_resolved::FunctionApplication> {
        use crate::pipeline::ast_transform::walk_transform_function;

        use super::resolving::domain_expressions::mode::{resolve_mode_call, Picked};

        match func {
            // A CALLEE THAT DECLARES A MODE ANSWERS THROUGH THE DECLARATION,
            // whatever its width and whichever spelling reached it. An
            // ordinary call comes straight back and takes the ordinary road.
            ast_unresolved::FunctionApplication::Standard(application) => {
                match resolve_mode_call(self, application.clone(), Picked::Whole)? {
                    Some(resolved) => Ok(resolved),
                    None => Ok(ast_resolved::FunctionApplication::Standard(
                        self.resolve_standard_application(application)?,
                    )),
                }
            }
            ast_unresolved::FunctionApplication::FieldSelect(select) => {
                let picked = Picked::Named(select.field);
                Ok(resolve_mode_call(self, select.application, picked)?
                    .expect("a named pick is answered or refused, never returned"))
            }
            // A RELATION BECOMES A VALUE in a fresh resolution context, and
            // the degree judgment is taken there, once.
            ast_unresolved::FunctionApplication::Scalarized(relation) => {
                Ok(ast_resolved::FunctionApplication::Scalarized(
                    super::resolving::domain_expressions::subqueries::resolve_scalar_relation_via_fold(
                        self, relation,
                    )?,
                ))
            }
            // A TEMPLATE RESOLVES WHERE IT STANDS. In value position
            // `transform_domain` reaches it first and builds the concat chain
            // the target spells; in cover position it is a `Callable::String`
            // and never arrives here at all.
            ast_unresolved::FunctionApplication::Template(template) => {
                let parts = template
                    .into_parts()
                    .into_iter()
                    .map(|part| match part {
                        ast_unresolved::ValueTemplatePart::Text(text) => {
                            Ok(ast_resolved::ValueTemplatePart::Text(text))
                        }
                        ast_unresolved::ValueTemplatePart::Interpolation(value) => {
                            Ok(ast_resolved::ValueTemplatePart::Interpolation(Box::new(
                                self.transform_domain(*value)?,
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(ast_resolved::FunctionApplication::Template(
                    ast_resolved::ValueTemplate::interpolating(parts)
                        .expect("a phase change preserves the interpolation invariant"),
                ))
            }

            // A constructed value: record spreads expand against the
            // available columns, and every other member resolves in place.
            ast_unresolved::FunctionApplication::Enclyph(enclyph) => {
                super::resolving::functions::resolve_function_enclyph_via_fold(self, enclyph)
            }

            // JsonPath: structural descent, then the wrong-aim check — pathing
            // reaches INTO a value, so a column whose declared type is plainly
            // scalar (INTEGER, REAL, BOOLEAN, dates) has no insides to reach
            // into. Without this check the failure is target-dependent and
            // silent-or-leaky (sqlite: 'malformed JSON' at runtime for text
            // that doesn't parse, silent NULLs for numbers that do). TEXT
            // stays permissive — documents live in TEXT columns — as do JSON,
            // NONE, and undeclared columns. Part of the DIALECT/interior-values
            // wrong-aim matrix; promise-ladder rung 1 preconditions.
            ast_unresolved::FunctionApplication::JsonAccess(_) => {
                let resolved = walk_transform_function(self, func)?;
                if let ast_resolved::FunctionApplication::JsonAccess(access) = &resolved {
                    let source = &access.source;
                    let wrong_aim = match source.as_ref() {
                        ast_resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(ColumnOccurrence { column, .. }),
                        )) => scalar_declaration_for(*column, &self.registry.identities)
                            .map(|decl| ("this column".to_string(), decl)),
                        _ => None,
                    };
                    if let Some((subject, decl)) = wrong_aim {
                        return Err(DelightQLError::ValidationError {
                            message: format!(
                                "cannot path into {subject}: it is declared {decl} — a \
                                     plain scalar has no insides to reach into. Pathing \
                                     ('col:{{.field}}' / 'col:[0]') expects a compound \
                                     value: something you built with {{...}}/[...], a \
                                     tree-group, or a document column (TEXT)."
                            ),
                            context: "resolver::json_path".to_string(),
                            subcategory: Some(crate::uri_registry::subcat::COMPOUND_SCALAR_COLUMN),
                        });
                    }
                }
                Ok(resolved)
            }

            // Everything else: walk handles structural descent
            // (infix, lambda, case, clause selection).
            other => walk_transform_function(self, other),
        }
    }

    fn transform_operator(&mut self, o: ast_unresolved::PipeOp) -> Result<ast_resolved::PipeOp> {
        let available = self.available.clone();
        let pivot = self.pivot_in_values.clone();
        let (resolved, output_columns) = self.resolve_operator_impl(o, &available, &pivot)?;
        self.last_operator_output = Some(output_columns);
        Ok(resolved)
    }
}

impl ResolverFold<'_, '_> {
    /// `#<n` / `#>n` — the authored row bound.
    ///
    /// The bound touches no heading, so it publishes its source's scope; the
    /// by-position fact is stamped HERE, where the bound is written, so every
    /// scope minted above it inherits it and no later reader goes looking for
    /// a LIMIT.
    fn r_resolve_bound(
        &mut self,
        source: ast_unresolved::Chain,
        bound: crate::pipeline::asts::core::TupleOrdinalClause,
        _outer_context: Option<&[crate::names::ColId]>,
        _grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        let (resolved_source, source_bubbled) = self.resolve_relational(source)?;
        let source_schema = super::extract_cpr_schema(&resolved_source);
        self.registry.identities.mark_row_bounded(source_schema);
        Ok((
            resolved_source.then(ast_resolved::Continuation::Bound {
                bound,
                cpr_schema: source_schema,
            }),
            source_bubbled,
        ))
    }

    /// `col ~= {…}` / `col ~= ~> {…}` — the destructure expansion.
    ///
    /// Unlike a restriction this ADDS columns, so it mints its own output
    /// scope and republishes the source's heading through it.
    fn r_resolve_destructure(
        &mut self,
        source: ast_unresolved::Chain,
        source_expr: ast_unresolved::DomainExpression,
        pattern: ast_unresolved::TreePattern,
        mode: crate::pipeline::asts::core::DestructureMode,
        _outer_context: Option<&[crate::names::ColId]>,
        _grounding: Option<&ast_unresolved::GroundedPath>,
    ) -> Result<(ast_resolved::Chain, BubbledState)> {
        let (resolved_source, source_bubbled) = self.resolve_relational(source)?;
        let source_schema = super::extract_cpr_schema(&resolved_source);
        let source_columns = if source_bubbled.i_provide.is_empty() {
            self.registry
                .identities
                .heading(source_schema)
                .columns_seen()
        } else {
            source_bubbled.i_provide.clone()
        };
        self.local_available = source_columns.clone();
        self.available = source_columns;
        self.qualifier_scope = source_bubbled.qualifier_scope.clone();
        let resolved_source_expr = self.transform_domain(source_expr)?;
        let unresolved_mappings =
            super::resolving::predicates::extract_key_mappings_from_unresolved_pattern(&pattern)?;
        super::resolving::predicates::validate_unresolved_pattern_for_mode(&pattern, &mode)?;
        super::resolving::predicates::validate_no_sibling_explosions(&pattern)?;
        super::resolving::predicates::validate_distinct_bindings(&pattern)?;
        let input = source_schema;
        let output = self.registry.identities.mint_derived_scope(
            crate::names::ScopeOrigin::Wrap {
                input,
                why: crate::names::WrapReason::Projection,
            },
            crate::names::Hint::None,
        );
        // A destructure predicate is a FILTER in the author's model:
        // it adds bound columns beside a heading it does not otherwise
        // touch. The wrap is the compiler's own, so the names beside
        // it must survive — each pass-through column rides the answer
        // its occurrence still has, because after the wrap no scope is
        // left to answer for it.
        for column in self.registry.identities.known_heading(input)? {
            let addressing = match self.registry.identities.addressing(column) {
                crate::names::Addressing::Published => {
                    match self.registry.identities.answering_reach(column) {
                        Some(answer) => crate::names::Addressing::BareAnswering(answer),
                        None => crate::names::Addressing::Published,
                    }
                }
                other => other,
            };
            self.registry.identities.republish_column(
                column,
                output,
                crate::names::Republish::Passthrough,
                self.registry.identities.published(column),
                addressing,
                |_| {},
            );
        }
        let mut columns = std::collections::HashMap::new();
        let mut key_mappings = Vec::new();
        for (json_key, column_name) in unresolved_mappings {
            let published = self.registry.identities.intern(&column_name, false);
            let symbol = self.registry.identities.canonical(published);
            let column = self.registry.identities.mint_column(
                output,
                crate::names::ColumnOrigin::Computed {
                    via: crate::names::Computation::Operator,
                },
                Some(published),
                crate::names::Addressing::Published,
                crate::names::ValueFacts::default(),
            );
            columns.insert(symbol, column);
            key_mappings.push(ast_resolved::DestructureMapping { json_key, column });
        }
        let pattern = super::resolving::predicates::convert_destructure_pattern_to_resolved(
            pattern,
            &columns,
            &self.registry.identities,
        )?;
        let bubbled = BubbledState::resolved(
            self.registry.identities.known_heading(output)?.to_vec(),
            &self.registry.identities,
        );
        Ok((
            resolved_source.then(ast_resolved::Continuation::Destructure {
                source: Box::new(resolved_source_expr),
                pattern,
                mode,
                schema: key_mappings,
                cpr_schema: output,
            }),
            bubbled,
        ))
    }

    /// `+p(a, b)` in truth position, resolved.
    ///
    /// The classification order is load-bearing and pinned by
    /// `sigma_guard_scope_tests` and `enlisted_guard_classification_tests`:
    /// sigma rules first, then tables/facts/consulted views, then the bin
    /// cartridge's own predicates. The first three EXPAND into an ordinary
    /// truth expression; only the fall-through survives as an application.
    /// Both outcomes are the same carrier, so no arm rewraps a truth it just
    /// produced and no caller unwraps one.
    /// A metadata level: the key is a REFERENCE into the enclosing relation,
    /// so it binds like any other, and the levels under it chain the same way
    /// the surface writes them.
    pub(in crate::pipeline::resolver) fn resolve_metadata_group(
        &mut self,
        group: ast_unresolved::MetadataGroup,
    ) -> Result<ast_resolved::MetadataGroup> {
        use super::unification::{unify_columns, ColumnReference, UnificationResult};
        use crate::pipeline::asts::core::MetadataTarget;

        let result = unify_columns(
            vec![ColumnReference::Named {
                name: group.key.name.clone(),
                qualifier: group.key.qualifier.clone(),
            }],
            &self.available,
            &self.qualifier_scope,
            &self.registry.identities,
        )
        .into_iter()
        .next()
        .expect("one metadata key produces one unification result");
        let key = match result {
            UnificationResult::Resolved(column) => column,
            UnificationResult::Unresolved(column) => {
                return Err(DelightQLError::column_not_found_error(
                    column,
                    "in metadata tree group key",
                ))
            }
            UnificationResult::Opaque => {
                return Err(crate::pipeline::resolver::opaque_reference_refusal())
            }
            UnificationResult::Refused(refusal) => return Err(refusal.into_error()),
            UnificationResult::Ambiguous { column, tables } => {
                return Err(DelightQLError::validation_error_categorized(
                    "resolution/ambiguous",
                    format!(
                        "Ambiguous metadata tree-group key '{column}' in scopes: {}",
                        tables.join(", ")
                    ),
                    "qualify the metadata tree-group key",
                ))
            }
        };
        Ok(ast_resolved::MetadataGroup {
            key: ColumnOccurrence {
                column: key,
                explicit_qualifier: false,
            },
            target: match group.target {
                MetadataTarget::Enclyph(enclyph) => MetadataTarget::Enclyph(
                    super::resolving::functions::resolve_enclyph_via_fold(self, enclyph)?,
                ),
                MetadataTarget::Group(nested) => {
                    MetadataTarget::Group(Box::new(self.resolve_metadata_group(*nested)?))
                }
            },
            cte_requirements: None,
        })
    }

    fn resolve_sigma_application(
        &mut self,
        call: ast_unresolved::PureCall,
        polarity: crate::pipeline::asts::core::Polarity,
    ) -> Result<ast_resolved::TruthExpression> {
        let reference = call.call().callee.clone();
        let arguments = call
            .call()
            .arguments
            .scalar_members()
            .iter()
            .filter_map(|member| match member {
                // A sigma's arguments are VALUES; a crossing carries its own
                // truth and is not one of them.
                crate::pipeline::asts::core::operators::ScalarArgument::Value(value) => {
                    value.domain().cloned()
                }
                crate::pipeline::asts::core::operators::ScalarArgument::Spread(_)
                | crate::pipeline::asts::core::operators::ScalarArgument::Callable(_)
                | crate::pipeline::asts::core::operators::ScalarArgument::Star
                | crate::pipeline::asts::core::operators::ScalarArgument::Context(_) => None,
            })
            .collect::<Vec<_>>();
        let functor = reference.name_text().to_string();
        let functor_stropped = reference.name_identifier().is_stropped();
        let namespace = reference.namespace_texts();
        // QUALIFIED sigma citation (`+HL.h(v)`, `+a::b.h(v)`): the
        // qualifier resolves through the SAME alias- and
        // scope-aware door as qualified relations
        // (`lookup_entity`: exact fq, session alias at the
        // prompt, the OWNING namespace's local alias inside a
        // definition, §IV plain-qualifier expansion) — dropping
        // the qualifier and resolving the bare functor would
        // silently reach the wrong rule (or none, dying at SQL
        // generation). A sigma-rule hit expands here; any other
        // outcome falls through to the unqualified probes below,
        // so an unqualified citation's resolution is unaffected.
        if !namespace.is_empty() {
            let fq = namespace.join("::");
            if let Some(entity) = self.registry.consult.lookup_entity(
                &functor,
                functor_stropped,
                &fq,
                self.config.resolution_namespace.as_deref(),
            ) {
                if entity.entity_type == crate::enums::EntityType::DqlTemporarySigmaRule {
                    let expanded = super::resolving::predicates::expand_consulted_sigma(
                        &entity.definition,
                        &functor,
                        arguments,
                    )?;
                    return self.observed_sigma_body(expanded, polarity);
                }
            }
            // Not a sigma rule: cite it as an inner-exists with
            // the qualifier STAMPED on the inner reference — the
            // qualified-relation machinery (aliases, exposure,
            // §IV expansion, and its loud refusals) resolves it.
            // A qualified citation always names a namespace
            // entity; it never falls through to the bin
            // predicates.
            return super::resolving::predicates::expand_table_as_sigma(
                self, &functor, namespace, arguments, polarity,
            );
        }
        // Check if functor matches a consulted sigma predicate
        // (entity_type = 9). Scope first: under a consulted scope
        // (resolution_namespace = Some(ns) — effect bodies, view
        // bodies, HO expansions), a sigma rule reachable from ns
        // (same file, or enlisted into it) wins over one enlisted
        // into main — mirroring the relation path's
        // scope-then-main-fallback (resolve_entity_with_alias).
        // Without the scoped probe, a SAME-FILE sigma guard fell
        // through to the bin-rewrite path and died at SQL
        // generation ("Unknown predicate rewrite"), and a
        // same-named enlisted sigma silently shadowed the scope's
        // own rule. Sigma stays FIRST in the classification order
        // (before the table probes and the bin fall-through).
        // Pinned by sigma_guard_scope_tests.
        // STRICT definition independence: inside a definition the
        // sigma lookup is scoped to the OWNING namespace — itself
        // + its own edges — with NO session fallback; at the
        // prompt (None) the scope is `home`. A scoped-probe-then-
        // session-fallback two-step is a caller-leak: a file's
        // rule could silently find a sigma through whatever the
        // CALLER happened to have enlisted.
        let consulted_sigma = self.registry.consult.lookup_enlisted_sigma(
            &functor,
            functor_stropped,
            self.config.resolution_namespace.as_deref(),
        )?;
        if let Some(entity) = consulted_sigma {
            let expanded = super::resolving::predicates::expand_consulted_sigma(
                &entity.definition,
                &functor,
                arguments,
            )?;
            return self.observed_sigma_body(expanded, polarity);
        }

        // Check if functor matches a known table, fact, or consulted
        // view (used as sigma).
        // Expand +table(args) → EXISTS (SELECT 1 FROM table WHERE table.col = arg)
        //
        // Three probes, cheapest first: the user connection's default
        // schema, enlisted DDL facts, then the SAME namespace-aware
        // authority the relation path uses (resolve_entity_with_alias,
        // honoring config.resolution_namespace + enlistment edges).
        // Without the third, a guard on a table reachable only through
        // an ENLISTED mount — or on a consulted rule visible only in
        // the Some(ns) scope — fell through to the bin-rewrite path
        // and died at SQL generation ("Unknown predicate rewrite").
        // Pinned by enlisted_guard_classification_tests.
        if self.registry.database.lookup_table(&functor)?.is_some()
            || self
                .registry
                .consult
                .lookup_enlisted_table(&functor, self.config.resolution_namespace.as_deref())?
            || {
                let spelled = if functor_stropped {
                    delightql_types::SqlIdentifier::stropped(functor.clone())
                } else {
                    delightql_types::SqlIdentifier::new(functor.clone())
                };
                self.functor_is_relation_entity(&spelled)?
            }
        {
            return super::resolving::predicates::expand_table_as_sigma(
                self,
                &functor,
                Vec::new(),
                arguments,
                polarity,
            );
        }

        // Fall through to existing path (bin cartridge sigma predicates)
        let resolved_args = arguments
            .into_iter()
            .map(|arg| self.transform_domain(arg))
            .collect::<Result<Vec<_>>>()?;
        let mut resolved_call = self.resolve_functor_call(call.into_inner())?;
        resolved_call.call_mut().arguments = ast_resolved::CallArguments::Scalar(
            resolved_args
                .into_iter()
                .map(ast_resolved::ScalarArgument::plain)
                .collect(),
        );
        Ok(ast_resolved::TruthExpression::Sigma(
            SigmaApplication::applied(
                polarity,
                crate::pipeline::asts::core::PureCall::from_inner(resolved_call),
            ),
        ))
    }

    /// A DQL truth rule's body, resolved and left UNDER its observation.
    ///
    /// The polarity is not applied here. It is a collapse — it turns UNKNOWN
    /// into a definite answer — and truth position has no expression for one;
    /// applying Kleene NOT instead is what made `\\+f(x)` drop the rows whose
    /// body is unknown from both polarities. So the application survives
    /// resolution carrying what it observes, and the lowering spells
    /// `IS TRUE` or `IS NOT TRUE`.
    fn observed_sigma_body(
        &mut self,
        body: ast_unresolved::TruthExpression,
        polarity: crate::pipeline::asts::core::Polarity,
    ) -> Result<ast_resolved::TruthExpression> {
        let body = self.transform_boolean(body)?;
        Ok(ast_resolved::TruthExpression::Sigma(SigmaApplication {
            polarity,
            proof: crate::pipeline::asts::core::NamedProof::Body(Box::new(body)),
        }))
    }
}
/// The SHAPES the mutation's source was piped through, outermost first: the
/// source chain's trailing run steps, classified for the mutation contract.
/// A non-run step ends the run exactly as the shared partition says it does.
fn classify_dml_source_shapes(source: &ast_unresolved::Chain) -> Vec<super::DmlPipeKind> {
    let mut kinds = Vec::new();
    for step in source.steps().iter().rev() {
        match step {
            ast_unresolved::Continuation::Pipe { operator, .. } => {
                kinds.push(super::classify_single_dml_op(operator));
            }
            ast_unresolved::Continuation::Structural(step) => kinds.push(match &step.form {
                ast_unresolved::StructuralForm::Ordering { .. } => {
                    super::DmlPipeKind::TupleOrdering
                }
                ast_unresolved::StructuralForm::Reposition { .. }
                | ast_unresolved::StructuralForm::Meta
                | ast_unresolved::StructuralForm::Witness { .. }
                | ast_unresolved::StructuralForm::SignedWitness
                | ast_unresolved::StructuralForm::Drill { .. }
                | ast_unresolved::StructuralForm::Narrow { .. } => super::DmlPipeKind::General,
            }),
            ast_unresolved::Continuation::Access { .. } => continue,
            ast_unresolved::Continuation::Restrict { .. }
            | ast_unresolved::Continuation::Correlate { .. }
            | ast_unresolved::Continuation::Bound { .. }
            | ast_unresolved::Continuation::Destructure { .. }
            | ast_unresolved::Continuation::Member { .. }
            | ast_unresolved::Continuation::BagOp { .. }
            | ast_unresolved::Continuation::ErJoin(_) => break,
        }
    }
    kinds
}

/// Put the resolved source into the call's source formal — the position
/// after the target, per the descriptor's layout for every mutation verb.
fn insert_dml_source_argument(call: &mut ast_resolved::FunctorCall, source: ast_resolved::Chain) {
    use crate::pipeline::asts::core::operators::{CallArguments, HoArgument};
    let members = match std::mem::replace(&mut call.call_mut().arguments, CallArguments::None) {
        CallArguments::HigherOrder(part) => {
            let mut members = part.members.into_vec();
            members.insert(1, HoArgument::Relation(source));
            members
        }
        CallArguments::None => vec![HoArgument::Relation(source)],
        CallArguments::Scalar(_) => {
            unreachable!("a mutation call carries a higher-order argument group")
        }
    };
    call.call_mut().arguments = CallArguments::higher_order(members);
}

/// Take a written DML call apart into the call and the relation it reads.
///
/// The two roles are counted, not searched: a mutation reads ONE source and
/// writes ONE target, and a call carrying two of either says nothing about
/// which one it meant. Taking the first would pick by argument order — the
/// one thing the roles exist so that nothing has to do.
fn split_dml_source(
    call: ast_unresolved::SealedCall,
) -> Result<(ast_unresolved::SealedCall, ast_unresolved::Chain)> {
    use crate::pipeline::asts::core::operators::{CallArguments, HoArgument};
    let effect = call.is_effect();
    let mut inner = call.into_inner();
    let mut relations = Vec::new();
    let mut kept = Vec::new();
    let members = match std::mem::replace(&mut inner.call_mut().arguments, CallArguments::None) {
        CallArguments::HigherOrder(part) => part.members.into_vec(),
        CallArguments::None => Vec::new(),
        other @ CallArguments::Scalar(_) => {
            inner.call_mut().arguments = other;
            return Err(DelightQLError::parse_error(
                "a mutation call carries a higher-order argument group",
            ));
        }
    };
    // THE DESCRIPTOR'S LAYOUT: the destination is the first relation formal
    // and the relation being read is the second — the position says what the
    // deleted role marks used to.
    for argument in members {
        match argument {
            HoArgument::Relation(relation) => relations.push(relation),
            value => kept.push(value),
        }
    }
    let mut relations = relations.into_iter();
    let (target, source, extra) = (relations.next(), relations.next(), relations.next());
    let Some(target) = target else {
        return Err(DelightQLError::validation_error_categorized(
            "dml/roles/target",
            "a mutation writes one relation; this call names 0".to_string(),
            "write the relation being mutated once: `|> update!(target(*))(*)`",
        ));
    };
    if extra.is_some() {
        return Err(DelightQLError::validation_error_categorized(
            "dml/roles/source",
            "a mutation reads one relation; this call names 2".to_string(),
            "pipe exactly one relation into the mutation",
        ));
    }
    let Some(source) = source else {
        return Err(DelightQLError::validation_error_categorized(
            "dml/roles/source",
            "a mutation reads one relation; this call names 0".to_string(),
            "pipe exactly one relation into the mutation",
        ));
    };
    kept.insert(0, HoArgument::Relation(target));
    inner.call_mut().arguments = CallArguments::higher_order(kept);
    Ok((
        ast_unresolved::SealedCall::from_inner(inner, effect),
        source,
    ))
}

fn extract_unresolved_base_ground_name(expr: &ast_unresolved::Chain) -> Option<String> {
    // Pipes leave the head alone; anything that brings another relation
    // means there is no single base to name.
    if !expr
        .steps()
        .iter()
        .all(|c| matches!(c, ast_unresolved::Continuation::Pipe { .. }))
    {
        return None;
    }
    match &expr.head {
        ast_unresolved::Grelex::Reference(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named { identifier, .. },
            ..
        }) => Some(identifier.name.to_string()),
        _ => None,
    }
}

fn direct_dml_terminal(expr: &ast_unresolved::Chain) -> Result<bool> {
    let Some(ast_unresolved::Relation::FunctorCall { call, .. }) = expr.as_read_relation() else {
        return Ok(false);
    };
    Ok(call.call().relations().nth(1).is_some()
        && Some(&call.call().callee).is_some_and(|reference| {
            matches!(
                crate::pipeline::asts::effects::directive_category(&reference.name_text()),
                crate::pipeline::asts::effects::DirectiveCategory::Dml(_)
            )
        }))
}

/// A `&` operand that is not a plain relation: edges are selected by their
/// terms, so there is nothing to select on.
/// The access a ground read was written under.
///
/// A mention and its parens are built together, so a mention arriving here
/// without one is a compiler fault and says so rather than defaulting to an
/// access nobody wrote.
fn read_access(access: Option<ast_unresolved::Access>) -> Result<ast_unresolved::Access> {
    access.ok_or_else(|| {
        DelightQLError::parse_error("a ground read reached resolution with no access beside it")
    })
}

/// One step of the trailing run a chain ends in.
///
/// An access and a pipe operator are different steps of ONE walk, so the run
/// carries both and parts only where each resolves.
/// The shared run partition, at the phase this walk resolves.
type RunStep = crate::pipeline::asts::core::expressions::chain::RunStep<
    crate::pipeline::asts::core::Unresolved,
>;

/// The same step, resolved. The structural member is the exact resolved
/// FORM: the stage name is already spent and the published scope is minted
/// by the run's shared tail, so the form is all a structural step still
/// owes.
enum ResolvedRunStep {
    Access(ast_resolved::Access),
    Operator(ast_resolved::PipeOp),
    Structural(ast_resolved::StructuralForm),
}

fn er_operand_error() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "grounding/er/operand_term",
        "an ER-join operand is not a relation-access term".to_string(),
        "edges are selected by their terms' canonical spellings",
    )
}

fn dml_multi_terminal_error() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "dml/shape/multi_terminal",
        "a DML terminal (insert!/update!/delete!) must be the final operation of a statement; multi-step DML via `,` (dataflow) or `;` (sequential) is not yet supported",
        "run each mutation as a separate statement",
    )
}

fn scalar_declaration_for(
    column: crate::names::ColId,
    identities: &crate::names::Registry,
) -> Option<String> {
    identities
        .facts(column)
        .declared_type
        .filter(|declaration| {
            crate::pipeline::asts::core::metadata::is_plainly_scalar_declaration(declaration)
        })
}

/// Build the catalog-wrapper query carrying the namespace's liminal
/// ledger (EFFECT-ALGEBRA §8, THE LIMINAL RELATION), as a TYPED query —
/// constructed, never spelled: a JSON-derived echo column enters as an
/// identifier value here, where the text road would have spliced it into
/// source and asked the grammar to re-read it. The shape mirrors the
/// stored wrapper (`_(*) :- sys::meta.generator("ns")(*)`,
/// system.rs::register_catalog_wrapper) with one addition: a CTE that reads
/// the namespace's `liminal_receipt` rows and packs them into a `liminal`
/// tree-group column whose keys are the receipt prefix (success, operation)
/// plus THIS namespace's corresponding-union echo columns — fixed in the
/// built query, per-namespace at resolve time. The tree group is what
/// hands the interior-drill machinery its schema; everything downstream
/// (drill resolution, json_each/json_extract emission) is the stock path
/// that answers `.entities(*)` and `.namespaces(*)`. Receipt rows are packed
/// in rowid = insertion = file-appearance order: the `#(lim_id)` step before
/// the tree group pins the pack order through an ORDER BY subquery —
/// without it, SQLite's automatic covering index on the join reorders the
/// receipts (observed live: alias! packed before enlist!). The presented
/// ledger still carries no sequence column: `lim_id` orders the pack and is
/// not a tree-group key. An empty ledger packs to `[]`, which the
/// drill's outer join presents as one all-NULL receipt row — exactly how an
/// empty `namespaces` drill presents. Pinned by effects/liminal--43, --45,
/// and `liminal_drill_presents_the_corresponding_union`.
///
/// Every provenance slot says COMPILER: the binding is a generated
/// subject with `CompilerGenerated` origin, and the two restrictions are
/// `Generated` — no author wrote any of them. The CTE's minted scope
/// therefore hints `cte`-prefixed rather than `lim_cte` (an internal SQL
/// alias only; rows and headings are unchanged).
fn liminal_wrapper_query(
    ns_fq: &str,
    echo_columns: &[String],
    identities: &std::rc::Rc<crate::names::Registry>,
) -> ast_unresolved::Query {
    use crate::pipeline::asts::core::expressions::enclyph::{Enclyph, Record, RecordMember};
    use crate::pipeline::asts::core::expressions::paths::{JsonAccess, Path, PathStep};
    use crate::pipeline::asts::core::{
        Access, Continuation, CteBinding, CteSubject, FilterOrigin,
        GroupSpec, LiteralValue, NamespacePath, OneOut, OrderingSpec, OutItem, PipeOp,
        ReductionItem, StructuralForm, StructuralStep,
    };
    use crate::pipeline::asts::vocabulary::Vec1;

    let mut cols: Vec<String> = vec!["success".to_string(), "operation".to_string()];
    for c in echo_columns {
        if !cols.contains(c) {
            cols.push(c.clone());
        }
    }

    let column = |qualifier: Option<&str>, name: &str| {
        ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
            AuthoredColumn {
                name: SqlIdentifier::new(name),
                qualifier: qualifier.map(SqlIdentifier::new),
                namespace_path: NamespacePath::empty(),
            },
        )))
    };
    let equals = |left: ast_unresolved::DomainExpression, right: ast_unresolved::DomainExpression| {
        ast_unresolved::TruthExpression::Comparison(crate::pipeline::asts::core::Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
            left: Box::new(left),
            right: Box::new(right),
        })
    };
    let sys_ns = NamespacePath::from_fq_string("sys::ns")
        .expect("the engine namespace spelling is constant");
    let read = |namespace_path: NamespacePath, name: &str, alias: &str, outer: bool| {
        ast_unresolved::Chain::read(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named {
                    identifier: ast_unresolved::QualifiedName {
                        namespace_path,
                        name: SqlIdentifier::new(name),
                    },
                    alias: Some(SqlIdentifier::new(alias)),
                    mutation_target: false,
                    passthrough: false,
                },
                outer,
                cpr_schema: (),
            },
            Access::All,
            (),
        )
    };

    // (lim_r.id as lim_id, lim_r.receipt:{.c} as c, …)
    let mut items = vec![OutItem::One(OneOut {
        expr: ast_unresolved::OutValue::Domain(column(Some("lim_r"), "id")),
        naming: Some(SqlIdentifier::new("lim_id")),
        output: (),
    })];
    items.extend(cols.iter().map(|c| {
        OutItem::One(OneOut {
            expr: ast_unresolved::OutValue::Domain(ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::JsonAccess(JsonAccess {
                    source: Box::new(column(Some("lim_r"), "receipt")),
                    path: Path::try_from_steps(vec![PathStep::Key(c.clone())])
                        .expect("a key step is a path"),
                }),
            )),
            naming: Some(SqlIdentifier::new(c)),
            output: (),
        })
    }));

    // %(~> {success, operation, …} as liminal)
    let members: Vec<RecordMember<Unresolved>> = cols
        .iter()
        .map(|c| {
            RecordMember::SelfKeyed(NamedReference(AuthoredColumn {
                name: SqlIdentifier::new(c),
                qualifier: None,
                namespace_path: NamespacePath::empty(),
            }))
        })
        .collect();
    let pack = ReductionItem::Out(OutItem::One(OneOut {
        expr: ast_unresolved::OutValue::Domain(ast_unresolved::DomainExpression::Application(
            ast_unresolved::FunctionApplication::Enclyph(Enclyph::Record(Record {
                members: Vec1::try_from_vec(members)
                    .expect("the receipt prefix keys are constant"),
            })),
        )),
        naming: Some(SqlIdentifier::new("liminal")),
        output: (),
    }));

    let ledger = read(sys_ns.clone(), "namespace", "lim_ns", false)
        .then(Continuation::Restrict {
            condition: equals(
                column(Some("lim_ns"), "fq_name"),
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(LiteralValue::String(
                        ns_fq.to_string(),
                    )),
                ),
            ),
            origin: FilterOrigin::Generated,
            cpr_schema: (),
        })
        .then(Continuation::Member {
            rhs: read(sys_ns, "liminal_receipt", "lim_r", true),
            correlation: None,
            join_type: Some(JoinType::LeftOuter),
            cpr_schema: (),
        })
        .then(Continuation::Restrict {
            condition: equals(
                column(Some("lim_r"), "namespace_id"),
                column(Some("lim_ns"), "id"),
            ),
            origin: FilterOrigin::Generated,
            cpr_schema: (),
        })
        .then(Continuation::Pipe {
            operator: PipeOp::Project(
                Vec1::try_from_vec(items).expect("lim_id is always projected"),
            ),
            named: None,
            cpr_schema: (),
        })
        .then(Continuation::Structural(StructuralStep {
            form: StructuralForm::Ordering {
                specs: vec![OrderingSpec {
                    column: column(None, "lim_id"),
                    direction: None,
                }],
            },
            named: None,
            cpr_schema: (),
        }))
        .then(Continuation::Pipe {
            operator: PipeOp::Group(GroupSpec::Reduce {
                keys: Vec::new(),
                reductions: Vec1::new(pack),
                plan: crate::pipeline::asts::core::expressions::ReductionPlan::empty(),
            }),
            named: None,
            cpr_schema: (),
        });

    // sys::meta.generator("ns")(*), lim_cte(*)
    let generator = ast_unresolved::Chain::read(
        ast_unresolved::Relation::FunctorCall {
            call: crate::pipeline::asts::core::expressions::functions::FunctorCall {
                callee: crate::pipeline::asts::vocabulary::Ref::written(
                    std::rc::Rc::clone(identities),
                    crate::pipeline::asts::vocabulary::Namespace::Path(Vec1::with_tail(
                        identities.intern("sys", false),
                        vec![identities.intern("meta", false)],
                    )),
                    identities.intern("generator", false),
                    crate::pipeline::asts::vocabulary::Mark::Plain,
                    crate::pipeline::asts::vocabulary::ResolutionMode::Normal,
                ),
                arguments: crate::pipeline::asts::core::operators::CallArguments::HigherOrder(
                    crate::pipeline::asts::core::operators::HoPart {
                        members: Box::new(Vec1::new(
                            crate::pipeline::asts::core::operators::HoArgument::Value(
                                ast_unresolved::ArgumentValue::plain(
                                    ast_unresolved::DomainExpression::Application(
                                        ast_unresolved::FunctionApplication::Ground(
                                            LiteralValue::String(ns_fq.to_string()),
                                        ),
                                    ),
                                ),
                            ),
                        )),
                        landing: None,
                    },
                ),
                marks: crate::pipeline::asts::vocabulary::FunctorMarks::with_evidence(false, false),
            }
            .into(),
            alias: None,
            cpr_schema: (),
        },
        Access::All,
        (),
    )
    .then(Continuation::Member {
        rhs: ast_unresolved::Chain::read(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::Named {
                    identifier: ast_unresolved::QualifiedName {
                        namespace_path: NamespacePath::empty(),
                        name: SqlIdentifier::new("lim_cte"),
                    },
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
                cpr_schema: (),
            },
            Access::All,
            (),
        ),
        correlation: None,
        join_type: None,
        cpr_schema: (),
    });

    ast_unresolved::Query {
        cfes: Vec::new(),
        ctes: vec![CteBinding {
            expression: ledger,
            subject: CteSubject::Generated {
                name: SqlIdentifier::new("lim_cte"),
            },
            authority: crate::pipeline::asts::core::CteAuthority {
                head: crate::pipeline::asts::core::definitions::Head::glob(),
                origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
                resolution_owner:
                    crate::pipeline::asts::core::provenance::CteResolutionOwner::Entity,
            },
            recursion: (),
        }],
        body: generator,
    }
}

#[cfg(test)]
mod liminal_drill_tests {
    //! Shape pins for the BUILT liminal wrapper query (EFFECT-ALGEBRA
    //! §8). End-to-end behavior is pinned by the effects-ball liminal--43/45
    //! baselines; these pin the construction contract the drill machinery
    //! depends on — read off the typed query, since no source text exists
    //! to pin.

    use super::liminal_wrapper_query;
    use crate::pipeline::ast_unresolved;
    use crate::pipeline::asts::core::expressions::enclyph::{Enclyph, RecordMember};
    use crate::pipeline::asts::core::{
        Continuation, GroupSpec, OutItem, PipeOp, ReductionItem, StructuralForm, StructuralStep,
    };

    fn wrapper(ns: &str, echoes: &[&str]) -> ast_unresolved::Query {
        let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
        let echoes: Vec<String> = echoes.iter().map(|s| s.to_string()).collect();
        liminal_wrapper_query(ns, &echoes, &registry)
    }

    /// The pack's record keys, in construction order.
    fn pack_keys(query: &ast_unresolved::Query) -> Vec<String> {
        let ctes = &query.ctes;
        assert!(
            !ctes.is_empty(),
            "the wrapper carries its ledger as a CTE binding"
        );
        let Some(Continuation::Pipe {
            operator: PipeOp::Group(GroupSpec::Reduce { reductions, .. }),
            ..
        }) = ctes[0].expression.continuations.last()
        else {
            panic!("the ledger ends at the pack");
        };
        let ReductionItem::Out(OutItem::One(one)) = &reductions[0] else {
            panic!("the pack is one out item");
        };
        let Some(ast_unresolved::DomainExpression::Application(
            ast_unresolved::FunctionApplication::Enclyph(Enclyph::Record(record)),
        )) = one.expr.domain()
        else {
            panic!("the pack is a record");
        };
        record
            .members
            .iter()
            .map(|member| match member {
                RecordMember::SelfKeyed(reference) => reference.0.name.as_str().to_string(),
                other => panic!("the pack promotes columns by their own names: {other:?}"),
            })
            .collect()
    }

    /// The tree-group keys are the receipt prefix plus the namespace's
    /// corresponding-union echo columns, in order — the drill's presented
    /// interior schema (NULL-padded by json_extract for rows lacking a key).
    #[test]
    fn liminal_drill_presents_the_corresponding_union() {
        let query = wrapper("fx", &["namespace", "into", "shorthand"]);
        assert_eq!(
            pack_keys(&query),
            ["success", "operation", "namespace", "into", "shorthand"],
            "tree-group keys = receipt prefix + union echoes, in order"
        );

        let (ctes, main) = (&query.ctes, &query.body);
        assert!(
            !ctes.is_empty(),
            "the wrapper carries its ledger as a CTE binding"
        );
        assert!(
            ctes[0].expression.continuations.iter().any(|step| matches!(
                step,
                Continuation::Structural(StructuralStep {
                    form: StructuralForm::Ordering { .. },
                    ..
                })
            )),
            "the pack is ordered by insertion (rowid) — SQLite's automatic \
             covering index otherwise reorders the receipts"
        );
        assert!(
            matches!(
                &main.head,
                crate::pipeline::asts::core::Grelex::Reference(
                    ast_unresolved::Relation::FunctorCall { .. }
                )
            ) && main
                .continuations
                .iter()
                .any(|step| matches!(step, Continuation::Member { .. })),
            "the ledger rides beside the stored wrapper's own generator join"
        );
    }

    /// Every provenance slot says COMPILER. No author wrote the binding,
    /// its name, or the join predicates, and the AST states exactly that:
    /// a generated subject, a generated origin, and generated filters.
    #[test]
    fn the_generated_wrapper_claims_no_author() {
        let query = wrapper("fx", &[]);
        let binding = &query.ctes[0];
        assert!(
            matches!(
                binding.subject,
                crate::pipeline::asts::core::CteSubject::Generated { .. }
            ),
            "the binding stands on a generated subject"
        );
        assert!(
            matches!(
                binding.authority.origin,
                crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated
            ),
            "origin says who constructed it"
        );
        let restricts: Vec<_> = binding
            .expression
            .continuations
            .iter()
            .filter_map(|step| match step {
                Continuation::Restrict { origin, .. } => Some(origin),
                _ => None,
            })
            .collect();
        assert_eq!(restricts.len(), 2, "the namespace and receipt joins");
        assert!(
            restricts.iter().all(|origin| matches!(
                origin,
                crate::pipeline::asts::core::FilterOrigin::Generated
            )),
            "no user wrote these predicates"
        );
    }

    /// An empty ledger (namespace created by other means) packs the bare
    /// receipt prefix — the drill then presents one all-NULL receipt row
    /// over `[]`, exactly like an empty namespaces drill.
    #[test]
    fn liminal_drill_empty_ledger_presents_the_bare_prefix() {
        assert_eq!(pack_keys(&wrapper("home", &[])), ["success", "operation"]);
    }

    /// A JSON-derived echo spelling is DATA here: it lands in the query as
    /// an identifier value, never as source for the grammar to re-read. On
    /// the deleted text road this spelling misparsed the whole wrapper.
    #[test]
    fn an_untrustworthy_echo_spelling_stays_data() {
        let hostile = "a\" |> nonsense(";
        assert_eq!(
            pack_keys(&wrapper("fx", &[hostile])),
            ["success", "operation", hostile]
        );
    }
}
