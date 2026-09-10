// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! RULE INVOCATION: the caller's actuals resolve in the demand site's world,
//! the selection is invoked through the authority's one closed operation,
//! and the invoked rule's clauses are walked one paired body at a time —
//! clauses are arms; one receipt table per rule.

use super::walk::{bare_name, make_pipe, scratch_read};
use super::{
    use_effect_rule, BoundInput, EffectSelection, EffectWalk, InvokedRule, PlanFacts, ReceiptSink,
    WalkCtx,
};
use crate::defuse::admitted::{EffectActuals, EffectInput};
use crate::defuse::application::{Actual, Application, ParamRowCompletion};
use crate::diagnostic::{EffectRule, EffectRun, Ho};
use crate::error::{DelightQLError, Result};
use crate::names::Registry;
use crate::pipeline::ast_unresolved::{self, Chain, Continuation, PipeOp, Relation};
use crate::pipeline::asts::core::operators::RelationProvenance;
use crate::pipeline::asts::core::{Comparison, DomainExpression, GroundForm, ReductionPlan, Step};
use crate::pipeline::asts::ddl::HoParam;
use crate::pipeline::effect_transformer::{internal, unsupported, DeferredSql, MarkedStepKind};
use std::collections::HashMap;
use std::rc::Rc;

/// The outer rule receipt's emptiness count, named so the gate that reads it
/// can say which column it means. A receipt's own heading is `success`,
/// `operation`, `returned`, so this name collides with no sibling receipt the
/// demand could stand beside.
const RECEIPT_CARDINALITY: &str = "__clause_count";

impl<'p, 'a> EffectWalk<'p, 'a> {
    /// INVOKE ONE EFFECT RULE through the definition-use authority, from the
    /// row the author wrote. THE ADMISSION COMES FIRST: the row, whole — the
    /// landed member among the others, exactly as the build left it — is
    /// paired with the selected rule's declared row by
    /// [`Application::admit`], and only the pairs are spent here: a scalar
    /// pair resolves row-free in the demand site's world, a rule pair closes
    /// its designator, the relation pair is walked and staged under the
    /// formal it stands at. Nothing here holds a relation apart from its
    /// pair or chooses a formal for one, and a malformed placement refuses
    /// before any part of the invocation is planned, so the body never acts.
    /// [`EffectSelection::invoke`] owns everything after — admission of the
    /// instance (the R6 refusal by family identity — a nested
    /// run_namespace! demand invokes the TARGET namespace's main! while an
    /// outer main! is open: same bare name, different family, so it admits;
    /// effects ball main--24), opening, head shaping, the declaration scope
    /// and the parameter frame, and their STRUCTURAL restoration.
    pub(super) fn invoke_rule(
        &mut self,
        selection: EffectSelection,
        arguments: &ast_unresolved::CallArguments,
        ctx: &WalkCtx,
        root: bool,
    ) -> Result<Chain> {
        let display_name = selection.rule_name().to_string();
        let declared = selection.declared_params()?;
        let application = Application::admit(
            &display_name,
            &declared,
            arguments,
            0,
            ParamRowCompletion::CompleteThrough(declared.len()),
        )?;

        // THE EFFECT SPEND OF EACH PAIR: what an effect rule's contract lets
        // stand at each formal. The formal is the pair's; this match cannot
        // move a member to another one.
        let mut supplied = Vec::new();
        let mut rule_designators = Vec::new();
        let mut input: Option<(&HoParam, &Chain, RelationProvenance)> = None;
        for pair in application.pairs() {
            match (pair.formal(), pair.actual()) {
                (HoParam::Scalar { .. }, Actual::Value(value)) => {
                    supplied.push((*value).clone());
                }
                // THE ROW IS THE VALUE when a relation standing at a scalar
                // formal is one row of one column — the lift's own
                // equivalence, read exactly as the pure road reads it.
                (HoParam::Scalar { name, .. }, actual @ Actual::Relation { .. }) => {
                    match actual.lifted_scalar() {
                        Some(value) => supplied.push(value),
                        None => {
                            return Err(DelightQLError::from(Ho::RelationalArgument {
                                message: format!(
                                    "parameter '{name}' of '{display_name}' is scalar, and \
                                     position {} supplies a relation — a relation binds \
                                     only a table parameter (T(*) or T(cols))",
                                    pair.position()
                                ),
                            }))
                        }
                    }
                }
                (HoParam::Scalar { name, .. }, Actual::Rule(_)) => {
                    return Err(DelightQLError::from(Ho::ResidualRole {
                        message: format!(
                            "effect scalar parameter '{name}' received a relation designator"
                        ),
                    }))
                }
                (
                    HoParam::Scalar { name, .. },
                    Actual::Lifted(_) | Actual::Opaque | Actual::Skip,
                ) => {
                    return Err(DelightQLError::from(EffectRule::Arguments {
                        message: format!("effect parameter '{name}' requires one concrete actual"),
                    }))
                }
                (HoParam::Rule { name, signature }, actual) => match actual.designator() {
                    Some(designator) => {
                        rule_designators.push((name.clone(), signature.clone(), designator.clone()))
                    }
                    None => {
                        return Err(DelightQLError::from(Ho::RuleValueForm {
                            message: format!(
                                "effect parameter '{name}' requires a closed rule value"
                            ),
                        }))
                    }
                },
                (
                    formal @ HoParam::Relation { .. },
                    Actual::Relation {
                        relation,
                        provenance,
                    },
                ) => {
                    if input.is_some() {
                        return Err(unsupported(format!(
                            "directive '{display_name}' has more than one relational argument"
                        )));
                    }
                    input = Some((formal, relation, *provenance));
                }
                (
                    HoParam::Relation { name, .. },
                    Actual::Rule(_)
                    | Actual::Value(_)
                    | Actual::Lifted(_)
                    | Actual::Opaque
                    | Actual::Skip,
                ) => {
                    return Err(DelightQLError::from(EffectRule::Arguments {
                        message: format!(
                            "effect parameter '{name}' requires a relation; position {} \
                             supplies none",
                            pair.position()
                        ),
                    }))
                }
                (HoParam::Ground { .. }, _) => {
                    return Err(unsupported(format!(
                        "effect rule '{display_name}' declares a ground-dispatched parameter; \
                         clause dispatch by ground value is not supported in v0.1 effect bodies"
                    )))
                }
            }
        }

        // THE RELATION IS WALKED AND STAGED AT THE DEMAND SITE, in the demand
        // site's own world, under the formal the admission paired it with,
        // carrying the provenance the admission recorded for it. It is
        // staged before any configured value closes, and every consumer
        // reads the same semantic relation; none can reconstruct or
        // re-evaluate it.
        // THE FORMAL'S FACE IS APPLIED BEFORE STAGING: an appointed formal's
        // pattern is spent over the walked relation as the one argumentative
        // read, so the staged snapshot every mention reads already publishes
        // the receiving interface; an open formal stages the relation as it
        // is.
        let input = match input {
            Some((formal, relation, provenance)) => {
                let walked = self.walk_value(relation.clone(), &ctx.without_sink())?;
                let faced = crate::defuse::carriers::faced_input(formal, walked);
                Some(EffectInput::staged(
                    formal.name().clone(),
                    self.stage_ho_input(faced, ctx)?,
                    provenance,
                ))
            }
            None => None,
        };
        // THE CONSTRUCTION ROW of this call's configured rule actuals is the
        // relation already standing at the designator's position — the one
        // a pipe landed — and nothing else: binding a relation formal is not
        // standing there, so an authored sibling argument answers none and a
        // configured value that needs caller data refuses as it does on the
        // pure road.
        let construction_row = input.as_ref().and_then(EffectInput::construction_row);

        // THE CALLER'S ACTUALS RESOLVE FIRST, in the demand site's own
        // environment — before any admission, so they can enter the
        // semantic instance key. Scalar effect parameters remain row-free;
        // rule-valued configured expressions resolve against the typed
        // construction row above. The replay pass replays the discovery
        // pass's resolution: it holds a sealed reader, and the walk order
        // is the plan's.
        let (resolved_arguments, rule_arguments) = if !self.plan.discovering() {
            self.plan.replayed_arguments()?
        } else {
            let (resolved, rule_arguments) = {
                let mut rule_arguments = HashMap::new();
                let mut registry = self.plan.resolver_core()?;
                for (name, signature, designator) in rule_designators {
                    let id = ctx.world.close_rule_value(
                        &mut registry,
                        self.plan.config().clone(),
                        &designator,
                        &signature,
                        construction_row,
                    )?;
                    rule_arguments.insert(name, id);
                }
                let resolved = if supplied.is_empty() {
                    Vec::new()
                } else {
                    let config = self.plan.config().clone();
                    ctx.world.resolve_values(&mut registry, config, supplied)?
                };
                (resolved, rule_arguments)
            };
            self.plan
                .record_arguments(resolved.clone(), rule_arguments.clone());
            (resolved, rule_arguments)
        };
        crate::probe::probe!(preminted, "invoke {}", display_name);
        let instances = self.plan.config().instances.clone();
        // THE CLOSED INVOCATION: the admitted use is consumed and the
        // resolved artifact returns; plan compilation runs inside, under a
        // world the invocation owns — under the read that selected the
        // rule — through `compile_invoked` below.
        selection.invoke(
            &instances,
            EffectActuals::of(resolved_arguments, rule_arguments, input),
            self,
            ctx,
            root,
        )
    }

    /// COMPILE AN INVOKED RULE. Reached only from the definition-use
    /// authority's closed invocation, with the ONE atom that invocation
    /// built: the rule's syntax is read off it, every clause context stands
    /// in its world through the atom's own operation, and the world itself
    /// is never in hand — nothing here can pair it with another rule or
    /// keep it past this call.
    pub(super) fn compile_invoked(
        &mut self,
        invoked: &InvokedRule,
        ctx: &WalkCtx<'_>,
    ) -> Result<Chain> {
        self.rule_stack.push(invoked.name().to_string());
        let result = self.invoke_rule_inner(invoked, ctx);
        self.rule_stack.pop();
        result
    }

    /// The nested `run_namespace!(ns)` demand: look up the TARGET
    /// namespace's `main!` and invoke it inline, with
    /// resolution scoped to the target namespace for the duration — its
    /// statements resolve against its own consulted rules and tables.
    /// Enclosing guards propagate (a gated demand stays gated).
    pub(super) fn invoke_namespace_main(
        &mut self,
        target_ns: &str,
        ctx: &WalkCtx,
    ) -> Result<Chain> {
        let rule = use_effect_rule(self.plan.system(), target_ns, "main!")?.ok_or_else(|| {
            DelightQLError::from(EffectRun::NoMain {
                message: format!(
                    "namespace '{}' has no main! to demand (EFFECT-ALGEBRA F3): \
                     consult a file that defines 'main!(*) :- …' into it first",
                    target_ns
                ),
            })
        })?;
        let nested_ctx = WalkCtx {
            world: ctx.world,
            guards: ctx.guards.clone(),
            sink: None,
            bindings: HashMap::new(),
            receipt_name: "main".to_string(),
        };
        // The world is the invocation's: the demanded main! carries its
        // own namespace and the one invocation road opens it.
        self.invoke_rule(
            EffectSelection::Consulted(rule),
            &ast_unresolved::CallArguments::None,
            &nested_ctx,
            true,
        )
    }

    pub(super) fn invoke_rule_inner(
        &mut self,
        invoked: &InvokedRule,
        ctx: &WalkCtx<'_>,
    ) -> Result<Chain> {
        let bare = bare_name(invoked.name()).to_string();

        // THE BOUND INPUT: the relation the admission paired with its formal,
        // staged at the demand site; every mention of that formal inside the
        // rule reads the same snapshot by its receipt. There is no head to
        // consult for where it lands — the invoked rule carries the answer.
        let mut bindings: HashMap<String, usize> = HashMap::new();
        if let Some(input) = invoked.input() {
            let idx = self.bound_inputs.len();
            self.bound_inputs.push(BoundInput { scope: input.row() });
            bindings.insert(input.formal().to_string(), idx);
        }

        // Every clause's ending receipt lands in ONE receipt
        // table — and under receipt universality EVERY receipt-era
        // ending qualifies: DML/DDL terminals write their own receipts
        // into the shell; compositional endings (utility payload
        // producers, nested user directives) are sunk by this loop with a
        // corresponding-aligned insert. A SINGLE-clause rule stays
        // compositional (no shell): its value is already receipt-shaped,
        // and skipping the table round trip preserves the payload's
        // interior schema for same-statement release.
        // The dispositions are the invocation's own judgment of its
        // clauses; what crosses is the judgment, never the clause.
        let ending_kinds = invoked.ending_kinds();
        let mut sink_columns: Vec<String> = Vec::new();
        let sink = if invoked.clause_count() == 1 {
            None
        } else if ending_kinds.iter().all(|s| s.is_some()) {
            for (shape, _) in ending_kinds.iter().flatten() {
                for col in shape {
                    if !sink_columns.contains(col) {
                        sink_columns.push(col.clone());
                    }
                }
            }
            let table = self
                .plan
                .alloc_receipt_shell_named(&sink_columns, &format!("__r_{}", bare))?;
            Some(ReceiptSink { table })
        } else {
            return Err(unsupported(format!(
                "multi-clause effect rule '{}' has a clause that does not end in a \
                 receipt-producing disposition (EFFECT-ALGEBRA R2)",
                invoked.name()
            )));
        };

        // Clauses execute in definition order.
        let mut clause_values = Vec::with_capacity(invoked.clause_count());
        for (index, kind) in ending_kinds.iter().enumerate() {
            let self_sinking = kind.as_ref().map(|(_, s)| *s).unwrap_or(false);
            // THE CLAUSE ARRIVES ALREADY PAIRED with the world its
            // declaration owns. This loop supplies the facts it owns — the
            // guards, the bound inputs, the receipt table this clause
            // writes into — and the invocation supplies the syntax and the
            // world together. Neither half is ever a value here, so there
            // is nothing to retain and nothing to re-pair.
            let value = invoked.walk_clause(
                index,
                if self_sinking { sink.clone() } else { None },
                PlanFacts {
                    guards: ctx.guards.clone(),
                    bindings: bindings.clone(),
                    receipt_name: bare.clone(),
                },
                self,
            )?;
            if let (Some(s), false) = (&sink, self_sinking) {
                // Corresponding-aligned sink of a compositional clause
                // receipt: shell columns the clause lacks pad with NULL.
                let clause_shape = kind
                    .as_ref()
                    .map(|(shape, _)| shape.clone())
                    .unwrap_or_default();
                let armed = self.plan.exit_armed();
                self.sink_compositional_receipt(
                    s.table.relation(),
                    &sink_columns,
                    &clause_shape,
                    &value,
                    ctx,
                )?;
                self.mark_step(MarkedStepKind::RuleBoundary, &bare, Some(ctx), armed)?;
            }
            clause_values.push(value);
        }

        // THE UNIVERSAL BOUNDARY: the
        // invocation's value is ONE zero-or-one outer receipt whose
        // `returned` payload tree-groups the clause-receipt union C —
        // for sinkable rules C is the shared receipt table; for a
        // single compositional clause C is its (already receipt-shaped)
        // value. Multiplicity moves into the payload; NO propagates.
        let has_shell = sink.is_some();
        let c_value = match sink {
            Some(s) => scratch_read(s.table),
            None => clause_values
                .pop()
                .expect("single-clause rule has one clause value"),
        };
        let receipt = Self::outer_rule_receipt(
            c_value,
            &bare,
            has_shell.then(|| sink_columns.as_slice()),
            &self.plan.names(),
        );
        Ok(Chain::authored(GroundForm::Reference(Relation::InnerRelation {
            pattern: crate::pipeline::asts::core::expressions::relational::InnerRelationPattern::Indeterminate {
                identifier: crate::pipeline::asts::core::expressions::helpers::QualifiedName {
                    namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
                    name: bare.into(),
                },
                subquery: Box::new(receipt),
            },
            alias: None,
            outer: false,
        })))
    }

    /// Sink a compositional clause's receipt VALUE into the shared shell
    /// (receipt universality): `INSERT INTO <shell> (<shell cols>)
    /// SELECT <clause col or NULL> FROM (<value sql>)` — corresponding
    /// alignment pads shell columns the clause receipt lacks with NULL.
    /// Context/exit gates ride the compiled value through the shipped
    /// wrap, exactly like every other emission.
    pub(super) fn sink_compositional_receipt(
        &mut self,
        shell: crate::relation::SemanticRelation,
        _shell_columns: &[String],
        _clause_shape: &[String],
        value: &Chain,
        ctx: &WalkCtx,
    ) -> Result<()> {
        let text = self.compile_value_text(value, ctx)?;
        let gates = self.gate_exprs(ctx, true)?;
        let gated = self.plan.wrap_shipped_with_gates(text.sql, gates)?;
        let target = shell;
        let target_columns: Vec<_> = crate::relation::published_ports(&self.plan.names(), &target)?
            .into_iter()
            .map(|port| port.column())
            .collect();
        let source_scope = self
            .plan
            .names()
            .common_scope(&text.columns)
            .ok_or_else(|| internal("clause receipt value has no output scope".to_string()))?;
        let alignment = self.plan.semantic_allocation(|registry| {
            Ok(registry
                .authority()
                .set_step(
                    crate::pipeline::asts::core::SetOperator::UnionCorresponding,
                    &[target, text.relation],
                )?
                .result())
        })?;
        let matrix = crate::relation::contributions(&self.plan.names(), &alignment)?
            .ok_or_else(|| internal("receipt alignment has no contribution matrix".to_string()))?;
        let source_columns: std::collections::HashMap<_, _> = text
            .ports
            .iter()
            .copied()
            .zip(text.columns.iter().copied())
            .collect();
        let target_ports = crate::relation::published_ports(&self.plan.names(), &target)?;
        if matrix.outputs().len() != target_ports.len() {
            return Err(internal(
                "a clause receipt publishes a position outside its shell".to_string(),
            ));
        }
        let alias = self.plan.names().carrier_wrap_scope(
            source_scope,
            crate::names::WrapReason::Projection,
            "clause",
        );
        let scratch_schema = self.plan.scratch_schema()?;
        let mut sql = vec![
            DeferredSql::text(format!("INSERT INTO {}.", scratch_schema)),
            DeferredSql::Scope(target.scope()),
            DeferredSql::text(" ("),
        ];
        for (index, column) in target_columns.iter().enumerate() {
            if index > 0 {
                sql.push(DeferredSql::text(", "));
            }
            sql.push(DeferredSql::Column(*column));
        }
        sql.push(DeferredSql::text(")\nSELECT "));
        for (index, ((target_column, target_port), output)) in target_columns
            .iter()
            .zip(target_ports.iter())
            .zip(matrix.outputs())
            .enumerate()
        {
            if index > 0 {
                sql.push(DeferredSql::text(", "));
            }
            let mut cells = output.by_arm().iter();
            match cells.next() {
                Some(crate::relation::set::Contribution::Port(port)) if port == target_port => {}
                _ => {
                    return Err(internal(
                        "receipt alignment changed a shell position".to_string(),
                    ))
                }
            }
            match cells.next() {
                Some(crate::relation::set::Contribution::Port(port)) => {
                    let source_column = source_columns.get(port).copied().ok_or_else(|| {
                        internal("receipt alignment names an unbound source port".to_string())
                    })?;
                    sql.push(DeferredSql::Scope(alias));
                    sql.push(DeferredSql::text("."));
                    sql.push(DeferredSql::Column(source_column));
                }
                Some(crate::relation::set::Contribution::Padding(_)) => {
                    sql.push(DeferredSql::text("NULL AS "));
                    sql.push(DeferredSql::Column(*target_column));
                }
                None => {
                    return Err(internal(
                        "receipt alignment omitted its clause arm".to_string(),
                    ))
                }
            }
        }
        sql.extend([
            DeferredSql::text("\nFROM ("),
            gated,
            DeferredSql::text(") AS "),
            DeferredSql::Scope(alias),
        ]);
        let conn = self.plan.route(text.connection_id)?;
        self.plan.emit_commented(
            DeferredSql::concat(sql),
            conn,
            Some("clause receipt sink".to_string()),
        );
        Ok(())
    }

    /// Consolidate a rule invocation's clause-receipt union `C` into the
    /// ONE outer rule receipt: a YES receipt
    /// whose `returned` payload is the tree-grouped C ledger, guarded so
    /// empty C answers NO. C is mentioned ONCE: the same whole-table
    /// aggregate that packages the ledger also counts it, and the count
    /// filter is the emptiness gate — decided before the widened receipt
    /// exists, so aggregation cannot manufacture a YES from zero
    /// successful clauses (and no cloned mention can collide aliases or
    /// re-evaluate anything).
    pub(super) fn outer_rule_receipt(
        c_value: Chain,
        bare: &str,
        shell_columns: Option<&[String]>,
        registry: &Rc<Registry>,
    ) -> Chain {
        use crate::pipeline::asts::core::expressions::metadata_types::FilterOrigin;
        use crate::pipeline::asts::core::literals::LiteralValue;
        use crate::pipeline::asts::core::specs::{GroupSpec, OneOut, OutItem, ReductionItem};

        use crate::pipeline::asts::core::TruthExpression;
        use crate::pipeline::asts::core::{Enclyph, Record, RecordMember};
        use crate::pipeline::asts::core::{Glob, Spread};

        // Shell reads lose interior schema (a table round trip cannot carry
        // it — the single-clause path skips the shell for exactly that
        // reason), so glob inference cannot know `returned` is a tree.
        // The transformer DOES know, by receipt universality: `returned`
        // is the payload column it mints, JSON-or-NULL by construction —
        // spell the json() re-splice explicitly instead of inferring it.
        let members = match shell_columns {
            None => vec![RecordMember::Spread(Spread::Glob(Glob::whole()))],
            Some(cols) => cols
                .iter()
                .map(|col| {
                    if col == "returned" {
                        RecordMember::Keyed {
                            key: "returned".to_string(),
                            value: Box::new(DomainExpression::Application(
                                crate::pipeline::asts::core::FunctionApplication::Standard(
                                    crate::pipeline::asts::core::StandardApplication::plain(
                                    crate::pipeline::asts::core::PureCall::from_inner(crate::pipeline::asts::core::FunctorCall::scalar(
                                        crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                                            registry,
                                            crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                                            "json",
                                        ),
                                        vec![DomainExpression::lvar_builder(
                                            "returned".to_string(),
                                        )
                                        .build()],
                                    )),
                                    ),
                                ),
                            )),
                        }
                    } else {
                        RecordMember::SelfKeyed(crate::pipeline::asts::core::NamedReference(
                            crate::pipeline::asts::core::AuthoredColumn {
                                name: col.as_str().into(),
                                qualifier: None,
                                namespace_path:
                                    crate::pipeline::asts::core::NamespacePath::empty(),
                            },
                        ))
                    }
                })
                .collect(),
        };
        let record = DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Enclyph(Enclyph::Record(
                Record::plain(
                    crate::pipeline::asts::vocabulary::Vec1::try_from_vec(members)
                        .expect("a receipt record always names at least one column"),
                ),
            )),
        );
        let count = DomainExpression::Application(
            crate::pipeline::asts::core::FunctionApplication::Standard(
                crate::pipeline::asts::core::StandardApplication::plain(
                    crate::pipeline::asts::core::PureCall::from_inner(
                        crate::pipeline::asts::core::FunctorCall::scalar_application(
                            crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                                registry,
                                crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                                "count",
                            ),
                            vec![
                                crate::pipeline::asts::core::operators::ScalarArgument::Spread(
                                    Spread::Glob(Glob::whole()),
                                ),
                            ],
                        ),
                    ),
                ),
            ),
        );
        let grouped = make_pipe(
            c_value,
            PipeOp::Group(GroupSpec::Reduce {
                plan: ReductionPlan::empty(),
                keys: Vec::new(),
                reductions: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    ReductionItem::Out(OutItem::One(OneOut::authored(
                        record,
                        Some("returned".into()),
                    ))),
                    // The gate reads this count by name, so the reduction
                    // publishes it under the name the gate addresses.
                    ReductionItem::Out(OutItem::One(OneOut::authored(
                        count,
                        Some(RECEIPT_CARDINALITY.into()),
                    ))),
                ])
                .expect("the receipt reduction carries its two members"),
            }),
        );
        let gated = grouped.then(Step::authored(Continuation::Restrict {
            condition: TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::GreaterThan,
                left: Box::new(
                    DomainExpression::lvar_builder(RECEIPT_CARDINALITY.to_string()).build(),
                ),
                right: Box::new(DomainExpression::Application(
                    crate::pipeline::asts::core::FunctionApplication::Ground(LiteralValue::Number(
                        "0".to_string(),
                    )),
                )),
            }),
            origin: FilterOrigin::UserWritten,
        }));
        let widened = make_pipe(
            gated,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        None,
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::Number("1".to_string()),
                            ),
                        ),
                        Some("success".into()),
                    )),
                    OutItem::One(OneOut::authored(
                        DomainExpression::Application(
                            crate::pipeline::asts::core::FunctionApplication::Ground(
                                LiteralValue::String(format!("{bare}!")),
                            ),
                        ),
                        Some("operation".into()),
                    )),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        );
        make_pipe(
            widened,
            PipeOp::Project(
                crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("success".to_string()).build(),
                        None,
                    )),
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("operation".to_string()).build(),
                        None,
                    )),
                    OutItem::one(OneOut::authored(
                        DomainExpression::lvar_builder("returned".to_string()).build(),
                        None,
                    )),
                ])
                .expect("the synthesized receipt projection is nonempty"),
            ),
        )
    }

    /// Splice a bound HO input at its reference site: the input was
    /// staged at the demand site, and every mention reads the SAME
    /// snapshot by its receipt.
    pub(super) fn splice_bound_input(&mut self, idx: usize) -> Result<Chain> {
        Ok(scratch_read(self.bound_inputs[idx].scope))
    }
}
