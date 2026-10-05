// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The middle's one door to retained code.
//!
//! Every retained function the middle calls is wrapped here. Retained
//! types are re-exported from here, never named elsewhere in the middle. A
//! door supplies the statement or the catalog, names, or SQL construction.
//! Three doors return a retained judgment the middle spends and never
//! re-decides: the head laws
//! ([`Input::assemble_heads`]), the query-local claim ledger
//! ([`Input::query_local`]) and the session's admission ([`admit`]). The
//! catalog is read as raw rows ([`CatalogRows`]): what a name refers to is
//! the new middle's own selection.

use std::rc::Rc;

pub(crate) use crate::error::Result;
/// Identifier name equality (its case folding and stropping) is a retained
/// service; the middle names the type through this re-export only.
pub(crate) use delightql_types::SqlIdentifier;
pub(crate) use crate::pipeline::asts::core::metadata::NamespacePath;
pub(crate) use crate::pipeline::asts::core::definitions::{
    ClauseFormals, DefKind, HeadItems, HoParam, MarkedScopeId, ResidualMode, ResidualSignature, Supply,
};
pub(crate) use crate::pipeline::asts::core::expressions::{QualifiedName, SelectorItem};
pub(crate) use crate::pipeline::asts::core::operators::AuthoredDrill;
pub(crate) use crate::pipeline::asts::core::Record;
pub(crate) use crate::pipeline::asts::core::CompileTimeInteger;
pub(crate) use crate::pipeline::asts::ddl::kind_name;
pub(crate) use crate::pipeline::asts::core::expressions::truth::{Membership, MembershipSource, NamedProof, Probe, RelationalMembership};
pub(crate) use crate::pipeline::asts::core::expressions::{
    CaseExpression, DomainExpression, Enclyph, FactFunctionMode, FieldSelect, FunctionApplication, InnerRelationPattern,
    NamedReference, Path, PathStep, Polarity, RecordMember, Reference, ScalarRelation,
    StandardApplication, TruthExpression, Tuple, TupleElement, ValueTemplate, ValueTemplatePart, WindowSpec,
};
pub(crate) use crate::pipeline::asts::core::expressions::{
    DestructurePattern, IterationPattern, MetadataBinding, MetadataGroup, MetadataTarget, NestedPattern, PatternTarget,
    RecordPattern, RecordPatternMember, TreePattern,
};
pub(crate) use crate::pipeline::asts::core::expressions::chain::{
    Continuation, ErJoinStep, GroundForm, StructuralForm, WholeHeading,
};
pub(crate) use crate::pipeline::asts::core::operators::{
    CallArguments, ColumnAlias, FrameBound, FrameMode, HoArgument, JoinRoles, MemberRole, PipeOp as AstPipeOp,
    ScalarArgument,
};
pub(crate) use crate::pipeline::asts::core::expressions::{Callable, RegexCase, RenameSource};
pub(crate) use crate::pipeline::asts::core::specs::{NameTarget, RenameSpec};
pub(crate) use crate::pipeline::asts::core::specs::{
    DelegateSpec, GroupSpec, OrderDirection, OrderingSpec, OutItem, PivotSpec, ReductionItem, RepositionSpec,
    TupleOrdinalClause,
    TupleOrdinalOperator,
};
pub(crate) use crate::pipeline::asts::core::queries::{CfeFormalRole, ContextMode};
pub(crate) use crate::pipeline::asts::core::{
    Access, CteBinding, CteSubjectView, GroundMention, LexicalHorizon, LiteralValue,
    QueryLocalDemand, QueryLocalKind, QueryLocalNames, Relation, SetOperator, Slot, Spread,
};

pub(crate) use crate::pipeline::asts::core::DomainHole;

pub(crate) use crate::pipeline::asts::ddl::{Clause, ClauseDecl, DdlBody};
/// A manifest's declared table as the shared front end reads its companion
/// cells: columns, declared types, and each cell's parsed constraint or
/// default.
pub(crate) use crate::ddl_pipeline::asts::{CreateTableDef as DeclaredTable, DdlConstraint, DdlDefault};
pub(crate) use crate::ddl_pipeline::sql_ast::{SqlColumnDef, SqlCreateTable, SqlDefaultClause, SqlTableConstraint};
pub(crate) use crate::pipeline::asts::unresolved::{Chain, InlineDdlSpec, Query};
pub(crate) use crate::pipeline::asts::vocabulary::{BinOp, CmpOp, Qualifier};

/// The compilation's name arena.
pub(crate) type Names = Rc<crate::names::Registry>;

pub(crate) use crate::bin_cartridge::registry::BinCartridgeRegistry as BinRegistry;
pub(crate) use crate::compiler_limits::Admission;
pub(crate) use crate::pipeline::compiled_query::{CompiledPlan, CompiledQuery};
pub(crate) use crate::pipeline::normalize::Goal;
pub(crate) use crate::pipeline::inline_ddl::Trailing;
pub(crate) use crate::system::DelightQLSystem;
pub(crate) use crate::pipeline::sqlite_affinity::Affinity as SqliteAffinity;
pub(crate) use crate::pipeline::type_classes::TypeClass as DeclaredClass;
pub(crate) use delightql_types::introspect::StoredRowIdentity;

/// What reaches the compiler about one stored table's row identity: the
/// serving backend's answer about its storage and the target's spellings of
/// its row locator, per part.
pub(crate) struct RowIdentityFacts {
    pub(crate) stored: std::result::Result<StoredRowIdentity, Unanswered>,
    pub(crate) spellings: Option<&'static [&'static [&'static str]]>,
}

/// Why no storage answer about a table's row identity reaches the compiler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Unanswered {
    /// The serving backend gives none.
    Backend,
    /// The serving backend serves another dialect than the statement's
    /// target, so its storage is not the target's.
    OtherDialect,
}

/// What text submits, as the retained front end reads it.
pub(crate) enum Submitted {
    /// One statement, and the settings it declares; the inline blocks
    /// written after it are the entrance's to admit.
    Goal { query: Query, settings: Settings },
    /// Definitions and no statement.
    Definitions,
}

/// Set where this compilation's SQL is stored: the database reached by each
/// backend schema in `database`, the primary's name `primary`.
pub(crate) fn store_in(names: &Names, database: &[Option<&str>], primary: &str) {
    names.store_in(database, primary);
}

/// A fresh name arena for one compilation, armed with the session's
/// admission: under an observation, an effect is refused before it runs.
pub(crate) fn open_names(admission: Admission) -> Names {
    Rc::new(match admission {
        Admission::Execute => crate::names::Registry::new(&[]),
        Admission::Observe => crate::names::Registry::observing(&[]),
    })
}

/// Publish the limits a compilation is armed with in the catalog, where
/// `sys::execution.compiler_limit` reads them.
pub(crate) fn publish_limits(system: &DelightQLSystem, names: &Names) {
    system.publish_compiler_limits(names.limits());
}

/// Spend the session's admission on an effect about to run: an observation
/// refuses it with the product's identity. A retained judgment, spent here
/// and never re-decided.
pub(crate) fn admit(admission: Admission, demand: &str) -> Result<()> {
    admission.admit(demand, crate::bin_cartridge::ExecutionClass::Effect)
}

/// Parse and normalize one submission, and take the one form it submits.
pub(crate) fn read_submission(names: &Names, text: &str) -> Result<Submitted> {
    let _running = crate::compiler_limits::Running::under(names.limits_shared());
    let tree = crate::pipeline::parse::prompt_submission(text, names.limits().nesting())?;
    let normalized = crate::pipeline::normalize::submission(&tree, Rc::clone(names))?;
    Ok(submitted(crate::pipeline::one_submission(normalized)?))
}

/// Parse and normalize one protocol submission through the entrance the
/// relay reads it by, and take the one form it submits.
pub(crate) fn read_wire_submission(names: &Names, text: &str) -> Result<Submitted> {
    let _running = crate::compiler_limits::Running::under(names.limits_shared());
    let tree = crate::pipeline::parse::submission_attributed(text, names.limits().nesting())
        .map_err(|refusal| refusal.error)?;
    let normalized = crate::pipeline::normalize::submission(&tree, Rc::clone(names))?;
    Ok(submitted(crate::pipeline::one_submission(normalized)?))
}

/// The session's primary connection: the one a table function the target
/// serves runs on.
pub(crate) const PRIMARY_CONNECTION: i64 = crate::system_vocabulary::PRIMARY_CONNECTION_ID;

/// A relation the catalog does not record, read on the target: the
/// connection serving it, the backend schema qualifying its read, and its
/// columns with their declared types. Its heading is the engine's own as
/// introspection answers it, or, for a target table function introspection
/// does not describe, the complete pattern the call declares (every type
/// unknown).
pub(crate) struct EngineRelation {
    pub(crate) connection: i64,
    pub(crate) schema: Option<String>,
    pub(crate) columns: Vec<(SqlIdentifier, Option<String>)>,
}

/// The danger and option settings one statement declares, as the new
/// middle reads them: which gates it acknowledges, and whether every
/// setting it declares is one the new middle covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Settings {
    pub(crate) min_multiplicity: bool,
    pub(crate) covered: bool,
}

/// The one reading of a statement's declared settings: the inline
/// `min_multiplicity` gate, acknowledged without a state word, is covered; every other danger and state is not. An option
/// is covered: it never changes what a statement means, and the front end
/// has already refused an option or a value its vocabulary does not hold.
/// The new middle has one realization, so an option it reads asks for
/// nothing.
pub(crate) fn settings(declared: &crate::pipeline::normalize::Sidecars) -> Settings {
    use crate::pipeline::asts::core::DangerState;
    let gate = |name: &str| {
        let uri = crate::pipeline::danger_gates::canonical_danger_uri(name);
        move |d: &crate::pipeline::asts::core::DangerSpec| d.uri == uri && d.state == DangerState::On
    };
    let min_multiplicity = gate("semantics/min_multiplicity");
    Settings {
        min_multiplicity: declared.dangers.iter().any(&min_multiplicity),
        covered: declared.dangers.iter().all(&min_multiplicity),
    }
}

fn submitted(submission: crate::pipeline::Submission) -> Submitted {
    match submission {
        crate::pipeline::Submission::Goal(goal) => Submitted::Goal {
            settings: settings(&goal.declared),
            query: goal.into_query(),
        },
        crate::pipeline::Submission::Definitions(_) => Submitted::Definitions,
    }
}

/// What the new middle's one selection says a mention refers to when it
/// is an entity the engine serves: that entity's namespace and name. `None`
/// for any other answer, which elaboration judges where the mention stands.
pub(crate) type ServedJudgment<'j> =
    &'j dyn Fn(&delightql_types::SqlIdentifier, Option<&Qualifier>) -> Result<Option<(String, delightql_types::SqlIdentifier)>>;

/// One relation the runtime serves, as the runtime takes it: the entity the
/// selection decided (its namespace path and name), the spelling a refusal
/// names it by, and its scalar arguments or the source rows lifted into it.
enum ServedSupply {
    Scalar {
        identity: String,
        namespace: Vec<String>,
        name: String,
        arguments: Vec<DomainExpression>,
        alias: Option<String>,
    },
    Lifted {
        identity: String,
        namespace: Vec<String>,
        name: String,
        source: Chain,
        /// The source's own supply, when the runtime serves the source.
        served_source: Option<Box<Result<ServedSupply>>>,
    },
}

impl ServedSupply {
    fn identity(&self) -> &str {
        match self {
            ServedSupply::Scalar { identity, .. } | ServedSupply::Lifted { identity, .. } => identity,
        }
    }
}

/// Whether the bin the selection decided executes, read by its identity.
fn executes(bins: &BinRegistry, namespace: &[String], name: &str) -> bool {
    bins.lookup_qualified_entity(namespace, name)
        .is_some_and(|entity| entity.as_effect_executable().is_some())
}

/// How the runtime serves a chain's head, in the shapes its executor runs:
/// a qualified read with its arguments in the parens; a qualified call with
/// scalar arguments; a call with one relation landed in it. Whether the
/// head is served is the one judgment of its mention, in the law's tiers:
/// a bare name a query-local block in scope claims is the statement's own
/// (`claims`), and only an unclaimed name is `judge`'s to answer. `None`
/// when the runtime does not serve the head. A served head in a shape the
/// runtime refuses is that refusal.
fn served_supply(
    bins: &BinRegistry,
    chain: &Chain,
    claims: &QueryLocalNames,
    judge: ServedJudgment<'_>,
) -> Option<Result<ServedSupply>> {
    use crate::pipeline::asts::core::{GroundMention, Relation};
    let GroundForm::Reference(relation) = chain.head().form() else {
        return None;
    };
    let served = |name: &delightql_types::SqlIdentifier, qualifier: Option<&Qualifier>| {
        // A claimed query-local name never falls through: the claim decides
        // the mention even where elaboration refuses it for kind or horizon.
        if qualifier.is_none() && claims.claim(name).is_some() {
            return None;
        }
        match judge(name, qualifier) {
            Ok(Some((namespace, name))) => {
                let namespace: Vec<String> = namespace.split("::").map(str::to_string).collect();
                executes(bins, &namespace, name.as_str()).then(|| Ok((namespace, name.as_str().to_string())))
            }
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    };
    match relation {
        Relation::Ground {
            mention: GroundMention::Named { identifier, alias, .. },
        } if !identifier.namespace_path.is_empty() => {
            let written = identifier.name.to_string();
            let access = chain.head_access()?;
            let (namespace, name) = match served(&identifier.name, Some(&identifier.namespace_path.qualifier()))? {
                Ok(selected) => selected,
                Err(error) => return Some(Err(error)),
            };
            let arguments = match access {
                Access::Slots(slots) => slots
                    .iter()
                    .enumerate()
                    .map(|(index, slot)| {
                        slot.term().ok_or_else(|| {
                            delightql_types::DelightQLError::from(delightql_types::diagnostic::EffectBin::ValuelessArgument {
                                message: format!(
                                    "'{written}' was given a slot that supplies no value at argument {}; this bin \
                                     relation takes values there",
                                    index + 1
                                ),
                            })
                        })
                    })
                    .collect::<Result<Vec<_>>>(),
                _ => Err(delightql_types::diagnostic::Directive::InvocationAccess {
                    message: format!("Bin relation '{written}' requires positional arguments"),
                }
                .into()),
            };
            Some(arguments.map(|arguments| ServedSupply::Scalar {
                identity: written,
                namespace,
                name,
                arguments,
                alias: alias.as_ref().map(|a| a.to_string()),
            }))
        }
        Relation::FunctorCall { call, alias } => {
            let callee = &call.call().callee;
            let written = callee.name_text();
            if written.ends_with('!') {
                return None;
            }
            let qualifier = callee.qualifier();
            let (namespace, name) = match served(&callee.name_identifier(), qualifier.as_ref())? {
                Ok(selected) => selected,
                Err(error) => return Some(Err(error)),
            };
            let identity = match &qualifier {
                Some(qualifier) => format!("{}.{written}", qualifier.segments().join("::")),
                None => written,
            };
            let arguments = &call.call().arguments;
            let landed = match arguments.judged() {
                Ok(judged) => judged.landed().map(|landed| landed.relation.clone()),
                Err(error) => return Some(Err(error)),
            };
            let tables = arguments
                .ho_members()
                .filter(|member| matches!(member, HoArgument::Relation(_) | HoArgument::Rule(_) | HoArgument::Landed(_)))
                .count();
            match landed {
                Some(source) if tables == 1 => {
                    let served_source = if source.continuations().is_empty() {
                        served_supply(bins, &source, claims, judge).map(Box::new)
                    } else {
                        None
                    };
                    Some(Ok(ServedSupply::Lifted {
                        identity,
                        namespace,
                        name,
                        source,
                        served_source,
                    }))
                }
                Some(_) => None,
                None if tables > 0 => Some(Err(served_scalar_arguments(&identity, arguments).err().unwrap_or_else(|| {
                    super::core::refuse::contract("a table-valued bin argument its argument rule admitted")
                }))),
                None if qualifier.is_none() => None,
                None => Some(served_scalar_arguments(&identity, arguments).map(|arguments| ServedSupply::Scalar {
                    identity: identity.clone(),
                    namespace,
                    name,
                    arguments,
                    alias: alias.as_ref().map(|a| a.to_string()),
                })),
            }
        }
        _ => None,
    }
}

/// The scalar arguments a bin executable takes in a call: each value, in
/// order. The access glob and the context marker supply none; a table-valued
/// argument refuses where it stands.
fn served_scalar_arguments(identity: &str, arguments: &CallArguments) -> Result<Vec<DomainExpression>> {
    let mut scalars = Vec::new();
    for member in arguments.scalar_members().iter() {
        match member {
            ScalarArgument::Value(value) => scalars.push(value.value.clone()),
            ScalarArgument::Callable(_) | ScalarArgument::Spread(_) | ScalarArgument::Star | ScalarArgument::Context(_) => {}
        }
    }
    for (index, argument) in arguments.ho_members().enumerate() {
        match argument {
            HoArgument::Value(value) => scalars.push(value.value.clone()),
            HoArgument::Landing(_) | HoArgument::Skip => {}
            HoArgument::Relation(_) | HoArgument::Rule(_) | HoArgument::Landed(_) => {
                return Err(delightql_types::diagnostic::EffectBin::TableArgument {
                    message: format!(
                        "{identity} received a table-valued argument at position {}; bin executables consume \
                         scalar arguments in this call shape. The table argument cannot be discarded or shift \
                         the later arguments. Pass scalar expressions in the first parentheses.",
                        index + 1
                    ),
                }
                .into());
            }
        }
    }
    Ok(scalars)
}

/// The rows a computed relation feeds into a served call, as the new
/// middle compiles and runs it: the caller's own compilation of the feed.
pub(crate) type Feed<'f> = &'f mut dyn FnMut(&mut DelightQLSystem, Query) -> Result<Vec<Vec<DomainExpression>>>;

/// A chain whose head is a mention: the heads the plan judges, counted in
/// the walk's order within one site.
fn mentions(chain: &Chain) -> bool {
    use crate::pipeline::asts::core::Relation;
    matches!(
        chain.head().form(),
        GroundForm::Reference(Relation::Ground { .. } | Relation::FunctorCall { .. })
    )
}

/// One served mention, judged where its text stands: its place among the
/// mention heads of its site, the head and access the judgment read, and
/// how the runtime serves it.
struct ServedMention {
    at: usize,
    head: (GroundForm, Option<Access>),
    supply: Result<ServedSupply>,
}

/// One place a statement's text stands: a relation binding's body, under
/// the bindings before it, or the statement's body, under them all. A feed
/// from a served call compiles where its text stands.
struct ServedSite {
    locals: crate::pipeline::asts::core::QueryLocals<crate::pipeline::asts::core::Unresolved>,
    served: Vec<ServedMention>,
}

/// The relations one statement's text reads from the runtime, judged before
/// anything runs: each relation binding's site in order, then the body's.
pub(crate) struct ServedPlan {
    bindings: Vec<ServedSite>,
    body: ServedSite,
}

impl ServedPlan {
    /// The first relation the text reads from the runtime, as its spelling
    /// names it.
    pub(crate) fn first(&self) -> Option<&str> {
        self.bindings
            .iter()
            .chain(std::iter::once(&self.body))
            .flat_map(|site| site.served.iter())
            .map(|mention| mention.supply.as_ref().map(ServedSupply::identity).unwrap_or("a runtime-served relation"))
            .next()
    }
}

/// The qualifiers a condition's references are written with, in written
/// order: each qualified column's and each qualified ordinal's, nested
/// relations included. A condition the walk cannot read refuses.
pub(crate) fn written_qualifiers(truth: &TruthExpression) -> Result<Vec<SqlIdentifier>> {
    use crate::pipeline::ast_visit::{walk_visit_boolean, AstVisit, Descent};
    use crate::pipeline::asts::core::Unresolved;
    struct Collect(Vec<SqlIdentifier>);
    impl AstVisit<Unresolved> for Collect {
        fn enter_domain(&mut self, e: &DomainExpression) -> Result<Descent> {
            match e {
                DomainExpression::Reference(Reference::Named(named)) => self.0.extend(named.0.qualifier.clone()),
                DomainExpression::Reference(Reference::Ordinal(ordinal)) => self.0.extend(ordinal.qualifier.clone()),
                DomainExpression::Reference(_) | DomainExpression::Application(_) => {}
            }
            Ok(Descent::Continue)
        }
    }
    let mut walk = Collect(Vec::new());
    walk_visit_boolean(&mut walk, truth)
        .map_err(|_| super::core::refuse::outside("a condition the qualifier walk cannot read"))?;
    Ok(walk.0)
}

/// Which relations the statement's text reads from the runtime: every chain
/// whose head the one judgment of its mention finds the engine serves, in a
/// shape the runtime runs. A served head's own read is the runtime's; the
/// steps on it are the statement's text and are judged in turn. A statement
/// the walk cannot read refuses.
pub(crate) fn served_plan(query: &Query, bins: &BinRegistry, judge: ServedJudgment<'_>) -> Result<ServedPlan> {
    use crate::pipeline::ast_visit::{walk_visit_continuation, walk_visit_relational, AstVisit, Descent};
    use crate::pipeline::asts::core::Unresolved;

    struct Collect<'a> {
        bins: &'a BinRegistry,
        claims: &'a QueryLocalNames,
        judge: ServedJudgment<'a>,
        seen: usize,
        served: Vec<ServedMention>,
    }

    impl AstVisit<Unresolved> for Collect<'_> {
        fn enter_relational(&mut self, chain: &Chain) -> Result<Descent> {
            if !mentions(chain) {
                return Ok(Descent::Continue);
            }
            let at = self.seen;
            self.seen += 1;
            let Some(supply) = served_supply(self.bins, chain, self.claims, self.judge) else {
                return Ok(Descent::Continue);
            };
            self.served.push(ServedMention {
                at,
                head: (chain.head().form().clone(), chain.head_access().cloned()),
                supply,
            });
            for step in chain.steps() {
                walk_visit_continuation(self, step.form())?;
            }
            Ok(Descent::SkipSubtree)
        }
    }

    // The claims are the block's, wherever its bindings stand.
    let claims = query.local_names();
    let site = |locals: &crate::pipeline::asts::core::QueryLocals<Unresolved>, chain: &Chain| -> Result<ServedSite> {
        let mut walk = Collect {
            bins,
            claims,
            judge,
            seen: 0,
            served: Vec::new(),
        };
        walk_visit_relational(&mut walk, chain)
            .map_err(|_| super::core::refuse::outside("a statement the routing walk cannot read"))?;
        Ok(ServedSite {
            locals: locals.clone(),
            served: walk.served,
        })
    };
    let mut bindings = Vec::new();
    let mut locals = query.locals.clone();
    locals.restate_ctes_in_order(|cte, reached| {
        bindings.push(site(reached, cte.body())?);
        Ok(cte)
    })?;
    let body = site(&query.locals, &query.body)?;
    Ok(ServedPlan { bindings, body })
}

/// Supply every relation the plan says the statement's text reads from the
/// runtime, and answer the statement as elaboration reads it: each served
/// mention's head and its own read replaced by the relation the runtime
/// answered for that mention, the steps on it kept. The runtime executes
/// each served mention once, under the compilation's admission. A served
/// call fed by a relation the statement computes receives the rows `feed`
/// compiles and runs, with the query-local declarations in scope where the
/// call stands. Nothing else of the statement is compiled or run here.
pub(crate) fn supply_served(
    system: &mut DelightQLSystem,
    plan: ServedPlan,
    query: Query,
    names: &Names,
    feed: Feed<'_>,
) -> Result<Query> {
    let bins = system.bin_registry();
    let Query { mut locals, body } = query;
    let ServedPlan { bindings, body: served_body } = plan;
    let mut sites = bindings.into_iter();
    locals.restate_ctes(|ctes| {
        let mut supplied = Vec::with_capacity(ctes.len());
        for cte in ctes {
            let site = sites
                .next()
                .ok_or_else(|| super::core::refuse::contract("a relation binding the served plan did not judge"))?;
            supplied.push(match site.served.is_empty() {
                true => cte,
                false => Supplying::over(&bins, system, names, &mut *feed, site).supply_binding(cte)?,
            });
        }
        Ok(supplied)
    })?;
    let body = if served_body.served.is_empty() {
        body
    } else {
        Supplying::over(&bins, system, names, feed, served_body).supply_chain(body)?
    };
    Ok(Query::binding(locals, body))
}

/// The walk that puts each served mention's relation in its place: it
/// counts the mention heads of one site in the plan's order and, at each the
/// plan judged served, has the runtime serve it.
struct Supplying<'w, 'f> {
    bins: &'w BinRegistry,
    system: &'w mut DelightQLSystem,
    names: &'w Names,
    feed: Feed<'f>,
    locals: crate::pipeline::asts::core::QueryLocals<crate::pipeline::asts::core::Unresolved>,
    served: std::iter::Peekable<std::vec::IntoIter<ServedMention>>,
    seen: usize,
}

impl<'w, 'f> Supplying<'w, 'f> {
    fn over(
        bins: &'w BinRegistry,
        system: &'w mut DelightQLSystem,
        names: &'w Names,
        feed: Feed<'f>,
        site: ServedSite,
    ) -> Self {
        Supplying {
            bins,
            system,
            names,
            feed,
            locals: site.locals,
            served: site.served.into_iter().peekable(),
            seen: 0,
        }
    }

    fn supply_binding(mut self, cte: CteBinding) -> Result<CteBinding> {
        let cte = cte.folded(&mut self)?;
        self.finished()?;
        Ok(cte)
    }

    fn supply_chain(mut self, chain: Chain) -> Result<Chain> {
        use crate::pipeline::ast_transform::AstTransform;
        let chain = self.transform_relational(chain)?;
        self.finished()?;
        Ok(chain)
    }

    fn finished(&mut self) -> Result<()> {
        match self.served.next() {
            None => Ok(()),
            Some(_) => Err(super::core::refuse::contract("a served mention the supply walk did not reach")),
        }
    }
}

impl crate::pipeline::ast_transform::AstTransform<crate::pipeline::asts::core::Unresolved, crate::pipeline::asts::core::Unresolved>
    for Supplying<'_, '_>
{
    crate::pipeline::ast_transform::same_phase_payload_folds!(crate::pipeline::asts::core::Unresolved);

    fn transform_relational(&mut self, chain: Chain) -> Result<Chain> {
        if !mentions(&chain) {
            return crate::pipeline::ast_transform::walk_transform_relational(self, chain);
        }
        let at = self.seen;
        self.seen += 1;
        if self.served.peek().map(|mention| mention.at) != Some(at) {
            return crate::pipeline::ast_transform::walk_transform_relational(self, chain);
        }
        let mention = self.served.next().expect("the peeked served mention");
        if (chain.head().form(), chain.head_access()) != (&mention.head.0, mention.head.1.as_ref()) {
            return Err(super::core::refuse::contract(
                "a served mention the supply walk reached in another place than the plan judged it",
            ));
        }
        let form = run_supply(self.bins, self.system, self.names, &self.locals, &mut *self.feed, mention.supply?)?;
        let span = chain.head_span();
        let (_, steps) = chain.into_parts();
        let mut supplied = Chain::authored(form);
        for step in steps.into_iter().skip(span) {
            supplied = supplied.then(step.folded(self)?);
        }
        Ok(supplied)
    }
}

fn run_supply(
    bins: &BinRegistry,
    system: &mut DelightQLSystem,
    names: &Names,
    locals: &crate::pipeline::asts::core::QueryLocals<crate::pipeline::asts::core::Unresolved>,
    feed: Feed<'_>,
    supply: ServedSupply,
) -> Result<GroundForm> {
    let (namespace, name) = match &supply {
        ServedSupply::Scalar { namespace, name, .. } | ServedSupply::Lifted { namespace, name, .. } => (namespace, name),
    };
    let entity = bins
        .lookup_qualified_entity(namespace, name)
        .ok_or_else(|| super::core::refuse::contract("a served relation whose entity is gone"))?;
    let executable = entity
        .as_effect_executable()
        .ok_or_else(|| super::core::refuse::contract("a served relation whose entity executes nothing"))?;
    let identity = supply.identity().to_string();
    let result = match supply {
        ServedSupply::Scalar { arguments, alias, .. } => {
            names.limits().admission().admit(&identity, executable.class())?;
            system.note_effect_executed();
            executable.execute(&arguments, alias, system)?
        }
        ServedSupply::Lifted {
            source, served_source, ..
        } => {
            let rows = served_source_rows(bins, system, names, locals, feed, &source, served_source)?;
            names.limits().admission().admit(&identity, executable.class())?;
            system.note_effect_executed();
            executable.execute_lifted(&rows, None, system)?
        }
    };
    let crate::bin_cartridge::EntityResult::Relation(form) = result;
    Ok(form)
}

/// The rows a served call's landed source lifts into it: the rows the
/// runtime serves for a served source, a written table's rows, or the rows
/// the new middle computes for any other relation, through `feed`.
fn served_source_rows(
    bins: &BinRegistry,
    system: &mut DelightQLSystem,
    names: &Names,
    locals: &crate::pipeline::asts::core::QueryLocals<crate::pipeline::asts::core::Unresolved>,
    feed: Feed<'_>,
    source: &Chain,
    served_source: Option<Box<Result<ServedSupply>>>,
) -> Result<Vec<Vec<DomainExpression>>> {
    if source.continuations().is_empty() {
        let form = match served_source {
            Some(supply) => Some(run_supply(bins, system, names, locals, &mut *feed, (*supply)?)?),
            None => match source.head().form() {
                GroundForm::Literal(anon) if anon.table().is_some() => Some(source.head().form().clone()),
                _ => None,
            },
        };
        if let Some(GroundForm::Literal(anon)) = &form {
            if let Some(table) = anon.table() {
                return Ok(table.rows().iter().map(|row| row.iter().map(|datum| datum.value()).collect()).collect());
            }
        }
    }
    feed(system, Query::binding(locals.clone(), source.clone()))
}

/// Run a compiled feed on its connection and read its rows as the runtime
/// reads a pipe source's: every cell a string literal, a null the four
/// characters `NULL`.
pub(crate) fn feed_rows(system: &DelightQLSystem, compiled: &CompiledQuery) -> Result<Vec<Vec<DomainExpression>>> {
    let connection = match compiled.connection_id {
        Some(id) => system.get_connection(id)?,
        None => std::sync::Arc::clone(&system.connection),
    };
    let conn = connection
        .lock()
        .map_err(|e| delightql_types::diagnostic::Runtime::poisoned("Failed to acquire the connection lock for a feed", e))?;
    let (_, rows) = conn.query_all_rows(&compiled.primary_sql, &[])?;
    Ok(rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| {
                    use delightql_types::DbValue;
                    let text = match value {
                        DbValue::Null => "NULL".to_string(),
                        DbValue::Integer(i) => i.to_string(),
                        DbValue::Real(f) => f.to_string(),
                        DbValue::Text(s) => s,
                        DbValue::Blob(b) => format!("<blob {} bytes>", b.len()),
                    };
                    DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(text)))
                })
                .collect()
        })
        .collect())
}

/// A compiled query run whole on its connection, as the relay's statement
/// road runs one: its staging, the checks it may not run without, the
/// statement, then the retirement of its staging. Answers its rows.
pub(crate) fn run_query(system: &DelightQLSystem, compiled: &CompiledQuery) -> Result<Vec<Vec<DbValue>>> {
    let connection = match compiled.connection_id {
        Some(id) => system.get_connection(id)?,
        None => std::sync::Arc::clone(&system.connection),
    };
    let conn = connection
        .lock()
        .map_err(|e| delightql_types::diagnostic::Runtime::poisoned("Failed to acquire the connection lock for a read", e))?;
    let answer = (|| {
        for sql in &compiled.prepare_sqls {
            conn.execute(sql, &[])?;
        }
        for obligation in &compiled.obligations {
            let (_, rows) = conn.query_all_rows(&obligation.sql, &[])?;
            let held = rows
                .first()
                .and_then(|row| row.first())
                .is_some_and(|cell| matches!(cell, DbValue::Integer(i) if *i != 0));
            if !held {
                return Err(obligation.refusal.clone());
            }
        }
        let (_, rows) = conn.query_all_rows(&compiled.primary_sql, &[])?;
        Ok(rows)
    })();
    for sql in &compiled.cleanup_sqls {
        let _ = conn.execute(sql, &[]);
    }
    answer
}

/// A database value as the runtime reads one.
pub(crate) use delightql_types::DbValue;

/// A consulted goal that compiles to a program: a load reads user data
/// only, through its goals' witnesses.
pub(crate) fn witness_writes(spelling: &str) -> delightql_types::DelightQLError {
    delightql_types::diagnostic::Consult::WitnessReadOnly {
        message: format!(
            "the consulted goal '?- {spelling}' compiled to a statement that writes: a consultation may READ user \
             data only, through a top-level goal that proves and records a YES/NO witness"
        ),
    }
    .into()
}

/// The session's bin registry.
pub(crate) fn bin_registry(system: &DelightQLSystem) -> std::sync::Arc<BinRegistry> {
    system.bin_registry()
}

/// A clause's per-row heading offers: a headerless fact clause's `as`
/// labels, row by row (none for any other clause).
pub(crate) type RowOffers = Vec<Vec<Option<SqlIdentifier>>>;

/// A declared clause's parts, moved into the clause shape a body reading
/// takes, with a headerless fact clause's row offers beside it. A data
/// move: no head, kind, offer or body is judged.
fn declared_clause(decl: ClauseDecl) -> (DefKind, Clause, RowOffers) {
    let kind = decl.front().kind();
    let offers = decl.fact_row_offers().to_vec();
    let head = decl.front().head().clone();
    let full_source = decl.full_source().to_string();
    let doc = decl.doc().map(str::to_string);
    let body_text = decl.body_text().cloned();
    let body = decl.into_body();
    (
        kind,
        Clause {
            head,
            body,
            full_source,
            doc,
            body_text,
        },
        offers,
    )
}

// ---------------------------------------------------------------------------
// INPUT: the catalog a compile window reads.
// ---------------------------------------------------------------------------

pub(crate) use crate::definition_catalog::{CandidateFact, EdgeDeclarationFact, MentionFact, NamespaceFact};
pub(crate) use crate::namespace::NamespaceKind;
pub(crate) use crate::pipeline::asts::vocabulary::QualifierRoute;

/// Descendancy by the `::` segments of the path, each compared exactly.
pub(crate) fn is_within(fq: &str, ancestor: &str) -> bool {
    crate::namespace::is_within(fq, ancestor)
}

/// The session catalog's raw rows, read outside a compile window (the
/// served-rows plan, judged before anything runs).
pub(crate) fn session_rows<'s>(system: &'s DelightQLSystem) -> CatalogRows<'s> {
    CatalogRows::Session(system)
}

/// The catalog a lifecycle operation holds open for its whole transaction.
pub(crate) type HeldCatalog<'c> = dyn crate::definition_catalog::DefinitionCatalog + 'c;

/// THE CATALOG'S RAW ROWS, as one read takes them: the session's (each
/// answer takes the catalog and releases it, so no other read inside a
/// compile window waits on it), or the catalog a lifecycle operation holds.
/// Rows only: no selection, route or judgment is answered here.
#[derive(Clone, Copy)]
pub(crate) enum CatalogRows<'c> {
    Session(&'c DelightQLSystem),
    Held(&'c HeldCatalog<'c>),
}

impl CatalogRows<'_> {
    fn with<T>(&self, read: impl FnOnce(&dyn crate::definition_catalog::DefinitionCatalog) -> Result<T>) -> Result<T> {
        use crate::host::CompilerHost;
        match self {
            CatalogRows::Session(system) => {
                let facts = system.definition_catalog("middle catalog rows")?;
                read(facts.as_ref())
            }
            CatalogRows::Held(facts) => read(*facts),
        }
    }

    pub(crate) fn namespaces(&self) -> Result<Vec<NamespaceFact>> {
        self.with(|f| f.namespaces())
    }

    pub(crate) fn session_enlists(&self, namespace: i64) -> Result<Vec<i64>> {
        self.with(|f| f.session_enlists(namespace))
    }

    pub(crate) fn namespace_imports(&self, namespace: i64) -> Result<Vec<i64>> {
        self.with(|f| f.lexical_imports(crate::definition_catalog::ImportOwner::Namespace(namespace)))
    }

    pub(crate) fn load_imports(&self, load: i64) -> Result<Vec<i64>> {
        self.with(|f| f.lexical_imports(crate::definition_catalog::ImportOwner::Cartridge(load)))
    }

    pub(crate) fn exposures(&self) -> Result<Vec<(i64, i64)>> {
        self.with(|f| f.exposures())
    }

    pub(crate) fn session_aliases(&self) -> Result<Vec<(String, i64)>> {
        self.with(|f| f.session_aliases())
    }

    pub(crate) fn load_aliases(&self, load: i64) -> Result<Vec<(String, i64)>> {
        self.with(|f| f.load_aliases(crate::definition_catalog::ImportOwner::Cartridge(load)))
    }

    pub(crate) fn namespace_aliases(&self, namespace: i64) -> Result<Vec<(String, i64)>> {
        self.with(|f| f.load_aliases(crate::definition_catalog::ImportOwner::Namespace(namespace)))
    }

    pub(crate) fn grounding_closure(&self, namespace: i64) -> Result<Vec<(i64, i64)>> {
        self.with(|f| f.grounding_closure(namespace))
    }

    pub(crate) fn candidates(&self, canonical: &str, namespaces: &[i64]) -> Result<Vec<CandidateFact>> {
        self.with(|f| f.candidates(canonical, namespaces))
    }

    pub(crate) fn edge_declarations(&self, namespaces: &[i64]) -> Result<Vec<EdgeDeclarationFact>> {
        self.with(|f| f.edge_declarations(namespaces))
    }

    pub(crate) fn overlay_candidates(&self, canonical: &str, owners: &[i64]) -> Result<Vec<(i64, CandidateFact)>> {
        self.with(|f| f.overlay_candidates(canonical, owners))
    }

    pub(crate) fn activation(&self, entity: i64) -> Result<Option<(String, i64)>> {
        self.with(|f| f.activation(entity))
    }

    pub(crate) fn mentions_of(&self, canonical: &str, except: i64) -> Result<Vec<MentionFact>> {
        self.with(|f| f.mentions_of(canonical, except))
    }

    pub(crate) fn qualified_mentions(&self) -> Result<Vec<MentionFact>> {
        self.with(|f| f.qualified_mentions())
    }
}
pub(crate) use crate::pipeline::asts::core::definitions::{Head, HeadItem};
pub(crate) use crate::pipeline::asts::core::expressions::FunctorCall;
pub(crate) use crate::pipeline::asts::core::{CfeDefinition, HoDefinition, SigmaDefinition};

pub(crate) use crate::enums::EntityType;

use crate::names::DmlVerb;

/// The mutation a built-in directive performs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MutationKind {
    Update,
    Insert,
    Delete,
}

/// What a built-in directive directs, as its descriptor declares it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DirectiveClass {
    /// Rows of a stored table.
    Rows(MutationKind),
    /// An object created from a relation: what it materializes, shape and
    /// residence.
    Creation(Creation),
    /// The run itself: a return, a sequence, a print, an assertion.
    Utility,
    /// The session's world: an act of the runtime on the namespace tree, or
    /// a manifest's materialization into a data namespace.
    Session(SessionFacts),
    /// The removal of one selected session-authored definition family.
    Retraction,
    /// A run: the demand of a consulted namespace's `main!`.
    Execution(ExecutionKind),
    /// A terminal disposition of the run: `exit!` or `abort!`.
    Terminal(TerminalKind),
    /// A directive the new middle does not claim.
    Unclaimed,
}

/// Which namespace an execution directive runs: one its file's
/// consultation makes (`run!`), or one already consulted
/// (`run_namespace!`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionKind {
    File,
    Namespace,
}

/// Which terminal disposition a directive is (TERMINAL DISPOSITIONS AND
/// ASSERTION): a graceful stop, or an erroneous one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TerminalKind {
    Exit,
    Abort,
}

/// The namespace a run of the file `path` consults into.
pub(crate) fn run_namespace_of(path: &str) -> String {
    crate::system::run_namespace_of(path)
}

/// The text of a bare, unqualified name written as a value.
pub(crate) fn bare_name(value: &DomainExpression) -> Option<String> {
    match value {
        DomainExpression::Reference(Reference::Named(NamedReference(column))) if column.qualifier.is_none() => {
            Some(column.name.to_string())
        }
        _ => None,
    }
}

/// A call's argument row from its members, in order; no members is no
/// argument group.
pub(crate) fn call_arguments(members: Vec<HoArgument>) -> CallArguments {
    match crate::pipeline::asts::vocabulary::Vec1::try_from_vec(members) {
        Some(members) => CallArguments::HigherOrder(crate::pipeline::asts::core::operators::HoPart::of(members)),
        None => CallArguments::None,
    }
}

/// The file a statement runs when its whole body is `run!(path, …)`, its
/// receipt access aside: the file is consulted before the statement is
/// compiled (`run!` = consult the file, then demand its `main!`).
pub(crate) fn run_file(query: &Query) -> Option<String> {
    use crate::pipeline::asts::effects::{kind_for_reference, DirectiveKind};
    if query.body.has_steps() || !query.ctes().is_empty() {
        return None;
    }
    let GroundForm::Reference(Relation::FunctorCall { call, .. }) = query.body.head().form() else {
        return None;
    };
    if kind_for_reference(&call.call().callee) != Some(DirectiveKind::Run) {
        return None;
    }
    let CallArguments::HigherOrder(part) = &call.call().arguments else {
        return None;
    };
    match part.members().iter().next() {
        Some(HoArgument::Value(value)) => match &value.value {
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(path))) => Some(path.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Whether a statement's whole body is an execution directive's call, its
/// receipt access aside.
pub(crate) fn whole_run(query: &Query) -> bool {
    use crate::pipeline::asts::effects::{descriptor_for_reference, DirectiveCategory};
    !query.body.has_steps()
        && matches!(
            query.body.head().form(),
            GroundForm::Reference(Relation::FunctorCall { call, .. })
                if descriptor_for_reference(&call.call().callee).is_some_and(|d| d.category == DirectiveCategory::Execution)
        )
}

/// What a session directive's descriptor declares beyond its receipt: its
/// parameters in order (each with whether it names a namespace and whether
/// it may be omitted), the heading of its receipt's `input` echo (empty
/// when it declares none), what its `returned` payload packages, and
/// whether it is legal only in a consulted file's liminal space.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionFacts {
    pub(crate) params: Vec<SessionParam>,
    pub(crate) input_echo: Vec<delightql_types::SqlIdentifier>,
    pub(crate) returned: SessionPayload,
    pub(crate) liminal_only: bool,
    /// Whether an effect body may demand it: `doc!` only annotates and never
    /// changes what is compiled; a DDL act executes by demand.
    pub(crate) by_demand: bool,
}

/// One declared parameter of a session directive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SessionParam {
    pub(crate) name: delightql_types::SqlIdentifier,
    pub(crate) namespace: bool,
    pub(crate) optional: bool,
}

/// What a session directive's `returned` payload packages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SessionPayload {
    None,
    /// The namespaces its arguments name.
    Namespaces,
    /// What the act finds when it runs, under no declared heading, which no
    /// act reports.
    Discovered,
    /// What the act reports when it runs, under the declared heading.
    Reported(Vec<delightql_types::SqlIdentifier>),
}

impl SessionPayload {
    /// A session act's payload, from the descriptor's declared kind: the
    /// namespaces its arguments name, for `consult!`; otherwise the rows
    /// the act reports under the kind's declared heading.
    fn of(payload: crate::pipeline::asts::effects::ReceiptPayload, named_by_arguments: bool) -> Self {
        use crate::pipeline::asts::effects::ReceiptPayload;
        match (payload, payload.heading()) {
            (ReceiptPayload::None, _) => SessionPayload::None,
            (ReceiptPayload::Namespaces, _) if named_by_arguments => SessionPayload::Namespaces,
            (_, Some(heading)) => {
                SessionPayload::Reported(heading.iter().map(|c| delightql_types::SqlIdentifier::new(*c)).collect())
            }
            (_, None) => SessionPayload::Discovered,
        }
    }
}

/// The relation a directive's receipt declares as its `returned` payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Payload {
    None,
    /// The piped input.
    Input,
    /// The other relational argument.
    Other,
    /// The witness of the verdict and the checked input.
    Assertion,
}

/// A built-in directive's descriptor facts: what it directs, its name as a
/// receipt reports it, its receipt's columns (each with whether it holds an
/// interior relation), its payload, and whether it acts on the world
/// beyond its receipt.
pub(crate) struct Directive {
    pub(crate) class: DirectiveClass,
    pub(crate) operation: String,
    pub(crate) receipt: Vec<(delightql_types::SqlIdentifier, bool)>,
    pub(crate) payload: Payload,
    pub(crate) side_effects: bool,
    /// The relational formals written before its relational input, when the
    /// input may be written in place (PIPE SUBSTITUTION); `None` for an
    /// input that is pipe-only (a DML source).
    pub(crate) formals_before_input: Option<usize>,
}


/// What the language itself knows of a call's name at an arity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KnownCall {
    Scalar,
    Window,
    Aggregate,
    Unknown,
}

/// The compile window's read of one session: the catalog and its target's
/// aggregates. Holding one borrows the session, so no
/// setup statement can run while a window is open.
pub(crate) struct Input<'s> {
    system: &'s crate::system::DelightQLSystem,
    aggregates: crate::pipeline::aggregate_catalog::TargetAggregates,
    /// The statement target's declared-type vocabulary.
    types: crate::pipeline::type_classes::TargetTypes,
    dialect: crate::pipeline::generator::SqlDialect,
}

impl<'s> Input<'s> {
    /// Open a window on the session: settle the statement's target and read
    /// its aggregate catalog once.
    pub(crate) fn open(system: &'s DelightQLSystem) -> Result<Self> {
        use crate::host::CompilerHost;
        let dialect = match system.stated_dialect()? {
            Some(stated) => stated,
            None => {
                let dialects = system.connection_dialects()?;
                match dialects.split_first() {
                    Some((first, rest)) if rest.iter().all(|d| d == first) => *first,
                    Some(_) => return Err(super::core::refuse::routed_session()),
                    None => system.dialect_for_connection(None),
                }
            }
        };
        let aggregates =
            crate::pipeline::aggregate_catalog::TargetAggregates::of(system.aggregate_catalog()?, dialect);
        let types = crate::pipeline::type_classes::TargetTypes::of(system.type_classes()?, dialect)?;
        Ok(Input {
            system,
            aggregates,
            types,
            dialect,
        })
    }

    /// The query-local claim ledger's judgment of one spelling at a body's
    /// horizon: the kind it names there, `None` when the block claims
    /// nothing (the only answer that licenses a catalog lookup), or its
    /// refusal. A shared decider of the retained front end, spent here and
    /// never re-decided.
    pub(crate) fn query_local(
        &self,
        claims: &QueryLocalNames,
        name: &delightql_types::SqlIdentifier,
        horizon: LexicalHorizon,
        demand: QueryLocalDemand,
    ) -> Result<Option<QueryLocalKind>> {
        claims.select(name, horizon, demand)
    }

    /// The kind a block claims a spelling as, wherever its horizon stands.
    pub(crate) fn query_local_claim(
        &self,
        claims: &QueryLocalNames,
        name: &delightql_types::SqlIdentifier,
    ) -> Option<QueryLocalKind> {
        claims.claim(name)
    }

    /// The horizon of a statement body: every declaration of its block.
    pub(crate) fn horizon_all(&self) -> LexicalHorizon {
        LexicalHorizon::all()
    }

    /// THE HEAD LAWS of a family's clauses, judged by the one fallible
    /// assembler every definition crosses: the parameter row, the badge,
    /// the head form, arity, the name offers, the ground-position rule and
    /// the output heading's collisions, in its order. A retained judgment,
    /// spent here and never re-decided.
    pub(crate) fn assemble_heads(
        &self,
        subject: &str,
        heads: &[&crate::pipeline::asts::core::definitions::Head],
    ) -> Result<()> {
        crate::pipeline::asts::core::definitions::assemble(subject, heads).map(|_| ())
    }

    /// A callee's written name, stropped as written, and its qualifier: a
    /// stropped callee names its definition by its exact spelling.
    pub(crate) fn callee_name(&self, callee: &crate::pipeline::asts::vocabulary::Ref) -> (SqlIdentifier, Option<Qualifier>) {
        (callee.name_identifier(), callee.qualifier())
    }

    /// Whether a callee is written as an engine route (`main/f`), naming the
    /// engine's own callable and never a DQL one.
    pub(crate) fn callee_engine_routed(&self, callee: &crate::pipeline::asts::vocabulary::Ref) -> bool {
        matches!(callee.resolution(), crate::pipeline::asts::vocabulary::ResolutionMode::TargetPassthrough)
    }

    /// A directive callee's name without its `!`, stropped as written.
    pub(crate) fn callee_bare(&self, callee: &crate::pipeline::asts::vocabulary::Ref) -> delightql_types::SqlIdentifier {
        let written = callee.name_identifier();
        let text = written.as_str().strip_suffix('!').unwrap_or_else(|| written.as_str());
        if written.is_stropped() {
            delightql_types::SqlIdentifier::stropped(text)
        } else {
            delightql_types::SqlIdentifier::new(text)
        }
    }

    /// A qualifier as the author wrote it.
    pub(crate) fn qualifier_spelled(&self, qualifier: &Qualifier) -> String {
        qualifier.spelled()
    }

    /// The marked scope a parameterized clause's reading opened for its own
    /// formals; `None` for a body read without formals.
    pub(crate) fn clause_scope(&self, query: &Query) -> Option<MarkedScopeId> {
        match &query.locals.clause_formals {
            crate::pipeline::asts::core::definitions::ClauseFormals::Marked(id) => Some(*id),
            crate::pipeline::asts::core::definitions::ClauseFormals::Bare(_) => None,
        }
    }

    /// A cover's callable as the value it denotes for one covered cell: the
    /// front end's one landing (`normalize::land`) spends it with the cell
    /// standing as the composition input `@`, which the elaborator binds.
    pub(crate) fn cover_application(&self, callable: &Callable) -> Result<DomainExpression> {
        let flowing = DomainExpression::Application(FunctionApplication::Open(DomainHole::CompositionInput));
        crate::pipeline::normalize::land(callable.clone(), &flowing)
    }

    /// The catalog's raw rows, for the new middle's selection.
    pub(crate) fn catalog_rows(&self) -> CatalogRows<'s> {
        CatalogRows::Session(self.system)
    }

    /// A parameterized clause's body, read once for the instance from its
    /// text.
    pub(crate) fn analysis_body(&self, clause: &Clause) -> Result<Query> {
        crate::ddl::reconstruct::analysis_clause_body(clause)
    }

    /// A selected family's stored source, read into its clauses, each with
    /// the kind its head declares and, for a headerless fact clause, its
    /// rows' heading offers. The source is parsed and normalized as a
    /// definition file and never assembled: registration judged the clause
    /// laws before the catalog stored it.
    pub(crate) fn family_clauses(&self, family: &super::select::Family) -> Result<Vec<(DefKind, Clause, RowOffers)>> {
        self.family_source(family).map(|(clauses, _)| clauses)
    }

    /// A selected family's clauses as `family_clauses` reads them, and the
    /// danger settings its stored source declares beside them.
    #[allow(clippy::type_complexity)]
    pub(crate) fn family_source(
        &self,
        family: &super::select::Family,
    ) -> Result<(Vec<(DefKind, Clause, RowOffers)>, Settings)> {
        let source = family.definition();
        crate::ddl::reconstruct::clauses_declared(source)
            .map(|(clauses, declared)| (clauses.into_iter().map(declared_clause).collect(), settings(&declared)))
    }

    /// A served entity's columns in order: name, declared type, and whether
    /// the catalog records it as holding a stored nested relation.
    pub(crate) fn served_columns(
        &self,
        entity: &super::select::Served,
    ) -> Result<Vec<(delightql_types::SqlIdentifier, Option<String>, bool)>> {
        let id = entity.entity_id();
        let mut columns = self.system.output_columns_for_entity(id)?;
        columns.sort_by_key(|c| c.position);
        Ok(columns
            .into_iter()
            .map(|c| (c.name.clone(), c.declared_type.clone(), c.interior))
            .collect())
    }

    /// A relation the statement target's engine serves on the primary
    /// connection and the catalog does not record (a system table, a table
    /// the engine made, a table function), as the engine's introspection
    /// answers it under `schema`: its columns in order, each with its
    /// declared type, a table function's hidden parameter columns left out.
    /// `None` when the engine answers no relation of that name in that
    /// schema: a relation the introspection finds in another schema (the
    /// connection's TEMP catalog, reached by every schema's spelling of its
    /// name) is not the asked schema's.
    pub(crate) fn engine_relation(&self, connection: i64, schema: Option<&str>, name: &str) -> Result<Option<EngineRelation>> {
        if connection != crate::system_vocabulary::PRIMARY_CONNECTION_ID {
            return Err(super::core::refuse::outside(
                "an engine relation on a connection whose introspection the session does not hold",
            ));
        }
        self.system.introspect_passthrough_relation(schema, name).map(|found| {
            found.filter(|relation| schema.is_none() || relation.backend_schema.as_deref() == schema).map(|relation| {
                let mut attributes = relation.entity.attributes;
                attributes.sort_by_key(|a| a.position);
                EngineRelation {
                    connection,
                    schema: relation.backend_schema,
                    columns: attributes
                        .into_iter()
                        .map(|a| {
                            let declared = (!a.data_type.is_empty()).then_some(a.data_type);
                            (a.name, declared)
                        })
                        .collect(),
                }
            })
        })
    }

    /// Where a namespace's own relations are read, as the catalog binds it:
    /// the connection, and the schema that qualifies a read of it (a mounted
    /// namespace's attach alias or engine schema; `None` where its reads are
    /// spelled unqualified). The storage fact the catalog records, read as
    /// data.
    pub(crate) fn namespace_schema(&self, namespace: &str) -> Result<(i64, Option<String>)> {
        let bound = self
            .system
            .resolve_namespace_path(&delightql_types::namespace::NamespacePath::from_fq_string(namespace))?;
        let (schema, connection) =
            bound.ok_or_else(|| super::core::refuse::contract("an engine reference whose namespace the catalog does not bind"))?;
        Ok((connection, schema))
    }

    /// Where a served entity's rows are read: the connection that serves
    /// them and the backend schema that qualifies the read.
    pub(crate) fn physical(&self, entity: &super::select::Served) -> Result<(i64, Option<String>)> {
        let read = self
            .system
            .physical_read_of(Some(entity.entity_id()), entity.namespace(), entity.name().as_str())?;
        Ok((read.connection_id, read.backend_schema))
    }

    /// The schema spelling that reaches a data namespace's durable relations
    /// past the session pool of `connection`, as the runtime's placement
    /// contract answers for the target serving it; `None` where it answers
    /// none. The creation-placement authority's judgment, through its own
    /// door.
    pub(crate) fn past_session_schema(&self, namespace: &str, connection: i64) -> Result<Option<String>> {
        use crate::host::CompilerHost;
        (|| {
            let facts = self.system.definition_catalog("durable read past the session pool")?;
            let data = crate::creation_target::DataTarget::read(
                facts.as_ref(),
                crate::definition_catalog::NamespaceKey::Fq(namespace),
                "a durable read",
            )?;
            Ok(data.durable().address_past_session(self.system.dialect_for_connection(Some(connection))))
        })()
    }

    /// Whether a served relation's storage guarantees its declared column
    /// types in the terms of this window's target: the backend serving it
    /// answers for its own storage (the introspection fact), and a type one
    /// backend declared is the target's own only when that backend serves
    /// the target's dialect.
    pub(crate) fn declared_types_guaranteed(
        &self,
        entity: &super::select::Served,
        connection: i64,
        schema: Option<&str>,
    ) -> bool {
        self.system.dialect_for_connection(Some(connection)) == self.dialect
            && self.system.storage_guarantees_declared_types(connection, schema, entity.name().as_str()) == Some(true)
    }

    /// The output doors of this window's compilation, over its name arena.
    pub(crate) fn output(&self, names: &Names) -> Output<'s> {
        Output {
            system: self.system,
            names: Rc::clone(names),
            dialect: self.dialect,
            published: std::cell::RefCell::new(std::collections::BTreeMap::new()),
            published_twice: std::cell::Cell::new(None),
        }
    }

    /// What the language knows of a call.
    pub(crate) fn known_call(&self, name: &str, arity: usize) -> KnownCall {
        match crate::resolution::registry::BuiltInRegistry::the().grade(name, arity) {
            crate::resolution::registry::CallGrade::Scalar => KnownCall::Scalar,
            crate::resolution::registry::CallGrade::Window => KnownCall::Window,
            crate::resolution::registry::CallGrade::Aggregate => KnownCall::Aggregate,
            crate::resolution::registry::CallGrade::Unknown => KnownCall::Unknown,
        }
    }

    /// The facts a marked read decides its table's row identity from: how
    /// the backend serving the table keeps its rows, or why that answer does
    /// not reach the compiler, and the names the statement target reads each
    /// part of its row locator by (`None`: it exposes none).
    pub(crate) fn row_identity(
        &self,
        entity: &super::select::Served,
        connection: i64,
        schema: Option<&str>,
    ) -> Result<RowIdentityFacts> {
        let stored = if self.system.dialect_for_connection(Some(connection)) == self.dialect {
            self.system.stored_row_identity(connection, schema, entity.name().as_str())?
            .ok_or(Unanswered::Backend)
        } else {
            Err(Unanswered::OtherDialect)
        };
        Ok(RowIdentityFacts {
            stored,
            spellings: self.dialect.row_locator_spellings(),
        })
    }

    /// The columns of a served stored table its storage computes, as the
    /// backend serving it answers; none where no answer reaches the compiler
    /// (a backend that cannot tell, or one serving another dialect than the
    /// target's).
    pub(crate) fn computed_columns(
        &self,
        entity: &super::select::Served,
        connection: i64,
        schema: Option<&str>,
    ) -> Result<Option<Vec<delightql_types::SqlIdentifier>>> {
        if self.system.dialect_for_connection(Some(connection)) != self.dialect {
            return Ok(None);
        }
        self.system.stored_computed_columns(connection, schema, entity.name().as_str())
    }

    /// The built-in directive a callee names, with its descriptor's facts.
    pub(crate) fn directive(&self, callee: &crate::pipeline::asts::vocabulary::Ref) -> Option<Directive> {
        use crate::pipeline::asts::effects::{DirectiveCategory, DirectiveKind, DirectiveParamKind, ReceiptPayload};
        let descriptor = crate::pipeline::asts::effects::descriptor_for_reference(callee)?;
        let class = match descriptor.category {
            DirectiveCategory::Dml(DmlVerb::Update) => DirectiveClass::Rows(MutationKind::Update),
            DirectiveCategory::Dml(DmlVerb::Insert) => DirectiveClass::Rows(MutationKind::Insert),
            DirectiveCategory::Dml(DmlVerb::Delete) => DirectiveClass::Rows(MutationKind::Delete),
            DirectiveCategory::Ddl => match DirectiveKind::from_name(descriptor.name) {
                Some(DirectiveKind::Imprint | DirectiveKind::ImprintReplace) => DirectiveClass::Session(SessionFacts {
                    params: descriptor
                        .params
                        .iter()
                        .map(|p| SessionParam {
                            name: delightql_types::SqlIdentifier::new(p.name),
                            namespace: false,
                            optional: p.optional,
                        })
                        .collect(),
                    input_echo: Vec::new(),
                    returned: SessionPayload::of(descriptor.receipt_payload, false),
                    liminal_only: false,
                    by_demand: true,
                }),
                kind => match kind.and_then(Materialization::of_directive).map(Creation) {
                    Some(m) => DirectiveClass::Creation(m),
                    None => DirectiveClass::Unclaimed,
                },
            },
            DirectiveCategory::Utility => match DirectiveKind::from_name(descriptor.name) {
                Some(DirectiveKind::Exit) => DirectiveClass::Terminal(TerminalKind::Exit),
                Some(DirectiveKind::Abort) => DirectiveClass::Terminal(TerminalKind::Abort),
                _ => DirectiveClass::Utility,
            },
            DirectiveCategory::Session if DirectiveKind::from_name(descriptor.name) == Some(DirectiveKind::Retract) => {
                DirectiveClass::Retraction
            }
            DirectiveCategory::Session => DirectiveClass::Session(SessionFacts {
                params: descriptor
                    .params
                    .iter()
                    .map(|p| SessionParam {
                        name: delightql_types::SqlIdentifier::new(p.name),
                        namespace: p.kind == DirectiveParamKind::Namespace,
                        optional: p.optional,
                    })
                    .collect(),
                input_echo: descriptor.receipt_input_echo.iter().map(|n| delightql_types::SqlIdentifier::new(*n)).collect(),
                returned: SessionPayload::of(
                    descriptor.receipt_payload,
                    DirectiveKind::from_name(descriptor.name) == Some(DirectiveKind::Consult),
                ),
                liminal_only: descriptor.realization == crate::pipeline::asts::effects::DirectiveRealization::LiminalOnly,
                by_demand: DirectiveKind::from_name(descriptor.name) == Some(DirectiveKind::Doc),
            }),
            DirectiveCategory::Execution => match DirectiveKind::from_name(descriptor.name) {
                Some(DirectiveKind::Run) => DirectiveClass::Execution(ExecutionKind::File),
                _ => DirectiveClass::Execution(ExecutionKind::Namespace),
            },
            DirectiveCategory::User => DirectiveClass::Unclaimed,
        };
        let formals_before_input = match class {
            DirectiveClass::Rows(_)
            | DirectiveClass::Session(_)
            | DirectiveClass::Retraction
            | DirectiveClass::Execution(_)
            | DirectiveClass::Unclaimed => {
                None
            }
            DirectiveClass::Terminal(_) => Some(0),
            DirectiveClass::Creation(_) | DirectiveClass::Utility => Some(
                descriptor
                    .params
                    .iter()
                    .filter(|p| matches!(p.kind, DirectiveParamKind::RelationTarget | DirectiveParamKind::RuleValue))
                    .count()
                    + usize::from(descriptor.receipt_payload == ReceiptPayload::OtherRelation),
            ),
        };
        Some(Directive {
            class,
            operation: format!("{}!", descriptor.name),
            receipt: descriptor
                .receipt_columns()
                .into_iter()
                .map(|(name, kind)| (delightql_types::SqlIdentifier::new(name), kind == "Interior"))
                .collect(),
            payload: match descriptor.receipt_payload {
                ReceiptPayload::None => Payload::None,
                ReceiptPayload::Input => Payload::Input,
                ReceiptPayload::OtherRelation => Payload::Other,
                ReceiptPayload::Assertion => Payload::Assertion,
                ReceiptPayload::RunResult
                | ReceiptPayload::Namespaces
                | ReceiptPayload::ConsultedFiles
                | ReceiptPayload::MaterializedEntities => Payload::None,
            },
            side_effects: descriptor.side_effects,
            formals_before_input,
        })
    }

    /// Where the runtime places and registers an object a creation
    /// directive makes in `namespace` (the session's default data-write
    /// target when the creation names none): the
    /// runtime's placement contract (the target namespace's backing, its
    /// connection, the session shadow a temp object is registered under,
    /// the schema its statements spell), and the session objects that hold
    /// its physical temp name, read beside it from the catalog.
    pub(crate) fn placement(&self, namespace: Option<&str>, name: &str, creation: Creation, operation: &str) -> Result<Placement> {
        let Creation(materialization) = creation;
        use crate::host::CompilerHost;
        let session = materialization.residence() == Residence::SessionShadow;
        (|| {
            let facts = self.system.definition_catalog("creation target selection")?;
            let data = crate::creation_target::DataTarget::read(
                facts.as_ref(),
                crate::definition_catalog::NamespaceKey::Fq(namespace.unwrap_or(crate::creation_target::DEFAULT_WRITE_TARGET)),
                &format!("{operation}({name})"),
            )?;
            let holders = if session {
                facts
                    .session_holders(data.connection_id(), name)?
                    .into_iter()
                    .map(|h| {
                        let kind = crate::enums::EntityType::from_i32(h.kind)
                            .map_err(|_| super::core::refuse::contract("a session object whose kind does not decode"))?;
                        Ok(Holder {
                            shape: if kind == crate::enums::EntityType::DbTemporaryView { ObjectShape::View } else { ObjectShape::Table },
                            owner: h.owner,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                Vec::new()
            };
            drop(facts);
            let dialect = self.system.dialect_for_connection(Some(data.connection_id()));
            let bare = operation.trim_end_matches('!');
            let target = crate::creation_target::CreationTarget::judge(data, name, materialization, bare, dialect)?;
            Ok(Placement { target, holders })
        })()
    }

    /// Whether the namespace a served entity lives in is engine-owned.
    pub(crate) fn engine_owned(&self, namespace: &str) -> Result<bool> {
        use crate::host::CompilerHost;
        self
            .system
            .definition_catalog("middle namespace kinds")
            .and_then(|facts| facts.namespaces())
            .map(|namespaces| {
                namespaces
                    .into_iter()
                    .any(|n| n.fq.as_deref() == Some(namespace) && n.kind == crate::namespace::NamespaceKind::System)
            })
    }

    /// The number of arguments a served sigma predicate's signature
    /// declares; `None` when the engine serves no signature for it.
    pub(crate) fn predicate_arity(&self, served: &super::select::Served) -> Option<usize> {
        let parts: Vec<&str> = served.namespace().split("::").collect();
        let (_, entity) = self
            .system
            .bin_registry()
            .lookup_qualified_entity_with_namespace(&parts, served.name().as_str())?;
        Some(entity.signature().parameters.len())
    }

    /// Whether the target's aggregate catalog lets the call take a
    /// whole-operand `*`; `None` when the catalog holds no row for it.
    pub(crate) fn target_admits_star(&self, name: &str, arity: usize) -> Option<bool> {
        self.aggregates.fact(name, arity).map(|fact| fact.admits_glob())
    }

    /// The class the target's type vocabulary gives a column declaration
    /// (numeric, date or boolean), or none.
    pub(crate) fn declared_class(&self, declared: Option<&str>) -> Option<DeclaredClass> {
        declared.and_then(|d| self.types.class(d))
    }

    /// The statement target's family name, as a diagnostic spells it.
    pub(crate) fn target_family(&self) -> &'static str {
        self.dialect.family_name()
    }

    /// Whether the statement's target knows the call as a reduction.
    pub(crate) fn target_reduces(&self, name: &str, arity: usize) -> bool {
        self.aggregates.reduces(name, arity)
    }
}

/// Whether a text names, in a compile-time integer position (a row bound, an
/// offset, a column ordinal), a scalar formal that no definition declared
/// inside the text owns. The text is its CTEs and body and every query-local
/// definition it declares, at any depth: a formal such a nested definition
/// declares is that definition's own and is decided at each of its uses;
/// any other formal belongs to the text's own clause or to one enclosing it.
pub(crate) fn names_enclosing_integer_formal(body: &Query) -> Result<bool> {
    use crate::pipeline::ast_visit::{walk_visit_boolean, walk_visit_domain, walk_visit_query, AstVisit, Descent};
    use crate::pipeline::asts::core::definitions::FormalSelector;
    use crate::pipeline::asts::core::{Continuation, DomainExpression, Reference, Unresolved};
    use std::collections::BTreeSet;
    struct Named(Vec<FormalSelector>);
    impl AstVisit<Unresolved> for Named {
        fn enter_continuation(&mut self, c: &Continuation<Unresolved>) -> Result<Descent> {
            self.0.extend(c.bound_formals());
            Ok(Descent::Continue)
        }
        fn enter_domain(&mut self, e: &DomainExpression<Unresolved>) -> Result<Descent> {
            if let DomainExpression::Reference(Reference::Ordinal(ordinal)) = e {
                self.0.extend(ordinal.position.formal());
            }
            Ok(Descent::Continue)
        }
    }
    fn text(query: &Query, nested: bool, owned: &mut BTreeSet<MarkedScopeId>, named: &mut Named) -> Result<()> {
        if nested {
            owned.extend(query.locals.clause_formals.marked_scope());
        }
        walk_visit_query(named, query)?;
        for cfe in query.cfes() {
            walk_visit_domain(named, &cfe.body)?;
        }
        let groups = query.hos().iter().map(|ho| ho.group()).chain(query.sigmas().iter().map(|sigma| sigma.group()));
        for group in groups {
            for clause in group.clauses() {
                match &clause.body {
                    DdlBody::Relational(body) => text(body, true, owned, named)?,
                    DdlBody::Scalar(value) => {
                        walk_visit_domain(named, value)?;
                    }
                    DdlBody::Truth(truth) => {
                        walk_visit_boolean(named, truth)?;
                    }
                    DdlBody::FactFunction(_) | DdlBody::Deferred => {}
                }
            }
        }
        Ok(())
    }
    let mut owned = BTreeSet::new();
    let mut named = Named(Vec::new());
    text(body, false, &mut owned, &mut named)?;
    Ok(named.0.iter().any(|selector| !owned.contains(&selector.scope())))
}

/// The directives a written effect body demands, in written order: each
/// call's name as written, whether it is qualified, and what the built-in
/// its complete reference selects is, if any. A walk of the statement and a
/// read of the directive descriptors; no catalog is read.
pub(crate) fn body_demands(body: &Query) -> Result<Vec<BodyDemand>> {
    use crate::pipeline::ast_visit::{walk_visit_query, AstVisit, Descent};
    use crate::pipeline::asts::core::{Relation, Unresolved};
    struct Census(Vec<BodyDemand>);
    impl AstVisit<Unresolved> for Census {
        fn enter_relation(&mut self, relation: &Relation) -> Result<Descent> {
            if let Relation::FunctorCall { call, .. } = relation {
                let callee = &call.call().callee;
                let name = callee.name_text();
                if name.ends_with('!') {
                    use crate::pipeline::asts::effects::{DirectiveCategory, DirectiveKind};
                    let built_in = crate::pipeline::asts::effects::kind_for_reference(callee).map(|kind| match kind {
                        DirectiveKind::Run => BodyDirective::Run,
                        DirectiveKind::Doc => BodyDirective::Doc,
                        kind if matches!(kind.descriptor().category, DirectiveCategory::Session) => {
                            BodyDirective::Session
                        }
                        _ => BodyDirective::Other,
                    });
                    self.0.push(BodyDemand {
                        name: name.to_string(),
                        qualified: callee.qualifier().is_some(),
                        built_in,
                    });
                }
            }
            Ok(Descent::Continue)
        }
    }
    let mut census = Census(Vec::new());
    // The census visitor never fails and the shared walker raises no error
    // of its own: a failure here is a broken contract.
    walk_visit_query(&mut census, body)
        .map_err(|_| super::core::refuse::contract("the demand census of an effect body failed to walk it"))?;
    Ok(census.0)
}

pub(crate) use crate::pipeline::asts::ddl::DefinitionGroup;

// ---------------------------------------------------------------------------
// OUTPUT: SQL construction, lowering and text.
// ---------------------------------------------------------------------------

pub(crate) use crate::names::{ColId, EntityId, Intrinsic, ScopeId};
pub(crate) use crate::pipeline::generator::SqlDialect;
pub(crate) use crate::pipeline::sql_ast::ordering::Limit;
pub(crate) use crate::pipeline::sql_ast::statements::RelationTarget;
pub(crate) use crate::pipeline::compiled_query::PlanStatement;
use crate::pipeline::compiled_query::{Materialization, Residence, Shape};

/// The shape of an object a creation directive makes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ObjectShape {
    Table,
    View,
}

/// Where an object a creation directive makes lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ObjectResidence {
    Durable,
    SessionShadow,
}

/// What a creation directive materializes, shape and residence, as its
/// descriptor declares it. The runtime's own value stays behind the door.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Creation(Materialization);

impl Creation {
    pub(crate) fn shape(self) -> ObjectShape {
        object_shape(self.0.shape())
    }
    pub(crate) fn residence(self) -> ObjectResidence {
        match self.0.residence() {
            Residence::Durable => ObjectResidence::Durable,
            Residence::SessionShadow => ObjectResidence::SessionShadow,
        }
    }
}

fn object_shape(shape: Shape) -> ObjectShape {
    match shape {
        Shape::Table => ObjectShape::Table,
        Shape::View => ObjectShape::View,
    }
}
pub(crate) use crate::pipeline::sql_ast::{
    BinaryOperator, Cte, DomainExpression as SqlExpr, JoinCondition, JoinType,
    OrderDirection as SqlDirection, OrderTerm, QueryExpression, SelectItem,
    SetOperator as SqlSetOperator, SqlFrameBound, SqlFrameMode, SqlStatement, SqlWindowFrame,
    TableExpression, TvfArgument, UnaryOperator, WhenClause,
};

/// One SELECT, as the realization states it: every clause, and the scope
/// it stands at.
pub(crate) struct Select {
    pub(crate) at: ScopeId,
    pub(crate) distinct: bool,
    pub(crate) items: Vec<SelectItem>,
    pub(crate) from: Vec<TableExpression>,
    pub(crate) filter: Option<SqlExpr>,
    pub(crate) group_by: Option<Vec<SqlExpr>>,
    pub(crate) having: Option<SqlExpr>,
    pub(crate) order_by: Vec<OrderTerm>,
    pub(crate) limit: Option<Limit>,
}

/// A statement's SQL text and, for a query, whether each result column's
/// name is authored or minted.
pub(crate) struct Rendered {
    pub(crate) sql: Vec<String>,
    pub(crate) naming: Vec<Option<Vec<delightql_protocol::Naming>>>,
}

/// The output doors of one compilation: the name arena's physical mints,
/// SQL AST construction, the lowering sandwich, baptism and the generator.
pub(crate) struct Output<'s> {
    system: &'s DelightQLSystem,
    names: Names,
    dialect: crate::pipeline::generator::SqlDialect,
    /// The names each scope publishes. Baptism takes a published name as
    /// the name its position answers to (`baptise_decided`), so no scope may
    /// publish one twice; the first scope that did, if any.
    published: std::cell::RefCell<std::collections::BTreeMap<ScopeId, std::collections::BTreeSet<crate::names::Sym>>>,
    published_twice: std::cell::Cell<Option<ScopeId>>,
}

impl Output<'_> {
    /// The target the SQL is written for: every target fact realization
    /// reads from it (`SqlDialect`'s answers) passes this door.
    pub(crate) fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    /// SQLite's affinity of a declared column type (its documented rule,
    /// stated once in the product); `None` where the catalog cannot
    /// establish it.
    pub(crate) fn declared_affinity(&self, declared: Option<&str>) -> Option<SqliteAffinity> {
        crate::pipeline::sqlite_affinity::declared_affinity(declared)
    }

    fn spelling(&self, name: &delightql_types::SqlIdentifier) -> crate::names::Spelling {
        self.names.intern(name.as_str(), name.is_stropped())
    }

    /// A catalog relation as the SQL names it: its own spelling, qualified
    /// by its backend schema when it has one.
    pub(crate) fn entity(&self, name: &delightql_types::SqlIdentifier, schema: Option<&str>) -> EntityId {
        let entity = self.names.mint_entity(self.spelling(name));
        let schema = schema.map(|s| self.names.intern(s, false));
        self.names.bind_entity_physical(entity, None, schema);
        entity
    }

    /// The scope a base-table read stands at, answering to `answer`.
    pub(crate) fn table_scope(&self, entity: EntityId, answer: &delightql_types::SqlIdentifier) -> ScopeId {
        self.names.base_table_scope(entity, self.spelling(answer))
    }

    /// A fresh scope, answering to `answer` when one is given.
    pub(crate) fn scope(&self, answer: Option<&delightql_types::SqlIdentifier>) -> ScopeId {
        self.names.anonymous_scope(answer.map(|a| self.spelling(a)))
    }

    /// A column of `scope`: published under `name` when one is given, else
    /// a hygienic column whose spelling baptism draws.
    pub(crate) fn column(&self, scope: ScopeId, name: Option<&delightql_types::SqlIdentifier>) -> ColId {
        match name {
            Some(name) => {
                let spelling = self.spelling(name);
                let fresh = self.published.borrow_mut().entry(scope).or_default().insert(self.names.canonical(spelling));
                if !fresh && self.published_twice.get().is_none() {
                    self.published_twice.set(Some(scope));
                }
                self.names.sql_column(scope, Some(spelling), crate::names::Addressing::Published)
            }
            None => self.names.sql_column(scope, None, crate::names::Addressing::Hygienic),
        }
    }

    /// The scope of a relation a program stages for the statements after
    /// it: a temporary table.
    pub(crate) fn scratch(&self) -> ScopeId {
        self.names.scratch_scope(crate::names::ScratchRole::Snapshot, "dml_source")
    }

    /// A value slot that publishes nothing.
    pub(crate) fn scaffolding(&self) -> ColId {
        self.names.scaffolding_slot()
    }

    /// Build one SELECT.
    pub(crate) fn select(&self, select: Select) -> Result<QueryExpression> {
        let mut builder = crate::pipeline::sql_ast::SelectBuilder::new().select_all(select.items);
        if select.distinct {
            builder = builder.distinct();
        }
        if !select.from.is_empty() {
            builder = builder.from_tables(select.from);
        }
        if let Some(filter) = select.filter {
            builder = builder.where_clause(filter);
        }
        if let Some(keys) = select.group_by {
            builder = builder.group_by(keys);
        }
        if let Some(having) = select.having {
            builder = builder.having(having);
        }
        for term in select.order_by {
            builder = builder.order_by(term);
        }
        if let Some(limit) = select.limit {
            builder = builder.limit_from(limit);
        }
        let statement = builder.standing_at(select.at).map_err(|message| {
            crate::diagnostic::Internal::invariant("middle-end realization", message)
        })?;
        Ok(QueryExpression::Select(Box::new(statement)))
    }

    /// A declared table's CREATE text, written by the shared DDL generator
    /// under one baptism of this compilation's names.
    pub(crate) fn create_table(&self, table: &SqlCreateTable) -> Result<String> {
        crate::ddl_pipeline::generator::generate(table, &self.names, self.system.bin_registry())
    }

    /// A function name the target spells from its dialect data.
    pub(crate) fn function(&self, name: &str, args: Vec<SqlExpr>) -> SqlExpr {
        SqlExpr::function(name, args)
    }

    /// A form the compiler chooses, spelled by the target's dialect data.
    pub(crate) fn intrinsic(&self, intrinsic: Intrinsic, args: Vec<SqlExpr>) -> SqlExpr {
        SqlExpr::intrinsic(intrinsic, args)
    }

    /// A table function the target's engine serves, by its own name, over
    /// argument columns the FROM clause already holds.
    pub(crate) fn target_function(
        &self,
        name: &delightql_types::SqlIdentifier,
        arguments: Vec<ColId>,
        alias: ScopeId,
    ) -> TableExpression {
        TableExpression::TVF {
            function: self.names.mint_function(self.spelling(name), Vec::new()),
            arguments: arguments.into_iter().map(TvfArgument::Column).collect(),
            alias,
        }
    }

    /// A table function over one column, standing at `alias`.
    pub(crate) fn table_function(&self, intrinsic: Intrinsic, argument: ColId, alias: ScopeId) -> TableExpression {
        TableExpression::TVF {
            function: self.names.mint_intrinsic(intrinsic),
            arguments: vec![TvfArgument::Column(argument)],
            alias,
        }
    }

    /// A recursive binding at `scope`: its anchor and its members, and
    /// whether the accumulation deduplicates.
    pub(crate) fn fixpoint(
        &self,
        scope: ScopeId,
        deduplicating: bool,
        anchor: QueryExpression,
        members: Vec<QueryExpression>,
    ) -> Cte {
        Cte::fixpoint(crate::pipeline::bindings::SqlFixpoint::from_parts(
            scope,
            deduplicating,
            anchor,
            members,
        ))
    }

    /// Lower each statement through the retained sandwich (the target's
    /// expansions, cleanup at the product's default level, legalization
    /// last) and write them as text, named by one baptism.
    pub(crate) fn render(&self, statements: Vec<SqlStatement>) -> Result<Rendered> {
        use crate::host::CompilerHost;
        crate::pipeline::arm_aggregate_catalog(&self.names, self.system)?;
        let mut legal = Vec::with_capacity(statements.len());
        for statement in statements {
            let expanded = crate::pipeline::sql_rewriter::rewrite(statement, self.dialect, &self.names)?;
            let cleaned = crate::pipeline::sql_optimizer::optimize(
                expanded,
                crate::pipeline::sql_optimizer::OptimizationLevel::Basic,
                &crate::pipeline::aggregate_catalog::TargetAggregates::of_arena(&self.names, self.dialect),
            )?;
            legal.push(crate::pipeline::sql_rewriter::legalize(cleaned, self.dialect, &self.names)?);
        }
        let pack = self.system.dialect_pack()?;
        if self.published_twice.get().is_some() {
            return Err(super::core::refuse::contract(
                "a scope publishes one name twice, where the heading law publishes each answering name once",
            ));
        }
        let refs: Vec<&SqlStatement> = legal.iter().collect();
        let baptised = crate::pipeline::generator::baptise_statements_decided(&self.names, &refs)
            .map_err(|e| e.into_delightql_error("SQL naming error"))?;
        let generator = crate::pipeline::generator::SqlGenerator::new(&baptised)
            .with_dialect(self.dialect)
            .with_bin_registry(self.system.bin_registry())
            .with_dialect_pack(pack);
        let mut sql = Vec::with_capacity(legal.len());
        for statement in &legal {
            sql.push(
                generator
                    .generate_statement(statement)
                    .map_err(|e| e.into_delightql_error("SQL generation error"))?,
            );
        }
        let naming = legal.iter().map(|statement| generator.heading_naming(statement)).collect();
        Ok(Rendered { sql, naming })
    }
}

/// A program as the realization states it: the scratch it creates before
/// every run, its runs in order (each one transaction on one connection),
/// the scratch it drops after them, and the probes its steps' gates sample.
pub(crate) struct Program {
    pub(crate) connection: i64,
    pub(crate) setup: Vec<PlanStatement>,
    pub(crate) runs: Vec<Vec<ProgramStep>>,
    /// Whether each run plays inside one transaction bracket, as the core
    /// decided it (`ProgramRun::bracketed`).
    pub(crate) bracketed: Vec<bool>,
    pub(crate) cleanup: Vec<PlanStatement>,
    /// Each gate's probe: a scalar count, open when it is above zero.
    pub(crate) gates: Vec<String>,
    /// The exit latch's probe, when an `exit!` stands in the program: a
    /// scalar count, set when it is above zero.
    pub(crate) exit: Option<String>,
    /// The objects each run creates, by run.
    pub(crate) created: Vec<Vec<CreatedObject>>,
}

/// An object a run creates, as the runtime registers it once the run
/// commits.
pub(crate) struct CreatedObject(crate::pipeline::compiled_query::PlanCreatedObject);

/// The runtime's placement of an object a creation directive makes, and
/// the session objects holding its physical temp name.
pub(crate) struct Placement {
    target: crate::creation_target::CreationTarget,
    holders: Vec<Holder>,
}

/// One directive a written effect body demands.
pub(crate) struct BodyDemand {
    /// The callee's name as written, with its `!`.
    pub(crate) name: String,
    pub(crate) qualified: bool,
    /// What the built-in the call's complete reference selects is, as its
    /// descriptor declares it; `None` for a user directive.
    pub(crate) built_in: Option<BodyDirective>,
}

/// A built-in directive an effect body demands, as the body laws read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BodyDirective {
    /// `run!`.
    Run,
    /// A session directive a body may demand: `doc!`.
    Doc,
    /// Any other session directive.
    Session,
    /// A directive of any other category.
    Other,
}

/// A session object holding a physical temp name: its shape, and its
/// recorded durable owner.
#[derive(Clone, Debug)]
pub(crate) struct Holder {
    pub(crate) shape: ObjectShape,
    pub(crate) owner: Option<String>,
}

impl Placement {
    /// The fully qualified selected target, as a receipt reports it.
    pub(crate) fn target_path(&self) -> String {
        self.target.target_path()
    }
    /// The exact catalog path of the object created.
    pub(crate) fn created_path(&self) -> String {
        self.target.created_path()
    }
    pub(crate) fn connection(&self) -> i64 {
        self.target.connection_id()
    }
    /// The namespace the created object's exact catalog path names.
    pub(crate) fn created_namespace(&self) -> String {
        self.target.created_namespace().to_string()
    }
    /// The schema the creation's statements spell the object with; `None`
    /// is unqualified.
    pub(crate) fn spelled_schema(&self) -> Option<String> {
        self.target.spelled_schema().map(str::to_string)
    }
    /// What the directive materializes: shape and residence.
    pub(crate) fn creation(&self) -> Creation {
        Creation(self.target.materialization())
    }
    /// The selected data namespace: the durable owner of a session object.
    pub(crate) fn namespace(&self) -> &str {
        self.target.namespace()
    }
    pub(crate) fn holders(&self) -> &[Holder] {
        &self.holders
    }
    /// A session object the statement's own earlier creation will hold the
    /// physical temp name with by the time this one runs.
    pub(crate) fn hold(&mut self, holder: Holder) {
        self.holders.push(holder);
    }
}

impl std::fmt::Debug for Placement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (created {})", self.target.target_path(), self.target.created_path())
    }
}

/// One step: the directive whose act it serves (its name as written), the
/// gates it samples when reached, whether it stands after an `exit!` (it
/// runs only while the exit latch is unset), and what it does.
pub(crate) struct ProgramStep {
    pub(crate) operation: String,
    pub(crate) gates: Vec<usize>,
    pub(crate) after_exit: bool,
    pub(crate) action: StepAction,
}

/// What a step does, in the pump's vocabulary.
pub(crate) enum StepAction {
    /// Materialize what later steps read.
    Stage(Vec<PlanStatement>),
    /// A check the data must pass before the act writes.
    Check {
        statement: PlanStatement,
        refusal: delightql_types::DelightQLError,
    },
    Dml(Vec<PlanStatement>),
    Ddl(Vec<PlanStatement>),
    /// Ship a relation to the client.
    Host {
        statements: Vec<PlanStatement>,
        ship: PlanStatement,
    },
    /// An assertion's verdict or an authored abort: when the probe has a
    /// row the run aborts under the label.
    Abort {
        statements: Vec<PlanStatement>,
        probe: PlanStatement,
        label: String,
        authored: bool,
    },
    /// A reached `exit!`: the latch statements, after which every later
    /// step of the program is skipped and the run commits.
    Exit(Vec<PlanStatement>),
    /// The statement's result.
    Return(PlanStatement),
    /// A session act: the runtime performs the directive once for each row
    /// the statement reads, each row its arguments in order; each row the
    /// act reports is written by the report statement (its width beside
    /// it), whose positions are the report markers.
    Session {
        directive: String,
        arguments: PlanStatement,
        report: Option<(PlanStatement, usize)>,
    },
}

/// The string literal standing for position `at` of an act's reported row
/// in the statement that writes it.
pub(crate) fn report_marker(at: usize) -> String {
    crate::pipeline::compiled_query::ReportFill::marker(at)
}

impl Output<'_> {
    /// The registration of an object a run creates: its placement, and the
    /// positions of its heading that hold nested relations.
    pub(crate) fn created_object(&self, placement: &Placement, interior: Vec<usize>) -> CreatedObject {
        CreatedObject(crate::pipeline::compiled_query::PlanCreatedObject::planned(placement.target.clone(), interior))
    }

    /// The object's name as its statements spell it: its placement's schema,
    /// if it has one, and its exact name.
    fn created_name(&self, placement: &Placement) -> String {
        let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        match placement.target.spelled_schema() {
            Some(schema) => format!("{}.{}", quote(schema), quote(placement.target.name())),
            None => quote(placement.target.name()),
        }
    }

    /// `CREATE … AS <select>` for an object its placement names: a durable
    /// table, or a temporary table or view.
    pub(crate) fn create_text(&self, placement: &Placement, select: &str) -> String {
        let materialization = placement.target.materialization();
        let create = match (materialization.residence(), materialization.shape()) {
            (Residence::Durable, _) => "CREATE TABLE",
            (_, Shape::Table) => "CREATE TEMPORARY TABLE",
            (_, Shape::View) => "CREATE TEMPORARY VIEW",
        };
        format!("{create} {} AS {select}", self.created_name(placement))
    }

    /// `DROP TABLE|VIEW IF EXISTS <temp schema>.<name>`: the replacement of
    /// a same-owner session object holding the name.
    pub(crate) fn drop_text(&self, placement: &Placement, view: bool) -> Result<String> {
        use crate::host::CompilerHost;
        let pack = self.system.dialect_pack()?;
        let schema = match pack.render(self.dialect.family_name(), "scratch.schema") {
            Some(rule) => rule
                .template()
                .map(str::to_string)
                .map_err(|e| super::core::refuse::contract(&format!("the scratch schema rule: {e}")))?,
            None => "temp".to_string(),
        };
        let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
        Ok(format!(
            "DROP {} IF EXISTS {schema}.{}",
            if view { "VIEW" } else { "TABLE" },
            quote(placement.target.name())
        ))
    }

    /// Whether a program may run on a connection: the pump plays a program
    /// in one bracket that aborts on a statement's failure, so a connection
    /// whose transport cannot surface a failure (siso) runs none.
    pub(crate) fn program_runs_on(&self, connection: i64) -> Result<()> {
        if !self.system.connection_is_siso(connection)? {
            return Ok(());
        }
        Err(delightql_types::diagnostic::EffectPlan::EngineUnsupported {
            message: "a program does not run over a siso connection: the siso transport cannot surface a \
                      statement's failure, so the program's bracket could not abort on one"
                .to_string(),
        }
        .into())
    }

    /// A realized query as the relay runs it.
    pub(crate) fn compiled_query(
        &self,
        sql: String,
        connection_id: Option<i64>,
        naming: Option<Vec<delightql_protocol::Naming>>,
    ) -> CompiledQuery {
        CompiledQuery {
            primary_sql: sql,
            kind: crate::pipeline::compiled_query::SqlKind::Query,
            obligations: Vec::new(),
            prepare_sqls: Vec::new(),
            cleanup_sqls: Vec::new(),
            connection_id,
            naming,
            trailing: crate::pipeline::inline_ddl::Trailing::after(Vec::new()),
        }
    }

    /// A realized program as the relay's pump plays it: the typed plan the
    /// pump walks and `sys::execution` projects, and its flat entries.
    /// Occurrences are labelled in the projection's own vocabulary (each
    /// step `operation#n` in program order, the result `return!#n`, and
    /// the control steps by kind).
    pub(crate) fn program(&self, program: Program) -> CompiledPlan {
        use crate::pipeline::compiled_query::{
            AbortProvenance, EffectAction, EffectRun, EffectStep, GuardDefinition, GuardPolarity, Requirement,
            TerminalAction, TypedEffectPlan,
        };
        let route = Some(program.connection);
        let mut ordinal = 0usize;
        // The exit latch's guard follows the program's own gates.
        let exit_guard = program.gates.len();
        let mut guard_sql = program.gates;
        if let Some(exit) = &program.exit {
            guard_sql.push(exit.clone());
        }
        let mut runs = Vec::with_capacity(program.runs.len());
        for ((steps, created), bracketed) in program.runs.into_iter().zip(program.created).zip(program.bracketed) {
            let mut typed_steps = Vec::with_capacity(steps.len());
            for s in steps {
                let (operation, action) = match s.action {
                    StepAction::Stage(statements) => (s.operation, EffectAction::Stage(statements)),
                    StepAction::Check { statement, refusal } => (
                        s.operation,
                        EffectAction::Check {
                            statement,
                            refusal: Some(refusal),
                        },
                    ),
                    StepAction::Dml(statements) => (s.operation, EffectAction::Dml(statements)),
                    StepAction::Ddl(statements) => (s.operation, EffectAction::Ddl(statements)),
                    StepAction::Host { statements, ship } => (s.operation, EffectAction::Host { statements, ship }),
                    StepAction::Abort {
                        statements,
                        probe,
                        label,
                        authored,
                    } => (
                        s.operation,
                        EffectAction::Terminal(TerminalAction::Abort {
                            statements,
                            probe,
                            provenance: if authored {
                                AbortProvenance::Authored { label }
                            } else {
                                AbortProvenance::Assertion { label }
                            },
                        }),
                    ),
                    StepAction::Exit(statements) => (s.operation, EffectAction::Terminal(TerminalAction::Exit { statements })),
                    StepAction::Return(ship) => (
                        "return!".to_string(),
                        EffectAction::Return {
                            statements: Vec::new(),
                            ship: Some(ship),
                        },
                    ),
                    StepAction::Session {
                        directive,
                        arguments,
                        report,
                    } => (
                        s.operation,
                        EffectAction::Session {
                            directive,
                            arguments,
                            report: report.map(|(statement, width)| {
                                crate::pipeline::compiled_query::ReportFill { statement, width }
                            }),
                        },
                    ),
                };
                typed_steps.push(EffectStep {
                    occurrence: format!("{operation}#{ordinal}"),
                    operation,
                    route,
                    requirements: s
                        .gates
                        .iter()
                        .map(|g| Requirement {
                            guard_id: *g,
                            polarity: GuardPolarity::Present,
                            reason: "comma",
                        })
                        .chain(s.after_exit.then_some(Requirement {
                            guard_id: exit_guard,
                            polarity: GuardPolarity::Absent,
                            reason: "exit",
                        }))
                        .collect(),
                    action,
                });
                ordinal += 1;
            }
            runs.push(EffectRun {
                connection_id: route,
                steps: typed_steps,
                created_objects: created.into_iter().map(|c| c.0).collect(),
                bracketed,
            });
        }
        let control = |name: &str, action: EffectAction| EffectStep {
            occurrence: name.to_string(),
            operation: name.to_string(),
            route,
            requirements: Vec::new(),
            action,
        };
        let typed = TypedEffectPlan {
            setup: (!program.setup.is_empty()).then(|| control("setup", EffectAction::Setup(program.setup))),
            runs,
            cleanup: (!program.cleanup.is_empty()).then(|| control("cleanup", EffectAction::Cleanup(program.cleanup))),
            guards: guard_sql
                .into_iter()
                .enumerate()
                .map(|(guard_id, sql)| GuardDefinition { guard_id, sql })
                .collect(),
        };
        CompiledPlan {
            entries: typed.flatten(),
            exit_probe_sql: program.exit,
            typed: Some(typed),
        }
    }
}

// ---------------------------------------------------------------------------
// SETUP: the product's own statement road, outside the compile window.
// ---------------------------------------------------------------------------

/// Admit the inline blocks a statement carries before its body, as the
/// product admits a prompt's leading blocks.
pub(crate) fn admit_leading_blocks(
    system: &mut DelightQLSystem,
    blocks: Vec<InlineDdlSpec>,
) -> Result<()> {
    crate::host::CompilerExecutionHost::register_prompt_blocks(system, blocks)
}

