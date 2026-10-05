// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The middle's entrance on the product's statement roads: the
//! relay's statement road and its error-hook road, and the inspection road.
//! The road is decided on the unresolved statement before any compilation
//! starts; a statement the new middle claims ends in its artifact or its
//! refusal, and is never handed back to the product.

use super::facade;

/// Where one statement goes.
pub(crate) enum Route {
    /// A query: the artifact the relay's statement road runs.
    Query(facade::CompiledQuery),
    /// A program: the plan the relay's pump plays, and the inline blocks
    /// written after the statement, admitted once it has run.
    Program(facade::CompiledPlan, facade::Trailing),
    /// The new middle's refusal.
    Refused(delightql_types::DelightQLError),
}

/// The file a statement runs, when its whole body is `run!(path, …)`:
/// `run!` is the consultation of the file, then the demand of its `main!`,
/// so the entrance has the runtime consult the file before it hands the
/// statement here, and the statement demands the `main!` that consultation
/// made.
pub(crate) fn run_file(goal: &facade::Goal) -> Option<String> {
    facade::run_file(goal.query())
}

/// The relay's entrance: one submission's goal, as the relay read it from
/// `wire_text`. `overridden` says the session carries danger or option
/// overrides.
pub(crate) fn statement(
    system: &mut facade::DelightQLSystem,
    wire_text: &str,
    mut goal: facade::Goal,
    admission: facade::Admission,
    overridden: bool,
) -> Route {
    if let Err(error) = served_plan(system, goal.query(), Stand::Session) {
        return Route::Refused(error);
    }
    if !facade::settings(&goal.declared).covered || overridden {
        return Route::Refused(uncovered_settings());
    }
    // The blocks written after the statement are admitted once it has run
    // (THE TWO AXES: one linear order); the session's admission is spent on
    // them now, as on the blocks it leads.
    let trailing = std::mem::take(&mut goal.blocks.trailing);
    if !trailing.is_empty() {
        if let Err(error) = facade::admit(admission, "an inline (~~ddl ~~) block") {
            return Route::Refused(error);
        }
    }
    let leading = std::mem::take(&mut goal.blocks.leading);
    if !leading.is_empty() {
        let admitted = facade::admit(admission, "an inline (~~ddl ~~) block")
            .and_then(|()| facade::admit_leading_blocks(system, leading));
        if let Err(error) = admitted {
            return Route::Refused(error);
        }
    }
    let outcome = window(system, wire_text, Reading::Wire, admission, Want::Route).and_then(Windowed::route);
    match outcome {
        Ok(realized) => {
            match realized {
                Route::Query(mut compiled) => {
                    compiled.trailing = facade::Trailing::after(trailing);
                    Route::Query(compiled)
                }
                Route::Program(plan, _) => Route::Program(plan, facade::Trailing::after(trailing)),
                other => other,
            }
        }
        Err(error) => Route::Refused(error),
    }
}

/// The inspection road's entrance (`sys::execution.compile`): the text as
/// typed, rendered at `stage`. Nothing runs.
///
/// - `sql`: the statement's SQL, or a program's statements.
/// - `ast-resolved` and `ast-refined`: the frozen graph, one line per node.
///   It is the new middle's one elaborated form, where every name is bound
///   and every judgment decided, so both stages show it.
/// - `ast-sql`: refused. The new middle renders its SQL tree only after
///   lowering, and `sql` shows that.
///
/// `cst` and `ast-unresolved` are the shared front end's, and never reach
/// this entrance.
pub(crate) fn inspect(
    system: &mut facade::DelightQLSystem,
    stage: &str,
    typed_text: &str,
) -> facade::Result<String> {
    let want = match stage {
        "sql" => Want::Route,
        "ast-resolved" | "ast-refined" => Want::Graph,
        _ => {
            let error = outside(&match stage {
                "ast-sql" => "the `ast-sql` stage: the new middle renders its SQL tree only after lowering, \
                              which the `sql` stage shows"
                    .to_string(),
                other => format!(
                    "the `{other}` stage, which is no stage: the stages are `cst` and `ast-unresolved` \
                     (the front end's), `ast-resolved` and `ast-refined` (the frozen graph) and `sql`"
                ),
            });
            return Err(error);
        }
    };
    match window(system, typed_text, Reading::Typed(Some(stage)), facade::Admission::Execute, want) {
        Ok(Windowed::Graph(lines)) => Ok(lines),
        Ok(Windowed::Route(Route::Query(compiled))) => Ok(compiled.primary_sql),
        Ok(Windowed::Route(Route::Program(plan, _))) => Ok(plan.render_sql()),
        Ok(Windowed::Route(Route::Refused(error))) | Err(error) => Err(error),
    }
}

/// How a window reads its text: the relay's protocol entrance, or the
/// prompt entrance typed text is read by, with the stage an inspection
/// renders when the window inspects (an inspection runs nothing, so a
/// relation the runtime would serve refuses).
#[derive(Clone, Copy)]
enum Reading<'s> {
    Wire,
    Typed(Option<&'s str>),
}

/// Where a window stops: at the relay's artifact, or at the frozen graph
/// an inspection shows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Want {
    Route,
    Graph,
}

/// What a window ends in.
enum Windowed {
    Route(Route),
    /// The frozen graph's text, one line per node.
    Graph(String),
}

impl Windowed {
    fn route(self) -> Result<Route, delightql_types::DelightQLError> {
        match self {
            Windowed::Route(route) => Ok(route),
            Windowed::Graph(_) => Err(super::core::refuse::contract("a relay window that stopped at its graph")),
        }
    }
}

/// One elaborated statement in its open compile window: the frozen graph,
/// the window's catalog read, and the compilation's name arena. The
/// realization entrance is an inherent method of this value, so nothing on
/// the judgment side names the realization.
pub(super) struct Window<'w, 's> {
    pub(super) graph: &'w super::core::graph::Graph,
    pub(super) input: &'w facade::Input<'s>,
    pub(super) names: &'w facade::Names,
}

/// One compile window: the front end, the runtime's rows for the relations
/// it serves, elaboration and its analyses, then realization.
fn window(
    system: &mut facade::DelightQLSystem,
    text: &str,
    reading: Reading<'_>,
    admission: facade::Admission,
    want: Want,
) -> Result<Windowed, delightql_types::DelightQLError> {
    let names = facade::open_names(admission);
    // The catalog publishes the limits this compilation is armed with
    // before the window opens: a statement reading them reads its own.
    facade::publish_limits(system, &names);
    compile(system, text, reading, admission, want, names)
}

/// Where a compiled statement's text stands: the prompt's session, or a
/// namespace's own text (a consulted file's goal).
#[derive(Clone, Copy)]
enum Stand<'n> {
    Session,
    In(&'n str),
}

impl Stand<'_> {
    fn standpoint(self, world: &super::select::World<'_>) -> facade::Result<super::select::Standpoint> {
        match self {
            Stand::Session => world.session(),
            Stand::In(namespace) => world.within(namespace),
        }
    }
}

/// Which relations a statement's text reads from the runtime: a head no
/// query-local block claims, which the one selection, made where the text
/// stands, answers with an entity the engine serves. A head whose judgment
/// answers anything else, or refuses, is not served; elaboration judges it
/// where it stands. A served head is supplied in place before elaboration,
/// so elaboration never meets it.
fn served_plan(
    system: &facade::DelightQLSystem,
    query: &facade::Query,
    stand: Stand<'_>,
) -> facade::Result<facade::ServedPlan> {
    use super::select::{Referent, Referred};
    let bins = facade::bin_registry(system);
    let world = super::select::World::open(facade::session_rows(system))?;
    let at = stand.standpoint(&world)?;
    let judge = |name: &facade::SqlIdentifier, qualifier: Option<&facade::Qualifier>| {
        Ok(match world.judge(&at, name, qualifier) {
            Ok(Referred::Linked(Referent::Served(s)) | Referred::Routed(Referent::Served(s)) | Referred::Grounded(Referent::Served(s))) => {
                Some((s.namespace().to_string(), s.name().clone()))
            }
            Ok(_) | Err(_) => None,
        })
    };
    facade::served_plan(query, &bins, &judge)
}

/// One statement elaborated where its text stands, in the catalog state the
/// window reads, with an imprint's forward declarations when its bodies are
/// compiled.
fn elaborated(
    input: &facade::Input<'_>,
    query: &facade::Query,
    stand: Stand<'_>,
    imprint: Option<std::rc::Rc<ImprintWorld>>,
    gates: super::elaborate::Gates,
) -> facade::Result<super::core::graph::Graph> {
    let mut world = super::select::World::open(input.catalog_rows())?;
    if let Some(imprint) = imprint {
        world = world.imprinting(imprint);
    }
    let at = stand.standpoint(&world)?;
    super::elaborate::statement(input, query, world, at, super::core::switches::Switches::default(), gates)
}

/// The rows a computed relation feeds into a served call: the new middle
/// compiles it as a statement of its own, in a separate arena, standing
/// where the statement it feeds stands, and runs it. Only a query feeds.
/// It is the statement's own text, under the statement's gates; `spent`
/// records that it spent the `min_multiplicity` gate, which the statement
/// judges as a whole.
fn feed(
    system: &mut facade::DelightQLSystem,
    query: facade::Query,
    admission: facade::Admission,
    stand: Stand<'_>,
    gates: super::elaborate::Gates,
    spent: &std::cell::Cell<bool>,
) -> facade::Result<Vec<Vec<facade::DomainExpression>>> {
    let names = facade::open_names(admission);
    let mut computed = |system: &mut facade::DelightQLSystem, query: facade::Query| {
        feed(system, query, admission, stand, gates, spent)
    };
    let plan = served_plan(system, &query, stand)?;
    let query = facade::supply_served(system, plan, query, &names, &mut computed)?;
    let input = facade::Input::open(&*system)?;
    let graph = elaborated(&input, &query, stand, None, super::elaborate::Gates { fed: true, ..gates })?;
    if super::elaborate::spends_gate(&graph) {
        spent.set(true);
    }
    let window = Window {
        graph: &graph,
        input: &input,
        names: &names,
    };
    match window.realize()? {
        Route::Query(compiled) => facade::feed_rows(system, &compiled),
        _ => Err(outside("a relation feeding a served call that is not a query")),
    }
}

/// The window's compilation, over the name arena the window opened.
fn compile(
    system: &mut facade::DelightQLSystem,
    text: &str,
    reading: Reading,
    admission: facade::Admission,
    want: Want,
    names: facade::Names,
) -> Result<Windowed, delightql_types::DelightQLError> {
    let submitted = match reading {
        Reading::Wire => facade::read_wire_submission(&names, text),
        Reading::Typed(_) => facade::read_submission(&names, text),
    };
    // The blocks written after the statement are the entrance's to admit
    // once the statement has run; nothing of them is compiled here.
    let (query, settings) = match submitted? {
        facade::Submitted::Goal { query, settings } => (query, settings),
        facade::Submitted::Definitions => return Err(outside("a definitions-only submission")),
    };
    if !settings.covered {
        return Err(uncovered_settings());
    }
    let gates = super::elaborate::Gates {
        min_multiplicity: settings.min_multiplicity,
        ..Default::default()
    };
    let spent = std::cell::Cell::new(false);
    let mut computed = |system: &mut facade::DelightQLSystem, query: facade::Query| {
        feed(system, query, admission, Stand::Session, gates, &spent)
    };
    let plan = served_plan(system, &query, Stand::Session);
    if let (Reading::Typed(Some(stage)), Ok(plan)) = (reading, &plan) {
        if let Some(served) = plan.first() {
            return Err(super::core::refuse::compile_purity(stage, served));
        }
    }
    let query = plan.and_then(|plan| facade::supply_served(system, plan, query, &names, &mut computed))?;
    let input = facade::Input::open(&*system)?;
    let graph = elaborated(
        &input,
        &query,
        Stand::Session,
        None,
        super::elaborate::Gates {
            spent_apart: spent.get(),
            ..gates
        },
    )?;
    if want == Want::Graph {
        return Ok(Windowed::Graph(super::core::dump::lines(&graph).join("\n")));
    }
    let program = matches!(graph.statement(), super::core::graph::Statement::Program(_));
    if program {
        facade::admit(admission, "an effect")?;
    }
    let window = Window {
        graph: &graph,
        input: &input,
        names: &names,
    };
    window.realize().map(Windowed::Route)
}

fn outside(what: &str) -> delightql_types::DelightQLError {
    super::core::refuse::outside(what)
}

/// A statement declaring a danger setting the new middle does not cover, or
/// compiled under the session's danger or option overrides.
fn uncovered_settings() -> delightql_types::DelightQLError {
    outside("a statement with danger settings or under session overrides")
}

/// THE BODY LAWS of one definition family, judged where it is declared: the
/// consultation that registers it, or the prompt block that admits it. The
/// new middle's one statement of them, which a statement's own query-local
/// families meet at the statement.
pub(crate) fn judge_declared_family(group: &facade::DefinitionGroup, effect: bool) -> facade::Result<()> {
    super::elaborate::judge_declared_family(group, effect)
}

/// The columns a consulted relational family publishes, where its every
/// head lists them: the family's heading the new middle decides from the
/// heads (`None` for an open head).
pub(crate) fn declared_columns(group: &facade::DefinitionGroup) -> facade::Result<Option<Vec<facade::SqlIdentifier>>> {
    super::elaborate::declared_columns(&group.name(), group.clauses().iter().map(|c| &c.head))
}

/// THE PROJECTION LAW where a consultation declares its relational
/// families: each family's heads judged against its bodies once the whole
/// load stands, standing in the namespace it was consulted into.
pub(crate) fn judge_declared_heads(
    system: &facade::DelightQLSystem,
    namespace: &str,
    families: &[String],
) -> facade::Result<()> {
    if families.is_empty() {
        return Ok(());
    }
    let input = facade::Input::open(system)?;
    let world = super::select::World::open(input.catalog_rows())?;
    let at = world.within(namespace)?;
    let names: Vec<facade::SqlIdentifier> = families.iter().map(|f| facade::SqlIdentifier::new(f)).collect();
    super::elaborate::declared_heads(&input, world, at, &names)
}

/// A consulted file's `?-` goal proved where its file's definitions are:
/// compiled as a statement whose primary definition context is the
/// consulted namespace, run on the connection it reads, and answered YES
/// when it has a row. A goal only reads: one that compiles to a program
/// refuses. Its leading blocks are admitted before it is compiled and its
/// trailing blocks after it has run.
pub(crate) fn prove_goal(system: &mut facade::DelightQLSystem, namespace: &str, mut goal: facade::Goal) -> facade::Result<bool> {
    if !goal.declared.dangers.is_empty() {
        return Err(outside("a consulted goal with danger settings"));
    }
    let leading = std::mem::take(&mut goal.blocks.leading);
    let trailing = std::mem::take(&mut goal.blocks.trailing);
    if !leading.is_empty() {
        facade::admit_leading_blocks(system, leading)?;
    }
    let spelling = goal.spelling.clone();
    let query = goal.into_query();
    let names = facade::open_names(facade::Admission::Execute);
    let spent = std::cell::Cell::new(false);
    let mut computed = |system: &mut facade::DelightQLSystem, query: facade::Query| {
        feed(system, query, facade::Admission::Execute, Stand::In(namespace), Default::default(), &spent)
    };
    let plan = served_plan(system, &query, Stand::In(namespace))?;
    let query = facade::supply_served(system, plan, query, &names, &mut computed)?;
    let rows = {
        let input = facade::Input::open(&*system)?;
        let graph = elaborated(&input, &query, Stand::In(namespace), None, Default::default())?;
        if matches!(graph.statement(), super::core::graph::Statement::Program(_)) {
            return Err(facade::witness_writes(&spelling));
        }
        let window = Window {
            graph: &graph,
            input: &input,
            names: &names,
        };
        match window.realize()? {
            Route::Query(compiled) => compiled,
            _ => return Err(super::core::refuse::contract("a consulted goal realized as no query")),
        }
    };
    let found = facade::feed_rows(system, &rows)?;
    facade::Trailing::after(trailing).admit(system)?;
    Ok(!found.is_empty())
}

/// What one recorded mention selects, judged at the body standpoint of the
/// definition that recorded it, for a lifecycle operation that holds the
/// catalog: the new middle's one judgment, over the rows that operation holds.
pub(crate) enum Reached {
    /// An authored family, and the namespace declaring it.
    Authored { namespace: String },
    /// An engine-served identity.
    Served,
    Missing,
    /// The ambiguity refusal, naming every candidate.
    Ambiguous(delightql_types::DelightQLError),
}

/// The road of the namespace law a recorded mention was judged by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Road {
    /// A bare name the body's own tiers answer: a lexical link.
    Link,
    /// A bare name the tiers miss: a free data name of the body, read in
    /// the data world its namespace's grounding bound.
    Hole,
    /// A qualified name, read in the one namespace its route reaches.
    Route,
}

/// `name`, bare or under the qualifier the catalog records, judged at the
/// body standpoint of the activated entity `body`.
pub(crate) fn recorded_mention(
    facts: &facade::HeldCatalog<'_>,
    body: i64,
    name: &str,
    qualifier: Option<&str>,
) -> facade::Result<(Road, Reached)> {
    use super::select::{Referent, Referred};
    let world = super::select::World::open(facade::CatalogRows::Held(facts))?;
    let at = world.body_of_entity(body)?;
    let qualifier = qualifier.map(facade::Qualifier::from_spelled);
    let reached = |referent: Referent| match referent {
        Referent::Family(family) => Reached::Authored {
            namespace: family.namespace().to_string(),
        },
        Referent::Served(_) | Referent::Declared(_) => Reached::Served,
    };
    let road = if qualifier.is_some() { Road::Route } else { Road::Link };
    Ok(match world.judge(&at, &facade::SqlIdentifier::new(name), qualifier.as_ref())? {
        Referred::Linked(r) => (Road::Link, reached(r)),
        Referred::Routed(r) => (Road::Route, reached(r)),
        Referred::Grounded(r) => (Road::Hole, reached(r)),
        Referred::Unanswered { free: Some(_) } => (Road::Hole, Reached::Missing),
        Referred::Unanswered { free: None } | Referred::Provider => (road, Reached::Missing),
        Referred::Ambiguous(refusal) => (road, Reached::Ambiguous(refusal)),
    })
}

/// The namespace (its catalog row) a written qualifier routes to from the
/// body standpoint of the activated entity `body`.
pub(crate) fn route_in_body(
    facts: &facade::HeldCatalog<'_>,
    body: i64,
    qualifier: &facade::Qualifier,
) -> facade::Result<Option<i64>> {
    let world = super::select::World::open(facade::CatalogRows::Held(facts))?;
    let at = world.body_of_entity(body)?;
    Ok(at.route(&world, qualifier).map(|namespace| namespace.id))
}

/// Every definition whose recorded mention selects the identity `identity`
/// (named `name`) at its own body's standpoint, as `namespace.entity`.
pub(crate) fn dependents(facts: &facade::HeldCatalog<'_>, identity: i64, name: &str) -> facade::Result<Vec<String>> {
    super::select::dependents(facade::CatalogRows::Held(facts), identity, name)
}

/// Every definition outside `namespace` whose qualified mention routes into
/// it from its own body's standpoint, as `namespace.entity`.
pub(crate) fn routed_into(facts: &facade::HeldCatalog<'_>, namespace: &str) -> facade::Result<Vec<String>> {
    super::select::routed_into(facade::CatalogRows::Held(facts), namespace)
}

pub(crate) use super::select::imprint::{Declaration as ImprintDeclaration, Imprint as ImprintWorld};

/// One read the new middle compiles from `text` at the prompt's site: the
/// compiled query and the heading it publishes (a position with no name is
/// `None`). Only a query is a read: a statement holding an act refuses.
fn compile_read(
    system: &facade::DelightQLSystem,
    text: &str,
    imprint: Option<std::rc::Rc<ImprintWorld>>,
) -> facade::Result<(facade::CompiledQuery, Vec<Option<String>>)> {
    let names = facade::open_names(facade::Admission::Execute);
    // A view's SQL is kept by the database it is stored in, so every
    // object it reads is spelled relative to that database.
    if let Some((database, primary)) = imprint.as_deref().and_then(ImprintWorld::stored_in) {
        facade::store_in(&names, &database, primary);
    }
    let (query, settings) = match facade::read_submission(&names, text)? {
        facade::Submitted::Goal { query, settings } => (query, settings),
        facade::Submitted::Definitions => return Err(outside("a definitions-only submission where a read is compiled")),
    };
    if !settings.covered {
        return Err(uncovered_settings());
    }
    if served_plan(system, &query, Stand::Session)?.first().is_some() {
        return Err(outside("a compiled read whose own text reads a relation the runtime serves or demands an act"));
    }
    let input = facade::Input::open(system)?;
    let graph = elaborated(
        &input,
        &query,
        Stand::Session,
        imprint,
        super::elaborate::Gates {
            min_multiplicity: settings.min_multiplicity,
            ..Default::default()
        },
    )?;
    let super::core::graph::Statement::Query(body) = graph.statement() else {
        return Err(outside("a read that holds an act"));
    };
    let heading = super::core::graph::Arena::rel(&graph, *body)
        .heading()
        .displayed()
        .map(|(_, p)| p.answering_name().map(|n| n.as_str().to_string()))
        .collect();
    let window = Window {
        graph: &graph,
        input: &input,
        names: &names,
    };
    match window.realize()? {
        Route::Query(compiled) => Ok((compiled, heading)),
        _ => Err(super::core::refuse::contract("a read realized as no query")),
    }
}

/// A read compiled by the new middle and run on its connection: the
/// heading it publishes and its rows (a library manifest's companions).
pub(crate) fn query_rows(
    system: &facade::DelightQLSystem,
    text: &str,
) -> facade::Result<(Vec<Option<String>>, Vec<Vec<facade::DbValue>>)> {
    let (compiled, heading) = compile_read(system, text, None)?;
    let rows = facade::run_query(system, &compiled)?;
    Ok((heading, rows))
}

/// An imprinted entity's rule, compiled whole by the new middle from the
/// imprint's derived root, its bodies reading the imprint's forward
/// declarations: the SQL that fills or defines the object and the heading
/// it publishes. A body that needs statements run before it is not covered.
pub(crate) fn imprint_body(
    system: &facade::DelightQLSystem,
    text: &str,
    world: std::rc::Rc<ImprintWorld>,
) -> facade::Result<(String, Vec<Option<String>>)> {
    let (compiled, heading) = compile_read(system, text, Some(world))?;
    if !compiled.prepare_sqls.is_empty() || !compiled.obligations.is_empty() {
        return Err(outside("an imprinted rule whose read needs statements run before it"));
    }
    Ok((compiled.primary_sql, heading))
}

/// A manifest's declared table, compiled by the new middle: its CREATE
/// text. Its cells stand in `manifest`, the namespace their companion rules
/// are declared in, and each cell's names are selected there; its columns
/// are the declared row's.
pub(crate) fn declared_table(
    system: &facade::DelightQLSystem,
    manifest: &str,
    table: &facade::DeclaredTable,
) -> facade::Result<String> {
    let names = facade::open_names(facade::Admission::Execute);
    let input = facade::Input::open(system)?;
    let world = super::select::World::open(input.catalog_rows())?;
    let at = world.within(manifest)?;
    let graph = super::elaborate::declaration(&input, table, world, at)?;
    let window = Window {
        graph: &graph,
        input: &input,
        names: &names,
    };
    window.declared_table()
}

/// The program the run of a consulted namespace's `main!` is, compiled by
/// the new middle and run by nothing (`sys::execution.explain_run`).
pub(crate) fn explain_main(system: &mut facade::DelightQLSystem, namespace: &str) -> facade::Result<facade::CompiledPlan> {
    let text = format!("run_namespace!(\"{}\")(*)", namespace.replace('"', "\"\""));
    match window(system, &text, Reading::Typed(None), facade::Admission::Execute, Want::Route)? {
        Windowed::Route(Route::Program(plan, _)) => Ok(plan),
        Windowed::Route(Route::Refused(error)) => Err(error),
        Windowed::Route(Route::Query(_)) | Windowed::Graph(_) => {
            Err(super::core::refuse::contract("a run that compiled to no program"))
        }
    }
}
