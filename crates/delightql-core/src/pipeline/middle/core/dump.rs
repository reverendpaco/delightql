// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A stable text rendering of a frozen graph, one line per node.

use super::graph::{Arena, Graph, Statement};
use super::ids::{ExprId, InstanceId, PassengerId, RelId, TruthId};
use super::node::{
    Arg, CaseTest, ExprKind, HeaderSlot, Passenger, PipeOp, Qual, ReadAccess, ReadSource, RelKind,
    SigmaProof, Slot, TruthKind,
};

pub(crate) fn lines(graph: &Graph) -> Vec<String> {
    let mut out = Vec::new();
    for i in 0..graph.rel_count() {
        let id = RelId::at(i);
        let node = graph.rel(id);
        out.push(format!(
            "{id:?} {} heading=({}) fv={:?}",
            rel_text(graph, node.kind()),
            node.heading().display().join(","),
            node.fv()
        ));
    }
    for i in 0..graph.expr_count() {
        let id = ExprId::at(i);
        out.push(format!("{id:?} {}", expr_text(graph, graph.expr(id).kind())));
    }
    for i in 0..graph.truth_count() {
        let id = TruthId::at(i);
        out.push(format!("{id:?} {}", truth_text(graph.truth(id).kind())));
    }
    for i in 0..graph.instance_count() {
        let id = InstanceId::at(i);
        let instance = graph.instance(id);
        out.push(format!(
            "{id:?} Instance {} formals={} body={:?}",
            instance.definition(),
            instance.formals().len(),
            instance.body()
        ));
    }
    for i in 0..graph.passenger_count() {
        let id = PassengerId::at(i);
        out.push(match graph.passenger(id) {
            Passenger::Configured {
                value,
                construction,
            } => format!("{id:?} Passenger configured value={value:?} construction={construction:?}"),
            Passenger::RowLocator(super::node::RowLocator::Pseudo(name)) => {
                format!("{id:?} Passenger row-locator {name}")
            }
            Passenger::RowLocator(super::node::RowLocator::Column(k)) => {
                format!("{id:?} Passenger row-locator column#{k}")
            }
            Passenger::Node { extraction } => format!("{id:?} Passenger node of {extraction:?}"),
        });
    }
    out.push(match graph.statement() {
        Statement::Query(r) => format!("statement query {r:?} effect={:?}", graph.statement().effect()),
        Statement::Program(p) => format!("statement program {:?} effect={:?}", p.result(), graph.statement().effect()),
        Statement::Declaration(d) => format!("statement declaration {} row={:?}", d.name(), d.row()),
    });
    out
}

fn rel_text(graph: &Graph, kind: &RelKind) -> String {
    match kind {
        RelKind::Read { source, access } => format!(
            "Read {} {}",
            match source {
                ReadSource::Catalog {
                    name,
                    namespace,
                    locator,
                    columns,
                    typed,
                    ..
                } => format!(
                    "catalog:{namespace}.{name}{}{} declared={:?}",
                    if locator.is_empty() { String::new() } else { format!("!! locator={locator:?}") },
                    if *typed { "" } else { " untyped" },
                    columns.iter().map(|c| &c.declared).collect::<Vec<_>>()
                ),
                ReadSource::Local(r) => format!("local:{r:?}"),
                ReadSource::Staged(r) => format!("staged:{r:?}"),
                ReadSource::Frontier(b) => format!("frontier:{b:?}"),
                ReadSource::Created { receipt, name, .. } => format!("created:{name} after {receipt:?}"),
                ReadSource::Function { name, args, columns, .. } => format!(
                    "function:{name}({args:?}) columns={:?}",
                    columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>()
                ),
            },
            match access {
                ReadAccess::All => "all".to_string(),
                ReadAccess::Unasked => "unasked".to_string(),
                ReadAccess::Slots(slots) => format!(
                    "slots({})",
                    slots
                        .iter()
                        .map(|s| match s {
                            Slot::Bind(n) => n.to_string(),
                            Slot::Anon => "_".to_string(),
                            Slot::Reuse { first, class } => format!("={first}:{class:?}"),
                            Slot::Constraint { value, class } => format!("{value:?}:{class:?}"),
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            }
        ),
        RelKind::Lit { header, rows, aliased } => format!(
            "Lit header=({}){} rows={}",
            header
                .iter()
                .map(|h| match h {
                    HeaderSlot::Bind(n) => n.to_string(),
                    HeaderSlot::Anon | HeaderSlot::Disregard => "_".to_string(),
                    HeaderSlot::Reuse { first, class } => format!("={first}:{class:?}"),
                    HeaderSlot::Constraint { value, class } => format!("={value:?}:{class:?}"),
                })
                .collect::<Vec<_>>()
                .join(","),
            if *aliased { " aliased" } else { "" },
            rows.len()
        ),
        RelKind::Receipt { act, header, cells } => format!(
            "Receipt {} header=({}) cells={cells:?} source={:?}",
            act.operation(),
            header.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(","),
            act.source(),
        ),
        RelKind::Run(run) => {
            let quals: Vec<String> = run
                .quals()
                .iter()
                .map(|q| match q {
                    Qual::Member(m) => format!(
                        "member {:?}:{:?}{}{}{}{} role={:?} dependence={:?} opens={:?} dep={:?}",
                        m.binder(),
                        m.rel(),
                        m.scope().map(|s| format!(" as {s}")).unwrap_or_default(),
                        if m.scope().is_some() && !m.names_scope() { " (unnamed)" } else { "" },
                        match m.mark() {
                            super::node::Mark::Unmarked => "",
                            super::node::Mark::Optional => " ?",
                            super::node::Mark::Spent => " ?spent",
                        },
                        match m.route() {
                            super::node::Route::Plain => "",
                            super::node::Route::Inline => " inline",
                            super::node::Route::Named => " named",
                        },
                        m.role(),
                        m.dependence(),
                        m.opens(),
                        m.dependent_on(graph)
                    ),
                    Qual::Guard(g) => format!("guard {:?} {:?}", g.truth(), g.placement()),
                    Qual::Bound(b) => format!("bound {:?}/{:?}", b.count, b.offset),
                })
                .collect();
            format!(
                "Run [{}] merges={:?} outputs={:?}",
                quals.join("; "),
                run.merges(),
                run.outputs()
            )
        }
        RelKind::Pipe { input, op } => format!(
            "Pipe {input:?} {}",
            match op {
                PipeOp::Project(items) => format!("project {:?}", items.iter().map(|i| i.expr).collect::<Vec<_>>()),
                PipeOp::Embed(items) => format!("embed {:?}", items.iter().map(|i| i.expr).collect::<Vec<_>>()),
                PipeOp::ProjectOut(selection) => format!("project_out {:?}", selection.items()),
                PipeOp::Cover(c) => format!("cover {:?}", c.items()),
                PipeOp::Group { keys, reductions } => format!(
                    "group keys={:?} reductions={:?}",
                    keys.iter().map(|i| i.expr).collect::<Vec<_>>(),
                    reductions.iter().map(|i| i.expr).collect::<Vec<_>>()
                ),
                PipeOp::Distinct(keys) => format!("distinct {:?}", keys.iter().map(|i| i.expr).collect::<Vec<_>>()),
                PipeOp::Carry(ps) => format!("carry {ps:?}"),
            }
        ),
        RelKind::Order { input, keys, bound } => format!(
            "Order {input:?} keys={:?} bound={bound:?}",
            keys.iter().map(|k| (k.expr, k.direction)).collect::<Vec<_>>()
        ),
        RelKind::SetOp {
            left,
            right,
            op,
            alignment,
            correlation,
        } => format!(
            "SetOp {op:?} {left:?} {right:?} alignment={alignment:?}{}",
            correlation
                .as_ref()
                .map(|c| format!(
                    " correlation={:?} membership={:?} class={:?}",
                    c.pairs(),
                    c.membership(),
                    c.class()
                ))
                .unwrap_or_default()
        ),
        RelKind::Minus { left, right, pairs, membership, class } => {
            format!("Minus {left:?} {right:?} pairs={pairs:?} membership={membership:?} class={class:?}")
        }
        RelKind::Meta { input } => format!("Meta {input:?}"),
        RelKind::Witnessed { input, empty } => format!("Witnessed {input:?} empty={empty:?}"),
        RelKind::Unnest { value, expansion } => format!(
            "Unnest {value:?} levels={} consumes={}",
            expansion
                .levels
                .iter()
                .map(|l| format!("{:?}<-{:?}:{}", l.reach, l.from.as_ref().map(|(p, _)| *p), l.binds.len()))
                .collect::<Vec<_>>()
                .join(","),
            expansion.consumes()
        ),
        RelKind::Family { clauses, .. } => format!(
            "Family {:?}",
            clauses.iter().map(|c| (c.guard, c.body)).collect::<Vec<_>>()
        ),
        RelKind::Fix(fix) => format!(
            "Fix frontier={:?} dedup={} anchors={:?} steps={:?}",
            fix.frontier(),
            fix.deduplicating(),
            fix.anchors(),
            fix.steps()
        ),
        RelKind::Apply { instance } => format!(
            "Apply {instance:?} {} body={:?}",
            graph.instance(*instance).definition(),
            graph.instance(*instance).body()
        ),
    }
}

fn expr_text(graph: &Graph, kind: &ExprKind) -> String {
    let args = |args: &[Arg]| -> String {
        args.iter()
            .map(|a| match a {
                Arg::Value { expr, distinct } => format!("{}{expr:?}", if *distinct { "%" } else { "" }),
                Arg::Star => "*".to_string(),
            })
            .collect::<Vec<_>>()
            .join(",")
    };
    match kind {
        ExprKind::Col(b, i) => format!("Col {b:?}.{i}"),
        ExprKind::Merged(m) => format!("Merged {m:?} ({})", graph.merge(*m).name()),
        ExprKind::Const(v) => format!("Const {v:?}"),
        ExprKind::Call { callee, args: a, grade } => format!("Call {}({}) {:?}", callee.name, args(a), grade),
        ExprKind::Window { callee, args: a, partition, order, frame } => format!(
            "Window {}({}) partition={partition:?} order={:?} frame={frame:?}",
            callee.name,
            args(a),
            order.iter().map(|k| k.expr).collect::<Vec<_>>()
        ),
        ExprKind::Infix(op, l, r) => format!("Infix {op:?} {l:?} {r:?}"),
        ExprKind::Case { anchor, arms, default } => format!(
            "Case anchor={anchor:?} arms={} default={default:?}",
            arms.iter()
                .map(|(t, r)| match t {
                    CaseTest::Literal { value, class } => format!("{value:?}:{class:?}->{r:?}"),
                    CaseTest::Truth(t) => format!("{t:?}->{r:?}"),
                })
                .collect::<Vec<_>>()
                .join(" ")
        ),
        ExprKind::Crossed(t) => format!("Crossed {t:?}"),
        ExprKind::Scalar { rel, cardinality } => format!("Scalar {rel:?} {cardinality:?}"),
        ExprKind::Construct { layout, members } => format!(
            "Construct {layout:?} [{}]",
            members.iter().map(|m| format!("{:?}:{:?}", m.naming, m.expr)).collect::<Vec<_>>().join(",")
        ),
        ExprKind::Path { source, path } => format!(
            "Path {source:?} .{}",
            path.steps().map(|s| s.spelling()).collect::<Vec<_>>().join(".")
        ),
        ExprKind::Passenger(p) => format!("Passenger {p:?}"),
        ExprKind::Across(actual) => format!("Across {actual:?}"),
        ExprKind::Argument { value, population, .. } => format!("Argument {value:?} over {population:?}"),
        ExprKind::Collect { members, .. } => format!("Collect members={}", members.len()),
        ExprKind::Metadata { level } => format!("Metadata key={:?}", level.key),
        ExprKind::Pick { value, rank } => format!("Pick value={value:?} rank={rank:?}"),
    }
}

fn truth_text(kind: &TruthKind) -> String {
    match kind {
        TruthKind::Cmp { op, left, right, class, .. } => format!("Cmp {op:?} {left:?} {right:?} {class:?}"),
        TruthKind::And(parts) => format!("And {parts:?}"),
        TruthKind::Or(parts) => format!("Or {parts:?}"),
        TruthKind::Not(p) => format!("Not {p:?}"),
        TruthKind::Exists { positive, rel } => format!("Exists {positive} {rel:?}"),
        TruthKind::Sigma { proof, args, positive } => match proof {
            SigmaProof::Served { namespace, name } => format!("Sigma served:{namespace}.{name} {args:?} {positive}"),
            SigmaProof::Rule { definition, expansion } => {
                format!("Sigma rule:{definition} expansion={expansion:?} {args:?} {positive}")
            }
        },
    }
}

