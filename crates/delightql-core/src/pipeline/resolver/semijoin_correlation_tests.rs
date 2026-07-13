// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! SQL-shape pins for argumentative semi/anti-join correlation
//! (bugs/correlated-semijoin-lost-correlation/ANALYSIS.md).
//!
//! `orders(*), +customers(customer_id)` must correlate the OUTER row's
//! `customer_id` with the fact row's positional column:
//! `orders.customer_id IS NOT DISTINCT FROM _fact.customer_id`. Before the
//! fix, the argument resolved to the INNER `_fact` alias, producing the
//! degenerate self-comparison `_fact.customer_id IS NOT DISTINCT FROM
//! _fact.customer_id` ("customers is nonempty").
//!
//! Red-first: every test in this file was observed failing against the
//! pre-fix compiler (self-comparison in generated SQL). The data-level
//! twins live in `new_test_suite/balls/correctness_bugs/
//! {semijoin,antijoin,semijoin_multicol}_lost_correlation.sef`.

use delightql_types::schema::{ColumnInfo, DatabaseSchema};

/// Two-table world: orders(order_id, customer_id, amount) and
/// customers(customer_id, name) — the shared `customer_id` is the
/// correlation lvar. `subs(id, user_id)` adds a second shared name for
/// the multi-column pin.
struct TwoTableSchema;

impl DatabaseSchema for TwoTableSchema {
    fn get_table_columns(&self, _schema: Option<&str>, table: &str) -> Option<Vec<ColumnInfo>> {
        let cols: &[&str] = match table {
            "orders" => &["order_id", "customer_id", "amount"],
            "customers" => &["customer_id", "name"],
            "subs" => &["order_id", "customer_id", "plan"],
            _ => return None,
        };
        Some(
            cols.iter()
                .enumerate()
                .map(|(i, name)| ColumnInfo {
                    name: (*name).into(),
                    nullable: true,
                    position: i,
                    declared_type: Some("TEXT".to_string()),
                })
                .collect(),
        )
    }

    fn table_exists(&self, _schema: Option<&str>, table: &str) -> bool {
        matches!(table, "orders" | "customers" | "subs")
    }
}

fn compile(dql: &str) -> String {
    crate::pipeline::compile_source_to_sql(dql, &TwoTableSchema)
        .unwrap_or_else(|e| panic!("compile failed for {dql:?}: {e}"))
}

#[test]
fn semi_join_argument_correlates_to_outer_row() {
    let sql = compile("orders(*), +customers(customer_id)");
    assert!(
        sql.contains("orders.customer_id IS NOT DISTINCT FROM _fact.customer_id"),
        "semi-join guard must correlate the outer column, got:\n{sql}"
    );
    assert!(
        !sql.contains("_fact.customer_id IS NOT DISTINCT FROM _fact.customer_id"),
        "semi-join guard degenerated to a self-comparison:\n{sql}"
    );
}

#[test]
fn anti_join_argument_correlates_to_outer_row() {
    let sql = compile(r"orders(*), \+customers(customer_id)");
    assert!(sql.contains("NOT EXISTS"), "anti-join must NOT EXISTS:\n{sql}");
    assert!(
        sql.contains("orders.customer_id IS NOT DISTINCT FROM _fact.customer_id"),
        "anti-join guard must correlate the outer column, got:\n{sql}"
    );
    assert!(
        !sql.contains("_fact.customer_id IS NOT DISTINCT FROM _fact.customer_id"),
        "anti-join guard degenerated to a self-comparison:\n{sql}"
    );
}

#[test]
fn multi_column_arguments_each_correlate_to_outer_row() {
    let sql = compile("orders(*), +subs(order_id, customer_id)");
    assert!(
        sql.contains("orders.order_id IS NOT DISTINCT FROM _fact.order_id"),
        "first argument must correlate the outer column, got:\n{sql}"
    );
    assert!(
        sql.contains("orders.customer_id IS NOT DISTINCT FROM _fact.customer_id"),
        "second argument must correlate the outer column, got:\n{sql}"
    );
    assert!(
        !sql.contains("_fact.order_id IS NOT DISTINCT FROM _fact.order_id"),
        "guard degenerated to a self-comparison:\n{sql}"
    );
}

#[test]
fn aliased_outer_relation_correlates_through_alias() {
    let sql = compile("orders(*) as o, +customers(customer_id)");
    assert!(
        sql.contains("o.customer_id IS NOT DISTINCT FROM _fact.customer_id"),
        "semi-join guard must correlate through the outer alias, got:\n{sql}"
    );
    assert!(
        !sql.contains("_fact.customer_id IS NOT DISTINCT FROM _fact.customer_id"),
        "semi-join guard degenerated to a self-comparison:\n{sql}"
    );
}

#[test]
fn argument_inside_expression_correlates_to_outer_row() {
    // Lvars nested in expressions in argument position are outer
    // references too (where.md: any domain expression may be argumentative).
    let sql = compile("orders(*), +customers((customer_id + 0))");
    assert!(
        sql.contains("(orders.customer_id + 0) IS NOT DISTINCT FROM _fact.customer_id"),
        "expression argument must correlate the outer column, got:\n{sql}"
    );
}

// --- verified-unaffected variants, pinned so they stay unaffected ---

#[test]
fn explicit_condition_form_stays_correct() {
    // The interior-notation form (where.md "Semi-Joins and Anti-Joins")
    // was never affected; pin it so the fix doesn't disturb it.
    let sql = compile("orders(*) as o, +customers(, o.customer_id = customers.customer_id)");
    assert!(
        sql.contains("o.customer_id IS NOT DISTINCT FROM customers.customer_id"),
        "explicit-condition semi-join changed shape:\n{sql}"
    );
}

#[test]
fn ground_argument_stays_ungrounded_selection() {
    // A literal argument grounds the positional column — no outer
    // reference involved; was never affected.
    let sql = compile("orders(*), +customers(42)");
    assert!(
        sql.contains("42 IS NOT DISTINCT FROM _fact.customer_id"),
        "ground-argument semi-join changed shape:\n{sql}"
    );
}
