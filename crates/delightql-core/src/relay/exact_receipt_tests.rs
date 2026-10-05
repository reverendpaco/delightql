// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A RECEIPT TRANSPORTS A RELATION, witnessed through the statement road:
//! a payload released from the packaged receipt holds exactly the rows the
//! fused release does — an all-NULL row and its duplicates included, and
//! none for an empty payload — while an authored tree still elides. Every
//! expected bag is the literal payload, never an answer this compiler gave.

use delightql_protocol::{ClientTerm, Handler, Orientation, Projection, ServerTerm};

use super::pump_tests::{fresh_system, relay_over, shared_sqlite, statement, TestRelay};

type Row = Vec<Option<String>>;

/// One statement's rows as text cells, in the order the engine answered.
fn rows(relay: &mut TestRelay<'_>, text: &str) -> Vec<Row> {
    let handle = match relay.handle(statement(text)) {
        ServerTerm::Header { handle, .. } => handle,
        ServerTerm::Error(error) => {
            panic!("{text}: {}", String::from_utf8_lossy(error.message()))
        }
        other => panic!("{text}: unexpected response {other:?}"),
    };
    let mut answered = Vec::new();
    loop {
        match relay.handle(ClientTerm::Fetch {
            handle: handle.clone(),
            projection: Projection::All,
            count: u64::MAX,
            orientation: Orientation::Rows,
        }) {
            ServerTerm::Data { cells } => answered.extend(cells.into_iter().map(|row| {
                row.into_iter()
                    .map(|cell| cell.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
                    .collect()
            })),
            ServerTerm::End => return answered,
            ServerTerm::Error(error) => {
                panic!("{text}: {}", String::from_utf8_lossy(error.message()))
            }
            other => panic!("{text}: unexpected fetch response {other:?}"),
        }
    }
}

/// Rows compared as a bag: order is not observed, multiplicity is.
fn bag(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort();
    rows
}

fn cells(values: &[&[Option<&str>]]) -> Vec<Row> {
    values
        .iter()
        .map(|row| row.iter().map(|cell| cell.map(str::to_string)).collect())
        .collect()
}

/// Each payload literal, with the bag of rows it spells.
const PAYLOADS: &[(&str, &str, &[&[Option<&str>]])] = &[
    ("_(a @ null)", "a", &[&[None]]),
    ("_(a @ null;null)", "a", &[&[None], &[None]]),
    ("_(a @ null;null;1)", "a", &[&[None], &[None], &[Some("1")]]),
    (
        "_(a,b @ null,null; null,7; null,null; 2,3; 2,3)",
        "a, b",
        &[
            &[None, None],
            &[None, Some("7")],
            &[None, None],
            &[Some("2"), Some("3")],
            &[Some("2"), Some("3")],
        ],
    ),
];

#[test]
fn the_packaged_payload_releases_the_rows_the_fused_release_does() {
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    for (payload, heading, expected) in PAYLOADS {
        let expected = bag(cells(expected));
        let drilled = rows(
            &mut relay,
            &format!("returning!({payload})(*).returned(*) |> ({heading})"),
        );
        let fused = rows(
            &mut relay,
            &format!("{payload} |> returning!(*) |> .returned(*) |> ({heading})"),
        );
        let observed = rows(
            &mut relay,
            &format!("{payload} |> returning!(*) : r!\nr!()(*) |> .returned(*) |> ({heading})"),
        );
        assert_eq!(bag(drilled), expected, "drilled {payload}");
        assert_eq!(bag(fused), expected, "fused {payload}");
        assert_eq!(bag(observed), expected, "observed then released {payload}");
    }
}

#[test]
fn an_empty_payload_is_one_yes_receipt_releasing_nothing() {
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    let receipt = rows(
        &mut relay,
        "_(a @ 1), a = 2 : e\nreturning!(e(*))(*) |> (success, returned)",
    );
    assert_eq!(receipt, cells(&[&[Some("1"), Some("[]")]]));
    let released = rows(
        &mut relay,
        "_(a @ 1), a = 2 : e\nreturning!(e(*))(*).returned(*) ~> count:(*) as n",
    );
    assert_eq!(released, cells(&[&[Some("0")]]));
}

#[test]
fn an_all_null_witness_is_a_packaged_witness() {
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    let witnesses = rows(
        &mut relay,
        "nul(T(*))(*) : T(*), a = 1 |> (null as a)\n\
         assert!(nul(*), \"w\", _(a @ null;1))(*).witnesses(*) |> (a)",
    );
    assert_eq!(witnesses, cells(&[&[None]]));
}

#[test]
fn an_authored_tree_still_elides_all_null_contributors() {
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    let released = rows(&mut relay, "_(a @ null;null;1) |> %(~> {a} as r) |> .r(*)");
    assert_eq!(released, cells(&[&[Some("1")]]));
    let recollected = rows(
        &mut relay,
        "returning!(_(a @ null;null;1))(*).returned(*) |> (a) |> %(~> {a} as r) |> .r(*)",
    );
    assert_eq!(recollected, cells(&[&[Some("1")]]));
}
