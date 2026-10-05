// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `name@expr` IS `_(name@expr)`: every admitted short spelling normalizes to
//! exactly the tree of its written twin, and every spelling whose written
//! twin means something else refuses.

use super::support::{lispy, query, refusal};

/// Short spelling, then its written twin.
const TWINS: &[(&str, &str)] = &[
    ("a@2", "_(a@2)"),
    ("a@2, b@\"hither\"", "_(a@2), _(b@\"hither\")"),
    ("_(x @ 1; 2), y@x + 1", "_(x @ 1; 2), _(y@x + 1)"),
    ("_(a @ 1; 2; 3), a@2", "_(a @ 1; 2; 3), _(a@2)"),
    ("_(x @ 1), a@x as g", "_(x @ 1), _(a@x) as g"),
    ("a@2; b@4", "_(a@2); _(b@4)"),
    ("_(a @ 1; 2) - a@2", "_(a @ 1; 2) - _(a@2)"),
    (
        "_(x @ 1; 5), m@x not in (1; 2)",
        "_(x @ 1; 5), _(m@x not in (1; 2))",
    ),
    ("_(x, y @ 1, 2), j@{x, y}", "_(x, y @ 1, 2), _(j@{x, y})"),
    // Words the grammar reads as names in a header are names here too.
    ("`true`@2", "_(`true`@2)"),
    ("`null`@2", "_(`null`@2)"),
    ("FALSE@2", "_(FALSE@2)"),
    ("from@2", "_(from@2)"),
    ("and@2", "_(and@2)"),
    ("not@2", "_(not@2)"),
];

#[test]
fn the_short_spelling_is_its_written_twin() {
    for (short, written) in TWINS {
        assert_eq!(
            lispy(&query(short)),
            lispy(&query(written)),
            "`{short}` must normalize exactly as `{written}`"
        );
    }
}

#[test]
fn a_literal_word_is_not_a_column_name() {
    // The twin comparison can see this difference: a header's literal word
    // constrains the row, and its stropped spelling binds a column.
    assert_ne!(lispy(&query("_(true@2)")), lispy(&query("_(`true`@2)")));
    for word in ["true", "false", "null"] {
        let refused = refusal(&format!("{word}@2"));
        assert!(
            refused.contains("is a literal, not a column name")
                && refused.contains(&format!("`{word}`@")),
            "{word}@2: {refused}"
        );
    }
}

#[test]
fn the_rows_teaching_is_built_from_the_parsed_spans() {
    // The stropped name holds an `@`; the correction must keep it whole.
    let refused = refusal("`a@b`@1;2");
    assert!(refused.contains("_(`a@b` @ 1; 2)"), "{refused}");
    let refused = refusal("a@x + 1; \"y\"");
    assert!(refused.contains("_(a @ x + 1; \"y\")"), "{refused}");
}
