// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// S-expression formatter for pretty-printing
///
/// This module provides utilities to format our lispy notation.

pub fn custom_pretty_print(sexp: &str) -> String {
    let mut result = String::new();
    let mut indent: usize = 0;
    let mut chars = sexp.chars().peekable();
    let mut after_open = false;
    // Recovery trees quote token text — `(MISSING ")")` — so parens
    // inside string literals are content, not structure. Without this
    // the quoted `)` decrements indent alongside the real one and the
    // arithmetic underflows on ordinary incomplete input.
    let mut in_string = false;

    while let Some(ch) = chars.next() {
        if in_string {
            result.push(ch);
            match ch {
                '\\' => {
                    if let Some(escaped) = chars.next() {
                        result.push(escaped);
                    }
                }
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => {
                result.push(ch);
                in_string = true;
                after_open = false;
            }
            '(' => {
                if !result.is_empty() && !result.ends_with('\n') && !after_open {
                    result.push('\n');
                    result.push_str(&"  ".repeat(indent));
                }
                result.push(ch);
                indent += 1;
                after_open = true;
            }
            ')' => {
                // Saturate: the printer is total over every tree the
                // parser can produce; malformed balance must not panic.
                indent = indent.saturating_sub(1);
                result.push(ch);
                after_open = false;
            }
            ' ' if after_open => {
                // First space after opening paren - check if we should break line
                if chars.peek() == Some(&'(') {
                    // Next is another list, put it on new line
                    result.push('\n');
                    result.push_str(&"  ".repeat(indent));
                } else {
                    result.push(ch);
                }
                after_open = false;
            }
            _ => {
                result.push(ch);
                if ch != ' ' {
                    after_open = false;
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_custom_pretty_print() {
        let flat = "(query (pipe (relation:ground (relation_identifier (schema nil) (name users)) (resolved (was (column_spec:glob)) (data (columns (output_columns id first_name) (column_types nil)))) (alias nil)) (unary_relational_operator:projection (containment_semantic:bracket) (column_spec:explicit_list ((expression:column_ref (qualifier nil) (name first_name)))))))";

        let pretty = custom_pretty_print(flat);
        println!("Custom pretty printed:\n{}", pretty);
    }
}

#[cfg(test)]
mod red_pins {
    use super::*;

    // RED pin (codex fresh-eyes 2026-07-20 F-24): the recovery tree of
    // the ordinary incomplete editor buffer `people(` drives the
    // indentation arithmetic below zero — attempt to subtract with
    // overflow. CST inspection is FOR malformed input; the printer must
    // be total over every tree the parser can produce.
    #[test]
    fn incomplete_table_call_pretty_prints_without_panic() {
        // parse_for_cst_output is the --to cst path: it tolerates error
        // trees, because inspecting malformed buffers is CST's job.
        let tree = crate::pipeline::parser::parse_for_cst_output("people(")
            .expect("CST parse tolerates error trees");
        let printed = custom_pretty_print(&tree.root_node().to_sexp());
        assert!(!printed.is_empty());
    }
}
