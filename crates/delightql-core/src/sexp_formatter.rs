/// S-expression formatter for pretty-printing
///
/// This module provides utilities to format our lispy notation.

pub fn custom_pretty_print(sexp: &str) -> String {
    let mut result = String::new();
    let mut indent = 0;
    let mut chars = sexp.chars().peekable();
    let mut after_open = false;
    
    while let Some(ch) = chars.next() {
        match ch {
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
                indent -= 1;
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