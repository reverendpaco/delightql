/// Operator precedence helpers for infix expressions

/// Determine if parentheses are needed based on operator precedence
pub fn needs_parentheses(child_op: &str, parent_op: &str, is_left: bool) -> bool {
    let precedence = |op: &str| -> i32 {
        match op {
            "concat" => 1, // Lowest precedence (string concatenation)
            "add" | "subtract" => 2,
            "multiply" | "divide" => 3,
            other => panic!(
                "catch-all hit in precedence.rs needs_parentheses operator: {}",
                other
            ),
        }
    };

    let child_prec = precedence(child_op);
    let parent_prec = precedence(parent_op);

    // Need parentheses if:
    // 1. Child has lower precedence than parent
    // 2. Child has same precedence as parent and we're on the right side (for left-associative operators)
    //    This handles cases like a - (b - c) which is different from a - b - c
    if child_prec < parent_prec {
        true
    } else if child_prec == parent_prec && !is_left {
        // For operators of the same precedence, we need parens on the right
        // to maintain correct associativity (except for commutative ops like add/multiply)
        matches!(child_op, "subtract" | "divide")
    } else {
        false
    }
}
