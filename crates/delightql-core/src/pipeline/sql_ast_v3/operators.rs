use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Logical
    And,
    Or,

    // String
    Concatenate,

    // SQL-specific
    Like,
    NotLike,
    Is,
    IsNot,
    IsNotDistinctFrom,
    IsDistinctFrom,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Minus,
    Plus,
}
