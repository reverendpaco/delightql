// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use std::collections::{HashMap, HashSet};

/// THE GRADE OF ONE CALL: what a callable does with the rows it is applied
/// over, judged from its name AND its arity — `max` at one argument reduces,
/// at two it computes per row. The language's own scalar and window forms
/// answer from [`BuiltInRegistry::grade`]; whether a target call reduces is
/// the target's aggregate catalog's answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallGrade {
    /// Reduces the rows it stands over to one value.
    Aggregate,
    /// Computes one value per row over a window it must be given.
    Window,
    /// Computes one value per row from that row alone.
    Scalar,
    /// A callable the compiler holds no record of: the target's, whose
    /// grade the author asserts by where the call stands.
    Unknown,
}

/// The language's own a priori callable forms: the scalar functions it
/// knows and the engine window builtins with their argument bounds. It
/// holds no aggregate: which target call reduces is the target catalog's
/// knowledge, and a name here is never a second answer to it.
#[derive(Clone)]
pub struct BuiltInRegistry {
    functions: HashSet<String>,
    /// The engine window builtins and their argument bounds (min, max).
    /// The one compile-time signature authority for these names — a
    /// rebuilt invocation is judged here, never by the engine's error.
    pub window_signatures: HashMap<&'static str, (u8, u8)>,
}

impl Default for BuiltInRegistry {
    fn default() -> Self {
        Self::new()
    }
}

static BUILT_IN: std::sync::LazyLock<BuiltInRegistry> =
    std::sync::LazyLock::new(BuiltInRegistry::new);

impl BuiltInRegistry {
    pub fn new() -> Self {
        let mut functions = HashSet::new();

        functions.insert("upper".to_string());
        functions.insert("lower".to_string());
        functions.insert("trim".to_string());
        functions.insert("length".to_string());
        functions.insert("substr".to_string());
        functions.insert("replace".to_string());
        functions.insert("coalesce".to_string());
        functions.insert("greatest".to_string());
        functions.insert("least".to_string());
        functions.insert("abs".to_string());
        functions.insert("round".to_string());

        let mut window_signatures = HashMap::new();
        window_signatures.insert("row_number", (0, 0));
        window_signatures.insert("rank", (0, 0));
        window_signatures.insert("dense_rank", (0, 0));
        window_signatures.insert("percent_rank", (0, 0));
        window_signatures.insert("cume_dist", (0, 0));
        window_signatures.insert("ntile", (1, 1));
        window_signatures.insert("lag", (1, 3));
        window_signatures.insert("lead", (1, 3));
        window_signatures.insert("first_value", (1, 1));
        window_signatures.insert("last_value", (1, 1));
        window_signatures.insert("nth_value", (2, 2));

        Self {
            functions,
            window_signatures,
        }
    }

    /// The one built-in registry: its tables are fixed at construction, so
    /// every compilation reads the same one.
    pub fn the() -> &'static BuiltInRegistry {
        &BUILT_IN
    }

    /// THE LANGUAGE'S GRADE OF A CALL OF `name` OVER `arity` ARGUMENTS:
    /// `Scalar` or `Window` for a form the language knows, `Unknown` for
    /// every other name — a target call, whose reduction only the target's
    /// catalog can state. An arity-distinguished overload is judged before
    /// the name: `max(v, 0)` is the scalar, whatever any catalog says of
    /// `max`.
    pub fn grade(&self, name: &str, arity: usize) -> CallGrade {
        if crate::names::Intrinsic::scalar_overload(name, arity).is_some() {
            return CallGrade::Scalar;
        }
        if self.window_signature(name).is_some() {
            return CallGrade::Window;
        }
        if self.is_known_function(name) {
            return CallGrade::Scalar;
        }
        CallGrade::Unknown
    }

    /// The argument bounds of an engine window builtin, if the name is one.
    pub fn window_signature(&self, name: &str) -> Option<(u8, u8)> {
        self.window_signatures
            .get(name.to_lowercase().as_str())
            .copied()
    }

    /// Whether the language knows `name` as one of its scalar functions.
    pub fn is_known_function(&self, name: &str) -> bool {
        self.functions.contains(&name.to_lowercase())
    }
}

