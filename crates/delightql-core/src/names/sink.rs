// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The only road out of the registry for characters.
//!
//! [`IdentSink`] is sealed: an implementor outside this file does not
//! compile. There is no `fn spell(&self, ..) -> &str` anywhere in this
//! module, so two written things cannot be compared without materialising
//! both into a sink the caller had to be granted.
//!
//! The right-sizing check is a grep — `impl IdentSink for` returns exactly
//! the readers below, and a fifth requires editing this file.

mod sealed {
    pub trait Sealed {}
}

/// Receives identifier characters together with their stroppedness.
///
/// The stropped bit is passed rather than re-derived, because re-deriving
/// it from characters is how it gets lost: a reader that inspects the text
/// to decide whether it was quoted is guessing at a fact the registry knew.
pub trait IdentSink: sealed::Sealed {
    fn push_ident(&mut self, text: &str, stropped: bool);
}

/// Reader 1 — SQL generation. Owns quoting and escaping through the
/// generator-supplied dialect writer.
pub struct SqlOut<'a> {
    output: &'a mut String,
    writer: &'a mut dyn FnMut(&mut String, &str, bool) -> Result<(), String>,
    error: Option<String>,
}

impl<'a> SqlOut<'a> {
    pub fn new(
        output: &'a mut String,
        writer: &'a mut dyn FnMut(&mut String, &str, bool) -> Result<(), String>,
    ) -> Self {
        Self {
            output,
            writer,
            error: None,
        }
    }

    pub fn finish(self) -> Result<(), String> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Reader 2 — diagnostics and teaching. Receives the user's spelling, and
/// a structural description where there is no user spelling.
pub struct Teaching<'a>(pub &'a mut String);

/// Reader 3 — `--to ast-*` serialization.
#[expect(
    dead_code,
    reason = "AST serialization retains a sealed sink but does not currently construct it"
)]
pub struct LispyOut<'a>(pub &'a mut String);

/// Reader 4 — test assertions.
#[cfg(test)]
pub struct Probe<'a>(pub &'a mut String);

impl sealed::Sealed for SqlOut<'_> {}
impl sealed::Sealed for Teaching<'_> {}
impl sealed::Sealed for LispyOut<'_> {}
#[cfg(test)]
impl sealed::Sealed for Probe<'_> {}

impl IdentSink for SqlOut<'_> {
    fn push_ident(&mut self, text: &str, stropped: bool) {
        if self.error.is_none() {
            self.error = (self.writer)(self.output, text, stropped).err();
        }
    }
}

impl IdentSink for Teaching<'_> {
    fn push_ident(&mut self, text: &str, _stropped: bool) {
        self.0.push_str(text);
    }
}

impl IdentSink for LispyOut<'_> {
    fn push_ident(&mut self, text: &str, stropped: bool) {
        if stropped {
            self.0.push('`');
            self.0.push_str(text);
            self.0.push('`');
        } else {
            self.0.push_str(text);
        }
    }
}

#[cfg(test)]
impl IdentSink for Probe<'_> {
    fn push_ident(&mut self, text: &str, stropped: bool) {
        if stropped {
            self.0.push('`');
            self.0.push_str(text);
            self.0.push('`');
        } else {
            self.0.push_str(text);
        }
    }
}
