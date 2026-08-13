// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Line-aware output accumulation.
//!
//! Written text may itself carry newlines — an echoed region keeps the
//! author's own line structure — so the builder splits on them rather than
//! letting a multi-line string sit inside one logical line. Fit decisions read
//! `current_line_length`, and a measure that counted three lines as one would
//! break every one of them.

pub struct OutputBuilder {
    lines: Vec<String>,
    current_line: String,
}

impl Default for OutputBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OutputBuilder {
    pub fn new() -> Self {
        Self {
            lines: Vec::new(),
            current_line: String::new(),
        }
    }

    /// Append text, honoring any newlines inside it.
    pub fn write(&mut self, text: &str) {
        let mut parts = text.split('\n');
        if let Some(first) = parts.next() {
            self.current_line.push_str(first);
        }
        for part in parts {
            self.lines.push(std::mem::take(&mut self.current_line));
            self.current_line.push_str(part);
        }
    }

    /// Start a new line. An empty current line is not a blank line — the
    /// visitor asks for a break at positions that may already be at one.
    pub fn newline(&mut self) {
        if !self.current_line.is_empty() {
            self.lines.push(std::mem::take(&mut self.current_line));
        }
    }

    pub fn newline_with_indent(&mut self, spaces: usize) {
        self.newline();
        self.current_line = " ".repeat(spaces);
    }

    /// One empty line, kept even where `newline` would collapse it.
    pub fn blank_line(&mut self) {
        self.newline();
        self.lines.push(String::new());
    }

    pub fn current_line_length(&self) -> usize {
        self.current_line.len()
    }

    pub fn build(mut self) -> String {
        if !self.current_line.is_empty() {
            self.lines.push(self.current_line);
        }
        self.lines.join("\n")
    }
}
