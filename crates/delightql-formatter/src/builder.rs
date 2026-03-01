/// String building utilities for the formatter

pub struct OutputBuilder {
    lines: Vec<String>,
    current_line: String,
    #[allow(dead_code)]
    indent_level: usize,
    #[allow(dead_code)]
    line_count_at_mark: Option<usize>,
}

impl OutputBuilder {
    pub fn new() -> Self {
        Self {
            lines: Vec::new(),
            current_line: String::new(),
            indent_level: 0,
            line_count_at_mark: None,
        }
    }

    /// Add text to the current line
    pub fn write(&mut self, text: &str) {
        self.current_line.push_str(text);
    }

    /// Start a new line with the current indentation
    pub fn newline(&mut self) {
        if !self.current_line.is_empty() {
            self.lines.push(self.current_line.clone());
            self.current_line.clear();
        }
    }

    /// Start a new line with specific indentation
    pub fn newline_with_indent(&mut self, spaces: usize) {
        self.newline();
        self.current_line = " ".repeat(spaces);
    }

    /// Add a blank line
    #[allow(dead_code)]
    pub fn blank_line(&mut self) {
        self.newline();
        self.lines.push(String::new());
    }

    /// Get the length of the current line
    pub fn current_line_length(&self) -> usize {
        self.current_line.len()
    }

    /// Mark the current position to check if newlines were added
    #[allow(dead_code)]
    pub fn mark_position(&mut self) {
        self.line_count_at_mark = Some(self.lines.len());
    }

    /// Check if newlines were added since the mark
    #[allow(dead_code)]
    pub fn has_newlines_since_mark(&self) -> bool {
        if let Some(mark) = self.line_count_at_mark {
            self.lines.len() > mark
        } else {
            false
        }
    }

    /// Build the final output string
    pub fn build(mut self) -> String {
        if !self.current_line.is_empty() {
            self.lines.push(self.current_line);
        }
        self.lines.join("\n")
    }
}
