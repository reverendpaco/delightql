use anyhow::Result;
use tree_sitter::Node;

use super::core::Formatter;

impl<'a> Formatter<'a> {
    /// Format a domain expression
    pub(super) fn format_domain_expression(&mut self, node: &Node) -> Result<()> {
        // First format the main expression content
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                // Handle binary expressions (arithmetic)
                "binary_expression" => {
                    self.format_binary_expression(&child)?;
                }
                // Handle piped expressions
                "piped_expression" => {
                    self.format_piped_expression(&child)?;
                }
                // Handle non-binary domain expressions
                "non_binary_domain_expression" => {
                    self.format_domain_expression(&child)?;
                }
                // Handle case expressions
                "case_expression" => {
                    self.format_case_expression(&child)?;
                }
                // Handle lvars (local variables/identifiers)
                "lvar" => {
                    // lvar can contain identifier or qualified_column
                    for lvar_child in child.children(&mut child.walk()) {
                        match lvar_child.kind() {
                            "identifier" => {
                                let text = self.node_text(&lvar_child).to_string();
                                self.output.write(&text);
                            }
                            "qualified_column" => {
                                // Qualified columns like users.id
                                let text = self.node_text(&lvar_child).to_string();
                                self.output.write(&text);
                            }
                            _ => self.flag_unhandled(&lvar_child),
                        }
                    }
                }
                // Handle literals
                "literal" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                // Handle function calls
                "function_call" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                // Handle bracket functions (INTERIOR-TUPLE): [expr1, expr2, ...]
                "bracket_function" => {
                    self.format_bracket_function(&child)?;
                }
                // Handle curly functions (INTERIOR-RECORD): {field1, "key": value, ...}
                "curly_function" => {
                    self.format_curly_function(&child)?;
                }
                // Handle metadata-oriented tree groups: column:~> {...}
                "metadata_tree_group" => {
                    self.format_metadata_tree_group(&child)?;
                }
                // Handle group inducer (wraps tree group functions with ~>)
                "group_inducer" => {
                    self.format_group_inducer(&child)?;
                }
                // Handle parenthesized expressions (including predicates)
                "parenthesized_expression" => {
                    self.output.write("(");
                    for paren_child in child.children(&mut child.walk()) {
                        if paren_child.kind() == "domain_expression"
                            || paren_child.kind() == "predicate"
                        {
                            // Format the inner expression or predicate
                            let text = self.node_text(&paren_child).to_string();
                            self.output.write(&text);
                        }
                    }
                    self.output.write(")");
                }
                // Handle predicates directly (boolean expressions)
                "predicate" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                // Handle column ordinals (e.g., |1|, |1..3|)
                "column_ordinal" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                // Handle column ranges
                "column_range" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                // Handle qualified globs (e.g., u.*)
                "glob" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                // Handle glob specs
                "glob_spec" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                // Handle pattern literals (e.g., /_name/)
                "pattern_literal" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                // Handle scalar subqueries
                "scalar_subquery" => {
                    self.format_scalar_subquery(&child)?;
                }
                // Handle tuple expressions (parenthesized expressions in projections)
                "tuple_expression" => {
                    self.output.write("(");
                    // Format all domain expressions inside the tuple
                    for tuple_child in child.children(&mut child.walk()) {
                        if tuple_child.kind() == "domain_expression" {
                            self.format_domain_expression(&tuple_child)?;
                        }
                    }
                    self.output.write(")");
                }
                _ => self.flag_unhandled(&child),
            }
        }

        // Then check for alias field
        if let Some(alias) = node.child_by_field_name("alias") {
            self.output.write(" as ");
            let text = self.node_text(&alias).to_string();
            self.output.write(&text);
        }

        Ok(())
    }

    /// Format a binary expression (e.g., arithmetic operations)
    pub(super) fn format_binary_expression(&mut self, node: &Node) -> Result<()> {
        // Get left operand
        if let Some(left) = node.child_by_field_name("left") {
            self.format_domain_expression(&left)?;
        }

        // Get operator
        if let Some(op) = node.child_by_field_name("operator") {
            // Add spaces around operator
            self.output.write(" ");
            // Extract operator text directly from CST to support any operator syntax
            for op_child in op.children(&mut op.walk()) {
                let text = self.node_text(&op_child).to_string();
                self.output.write(&text);
            }
            self.output.write(" ");
        }

        // Get right operand
        if let Some(right) = node.child_by_field_name("right") {
            self.format_domain_expression(&right)?;
        }

        Ok(())
    }

    /// Format a piped expression (value /-> transform)
    pub(super) fn format_piped_expression(&mut self, node: &Node) -> Result<()> {
        // Format the value part (left-hand side of /->)
        if let Some(value) = node.child_by_field_name("value") {
            match value.kind() {
                "lvar" => {
                    // Handle lvar specially - can contain identifier or qualified_column
                    for child in value.children(&mut value.walk()) {
                        match child.kind() {
                            "identifier" | "qualified_column" => {
                                let text = self.node_text(&child).to_string();
                                self.output.write(&text);
                            }
                            _ => self.flag_unhandled(&child),
                        }
                    }
                }
                "binary_expression" => {
                    self.format_binary_expression(&value)?;
                }
                "case_expression" => {
                    self.format_case_expression(&value)?;
                }
                _ => {
                    // For parenthesized_expression, literal, function_call, etc.
                    // Output as-is — format_domain_expression doesn't work when
                    // called ON a leaf node (it iterates children).
                    let text = self.node_text(&value).to_string();
                    self.output.write(&text);
                }
            }
        }

        // Format all pipe operators and transforms (for chained pipes)
        let mut cursor = node.walk();
        let transforms: Vec<Node> = node
            .children_by_field_name("transform", &mut cursor)
            .collect();

        // Count pipe operators
        let mut pipe_operators = Vec::new();
        for child in node.children(&mut node.walk()) {
            if child.kind() == "functional_pipe_operator" {
                pipe_operators.push(child);
            }
        }

        // Format each pipe and transform pair
        for (i, transform) in transforms.iter().enumerate() {
            // Write the pipe operator before each transform, reading text from CST
            if i < pipe_operators.len() {
                self.output.write(" ");
                self.output
                    .write(&self.node_text(&pipe_operators[i]).to_string());
                self.output.write(" ");
            }

            // Format the transform
            match transform.kind() {
                "case_expression" => {
                    self.format_case_expression(transform)?;
                }
                "domain_expression" => {
                    self.format_domain_expression(transform)?;
                }
                "function_call" => {
                    let text = self.node_text(transform).to_string();
                    self.output.write(&text);
                }
                _ => {
                    let text = self.node_text(transform).to_string();
                    self.output.write(&text);
                }
            }
        }

        Ok(())
    }

    /// Format a list of expressions with proper line breaks
    #[allow(dead_code)]
    pub(super) fn format_expression_list(&mut self, node: &Node, indent: usize) -> Result<()> {
        let mut first = true;
        let mut cursor = node.walk();

        for child in node.children(&mut cursor) {
            if child.kind() == "domain_expression" {
                if !first {
                    self.output.write(",");
                    self.output.newline_with_indent(indent);
                }

                // Check if this domain expression contains a CASE expression
                if let Some(_case_node) = self.find_child_recursive(&child, "case_expression") {
                    self.format_domain_expression_with_case(&child)?;
                } else {
                    let child_text = self.node_text(&child).to_string();
                    self.output.write(&child_text);
                }
                first = false;
            }
        }
        Ok(())
    }

    /// Format domain expression that contains a CASE expression
    #[allow(dead_code)]
    pub(super) fn format_domain_expression_with_case(&mut self, node: &Node) -> Result<()> {
        let mut found_case = false;
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "case_expression" => {
                    self.format_case_expression(&child)?;
                    found_case = true;
                }
                "function_call" => {
                    // Check if this function contains a CASE expression
                    if self
                        .find_child_recursive(&child, "case_expression")
                        .is_some()
                    {
                        self.format_function_call_with_case(&child)?;
                    } else {
                        let text = self.node_text(&child).to_string();
                        self.output.write(&text);
                    }
                }
                "_as" => self.output.write(" as "),
                "identifier" if found_case => {
                    // This is likely an alias after the CASE
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                _ => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
            }
        }
        Ok(())
    }

    /// Format function call that contains a CASE expression
    #[allow(dead_code)]
    pub(super) fn format_function_call_with_case(&mut self, node: &Node) -> Result<()> {
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "identifier" => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
                ":" => self.output.write(":"),
                "(" => self.output.write("("),
                ")" => self.output.write(")"),
                "case_expression" => self.format_case_expression(&child)?,
                "domain_expression" => {
                    if self
                        .find_child_recursive(&child, "case_expression")
                        .is_some()
                    {
                        self.format_domain_expression_with_case(&child)?;
                    } else {
                        let text = self.node_text(&child).to_string();
                        self.output.write(&text);
                    }
                }
                _ => {
                    let text = self.node_text(&child).to_string();
                    self.output.write(&text);
                }
            }
        }
        Ok(())
    }

    /// Format scalar subquery: table:(, condition ~> aggregation)
    pub(super) fn format_scalar_subquery(&mut self, node: &Node) -> Result<()> {
        // Write schema.table if present
        if let Some(schema) = node.child_by_field_name("schema") {
            let schema_text = self.node_text(&schema).to_string();
            self.output.write(&schema_text);
            self.output.write(".");
        }

        // Write table name
        if let Some(table) = node.child_by_field_name("table") {
            let table_text = self.node_text(&table).to_string();
            self.output.write(&table_text);
        }

        // Write opening :(
        self.output.write(":(");

        // Get the relational_continuation - it's a direct child
        for child in node.children(&mut node.walk()) {
            if child.kind() == "relational_continuation" {
                // Get current line length to calculate indent
                let current_indent = self.output.current_line_length();

                // Add 2 spaces for continuation indent
                let continuation_indent = current_indent + 2;

                // Format the continuation: comma/pipe stays on current line, content drops down
                self.format_scalar_subquery_continuation(&child, continuation_indent)?;
                break;
            }
        }

        // Write closing )
        self.output.write(")");

        Ok(())
    }

    /// Format relational continuation within a scalar subquery (special handling)
    /// For now, just output the text as-is on one line (simpler approach)
    fn format_scalar_subquery_continuation(&mut self, node: &Node, _indent: usize) -> Result<()> {
        let text = self.node_text(node).to_string();
        self.output.write(&text);
        Ok(())
    }

    /// Format bracket function (INTERIOR-TUPLE): [expr1, expr2, ...]
    pub(super) fn format_bracket_function(&mut self, node: &Node) -> Result<()> {
        self.output.write("[");

        // Find domain_expression_list child
        for child in node.children(&mut node.walk()) {
            if child.kind() == "domain_expression_list" {
                self.format_domain_expression_list_inline(&child)?;
                break;
            }
        }

        self.output.write("]");
        Ok(())
    }

    /// Format curly function (INTERIOR-RECORD): {field1, "key": value, ...}
    pub(super) fn format_curly_function(&mut self, node: &Node) -> Result<()> {
        // Check if this should be formatted multi-line (contains group inducers)
        let should_multiline = self.curly_has_group_inducers(node);

        if should_multiline {
            // Multi-line format with nested reductions
            self.format_curly_function_multiline(node)
        } else {
            // Inline format
            self.output.write("{");

            for child in node.children(&mut node.walk()) {
                if child.kind() == "curly_function_members" {
                    self.format_curly_function_members_inline(&child)?;
                    break;
                }
            }

            self.output.write("}");
            Ok(())
        }
    }

    /// Check if a curly function contains any group inducers (nested ~>)
    fn curly_has_group_inducers(&self, node: &Node) -> bool {
        // Recursively check if any member has a group_inducer value
        for child in node.children(&mut node.walk()) {
            if child.kind() == "curly_function_members" {
                for member in child.children(&mut child.walk()) {
                    if member.kind() == "curly_function_member" {
                        // Check if this member has a value field that's a group_inducer
                        if let Some(value) = member.child_by_field_name("value") {
                            if value.kind() == "group_inducer" {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        false
    }

    /// Format curly function in multi-line mode
    fn format_curly_function_multiline(&mut self, node: &Node) -> Result<()> {
        self.output.write("{");

        // Get current indent for the opening brace
        let base_indent = self.output.current_line_length();

        for child in node.children(&mut node.walk()) {
            if child.kind() == "curly_function_members" {
                self.format_curly_function_members_multiline(&child, base_indent)?;
                break;
            }
        }

        // Closing brace at base indent
        self.output.newline_with_indent(base_indent);
        self.output.write("}");
        Ok(())
    }

    /// Format members of a curly function (inline mode)
    fn format_curly_function_members_inline(&mut self, node: &Node) -> Result<()> {
        let mut first = true;

        for child in node.children(&mut node.walk()) {
            if child.kind() == "curly_function_member" {
                if !first {
                    self.output.write(", ");
                }
                self.format_curly_function_member_inline(&child)?;
                first = false;
            }
        }

        Ok(())
    }

    /// Format members of a curly function (multi-line mode)
    fn format_curly_function_members_multiline(
        &mut self,
        node: &Node,
        base_indent: usize,
    ) -> Result<()> {
        let member_indent = base_indent + self.config.curly_member_indent;
        let mut first = true;

        for child in node.children(&mut node.walk()) {
            if child.kind() == "curly_function_member" {
                if first {
                    // First member: check if opening brace should be inline
                    if self.config.curly_opening_brace_inline {
                        self.output.write(" ");
                    } else {
                        self.output.newline_with_indent(member_indent);
                    }
                } else {
                    // Subsequent members: comma + newline
                    self.output.write(",");
                    self.output.newline_with_indent(member_indent);
                }
                self.format_curly_function_member_multiline(&child, member_indent)?;
                first = false;
            }
        }

        Ok(())
    }

    /// Format a single member of a curly function (inline mode)
    fn format_curly_function_member_inline(&mut self, node: &Node) -> Result<()> {
        // Check if this has key and value fields (structured key-value pair)
        if let Some(key) = node.child_by_field_name("key") {
            let key_text = self.node_text(&key).to_string();
            self.output.write(&key_text);
            self.output.write(": ");

            // Check if value is a group_inducer
            if let Some(value) = node.child_by_field_name("value") {
                if value.kind() == "group_inducer" {
                    self.output.write("~> ");
                    self.format_group_inducer(&value)?;
                } else {
                    self.format_domain_expression(&value)?;
                }
            }

            return Ok(());
        }

        // Check for comparison (shorthand predicate)
        let children: Vec<_> = node.children(&mut node.walk()).collect();
        for child in &children {
            if child.kind() == "comparison" {
                let text = self.node_text(child).to_string();
                self.output.write(&text);
                return Ok(());
            }
        }

        // Default: simple lvar shorthand
        for child in &children {
            if child.kind() == "lvar" {
                let text = self.node_text(child).to_string();
                self.output.write(&text);
                return Ok(());
            }
        }

        // Fallback
        let text = self.node_text(node).to_string();
        self.output.write(&text);
        Ok(())
    }

    /// Format a single member of a curly function (multi-line mode)
    fn format_curly_function_member_multiline(
        &mut self,
        node: &Node,
        base_indent: usize,
    ) -> Result<()> {
        // Check if this has key and value fields (structured key-value pair)
        if let Some(key) = node.child_by_field_name("key") {
            let key_text = self.node_text(&key).to_string();
            self.output.write(&key_text);
            self.output.write(":");

            // Check if value is a group_inducer
            if let Some(value) = node.child_by_field_name("value") {
                if value.kind() == "group_inducer" {
                    // For group inducers, put ~> on next line with extra indent
                    let inducer_indent = base_indent + self.config.curly_inducer_indent;
                    self.output.newline_with_indent(inducer_indent);
                    self.output.write("~> ");
                    self.format_group_inducer(&value)?;
                } else {
                    // Regular value, just add space
                    self.output.write(" ");
                    self.format_domain_expression(&value)?;
                }
            }

            return Ok(());
        }

        // Check for comparison (shorthand predicate)
        let children: Vec<_> = node.children(&mut node.walk()).collect();
        for child in &children {
            if child.kind() == "comparison" {
                let text = self.node_text(child).to_string();
                self.output.write(&text);
                return Ok(());
            }
        }

        // Default: simple lvar shorthand
        for child in &children {
            if child.kind() == "lvar" {
                let text = self.node_text(child).to_string();
                self.output.write(&text);
                return Ok(());
            }
        }

        // Fallback
        let text = self.node_text(node).to_string();
        self.output.write(&text);
        Ok(())
    }

    /// Format domain expression list inline (comma-separated)
    fn format_domain_expression_list_inline(&mut self, node: &Node) -> Result<()> {
        let mut first = true;

        for child in node.children(&mut node.walk()) {
            if child.kind() == "domain_expression" {
                if !first {
                    self.output.write(", ");
                }
                self.format_domain_expression(&child)?;
                first = false;
            }
        }

        Ok(())
    }

    /// Format metadata-oriented tree group: column:~> {...}
    pub(super) fn format_metadata_tree_group(&mut self, node: &Node) -> Result<()> {
        // Get the key (lvar)
        if let Some(key) = node.child_by_field_name("key") {
            let key_text = self.node_text(&key).to_string();
            self.output.write(&key_text);
        }

        // Write :~> with no space before it
        self.output.write(":~> ");

        // Format the curly or bracket function that follows
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "curly_function" => {
                    self.format_curly_function(&child)?;
                    break;
                }
                "bracket_function" => {
                    self.format_bracket_function(&child)?;
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Format group inducer: wraps curly or bracket functions for nested reductions
    pub(super) fn format_group_inducer(&mut self, node: &Node) -> Result<()> {
        // A group_inducer is just a wrapper around curly_function or bracket_function
        // Format the child directly
        for child in node.children(&mut node.walk()) {
            match child.kind() {
                "curly_function" => {
                    self.format_curly_function(&child)?;
                    break;
                }
                "bracket_function" => {
                    self.format_bracket_function(&child)?;
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }
}
