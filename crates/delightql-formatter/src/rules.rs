/// Formatting rules and configuration

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CteStyle {
    /// CTE name is subordinate to query (default)
    Subordinate,
    /// Query is indented, CTE name at margin
    Centric,
    /// CTE query slightly indented, name right-aligned
    Columnar,
    /// Traditional definition style: name first, then indented query
    Traditional,
}

#[derive(Clone)]
pub struct FormatConfig {
    /// Maximum length before breaking projection-style operators
    pub projection_length: usize,
    /// Maximum length before breaking comma continuations
    pub continuation_length: usize,
    /// Indentation for pipe operators
    pub pipe_indent: usize,
    /// Indentation for comma continuations
    pub continuation_indent: usize,
    /// Extra indentation for map-cover parameters
    pub map_cover_extra_indent: usize,
    /// Indentation for aggregation arrow operator
    pub aggregation_arrow_indent: usize,
    /// Indentation for CTE names (used in subordinate style)
    pub cte_indent: usize,
    /// CTE formatting style
    pub cte_style: CteStyle,
    /// Padding added to max(projection_length, continuation_length) for CTE name alignment in columnar mode
    pub cte_columnar_padding: usize,
    /// Indentation for curly function members (tree groups)
    pub curly_member_indent: usize,
    /// Extra indentation for group inducer ~> operators
    pub curly_inducer_indent: usize,
    /// Put opening brace on same line as ~> (false = new line after {)
    pub curly_opening_brace_inline: bool,
}

impl Default for FormatConfig {
    fn default() -> Self {
        Self {
            projection_length: 40,
            continuation_length: 40,
            pipe_indent: 3,
            continuation_indent: 5,
            map_cover_extra_indent: 4,
            aggregation_arrow_indent: 2,
            cte_indent: 3, // Default to same as pipe_indent
            cte_style: CteStyle::Subordinate,
            cte_columnar_padding: 7,
            curly_member_indent: 5,
            curly_inducer_indent: 3,
            curly_opening_brace_inline: false, // Opening brace on new line by default
        }
    }
}
