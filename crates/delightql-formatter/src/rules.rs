// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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

impl CteStyle {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "subordinate" => Some(CteStyle::Subordinate),
            "centric" => Some(CteStyle::Centric),
            "columnar" => Some(CteStyle::Columnar),
            "traditional" => Some(CteStyle::Traditional),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            CteStyle::Subordinate => "subordinate",
            CteStyle::Centric => "centric",
            CteStyle::Columnar => "columnar",
            CteStyle::Traditional => "traditional",
        }
    }
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
    /// Extra indentation for case arms past the base indent
    pub case_arm_indent: usize,
    /// Pipe break trigger: "always" breaks before every |>;
    /// "fit" keeps the pipe inline while the line fits pipe_break_width
    pub pipe_break: BreakMode,
    /// Line width governing pipe_break=fit
    pub pipe_break_width: usize,
    /// Comma-clause break: "fit" joins clauses that fit
    /// continuation_length; "each" puts every clause on its own line
    pub comma_clause_break: ClauseBreak,
    /// Comma spelling in call/functor arguments: "tight" (a,b),
    /// "oxford" (a, b), "loose" (a , b)
    pub comma_join_args: CommaJoin,
    /// Interior padding of tree-group braces: "none" ({a}) or
    /// "space" ({ a })
    pub brace_padding: Padding,
    /// Tree-group member landing: "offset" indents members by
    /// curly_member_indent; "align" lands them at the column after
    /// the opening brace plus member_landing_pad
    pub member_landing: Landing,
    /// Pad past the brace column under member_landing=align
    pub member_landing_pad: usize,
    /// Closing brace/bracket of a tree group: "own_line" or
    /// "trailing" (rides the last member's line)
    pub closer_placement: CloserPlacement,
    /// Tree inducer (~>) break: "always" or "fit" (whole group
    /// inline while it fits pipe_break_width)
    pub tree_inducer_break: BreakMode,
    /// A member's value after "key": — "always" breaks, "fit" stays
    /// inline while it fits pipe_break_width
    pub member_value_break: BreakMode,
    /// Annotation ((~~…~~)) placement: "inline" after the expression
    /// or "own_line" indented below it
    pub annotation_placement: Placement,
    /// `under ctx:` directive: "own_line" or "inline" with its query
    pub under_context_placement: Placement,
    /// Blank lines between queries: "collapse" or "preserve"
    pub blank_lines: BlankLines,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BreakMode {
    Always,
    Fit,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClauseBreak {
    /// Join clauses that fit continuation_length (greedy, per clause)
    Fit,
    /// Every clause on its own line, unconditionally
    Each,
    /// Group decision: the whole chain inline if it fits
    /// pipe_break_width, otherwise EVERY clause on its own line
    Cascade,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CommaJoin {
    Tight,
    Oxford,
    Loose,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Padding {
    None,
    Space,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Landing {
    Offset,
    Align,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CloserPlacement {
    OwnLine,
    Trailing,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Placement {
    Inline,
    OwnLine,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BlankLines {
    Collapse,
    Preserve,
}

// The frozen defaults ARE the style (ruling 2). These values are
// FITTED from the book by ./new_test_suite/book_format_check.py --fit
// — the book is the objective function, not opinion. Refit after
// behavior changes; the sys::format.bundle 'book' row must match
// (welded by delightql-cli).
impl Default for FormatConfig {
    fn default() -> Self {
        Self {
            projection_length: 72,
            continuation_length: 40,
            pipe_indent: 2,
            continuation_indent: 2,
            map_cover_extra_indent: 4,
            aggregation_arrow_indent: 2,
            cte_indent: 3,
            cte_style: CteStyle::Subordinate,
            cte_columnar_padding: 7,
            curly_member_indent: 5,
            curly_inducer_indent: 3,
            curly_opening_brace_inline: false, // Opening brace on new line by default
            case_arm_indent: 3,
            pipe_break: BreakMode::Fit,
            pipe_break_width: 80,
            comma_clause_break: ClauseBreak::Cascade,
            comma_join_args: CommaJoin::Oxford,
            brace_padding: Padding::None,
            member_landing: Landing::Offset,
            member_landing_pad: 2,
            closer_placement: CloserPlacement::OwnLine,
            tree_inducer_break: BreakMode::Always,
            member_value_break: BreakMode::Always,
            annotation_placement: Placement::Inline,
            under_context_placement: Placement::Inline,
            blank_lines: BlankLines::Preserve,
        }
    }
}

/// One formatter knob: its external name, and how to read/write it as
/// a string. This registry is the single source of truth for every
/// layout opinion the formatter takes — `.dql-format` parsing, the
/// `sys::format.bundle` system table, and the welds all key off it.
/// Every knob governs WHITESPACE only; no knob can change a token.
pub struct Knob {
    pub name: &'static str,
    /// SQLite column type for the system-table projection.
    pub data_type: &'static str,
    /// Parse `value` and apply; false means the value did not parse.
    pub set: fn(&mut FormatConfig, &str) -> bool,
    /// Current value, in the same spelling `set` accepts.
    pub get: fn(&FormatConfig) -> String,
}

/// Upper bound for every numeric knob. Values are indents and line
/// widths; anything beyond this is nonsense, and unbounded values
/// reach `" ".repeat(n)` allocations — a config file must refuse,
/// never panic.
pub const NUMERIC_KNOB_MAX: usize = 500;

macro_rules! usize_knob {
    ($name:literal, $field:ident) => {
        Knob {
            name: $name,
            data_type: "INTEGER",
            set: |c, v| match v.parse::<usize>() {
                Ok(n) if n <= NUMERIC_KNOB_MAX => {
                    c.$field = n;
                    true
                }
                _ => false,
            },
            get: |c| c.$field.to_string(),
        }
    };
}

macro_rules! enum_knob {
    ($name:literal, $field:ident, $( $spelling:literal => $variant:path ),+ $(,)?) => {
        Knob {
            name: $name,
            data_type: "TEXT",
            set: |c, v| match v {
                $( $spelling => { c.$field = $variant; true } )+
                _ => false,
            },
            // Exhaustive match, deliberately no catch-all: adding an
            // enum variant without a spelling here is a COMPILE error,
            // not a runtime surprise.
            get: |c| match c.$field {
                $( $variant => $spelling.to_string(), )+
            },
        }
    };
}

pub const KNOBS: &[Knob] = &[
    usize_knob!("projection_length", projection_length),
    usize_knob!("continuation_length", continuation_length),
    usize_knob!("pipe_indent", pipe_indent),
    usize_knob!("continuation_indent", continuation_indent),
    usize_knob!("map_cover_extra_indent", map_cover_extra_indent),
    usize_knob!("aggregation_arrow_indent", aggregation_arrow_indent),
    usize_knob!("cte_indent", cte_indent),
    usize_knob!("cte_columnar_padding", cte_columnar_padding),
    usize_knob!("curly_member_indent", curly_member_indent),
    usize_knob!("curly_inducer_indent", curly_inducer_indent),
    usize_knob!("case_arm_indent", case_arm_indent),
    usize_knob!("pipe_break_width", pipe_break_width),
    usize_knob!("member_landing_pad", member_landing_pad),
    enum_knob!("pipe_break", pipe_break,
        "always" => BreakMode::Always, "fit" => BreakMode::Fit),
    enum_knob!("comma_clause_break", comma_clause_break,
        "fit" => ClauseBreak::Fit, "each" => ClauseBreak::Each,
        "cascade" => ClauseBreak::Cascade),
    enum_knob!("comma_join_args", comma_join_args,
        "tight" => CommaJoin::Tight, "oxford" => CommaJoin::Oxford,
        "loose" => CommaJoin::Loose),
    enum_knob!("brace_padding", brace_padding,
        "none" => Padding::None, "space" => Padding::Space),
    enum_knob!("member_landing", member_landing,
        "offset" => Landing::Offset, "align" => Landing::Align),
    enum_knob!("closer_placement", closer_placement,
        "own_line" => CloserPlacement::OwnLine, "trailing" => CloserPlacement::Trailing),
    enum_knob!("tree_inducer_break", tree_inducer_break,
        "always" => BreakMode::Always, "fit" => BreakMode::Fit),
    enum_knob!("member_value_break", member_value_break,
        "always" => BreakMode::Always, "fit" => BreakMode::Fit),
    enum_knob!("annotation_placement", annotation_placement,
        "inline" => Placement::Inline, "own_line" => Placement::OwnLine),
    enum_knob!("under_context_placement", under_context_placement,
        "own_line" => Placement::OwnLine, "inline" => Placement::Inline),
    enum_knob!("blank_lines", blank_lines,
        "collapse" => BlankLines::Collapse, "preserve" => BlankLines::Preserve),
    Knob {
        name: "cte_style",
        data_type: "TEXT",
        set: |c, v| match CteStyle::parse(v) {
            Some(s) => {
                c.cte_style = s;
                true
            }
            None => false,
        },
        get: |c| c.cte_style.name().to_string(),
    },
    Knob {
        name: "curly_opening_brace_inline",
        data_type: "INTEGER",
        set: |c, v| match v {
            "true" | "1" | "yes" => {
                c.curly_opening_brace_inline = true;
                true
            }
            "false" | "0" | "no" => {
                c.curly_opening_brace_inline = false;
                true
            }
            _ => false,
        },
        get: |c| if c.curly_opening_brace_inline { "1" } else { "0" }.to_string(),
    },
];

impl FormatConfig {
    /// Apply one (name, value) pair through the knob registry.
    /// `Err` carries a description of what was wrong.
    pub fn apply(&mut self, name: &str, value: &str) -> std::result::Result<(), String> {
        match KNOBS.iter().find(|k| k.name == name) {
            None => Err(format!("unknown knob '{name}'")),
            Some(k) => {
                if (k.set)(self, value) {
                    Ok(())
                } else {
                    Err(format!("invalid value '{value}' for knob '{name}'"))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_knobs_refuse_absurd_values() {
        let mut c = FormatConfig::default();
        // A value that parses as usize but would drive " ".repeat(n)
        // allocations must be an invalid-value error, never a panic.
        assert!(c.apply("pipe_indent", "18446744073709551615").is_err());
        assert!(c.apply("pipe_break_width", &(NUMERIC_KNOB_MAX + 1).to_string()).is_err());
        assert!(c.apply("pipe_indent", &NUMERIC_KNOB_MAX.to_string()).is_ok());
        assert!(c.apply("pipe_indent", "not-a-number").is_err());
    }

    #[test]
    fn unknown_and_invalid_knobs_are_errors() {
        let mut c = FormatConfig::default();
        assert!(c.apply("definitely_not_a_knob", "1").is_err());
        assert!(c.apply("pipe_break", "sometimes").is_err());
        // The legacy cte_centric spelling is gone; it now errs like
        // any unknown knob instead of silently accepting any value.
        assert!(c.apply("cte_centric", "maybe").is_err());
    }

    #[test]
    fn every_knob_round_trips_its_default() {
        let c = FormatConfig::default();
        let mut d = FormatConfig::default();
        for k in KNOBS {
            let v = (k.get)(&c);
            assert!(
                (k.set)(&mut d, &v),
                "knob {} refuses its own default '{v}'",
                k.name
            );
            assert_eq!((k.get)(&d), v, "knob {} does not round-trip", k.name);
        }
    }
}
