//! Grammar-based fuzzer for DelightQL
//!
//! Generates random syntactically valid DelightQL queries by walking the grammar.json
//! produced by tree-sitter. Every generated query is guaranteed to parse successfully.

#![allow(dead_code)]
#![allow(unreachable_patterns)]

use delightql_core::api::internals::{builder_v2, parser};
use delightql_core::error::{DelightQLError, KnownLimitationType};
use rand::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Maximum recursion depth to prevent stack overflow
/// Note: 1000 might be excessive - most real queries have depth < 20
/// But this ensures we can generate deeply nested expressions for stress testing
const MAX_DEPTH: usize = 1000;

/// Maximum repetitions for REPEAT nodes (0..MAX_REPEAT)
const MAX_REPEAT: usize = 5;

/// Command line arguments
#[derive(Debug)]
struct Args {
    /// Number of queries to generate (default 1)
    count: usize,
    /// Random seed for reproducibility (optional)
    seed: Option<u64>,
    /// Test each generated query through the builder
    test: bool,
    /// Maximum recursion depth (default MAX_DEPTH)
    max_depth: usize,
    /// Verbose output (show AST)
    verbose: bool,
    /// Only output failures when testing
    failures_only: bool,
    /// Generate only the Nth query instead of all queries from 1 to N
    exact: bool,
}

/// Tree-sitter grammar.json structure
#[derive(Debug, Deserialize)]
struct Grammar {
    name: String,
    rules: HashMap<String, Rule>,
    #[serde(default)]
    extras: Vec<Rule>,
}

/// Grammar rule types
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum Rule {
    #[serde(rename = "SYMBOL")]
    Symbol { name: String },

    #[serde(rename = "STRING")]
    String { value: String },

    #[serde(rename = "PATTERN")]
    Pattern { value: String },

    #[serde(rename = "BLANK")]
    Blank,

    #[serde(rename = "CHOICE")]
    Choice { members: Vec<Rule> },

    #[serde(rename = "SEQ")]
    Seq { members: Vec<Rule> },

    #[serde(rename = "REPEAT")]
    Repeat { content: Box<Rule> },

    #[serde(rename = "REPEAT1")]
    Repeat1 { content: Box<Rule> },

    #[serde(rename = "PREC")]
    Prec { value: i32, content: Box<Rule> },

    #[serde(rename = "PREC_LEFT")]
    PrecLeft { value: i32, content: Box<Rule> },

    #[serde(rename = "PREC_RIGHT")]
    PrecRight { value: i32, content: Box<Rule> },

    #[serde(rename = "PREC_DYNAMIC")]
    PrecDynamic { value: i32, content: Box<Rule> },

    #[serde(rename = "TOKEN")]
    Token { content: Box<Rule> },

    #[serde(rename = "IMMEDIATE_TOKEN")]
    ImmediateToken { content: Box<Rule> },

    #[serde(rename = "ALIAS")]
    Alias {
        content: Box<Rule>,
        named: bool,
        value: String,
    },

    #[serde(rename = "FIELD")]
    Field { name: String, content: Box<Rule> },
}

/// Generator state
struct Generator<'a, R: Rng> {
    grammar: &'a Grammar,
    rng: &'a mut R,
    depth: usize,
    max_depth: usize,
}

impl<'a, R: Rng> Generator<'a, R> {
    fn new(grammar: &'a Grammar, rng: &'a mut R, max_depth: usize) -> Self {
        Self {
            grammar,
            rng,
            depth: 0,
            max_depth,
        }
    }

    /// Check if we're at a CPR boundary (where we can safely stop)
    /// Returns true if this is an optional relational_continuation
    fn is_cpr_boundary(&self, rule: &Rule) -> bool {
        // Check if this is a CHOICE between a relational_continuation and BLANK
        if let Rule::Choice { members } = rule {
            if members.len() == 2 {
                // Check if one option is relational_continuation and the other is BLANK
                let has_continuation = members.iter().any(
                    |m| matches!(m, Rule::Symbol { name } if name == "relational_continuation"),
                );
                let has_blank = members.iter().any(|m| matches!(m, Rule::Blank));
                return has_continuation && has_blank;
            }
        }
        false
    }

    /// Calculate probability of stopping at a CPR boundary based on depth
    /// Returns value from 0.0 to 1.0
    fn stop_probability(&self) -> f64 {
        // Linear ramp from 0% at depth 0 to 100% at 80% of max_depth
        // This ensures we always stop before hitting max_depth
        let threshold = (self.max_depth as f64 * 0.8) as usize;
        if self.depth >= threshold {
            1.0 // Always stop at or beyond threshold
        } else {
            self.depth as f64 / threshold as f64
        }
    }

    /// Generate a string from a rule
    fn generate(&mut self, rule: &Rule) -> String {
        // Depth limit check - if too deep, prefer simple choices
        if self.depth >= self.max_depth {
            return self.generate_minimal(rule);
        }

        self.depth += 1;
        let result = match rule {
            Rule::Symbol { name } => {
                // Look up the named rule and generate from it
                if let Some(named_rule) = self.grammar.rules.get(name) {
                    self.generate(named_rule)
                } else {
                    // Unknown rule - this is a fuzzer bug!
                    panic!("Fuzzer bug: Unknown grammar rule '{}' - this rule is not in grammar.rules!", name);
                }
            }

            Rule::String { value } => value.clone(),

            Rule::Pattern { value } => {
                // Generate a string matching the pattern
                // For now, use simple heuristics
                self.generate_from_pattern(value)
            }

            Rule::Blank => String::new(),

            Rule::Choice { members } => {
                // Check if this is a CPR boundary (optional relational_continuation)
                if self.is_cpr_boundary(rule) {
                    // At a CPR boundary - decide whether to stop based on depth
                    let stop_prob = self.stop_probability();
                    if self.rng.gen_bool(stop_prob) {
                        // Choose to stop here (select BLANK option)
                        String::new()
                    } else {
                        // Continue with relational_continuation
                        if let Some(continuation) =
                            members.iter().find(|m| !matches!(m, Rule::Blank))
                        {
                            self.generate(continuation)
                        } else {
                            String::new()
                        }
                    }
                } else {
                    // Regular choice - randomly select any option
                    if let Some(chosen) = members.choose(self.rng) {
                        self.generate(chosen)
                    } else {
                        String::new()
                    }
                }
            }

            Rule::Seq { members } => {
                // Generate all members in sequence with proper spacing
                let mut parts = Vec::new();

                for m in members {
                    let part = self.generate(m);
                    if !part.is_empty() {
                        parts.push(part);
                    }
                }

                // Join parts with appropriate spacing
                let mut result = String::new();
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        // Add space before this part unless:
                        // - Previous part was a dot
                        // - Current part is a dot
                        // - Current part is '*' and previous was '.'
                        // - Current part is ':' (function call syntax)
                        // - Current part is '(' and previous was ':'
                        let prev = &parts[i - 1];
                        if prev != "."
                            && part != "."
                            && !(part == "*" && prev == ".")
                            && part != ":"
                            && !(part == "(" && prev == ":")
                        {
                            result.push(' ');
                        }
                    }
                    result.push_str(part);
                }

                result
            }

            Rule::Repeat { content } => {
                // Generate 0 to MAX_REPEAT repetitions
                let count = self.rng.gen_range(0..=MAX_REPEAT);
                (0..count)
                    .map(|_| self.generate(content))
                    .collect::<Vec<_>>()
                    .join("")
            }

            Rule::Repeat1 { content } => {
                // Generate 1 to MAX_REPEAT repetitions
                let count = self.rng.gen_range(1..=MAX_REPEAT);
                (0..count)
                    .map(|_| self.generate(content))
                    .collect::<Vec<_>>()
                    .join("")
            }

            // Precedence rules just pass through to their content
            Rule::Prec { content, .. }
            | Rule::PrecLeft { content, .. }
            | Rule::PrecRight { content, .. }
            | Rule::PrecDynamic { content, .. }
            | Rule::Token { content }
            | Rule::ImmediateToken { content } => self.generate(content),

            Rule::Alias { content, .. } => {
                // Generate the underlying content
                self.generate(content)
            }

            Rule::Field { content, .. } => {
                // Generate the field content
                self.generate(content)
            }
        };

        self.depth -= 1;
        result
    }

    /// Generate minimal version when at max depth
    fn generate_minimal(&mut self, rule: &Rule) -> String {
        match rule {
            Rule::Symbol { name } => {
                // For common symbols, return simple examples
                match name.as_str() {
                    // Core query structure - must be complete valid queries
                    "source_file" => "users(*)".to_string(), // Minimal valid query
                    "query" => "users(*)".to_string(),       // The actual query rule
                    "pipe_expression" => "users(*)".to_string(), // Minimal pipe expression
                    "table_scan" => "users(*)".to_string(),  // Minimal table scan
                    "base_expression" => "users(*)".to_string(), // Base of relational expression
                    "relational_continuation" => "".to_string(), // Empty - it's optional
                    "binary_operator_expression" => "".to_string(), // Empty at min depth
                    "unary_operator_expression" => "".to_string(), // Empty at min depth

                    // Identifiers and literals
                    "identifier" => "x".to_string(),
                    "number_literal" => "1".to_string(),
                    "integer_literal" => "1".to_string(), // Integer without decimal
                    "string_literal" => "\"s\"".to_string(),

                    // Hidden tokens - these should never appear in output
                    "_comma" => ",".to_string(),
                    "_colon" => ":".to_string(),
                    "_lparen" => "(".to_string(),
                    "_rparen" => ")".to_string(),
                    "_lbracket" => "[".to_string(),
                    "_rbracket" => "]".to_string(),
                    "_as" => "as".to_string(),
                    "_minus" => "-".to_string(),
                    "_pipe" => "|>".to_string(),

                    // Common expression types
                    "domain_expression" | "expression" | "simple_expression" => "1".to_string(),
                    "relational_expression" => "users(*)".to_string(), // Must be complete
                    "table_reference" => "users".to_string(),          // Just the table name
                    "literal" => "1".to_string(),
                    "lvar" | "column_ref" => "x".to_string(),
                    "predicate" | "condition" => "true".to_string(),
                    "binary_expression" => "1 + 1".to_string(),
                    "function_call" => "f:()".to_string(),
                    "glob" => "*".to_string(),
                    "projection" => "*".to_string(), // Simple projection

                    // For any other unknown symbol, try to look it up in the grammar
                    _ => {
                        // If we hit an unknown symbol at max depth, we should still generate
                        // valid syntax. Default to a simple identifier to avoid breaking syntax.
                        if name.starts_with('_') {
                            // Hidden token we don't recognize - this is a bug
                            panic!("Fuzzer bug: Unknown hidden token '{}' at max depth", name);
                        } else {
                            // Regular symbol - generate a simple identifier
                            "x".to_string()
                        }
                    }
                }
            }
            Rule::String { value } => value.clone(),
            Rule::Pattern { .. } => "a".to_string(),
            Rule::Blank => String::new(),
            Rule::Choice { members } => {
                // Pick first (usually simplest) option
                members
                    .first()
                    .map(|m| self.generate_minimal(m))
                    .unwrap_or_default()
            }
            Rule::Seq { members } => {
                // At max depth, we need to generate valid syntax, not just concatenate tokens
                // Strategy: Generate only essential parts to form valid syntax

                // Special case: If this looks like it's trying to generate multiple items,
                // just generate one to avoid syntax errors like "1 a x"
                if members.len() > 2 {
                    // Check if this might be a list of similar items (common pattern)
                    // Just generate the first item to keep it simple and valid
                    if let Some(first) = members.first() {
                        return self.generate_minimal(first);
                    }
                }

                // For smaller sequences, generate minimally but maintain structure
                // This handles things like function calls: name : ( )
                let parts: Vec<String> = members
                    .iter()
                    .filter_map(|m| {
                        let s = self.generate_minimal(m);
                        if s.is_empty() {
                            None
                        } else {
                            Some(s)
                        }
                    })
                    .collect();

                // Join without spaces for minimal - the regular generator handles spacing
                parts.join("")
            }
            Rule::Repeat { .. } => String::new(), // 0 repetitions
            Rule::Repeat1 { content } => self.generate_minimal(content), // 1 repetition
            Rule::Prec { content, .. }
            | Rule::PrecLeft { content, .. }
            | Rule::PrecRight { content, .. }
            | Rule::PrecDynamic { content, .. }
            | Rule::Token { content }
            | Rule::ImmediateToken { content }
            | Rule::Alias { content, .. }
            | Rule::Field { content, .. } => self.generate_minimal(content),
        }
    }

    /// Generate a string from a regex pattern
    fn generate_from_pattern(&mut self, pattern: &str) -> String {
        // Handle common tree-sitter patterns
        match pattern {
            // Case-insensitive keyword patterns
            "[aA][sS]" => "as".to_string(),
            // Identifier pattern - handle both JS regex and escaped versions
            "[a-zA-Z_][a-zA-Z0-9_]*" | r"[a-zA-Z_][a-zA-Z0-9_]*" => {
                // Reserved keywords to avoid
                const RESERVED: &[&str] = &["as", "and", "or", "ascending", "descending"];

                loop {
                    let length = self.rng.gen_range(1..=8);
                    let first = *b"abcdefghijklmnopqrstuvwxyz_".choose(self.rng).unwrap() as char;
                    let rest: String = (0..length - 1)
                        .map(|_| {
                            *b"abcdefghijklmnopqrstuvwxyz0123456789_"
                                .choose(self.rng)
                                .unwrap() as char
                        })
                        .collect();
                    let identifier = format!("{}{}", first, rest);

                    // If it's not a reserved word, use it
                    if !RESERVED.contains(&identifier.as_str()) {
                        break identifier;
                    }
                    // Otherwise loop and try again
                }
            }
            // Number pattern with optional decimal
            "-?[0-9]+(\\.[0-9]+)?" | r"-?[0-9]+(\.[0-9]+)?" => {
                // 30% chance of decimal for number_literal
                if self.rng.gen_bool(0.3) {
                    format!(
                        "{}.{}",
                        self.rng.gen_range(0..100),
                        self.rng.gen_range(0..100)
                    )
                } else {
                    format!("{}", self.rng.gen_range(0..100))
                }
            }
            // Integer pattern (no decimal allowed)
            "-?[0-9]+" => {
                // Always generate integer without decimal
                format!("{}", self.rng.gen_range(0..100))
            }
            // Whitespace pattern
            "\\s" | r"\s" => {
                // Generate a space (could be tab/newline but space is simpler)
                " ".to_string()
            }
            // String content patterns (inside quotes)
            "[^']*" | r"[^']*" => {
                // Content for single-quoted strings
                let length = self.rng.gen_range(0..=10);
                (0..length)
                    .map(|_| {
                        *b"abcdefghijklmnopqrstuvwxyz0123456789 "
                            .choose(self.rng)
                            .unwrap() as char
                    })
                    .collect()
            }
            "[^\"]*" | r#"[^"]*"# => {
                // Content for double-quoted strings
                let length = self.rng.gen_range(0..=10);
                (0..length)
                    .map(|_| {
                        *b"abcdefghijklmnopqrstuvwxyz0123456789 "
                            .choose(self.rng)
                            .unwrap() as char
                    })
                    .collect()
            }
            // Match anything patterns
            "." => {
                // Single character
                (*b"abcdefghijklmnopqrstuvwxyz".choose(self.rng).unwrap() as char).to_string()
            }
            // Anonymous table separator pattern (three or more dashes)
            "---+" => {
                // Generate 3-5 dashes
                let dash_count = self.rng.gen_range(3..=5);
                "-".repeat(dash_count)
            }
            // Default: if we don't recognize the pattern, PANIC with details
            _ => {
                panic!(
                    "Fuzzer bug: Unknown pattern '{}' - add support for this pattern!",
                    pattern
                );
            }
        }
    }
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut result = Args {
        count: 1,
        seed: None,
        test: false,
        max_depth: MAX_DEPTH,
        verbose: false,
        failures_only: false,
        exact: false,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--count" | "-n" => {
                i += 1;
                if i < args.len() {
                    result.count = args[i].parse().unwrap_or(1);
                }
            }
            "--seed" | "-s" => {
                i += 1;
                if i < args.len() {
                    result.seed = args[i].parse().ok();
                }
            }
            "--test" | "-t" => {
                result.test = true;
            }
            "--max-depth" | "-d" => {
                i += 1;
                if i < args.len() {
                    result.max_depth = args[i].parse().unwrap_or(MAX_DEPTH);
                }
            }
            "--verbose" | "-v" => {
                result.verbose = true;
            }
            "-f" | "--failures-only" => {
                result.failures_only = true;
                result.test = true; // failures-only implies testing
            }
            "--exact" => {
                result.exact = true;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    result
}

fn print_help() {
    eprintln!("fuzzgen - Grammar-based fuzzer for DelightQL");
    eprintln!("         Always tests with grammar v2 + builder_v2");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("    fuzzgen [OPTIONS]");
    eprintln!();
    eprintln!("OPTIONS:");
    eprintln!("    -n, --count <N>      Generate N queries (default: 1)");
    eprintln!("    -s, --seed <SEED>    Use specific random seed for reproducibility");
    eprintln!("    -t, --test           Test each query through parser and builder");
    eprintln!("    -d, --max-depth <D>  Maximum recursion depth (default: 1000)");
    eprintln!("    -v, --verbose        Show AST output when testing");
    eprintln!("    -f, --failures-only  Test and only output failures (implies -t)");
    eprintln!(
        "    --exact              Generate only query N (Note: still generates 1..N-1 internally)"
    );
    eprintln!("    --use-old-builder    Use old builder v1 (deprecated)");
    eprintln!("    -h, --help           Show this help message");
    eprintln!();
    eprintln!("EXAMPLES:");
    eprintln!("    fuzzgen                    # Generate one random query");
    eprintln!("    fuzzgen -n 100             # Generate 100 queries");
    eprintln!("    fuzzgen -t                 # Generate and test");
    eprintln!("    fuzzgen -s 42 -t           # Reproducible test with seed 42");
    eprintln!("    fuzzgen -t -f              # Show only failures");
    eprintln!("    seq 100 | xargs -I {{}} fuzzgen -s {{}} -t  # Test 100 seeds");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    // Find grammar.json - always use grammar_dql directory
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let grammar_dir = "grammar_dql";
    let grammar_path = manifest_dir
        .parent() // up to crates
        .unwrap()
        .parent() // up to workspace root
        .unwrap()
        .join(grammar_dir)
        .join("src")
        .join("grammar.json");

    // Load and parse grammar
    let grammar_json = fs::read_to_string(&grammar_path)?;
    let grammar: Grammar = serde_json::from_str(&grammar_json)?;

    // Set up RNG
    let mut rng: Box<dyn RngCore> = if let Some(seed) = args.seed {
        Box::new(StdRng::seed_from_u64(seed))
    } else {
        Box::new(thread_rng())
    };

    // Generate queries
    let start_index = if args.exact {
        // With --exact, jump directly to query N-1 (since we're 0-indexed)
        if args.count > 0 {
            args.count - 1
        } else {
            0
        }
    } else {
        0
    };

    let end_index = if args.exact {
        // With --exact, only generate the single query at position N
        if args.count > 0 {
            args.count
        } else {
            1
        }
    } else {
        // Normal mode: generate all N queries
        args.count
    };

    // We need to consume the RNG to get to the right position
    // Generate and discard queries before the start_index
    for _ in 0..start_index {
        let mut generator = Generator::new(&grammar, &mut rng, args.max_depth);
        if let Some(source_rule) = grammar.rules.get("source_file") {
            generator.generate(source_rule); // Generate but discard
        }
    }

    for i in start_index..end_index {
        let mut generator = Generator::new(&grammar, &mut rng, args.max_depth);

        // Start from source_file rule
        let query = if let Some(source_rule) = grammar.rules.get("source_file") {
            generator.generate(source_rule)
        } else {
            eprintln!("Warning: No source_file rule found in grammar");
            continue;
        };

        // Output the query (unless failures_only is set)
        if !args.failures_only {
            println!("{}", query);
        }

        // Optionally test it
        if args.test {
            match parser::parse(&query) {
                Ok(tree) => {
                    // Always use builder_v2 (old builder has been removed)
                    let builder_result = builder_v2::parse_query(&tree, &query);

                    match builder_result {
                        Ok(ast) => {
                            if !args.failures_only {
                                if args.verbose {
                                    eprintln!("Query {}: [PASS] Parsed successfully", i + 1);
                                    eprintln!("AST: {:?}", ast);
                                } else {
                                    eprintln!("Query {}: [PASS]", i + 1);
                                }
                            }
                        }
                        Err(e) => {
                            let error_str = e.to_string();

                            // Expected semantic errors that the builder should catch
                            let is_expected = error_str.contains("column count mismatch")
                                || error_str.contains("headers but row")
                                || error_str.contains("Duplicate column name")
                                || error_str.contains("Unknown column")
                                || error_str.contains("Type mismatch");

                            if is_expected {
                                // This is an expected semantic validation - builder is working correctly
                                if !args.failures_only {
                                    if args.verbose {
                                        eprintln!(
                                            "Query {}: [INFO] Expected validation: {}",
                                            i + 1,
                                            error_str
                                        );
                                    } else {
                                        eprintln!("Query {}: [INFO]", i + 1); // Expected semantic errors
                                    }
                                }
                            } else {
                                // Unexpected error - this might be a bug
                                if args.failures_only {
                                    // In failures-only mode, output tab-delimited: seed<tab>n<tab>query
                                    let seed_str =
                                        args.seed.map_or("RANDOM".to_string(), |s| s.to_string());
                                    println!("{}\t{}\t{}", seed_str, i + 1, query);
                                    eprintln!("# Builder error: {}", e);
                                } else {
                                    eprintln!("Query {}: [FAIL] Builder error: {}", i + 1, e);
                                    eprintln!("  Query: {}", query);
                                    if args.seed.is_some() {
                                        eprintln!("  (seed: {})", args.seed.unwrap());
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    // Check if this is a known limitation
                    let is_known_limitation = matches!(
                        &e,
                        DelightQLError::KnownLimitation {
                            limitation_type: KnownLimitationType::QualifiedNameAmbiguity,
                            ..
                        }
                    );

                    if is_known_limitation {
                        // This is a known tree-sitter limitation, not a real bug
                        if !args.failures_only {
                            if args.verbose {
                                eprintln!("Query {}: [WARN] Known limitation: {}", i + 1, e);
                                eprintln!("  Query: {}", query);
                            } else {
                                eprintln!("Query {}: [WARN]", i + 1); // Known tree-sitter issue
                            }
                        }
                    } else {
                        // Real parse error - this might be a bug
                        if args.failures_only {
                            // In failures-only mode, output tab-delimited: seed<tab>n<tab>query
                            let seed_str =
                                args.seed.map_or("RANDOM".to_string(), |s| s.to_string());
                            println!("{}\t{}\t{}", seed_str, i + 1, query);
                            eprintln!("# Parse error: {}", e);
                        } else {
                            eprintln!("Query {}: [FAIL] Parse error: {}", i + 1, e);
                            eprintln!("  Query: {}", query);
                            if args.seed.is_some() {
                                eprintln!("  (seed: {})", args.seed.unwrap());
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
