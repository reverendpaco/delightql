// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The compilation pipeline: text to SQL, one typed stage at a time.
//!
//! Each stage takes the previous stage's type and returns its own, so a value
//! carries which stage produced it and nothing downstream can consume a stage
//! that has not run.

// ============================================================================
// CRITICAL PIPELINE INVARIANTS - DO NOT MODIFY OR REMOVE
// ============================================================================
// These directives enforce exhaustive pattern matching across the ENTIRE
// pipeline. They are essential to the "NO LIES" principle that prevents
// silent failures and data loss.
//
// WHY THESE MATTER:
// - They force every enum variant to be explicitly handled
// - They prevent defaulting to wrong values when we don't know what to do
// - They make missing implementations visible at compile time (with clippy)
// - They ensure information flows forward without silent drops
//
// WHAT THEY DO:
// - unreachable_patterns: Catches duplicate/dead match arms (rustc built-in)
// - wildcard_enum_match_arm: Bans _ catch-alls in enum matches (clippy only)
// - match_wildcard_for_single_variants: Bans _ when specific variants exist
//
// IF YOU THINK YOU NEED TO DISABLE THESE:
// 1. You probably don't - rethink your approach
// 2. If you REALLY do, use #[allow(...)] at the specific location
// 3. Document WHY that specific case needs an exception
//
// These directives cascade to ALL modules under pipeline/, including:
// - Every compilation stage (parse, normalize, middle, etc.)
// - Every AST contract (asts::unresolved, sql_ast, etc.)
// ============================================================================
#![deny(unreachable_patterns)] // Works with cargo build
#![deny(clippy::wildcard_enum_match_arm)] // Requires cargo clippy
#![deny(clippy::match_wildcard_for_single_variants)] // Requires cargo clippy

pub mod asts;
pub mod normalize; // Phase 1: typed CST → AST(unresolved)
pub mod parse; // Phase 0: Text → typed CST
pub mod query_features; // Query feature detection
pub mod syntax; // The typed-CST boundary, re-exported under one internal name

// The phase ASTs under their short internal names.
pub use asts::unresolved as ast_unresolved;

// A comment here TRAILS the module it describes, in the order rustfmt
// already sorts these declarations into. A leading comment does not survive:
// the formatter reorders a run of `mod` declarations and leaves comments
// where they stood, so one written above a module lands above whichever
// module sorted into that line.
pub mod ast_transform; // Unified AST walk infrastructure
pub mod ast_visit; // Non-consuming whole-tree inspection/collection sibling of ast_transform
pub(crate) mod bindings;
pub mod compiled_query; // Compiled query output bundle (primary SQL + assertions + emits)
pub mod resolver;
pub mod sql_ast; // CONTRACT for Phase 4 (proper SQL syntax tree with builders - PRODUCTION)
pub mod sql_optimizer;
pub mod sql_rewriter;

pub mod aggregate_catalog; // Per-compile image of the aggregates targeting table
pub(crate) mod sqlite_affinity; // SQLite's type affinity of a declared column type
pub(crate) mod type_classes; // Per-compile image of the type_classes targeting table (THE DOCUMENT DENYLIST)
pub mod danger_gates; // Danger gate system (named safety boundaries, OFF by default)
pub mod dialect_pack; // Per-compile image of the dialect_* targeting tables

pub mod generator; // Phase 5: SQL AST v3 → SQL String (PRODUCTION)
pub mod option_map; // Option map system (strategy/preference selection)

// Per-function recursion depth tracking
#[cfg(feature = "recursion_stats")]
pub mod recursion_stats;

pub mod inline_ddl; // Registration of typed inline (~~ddl ~~) blocks
pub mod verdict; // Verdict types for assertion and error hook outcomes

pub(crate) mod middle;

use crate::diagnostic::{Parse, Runtime};
use crate::error::Result;
use crate::host::CompilerHost;
use crate::lispy::ToLispy;
use crate::names::Registry;
use crate::sexp_formatter;
use syntax::SyntaxTree;

/// THE FRONT END OF ONE PROMPT TEXT: its typed CST and the unresolved
/// statement that CST normalizes to. Every stage past these two belongs to
/// the middle; this value never resolves, refines, or lowers.
pub(crate) struct Pipeline {
    /// The compilation's identity arena; it carries the nesting budget the
    /// parse measures against.
    names: std::rc::Rc<Registry>,
    query_text: String,
    cst: Option<SyntaxTree>,
    query_unresolved: Option<ast_unresolved::Query>,
    /// The statement's inline blocks, read off its goal.
    blocks: ast_unresolved::StatementBlocks,
}

impl Pipeline {
    /// Open the front end over `source`. The arena armed both budgets where
    /// it was minted — shared with whatever compilation was EXECUTING then,
    /// or from policy if none was — and what is published to the system is
    /// exactly that, never a re-read of policy a host may have moved since.
    pub fn new(source: &str, system: &crate::system::DelightQLSystem) -> Self {
        let names = std::rc::Rc::new(Registry::new(&[]));
        system.publish_compiler_limits(names.limits());
        Self {
            names,
            query_text: source.to_string(),
            cst: None,
            query_unresolved: None,
            blocks: ast_unresolved::StatementBlocks::default(),
        }
    }

    /// Get reference to the unresolved query if available
    pub fn query_unresolved(&self) -> Option<&ast_unresolved::Query> {
        self.query_unresolved.as_ref()
    }

    /// Whether the source carries inline `(~~ddl ~~)` blocks. Processing
    /// them registers namespaces/entities, so a pure inspection surface
    /// (compile purity) must refuse before that happens.
    pub(crate) fn has_inline_ddl_blocks(&self) -> bool {
        !self.blocks.is_empty()
    }

    /// Render the front end at a named stage as a pretty-printed string:
    /// `"cst"` or `"ast-unresolved"`.
    pub(crate) fn render_stage(&mut self, stage: &str) -> Result<String> {
        match stage {
            "cst" => {
                let tree = self.execute_to_cst_for_output()?;
                Ok(sexp_formatter::custom_pretty_print(
                    &tree.raw().root_node().to_sexp(),
                ))
            }
            "ast-unresolved" => {
                self.execute_to_query_unresolved()?;
                let query = self.query_unresolved().unwrap();
                Ok(sexp_formatter::custom_pretty_print(&query.to_lispy()))
            }
            _ => Err(Runtime::catalog(
                format!("Unknown stage: '{}'. Valid: cst, ast-unresolved", stage),
                "Invalid stage",
            )),
        }
    }

    /// The extent of this front end's EXECUTION.
    ///
    /// Opened by every method that runs compiler work and closed when it
    /// returns, so what answers a parse reached from too deep to be handed
    /// anything is the compilation whose work it is — not whichever pipeline
    /// object happens to still be alive beside it.
    fn running(&self) -> crate::compiler_limits::Running {
        crate::compiler_limits::Running::under(self.names.limits_shared())
    }

    /// Execute pipeline to CST (parse only)
    ///
    /// A pipeline's text is PROMPT TEXT: what a user typed, at the entrance
    /// it names, with unmarked text read as one goal. The pipeline stands as
    /// that text's host.
    pub fn execute_to_cst(&mut self) -> Result<&SyntaxTree> {
        let _running = self.running();
        if self.cst.is_some() {
            return Ok(self.cst.as_ref().unwrap());
        }

        let tree = parse::prompt_submission(&self.query_text, self.names.limits().nesting())?;

        self.cst = Some(tree);
        Ok(self.cst.as_ref().unwrap())
    }

    /// Execute pipeline to CST for output (includes ERROR nodes for display)
    ///
    /// Showing a bad parse is this entry's whole point; the nesting budget
    /// still applies, because rendering the tree walks it recursively.
    pub fn execute_to_cst_for_output(&mut self) -> Result<&SyntaxTree> {
        let _running = self.running();
        if self.cst.is_some() {
            return Ok(self.cst.as_ref().unwrap());
        }

        let tree =
            parse::prompt_submission_showing_defects(&self.query_text, self.names.limits().nesting())?;

        self.cst = Some(tree);
        Ok(self.cst.as_ref().unwrap())
    }

    /// Execute pipeline to unresolved Query
    pub fn execute_to_query_unresolved(&mut self) -> Result<&ast_unresolved::Query> {
        let _running = self.running();
        if self.query_unresolved.is_some() {
            return Ok(self.query_unresolved.as_ref().unwrap());
        }

        self.execute_to_cst()?;
        let tree = self.cst.as_ref().unwrap();
        let normalized = normalize::submission(tree, std::rc::Rc::clone(&self.names))?;

        let mut goal = one_goal(normalized)?;

        self.blocks = std::mem::take(&mut goal.blocks);
        self.query_unresolved = Some(goal.into_query());
        Ok(self.query_unresolved.as_ref().unwrap())
    }
}

/// Split a query sequence into its individual statement texts.
///
/// One `String` per statement, cut at the boundaries the sequence root draws.
/// A consumer that executes one statement per call sends these.
///
/// A DEFECTIVE submission still divides. The statement that failed is the one
/// that should carry the refusal — with the teaching its own tokens choose and
/// the hook its own text declares — and a splitter that refused the whole
/// submission instead would hand every statement one statement's failure.
/// What recovery could not divide travels as one piece, which is the same
/// answer the ownership rule gives everywhere else.
pub fn split_queries(source: &str) -> Result<Vec<String>> {
    let tree = parse::query_sequence_showing_defects(source)?;
    let extents = parse::statement_extents(&tree);
    if extents.is_empty() {
        return Err(crate::diagnostic::DelightQLError::from(
            crate::diagnostic::Parse::General {
                message: "no queries found in source".to_string(),
            },
        ));
    }
    Ok(extents.into_iter().map(|s| source[s].to_string()).collect())
}

/// What one submission asks of a session.
pub(crate) enum Submission {
    /// One goal to run, with everything it declared and the blocks that
    /// lead and trail it.
    Goal(normalize::Goal),
    /// Definitions and blocks and no goal: admitted the way an unnamed
    /// `(~~ddl ~~)` block at the prompt is, into `home`.
    Definitions(ast_unresolved::InlineDdlSpec),
}

/// The ONE thing a single submission asks for.
///
/// A submission runs one goal or declares definitions. One with two goals is
/// a caller that should have used the sequence entrance; one with a goal and
/// a definition would leave the definition nowhere to go, so it refuses
/// rather than dropping it; one that states nothing declared nothing to run.
///
/// Whatever the submission stated OUTSIDE the goal — a definition's own
/// declarations — travels with the goal: there is one form here, so there is
/// nothing for a file-level sidecar to belong to instead. A file-level block
/// keeps its side of the goal: one written before it leads it, one after
/// trails it.
pub(crate) fn one_submission(mut normalized: normalize::Normalized) -> Result<Submission> {
    let file_level = std::mem::take(&mut normalized.declared);
    let file_blocks = std::mem::take(&mut normalized.blocks);
    let mut definitions = Vec::new();
    let mut queries = Vec::new();
    for form in normalized.forms {
        match form {
            normalize::TopLevelForm::Definition(clause) => definitions.push(clause),
            normalize::TopLevelForm::Goal(goal) => queries.push(goal),
        }
    }
    if queries.len() > 1 {
        // ONE FACT, ONE TEACHING. A submission holding several queries is
        // refused here when it PARSED as a sequence and at the entrance when
        // it did not; both are the same fact about the same submission, so
        // they carry the same identity and say the same thing.
        return Err(Parse::MultiQuery {
            count: queries.len(),
        }
        .into());
    }
    let Some(mut goal) = queries.pop() else {
        let ast_unresolved::StatementBlocks { leading, trailing } = file_blocks;
        let body = ast_unresolved::InlineDdlBody {
            definitions,
            ddl_blocks: leading.into_iter().chain(trailing).collect(),
        };
        if body.is_empty() {
            return Err(Parse::General {
                message: "this submission declares nothing to run".to_string(),
            }
            .into());
        }
        return Ok(Submission::Definitions(ast_unresolved::InlineDdlSpec {
            body,
            namespace: None,
        }));
    };
    if !definitions.is_empty() {
        return Err(Parse::DefinitionsBesideGoal {
            count: definitions.len(),
        }
        .into());
    }
    goal.declared.dangers.extend(file_level.dangers);
    goal.declared.options.extend(file_level.options);
    let ast_unresolved::StatementBlocks { leading, trailing } = file_blocks;
    goal.blocks.leading.splice(0..0, leading);
    goal.blocks.trailing.extend(trailing);
    if goal.declared.expected_error.is_none() {
        goal.declared.expected_error = file_level.expected_error;
    }
    Ok(Submission::Goal(goal))
}

/// The ONE goal a single submission carries, for a reader that runs a goal
/// and nothing else.
pub(crate) fn one_goal(normalized: normalize::Normalized) -> Result<normalize::Goal> {
    match one_submission(normalized)? {
        Submission::Goal(goal) => Ok(goal),
        Submission::Definitions(_) => Err(Parse::General {
            message: "this submission declares definitions and nothing to run".to_string(),
        }
        .into()),
    }
}

/// Arm `names` with its compilation's aggregate catalog, read from `host`
/// unless an enclosing entrance already armed the arena this nested work
/// shares.
pub(crate) fn arm_aggregate_catalog(
    names: &Registry,
    host: &(impl CompilerHost + ?Sized),
) -> Result<()> {
    if !names.has_aggregate_catalog() {
        names.arm_aggregate_catalog(host.aggregate_catalog()?);
    }
    Ok(())
}
