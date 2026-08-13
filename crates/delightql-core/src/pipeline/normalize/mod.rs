// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! CST-to-AST normalization — the one authority that decides which authored
//! spelling distinctions disappear.
//!
//! The typed CST says what was WRITTEN. `Query<Unresolved>` and its
//! definition-side siblings say what it MEANS. Everything between the two —
//! position classification, groundness, arity, hole counting, which of four
//! `%` homes a sigil is in — is decided here, once, and nothing downstream
//! re-decides it.
//!
//! ## How it reads the tree
//!
//! Through the generated typed API only. Each supertype in the grammar is a
//! Rust enum, so a `match` on [`cst::Continuation`] cannot silently miss a
//! member: adding an alternative to the grammar becomes a compile error here
//! rather than a branch that quietly does nothing. Raw node kinds, subtree
//! searches, and re-parsing regenerated DQL text have no road in.
//!
//! Reading a terminal's authored bytes IS how an identifier or a literal is
//! obtained — that is what the token carries. What must never happen is a
//! CATEGORY read off text: whether a `_` is the disregarded anaphor or the
//! deictic stage, whether a cell is a constraint or a default, whether a paren
//! group is parameters or receipt access. Those are answered by position and
//! by the entrance the host named.
//!
//! ## Entrances
//!
//! One per category the host already knows. `f(1, 2)` is a fact in a
//! definition file and an argumentative query in a query sequence — identical
//! bytes — so nothing here guesses.

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::{
    AssertionSpec, CteBinding, DangerSpec, InlineDdlSpec, OptionSpec, Query, Unresolved,
};
use crate::pipeline::asts::ddl::ClauseDecl;
use crate::pipeline::query_features::{FeatureCollector, HoParamBindings};
use crate::pipeline::syntax::{cst, SyntaxTree, TypedNode};
use crate::pipeline::verdict::ExpectedError;
use std::rc::Rc;

mod definitions;
pub(crate) use definitions::awaits_substitution;
mod effects;
mod ground;
mod landing;
pub(crate) mod names;
mod relex;
mod spec;
mod truth;
mod value;

pub mod companion;

#[cfg(test)]
mod tests;

/// What one form DECLARED about itself.
///
/// An annotation decorates a POSITION, so what it says belongs to the form it
/// stood in. A submission that pooled these could say only that some query
/// acknowledged a danger, and nothing downstream could recover which — which
/// is the fact a per-query road needs and cannot reconstruct.
#[derive(Debug, Default)]
pub struct Sidecars {
    pub assertions: Vec<AssertionSpec>,
    pub dangers: Vec<DangerSpec>,
    pub options: Vec<OptionSpec>,
    pub ddl_blocks: Vec<InlineDdlSpec>,
    /// The error this form declares it expects. One per form: two would be
    /// two claims about one outcome.
    pub expected_error: Option<ExpectedError>,
}

impl Sidecars {
    /// Whether the form declared anything at all.
    pub fn is_empty(&self) -> bool {
        self.assertions.is_empty()
            && self.dangers.is_empty()
            && self.options.is_empty()
            && self.ddl_blocks.is_empty()
            && self.expected_error.is_none()
    }
}

/// One goal and what it declared.
#[derive(Debug)]
pub struct Goal {
    pub query: Query<Unresolved>,
    pub declared: Sidecars,
}

/// Everything one submission yields.
///
/// Queries and definitions travel TOGETHER because a canonical file may hold
/// both, in authored order.
#[derive(Debug, Default)]
pub struct Normalized {
    /// Goals in authored order. A query sequence's bare queries and a
    /// definition file's `?-` goals reach the same vector.
    pub queries: Vec<Goal>,
    /// One clause per authored definition, in authored order. Grouping
    /// clauses by subject and assembling them is the catalog's job, not
    /// normalization's: the assembler is the one place a subject's clauses
    /// meet.
    pub definitions: Vec<ClauseDecl>,
    /// What no GOAL claimed: a definition's own declarations, and anything a
    /// canonical file states outside a goal. A goal's own travel on the goal.
    pub declared: Sidecars,
}

/// The canonical entrance: definitions and explicit `?-` goals. A naked query
/// has no derivation here, so nothing needs to refuse one.
pub fn definition_file(
    tree: &SyntaxTree,
    registry: Rc<crate::names::Registry>,
) -> Result<Normalized> {
    Normalizer::new(tree, registry).run(Entrance::DefinitionFile)
}

/// The utility entrance: bare queries executed in order.
pub fn query_sequence(
    tree: &SyntaxTree,
    registry: Rc<crate::names::Registry>,
) -> Result<Normalized> {
    Normalizer::new(tree, registry).run(Entrance::QuerySequence)
}

/// One whole submission, at the entrance its TREE was read through.
///
/// The tree records which root the host named, so this re-decides nothing:
/// it reads the answer back rather than looking at the text a second time.
pub fn submission(tree: &SyntaxTree, registry: Rc<crate::names::Registry>) -> Result<Normalized> {
    match tree.entrance() {
        crate::pipeline::syntax::Root::QuerySequence => query_sequence(tree, registry),
        crate::pipeline::syntax::Root::DefinitionFile => definition_file(tree, registry),
        // A companion cell is a column's, never a submission's.
        crate::pipeline::syntax::Root::CompanionCell => Err(
            crate::error::DelightQLError::parse_error("a companion cell is not a submission"),
        ),
    }
}

/// One INVOCATION of a parameterized definition: the same source, normalized
/// again with the call site's bindings in hand.
///
/// The bindings are supplied at the entrance rather than applied afterwards
/// because substitution is a CST-to-AST judgment: a formal in relation
/// position becomes the supplied relation, a formal in value position becomes
/// the supplied value, and a formal in a bound becomes the supplied integer.
/// Walking a built tree to rewrite them would have to re-decide, from the
/// AST, which positions were formals — the question this boundary already
/// answered.
pub fn bound_definition_file(
    tree: &SyntaxTree,
    registry: Rc<crate::names::Registry>,
    bindings: HoParamBindings,
) -> Result<Normalized> {
    Normalizer::bound(tree, registry, bindings).run(Entrance::DefinitionFile)
}

pub fn bound_query_sequence(
    tree: &SyntaxTree,
    registry: Rc<crate::names::Registry>,
    bindings: HoParamBindings,
) -> Result<Normalized> {
    Normalizer::bound(tree, registry, bindings).run(Entrance::QuerySequence)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Entrance {
    DefinitionFile,
    QuerySequence,
}

/// The normalization context: the authored text, the registry every name is
/// interned into, and the sidecar collector the pipeline already reads.
pub(crate) struct Normalizer<'t> {
    tree: &'t SyntaxTree,
    registry: Rc<crate::names::Registry>,
    features: FeatureCollector,
    /// Whether the walk is inside an assertion body. `equals` is assertion
    /// SYNTAX, not a view: outside that context, building it would silently
    /// discard the comparison, so the position has to be known.
    in_assertion: bool,
    /// Whether the walk is inside a COMPANION CELL. THE TWO-ANAPHOR LAW: `@`
    /// is the composition input in a query and the COLUMN SELF-REFERENCE in a
    /// companion sigil — one glyph, two carriers, and position classifies. The
    /// ROOT is that position, so the flag is set at the entrance and read
    /// where the hole is built.
    /// The error hook the form under construction has declared, waiting for
    /// that form to finish. A hook decorates a POSITION, and the form it
    /// stands in is what claims it.
    pending_error: Option<ExpectedError>,
    /// The authored spelling of the relational term the chain currently ends
    /// in. An edge SELECTS by exactly those bytes — IDENTITY IS THE CANONICAL
    /// SPELLING — and the built relation cannot hand them back: an interior
    /// functor becomes a derived table whose authored form is gone. Only the
    /// CST has them, so the walk carries them.
    last_term: Option<String>,
    /// The authored extent of the sequence form under construction, kept so a
    /// refusal can name the query it belongs to. A sidecar belongs to the
    /// form that declared it, and so does a refusal — the caller that judges
    /// one against a declaration needs both to be the same form's.
    building: Option<std::ops::Range<usize>>,
    /// Bindings a nested preamble declared, waiting for the form that will
    /// carry them. A DML source is a relex, so it may open its own let block
    /// (`emp!!(*) : marked` before the statement that consumes `marked`) —
    /// but a source builds a CHAIN, and only the form around it builds the
    /// query the bindings belong to. They travel here rather than being
    /// rebuilt from the tree by whoever notices them.
    hoisted_ctes: Vec<CteBinding<Unresolved>>,
    out: Normalized,
}

impl<'t> Normalizer<'t> {
    pub(crate) fn new(tree: &'t SyntaxTree, registry: Rc<crate::names::Registry>) -> Self {
        Normalizer {
            features: FeatureCollector::new(),
            tree,
            registry,
            in_assertion: false,
            pending_error: None,
            last_term: None,
            building: None,
            hoisted_ctes: Vec::new(),
            out: Normalized::default(),
        }
    }

    /// The same, with a call site's bindings in hand.
    pub(crate) fn bound(
        tree: &'t SyntaxTree,
        registry: Rc<crate::names::Registry>,
        bindings: HoParamBindings,
    ) -> Self {
        let mut normalizer = Self::new(tree, registry);
        normalizer.features.ho_bindings = Some(bindings);
        normalizer
    }

    /// The bindings the call site supplied, if any.
    pub(crate) fn bindings(&self) -> Option<&HoParamBindings> {
        self.features.ho_bindings.as_ref()
    }

    /// The authored bytes under a node. Spans survive the grammar for exactly
    /// this reason: a spelling that normalization drops is still readable.
    pub(crate) fn text<T: TypedNode<'t>>(&self, node: T) -> &'t str {
        // The façade borrows from the tree, which outlives this normalizer.
        let source: &'t str = self.tree.source();
        match self.tree.byte_range(node) {
            Some(range) => &source[range],
            None => "",
        }
    }

    /// A node's authored span, for a diagnostic that wants to point.
    pub(crate) fn span<T: TypedNode<'t>>(&self, node: T) -> Option<(usize, usize)> {
        self.tree.byte_range(node).map(|r| (r.start, r.end))
    }

    pub(crate) fn features(&mut self) -> &mut FeatureCollector {
        &mut self.features
    }

    fn run(mut self, entrance: Entrance) -> Result<Normalized> {
        self.run_into(entrance)?;
        Ok(self.finish())
    }

    /// The walk itself, leaving the normalizer in the caller's hands. A
    /// refusal that has to be ATTRIBUTED needs the normalizer to survive it,
    /// because the form it was building is recorded there.
    fn run_into(&mut self, entrance: Entrance) -> Result<()> {
        let Some(branch) = self.tree.root_branch() else {
            // A canonical file declaring nothing declares nothing. Emptiness
            // is lawful and has no form to show.
            return Ok(());
        };
        match (entrance, branch) {
            (Entrance::DefinitionFile, cst::SourceFileChild::DefinitionFile(file)) => {
                self.definition_file(file)?
            }
            (Entrance::QuerySequence, cst::SourceFileChild::QuerySequenceRoot(root)) => {
                self.query_sequence(root)?
            }
            // The entrance is what the CALLER named; the tree carrying another
            // branch means the selector and the parse disagree, which is a
            // façade defect rather than an authoring mistake.
            (_, branch) => {
                return Err(DelightQLError::parse_error(format!(
                    "the {entrance:?} entrance parsed a {} root",
                    branch_name(branch)
                )))
            }
        }
        Ok(())
    }

    /// A finished form CLAIMS what its own text declared, and the collector
    /// starts empty for the next one. Draining at the boundary is what keeps
    /// a sidecar from bleeding forward — or backward, since nothing is read
    /// until the form that could own it has closed.
    fn drain(&mut self) -> Sidecars {
        let fresh = FeatureCollector::inheriting_ho_bindings(&self.features);
        let mut collector = std::mem::replace(&mut self.features, fresh);
        Sidecars {
            assertions: collector.take_assertions(),
            dangers: collector.take_dangers(),
            options: collector.take_options(),
            ddl_blocks: collector.take_ddl_blocks(),
            expected_error: self.pending_error.take(),
        }
    }

    fn push_goal(&mut self, query: Query<Unresolved>) {
        let declared = self.drain();
        self.out.queries.push(Goal { query, declared });
    }

    /// A definition's declarations are the FILE's: a definition is itself a
    /// file-level form. Draining here is what stops them reaching the next
    /// goal, which did not write them.
    fn absorb_file_level(&mut self) {
        let declared = self.drain();
        self.out.declared.assertions.extend(declared.assertions);
        self.out.declared.dangers.extend(declared.dangers);
        self.out.declared.options.extend(declared.options);
        self.out.declared.ddl_blocks.extend(declared.ddl_blocks);
        if let Some(hook) = declared.expected_error {
            self.out.declared.expected_error = Some(hook);
        }
    }

    fn finish(mut self) -> Normalized {
        self.absorb_file_level();
        self.out
    }

    fn definition_file(&mut self, file: cst::DefinitionFile<'t>) -> Result<()> {
        for child in file.children() {
            match child {
                cst::DefinitionFileChild::EntityDefinition(definition) => {
                    let clause = self.entity_definition(definition)?;
                    self.out.definitions.push(clause);
                    self.absorb_file_level();
                }
                cst::DefinitionFileChild::TopLevelGoal(goal) => {
                    let query = self.top_level_goal(goal)?;
                    self.push_goal(query);
                }
                // A subordinate block belongs to the FILE, so it lands where
                // every other file-level declaration lands. It is not a goal
                // and not a definition: consultation runs it inside the same
                // transaction, in this file's namespace or in the child the
                // block names.
                cst::DefinitionFileChild::DdlAnnotation(block) => {
                    let spec = self.ddl_annotation(block)?;
                    self.features().add_ddl_block(spec);
                    self.absorb_file_level();
                }
            }
        }
        Ok(())
    }

    fn query_sequence(&mut self, root: cst::QuerySequenceRoot<'t>) -> Result<()> {
        // A utility file declaring nothing declares nothing, exactly as a
        // canonical one does: the header stands and the sequence is absent.
        // The header itself is a READER DIRECTIVE and carries nothing here.
        let Some(sequence) = root.children().find_map(|child| match child {
            cst::QuerySequenceRootChild::QuerySequence(sequence) => Some(sequence),
            cst::QuerySequenceRootChild::QuerySequenceHeader(_) => None,
        }) else {
            return Ok(());
        };
        for child in sequence.children() {
            // The extent is taken BEFORE the form is built, so a refusal
            // raised anywhere inside it can be attributed to this form and
            // to no other.
            self.building = match child {
                cst::QuerySequenceChild::Relex(relex) => self.tree.byte_range(relex),
                cst::QuerySequenceChild::Effrelex(effrelex) => self.tree.byte_range(effrelex),
            };
            let query = match child {
                cst::QuerySequenceChild::Relex(relex) => self.relex_query(relex)?,
                cst::QuerySequenceChild::Effrelex(effrelex) => self.effrelex_query(effrelex)?,
            };
            self.push_goal(query);
            self.building = None;
        }
        Ok(())
    }

    /// `?- body` ≡ `_ :- body`: the goal marker names the category and
    /// carries nothing into the AST.
    fn top_level_goal(&mut self, goal: cst::TopLevelGoal<'t>) -> Result<Query<Unresolved>> {
        match self.require(goal.goal(), "a top-level goal has a body")? {
            cst::TopLevelGoalGoal::Relex(relex) => self.relex_query(relex),
            cst::TopLevelGoalGoal::Effrelex(effrelex) => self.effrelex_query(effrelex),
        }
    }

    /// A grammar-required field that is absent means the tree is defective —
    /// a parse this normalizer should never have been handed.
    pub(crate) fn require<T>(&self, slot: Option<T>, what: &str) -> Result<T> {
        slot.ok_or_else(|| {
            DelightQLError::parse_error(format!("the grammar requires {what}, and it is absent"))
        })
    }
}

/// What ONE QUERY declared it expects to fail with, read from the tree alone.
///
/// The ordinary road collects this while normalizing, which is right for
/// every submission that normalizes. A submission that does NOT is exactly
/// the one whose declaration matters most: an error hook DECORATES a
/// position and is never a step in the relation, so what it says survives the
/// refusal of the query it stands beside.
///
/// A HOOK BELONGS TO THE QUERY IT STANDS IN. `span` is that query's authored
/// extent, and nothing outside it is read — a submission is a sequence of
/// forms, and a later form declaring an earlier form's outcome is the
/// ownership `Goal { query, declared }` exists to prevent. The caller proves
/// the extent structurally before asking; there is no whole-tree reading to
/// fall back on.
pub(crate) fn declared_error_within(
    tree: &SyntaxTree,
    span: &std::ops::Range<usize>,
) -> Option<ExpectedError> {
    let mut found = None;
    for any in crate::pipeline::syntax::walk(tree) {
        if any.typed_kind() != Some(cst::Kind::ErrorAnnotation) {
            continue;
        }
        let hook = cst::ErrorAnnotation::cast(any.node())?;
        // A hook recovery had to REPAIR is not the hook the author wrote.
        // `(~~error nonsense ~~)` recovers as an `error_annotation` whose URI
        // is absent — which is the BARE hook's shape, and the bare hook
        // accepts any error — so reading it would let a mistyped annotation
        // catch the very parse failure the mistyping caused.
        if hook.node().has_error() {
            continue;
        }
        let Some(at) = tree.byte_range(hook) else {
            continue;
        };
        if at.start < span.start || at.end > span.end {
            continue;
        }
        // ONE GOAL DECLARES ONE EXPECTED ERROR. Refusing the pair is the
        // normalizer's; here a second one only means this reader cannot say
        // which was meant, so it says nothing.
        if found.is_some() {
            return None;
        }
        found = Some(ExpectedError {
            uri_segments: match hook.uri() {
                None => Vec::new(),
                Some(uri) => uri
                    .children()
                    .map(|segment| tree.text(segment).to_string())
                    .collect(),
            },
        });
    }
    // A hook standing INSIDE the span recovery could not read has no typed
    // node left to cast — and that is exactly the query whose refusal
    // arrived before the hook was reached. What the author typed is still
    // there in the bytes, read within the same extent.
    found.or_else(|| declared_error_after_recovery(tree, span))
}

/// The declaration, read off the author's own bytes, inside ONE query's
/// extent.
///
/// An error hook is LEXICALLY delimited — `(~~error` opens it, `~~)` closes
/// it, and only a URI stands between — so once the parse has failed it can
/// still be read exactly. It has to be: recovery does not merely lose the
/// annotation's node, it re-lexes the annotation's own characters, and the
/// `//` in `error://…` then opens a line comment that swallows the closing
/// delimiter. A reader keyed on the pieces recovery hands back would be
/// reading the wreckage rather than what the author typed.
///
/// The opener is only recognized where a TOKEN begins, which is what keeps a
/// string literal from declaring anything: `_(x @ "(~~error://a ~~)")` puts
/// those bytes inside one token that begins with a quote, so no hook opens in
/// it. A hook that opens or closes outside `span` belongs to another form.
fn declared_error_after_recovery(
    tree: &SyntaxTree,
    span: &std::ops::Range<usize>,
) -> Option<ExpectedError> {
    const OPEN: &str = "(~~error";
    const CLOSE: &str = "~~)";
    let source = tree.source();
    let mut found: Option<ExpectedError> = None;
    for token in tree.tokens() {
        if token.start < span.start || !source[token.start..].starts_with(OPEN) {
            continue;
        }
        let body_start = token.start + OPEN.len();
        let Some(close) = source[body_start..].find(CLOSE) else {
            continue;
        };
        let end = body_start + close + CLOSE.len();
        if end > span.end {
            // A hook the extent cuts in half is a hook this reader cannot
            // attribute; leaving it is the closed answer.
            continue;
        }
        // A body the grammar would not have admitted is not a declaration.
        // Reading it as the bare hook would be the worst possible answer: the
        // bare hook accepts ANY error, so a mistyped annotation would catch
        // the very parse failure the mistyping caused.
        let Some(uri_segments) = uri_segments(&source[body_start..body_start + close]) else {
            continue;
        };
        // ONE GOAL DECLARES ONE EXPECTED ERROR; a second means this reader
        // cannot say which was meant.
        if found.is_some() {
            return None;
        }
        found = Some(ExpectedError { uri_segments });
    }
    found
}

/// What an error hook's body says, held to what the grammar admits there.
///
/// `error_annotation = '(~~error' annotation_uri? '~~)'`, so there are exactly
/// two lawful bodies: nothing at all — the bare hook, which accepts any error
/// — and `'://' uri_segment ('/' uri_segment)*`. `None` is the third answer,
/// and it is the one that matters: anything else is a body the grammar refuses,
/// and a refused body declares nothing.
fn uri_segments(body: &str) -> Option<Vec<String>> {
    /// `uri_segment: /[a-zA-Z0-9_][a-zA-Z0-9_.-]*/`, spelled here because the
    /// tree that would have carried it is the tree recovery destroyed.
    fn is_segment(segment: &str) -> bool {
        let mut chars = segment.chars();
        chars
            .next()
            .is_some_and(|first| first.is_ascii_alphanumeric() || first == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    }

    let body = body.trim();
    if body.is_empty() {
        return Some(Vec::new());
    }
    let uri = body.strip_prefix("://")?;
    let segments: Vec<String> = uri.split('/').map(str::to_string).collect();
    segments
        .iter()
        .all(|segment| is_segment(segment))
        .then_some(segments)
}

fn branch_name(branch: cst::SourceFileChild<'_>) -> &'static str {
    match branch {
        cst::SourceFileChild::DefinitionFile(_) => "definition-file",
        cst::SourceFileChild::QuerySequenceRoot(_) => "query-sequence",
        cst::SourceFileChild::CompanionCellRoot(_) => "companion-cell",
    }
}

/// The forms this road DEFERS: lawful, admitted by the grammar, and without a
/// carrier in the surviving AST to hold them.
///
/// The list is CLOSED, and that is the point. Every other thing the grammar
/// over-admits is answered — implemented, or refused under the identity its
/// own law gives it — so a third deferral cannot appear without a variant
/// appearing here, which is a visible act rather than a generic exit.
///
/// Each names why it waits: what the law admits, and what the AST cannot yet
/// say.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Deferred {
    /// `t(*.( |1| ))`, `t(*) .( |1| )` — the law admits a `reference` in a
    /// dequalifying access, and `Access::Dequalify` holds spellings. One
    /// family for both spellings: the run is the same run wherever it stands.
    DequalifyOrdinal,
    /// `|> .t( |1| )` — the law admits a `reference` where an operator
    /// addresses a column, and the operator holds spellings.
    OperatorOrdinal,
}

impl Deferred {
    /// The family a measurement groups by.
    pub fn family(self) -> &'static str {
        match self {
            Deferred::DequalifyOrdinal => "dequalify",
            Deferred::OperatorOrdinal => "operator column",
        }
    }
}

#[cfg(test)]
impl Deferred {
    /// Every deferral, so a test can enumerate them rather than restate them.
    pub const ALL: &'static [Deferred] = &[
        Deferred::DequalifyOrdinal,
        Deferred::OperatorOrdinal,
    ];
}

/// A form the consolidated grammar admits, the law allows, and the surviving
/// AST has no carrier for.
///
/// It carries the FAMILY from a closed vocabulary, not a free string: the
/// whole-surface measurement groups by it, and nothing can mint a deferral
/// the inventory has not already named.
pub(crate) fn gap(family: Deferred, detail: impl std::fmt::Display) -> DelightQLError {
    DelightQLError::parse_error_categorized(
        "normalize/gap",
        format!(
            "{}: {detail} is admitted by the grammar and has no carrier yet",
            family.family()
        ),
    )
}
