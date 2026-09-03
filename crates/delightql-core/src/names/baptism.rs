// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Baptism — the single pass that assigns emitted names.
//!
//! Nothing the compiler invents has a name before this runs. That is the
//! whole point: a local road cannot answer "what does this name mean",
//! because during compilation there is no name to mean anything.
//!
//! **Baptism seals a bundle, not a statement.** A program creates a
//! temporary object in one statement, fills it in a second and reads it in
//! a third; naming each statement independently cannot guarantee the three
//! agree. The unit is therefore everything one compilation produces.

use std::collections::{HashMap, HashSet};

use super::id::{ColId, EntityId, FnId, ScopeId};
use super::origin::{Addressing, CteRole, FnOrigin, ScopeKind, WrapReason};
use super::policy::{Mint, NamePolicy};
use super::registry::Registry;
use super::sink::IdentSink;

/// One statement's contribution to a bundle.
///
/// The SQL AST enumerates this shape before emission.
#[derive(Clone, Debug, Default)]
pub struct Statement {
    /// Scopes in deterministic tree order.
    pub scopes: Vec<ScopeId>,
    /// Output headings, each an ordered list of columns.
    pub headings: Vec<Vec<ColId>>,
    /// Every column this statement references.
    pub refs: Vec<ColId>,
}

/// The open/reserved naming states a bundle passes through. Uninhabited:
/// they select capabilities, they are never values.
#[derive(Clone, Debug)]
pub enum OpenNames {}
#[derive(Clone, Debug)]
pub enum NamesReserved {}

/// Everything one compilation produces.
///
/// TYPESTATE: a bundle is gathered OPEN, must reserve its complete
/// authored-name inventory, and only the RESERVED state reaches baptism —
/// so no invented name can be allocated before every authored spelling
/// owns its characters.
#[derive(Clone, Debug)]
pub struct Bundle<S = NamesReserved> {
    pub statements: Vec<Statement>,
    _names: std::marker::PhantomData<S>,
}

impl Default for Bundle<OpenNames> {
    fn default() -> Self {
        Bundle {
            statements: Vec::new(),
            _names: std::marker::PhantomData,
        }
    }
}

impl Bundle<OpenNames> {
    /// Gather one compilation's statements, names still open.
    pub fn gather(statements: Vec<Statement>) -> Self {
        Bundle {
            statements,
            _names: std::marker::PhantomData,
        }
    }

    /// Reserve the COMPLETE authored inventory this bundle publishes —
    /// every answering scope name and every published column spelling —
    /// with the compilation registry, sealing the bundle for baptism.
    /// Position admission already reserved what this compilation authored;
    /// this walk is what makes the enumeration COMPLETE for names that
    /// arrived by other roads (consulted definitions, catalog spellings).
    pub fn reserve_authored(self, registry: &Registry) -> Bundle<NamesReserved> {
        for statement in &self.statements {
            for scope in &statement.scopes {
                if let Some(spelling) = registry.answer_spelling(*scope) {
                    let (text, stropped) = registry.spelling_text(spelling);
                    registry.reserve_authored(&text, stropped);
                }
            }
            for heading in &statement.headings {
                for column in heading {
                    if let Some(spelling) = registry.published(*column) {
                        let (text, stropped) = registry.spelling_text(spelling);
                        registry.reserve_authored(&text, stropped);
                    }
                }
            }
        }
        Bundle {
            statements: self.statements,
            _names: std::marker::PhantomData,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaptismError {
    /// A column is referenced whose scope was never named.
    DanglingScope { col: ColId, scope: ScopeId },
    /// `DQL_NAME_POLICY` names a policy the mint does not have. Falling back
    /// would report a heading contract nobody asked for.
    UnknownNamePolicy,
}

/// The capability to spell a minted thing.
///
/// Produced only by [`baptise`]. Scope and column names are unreachable
/// without it, so the compiler literally cannot ask for the name of a wrap
/// alias before the bundle is finished.
pub struct Baptised<'r> {
    reg: &'r Registry,
    scopes: HashMap<ScopeId, String>,
    cols: HashMap<ColId, (String, bool)>,
    /// What a relation nobody named REPORTS itself as. Distinct from the
    /// emission alias beside it: an alias is unique only inside the statement
    /// that invented it, while this value travels in a row and has to tell
    /// one relation from another wherever it lands.
    reports: HashMap<ScopeId, String>,
    /// The columns whose emitted name the mint DREW. Only baptism knows: an
    /// authored spelling that lost an ambiguity is drawn too, and the
    /// registry still holds the spelling its author wrote.
    drawn: HashSet<ColId>,
}

impl Baptised<'_> {
    pub fn write_scope<W: IdentSink>(&self, s: ScopeId, w: &mut W) {
        if let Some(name) = self.scopes.get(&s) {
            w.push_ident(name, false);
        }
    }

    pub fn write_column<W: IdentSink>(&self, c: ColId, w: &mut W) {
        if let Some((name, stropped)) = self.cols.get(&c) {
            w.push_ident(name, *stropped);
        }
    }

    /// A column reported as a VALUE: the exact name the occurrence has in
    /// the emitted heading. A column emitting the name its author wrote
    /// reports that name; a NEVER-NAMED column reports the characters the
    /// mint drew for it — the same ones the heading displays, drawn per
    /// compilation and unforgeable, exactly like an unnamed relation's
    /// scope report. An ordinal is not a naming authority.
    ///
    /// The one exception is an AUTHORED spelling that lost an ambiguity:
    /// its drawn emission characters carry no meaning the author chose, so
    /// it reports the qualified ordinal reference that still reaches it.
    ///
    /// The emission spelling is reported as plain CHARACTERS — it is what a
    /// client matches the heading against, not a reference anyone types, so
    /// it carries no stropping. The ordinal road is the opposite: what it
    /// writes IS a reference, and its qualifier keeps the bit that makes it
    /// one.
    pub fn write_column_report<W: IdentSink>(&self, c: ColId, w: &mut W) {
        if self.drawn.contains(&c)
            && self.reg.published(c).is_some()
            && self.reg.write_ordinal_report(c, w)
        {
            return;
        }
        if let Some((name, _stropped)) = self.cols.get(&c) {
            w.push_ident(name, false);
        }
    }

    pub fn write_entity<W: IdentSink>(&self, entity: EntityId, w: &mut W) {
        self.reg.write_entity(entity, w);
    }

    /// A scope reported as a value, not as an alias.
    ///
    /// A relation the user named reports that name. A relation nobody named
    /// reports one minted FOR IT — different from every other relation's, so
    /// a reader of meta-ize's `scope` column can tell two unnamed relations
    /// apart, and drawn rather than derived, so nobody can rely on which one
    /// they got.
    pub fn write_answers_to<W: IdentSink>(&self, s: ScopeId, w: &mut W) {
        match self.reg.answer_spelling(s) {
            Some(spelling) => self.reg.write(spelling, w),
            None => {
                if let Some(report) = self.reports.get(&s) {
                    w.push_ident(report, false);
                }
            }
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the sealed whole-function spelling capability has no production caller"
        )
    )]
    pub fn write_function<W: IdentSink>(
        &self,
        function: FnId,
        w: &mut W,
    ) -> Result<(), super::origin::FunctionSpellingError> {
        self.reg.write_function(function, w)
    }

    pub fn write_function_namespace<W: IdentSink>(&self, function: FnId, w: &mut W) {
        self.reg.write_function_namespace(function, w);
    }

    pub fn write_function_name<W: IdentSink>(
        &self,
        function: FnId,
        w: &mut W,
    ) -> Result<(), super::origin::FunctionSpellingError> {
        self.reg.write_function_name(function, w)
    }

    pub fn function_origin(&self, function: FnId) -> FnOrigin {
        self.reg.function_origin(function)
    }

    /// Spell a reference as the emitting statement must carry it.
    ///
    /// Qualification is DERIVED, never carried: the qualifier of a column
    /// is the scope it lives in, and whether to print it is a property of
    /// the emitting select rather than of the column.
    ///
    /// A column the statement PRODUCES needs no qualifier — except where
    /// the statement also READS the scope it produces, which is what a
    /// recursive CTE's step member does: `FROM c` beside `UNION ALL`
    /// under `WITH RECURSIVE c`. There the bare spelling names a column
    /// two FROM entries may publish, and the engine, not the compiler,
    /// is left to notice. `reflexive` is that fact, and only its own
    /// statement can know it.
    pub fn write_ref<W: IdentSink>(&self, c: ColId, at: ScopeId, reflexive: bool, w: &mut W) {
        let owner = self.reg.scope_of(c);
        if owner != at || reflexive {
            self.write_scope(owner, w);
            w.push_ident(".", false);
        }
        self.write_column(c, w);
    }

    /// Whether a thing was named at all. Answers the question a caller
    /// would otherwise reach for characters to answer.
    pub fn knows_scope(&self, s: ScopeId) -> bool {
        self.scopes.contains_key(&s)
    }

    pub(crate) fn is_scratch_scope(&self, s: ScopeId) -> bool {
        matches!(self.reg.kind_of(s), ScopeKind::Scratch { .. })
    }

    pub fn knows_column(&self, c: ColId) -> bool {
        self.cols.contains_key(&c)
    }

    pub(crate) fn column_belongs_to(&self, c: ColId, scope: ScopeId) -> bool {
        self.reg.scope_of(c) == scope
    }
}

/// Assign emitted names to the bundle's listed scopes and output headings
/// in one deterministic sweep, then verify that every referenced column's
/// owner scope was listed.
///
/// Only a RESERVED bundle enters: `baptise(&open_bundle)` is a compile
/// error, because no entrance accepts `Bundle<OpenNames>`.
pub fn baptise<'r>(
    reg: &'r Registry,
    bundle: &Bundle<NamesReserved>,
) -> Result<Baptised<'r>, BaptismError> {
    baptise_with_policy(
        reg,
        bundle,
        NamePolicy::from_env().map_err(|()| BaptismError::UnknownNamePolicy)?,
    )
}

/// The same, under an explicit policy — the test entrance for laws that
/// need deterministic draws without touching process environment.
pub(super) fn baptise_with_policy<'r>(
    reg: &'r Registry,
    bundle: &Bundle<NamesReserved>,
    policy: NamePolicy,
) -> Result<Baptised<'r>, BaptismError> {
    let mut mint = Mint::new(policy);
    // ALIAS ALWAYS PRE-EMPTS A MINT: every authored reservation — made at
    // position admission and completed by the bundle seal — stands in the
    // collision universe before the first invention is allocated.
    let authored_reserved: HashSet<Vec<u8>> = reg
        .authored_reserved()
        .into_iter()
        .map(|s| reg.canon_bytes(s))
        .collect();
    let mut used: HashSet<Vec<u8>> = reg
        .reserved()
        .into_iter()
        .map(|s| reg.canon_bytes(s))
        .chain(authored_reserved.iter().cloned())
        .collect();

    let mut scopes: HashMap<ScopeId, String> = HashMap::new();
    let mut cols: HashMap<ColId, (String, bool)> = HashMap::new();
    let mut reports: HashMap<ScopeId, String> = HashMap::new();
    // A drawn name belongs to a PORT — one output position of one emitted
    // heading. Every port draws its own, which is what keeps two sibling
    // publications of one value two addressable slots; no two emitted
    // positions are ever required to share characters, because nothing
    // the generator spells joins by name.
    //
    // The tempting regressions are both keys: by value progenitor, and by
    // republication chain. Each makes siblings share characters, because
    // siblings are related under both.
    let mut minted: HashMap<ColId, String> = HashMap::new();
    let mut drawn: HashSet<ColId> = HashSet::new();
    let mut invented = 0u32;
    let mut authored_used: HashSet<Vec<u8>> = HashSet::new();
    // Local to this call. Not a static, so nothing about an emitted name
    // can depend on how many queries this process compiled earlier.
    let mut n = 0u32;

    // 1. Enumerate every scope once, then bind authored spellings before
    //    assigning inventions. This makes the finished bundle itself part
    //    of the collision universe even when the registry has no catalog
    //    reservation seed.
    let mut scope_order = Vec::new();
    let mut seen_scopes = HashSet::new();
    for stmt in &bundle.statements {
        for scope in &stmt.scopes {
            if seen_scopes.insert(*scope) {
                scope_order.push(*scope);
            }
        }
    }
    // A scope whose name IS a relation's is settled before anything that
    // could take those characters: it has no second spelling to fall back
    // to, and a statement naming the relation twice must say one word.
    scope_order.sort_by_key(|scope| {
        (
            !reg.is_fixed_relation(*scope),
            reg.answer_spelling(*scope).is_none(),
        )
    });

    // A scope that appears in several statements is named once, which is
    // what makes a plan-lifetime object agree with itself everywhere.
    let mut reported = 0u32;
    for scope in scope_order {
        let authored = reg.answer_spelling(scope);
        // A relation that answers to nothing still has to be able to say
        // which relation it is. That report is minted per RELATION, so two
        // anonymous relations in one query are two answers rather than one
        // shared mark — the distinction meta-ize exists to publish.
        if authored.is_none() {
            let report = loop {
                reported += 1;
                let candidate = mint.spell(reported);
                if !authored_reserved.contains(&canonical_key(&candidate)) {
                    break candidate;
                }
            };
            reports.insert(scope, report);
        }
        let emission_prefix = reg.emission_prefix(scope);
        let emission_name = reg.emission_name(scope);
        // Witness source aliases are lexical names for the right side of
        // their own wrapper. Multiple witness arms may therefore reuse the
        // canonical `r` spelling without colliding in one SQL name space;
        // their enclosing SELECTs are distinct. Other exact compiler names
        // remain globally reserved and are uniquified as usual.
        let fixed_witness_alias = matches!(
            (reg.kind_of(scope), emission_name),
            (
                ScopeKind::Wrap {
                    why: WrapReason::Witness,
                    ..
                },
                Some(spelling)
            ) if reg.spelling_text(spelling).0 == "r"
        );
        let base = match (authored, emission_name, emission_prefix) {
            (Some(spelling), _, _) => reg.spelling_text(spelling).0,
            (None, Some(spelling), _) => reg.spelling_text(spelling).0,
            (None, None, Some(prefix)) => {
                n += 1;
                format!("{}_{}", prefix, n)
            }
            (None, None, None) => {
                n += 1;
                // Exhaustive: a new kind of invention does not compile
                // until this match has an answer for it.
                let prefix = match reg.kind_of(scope) {
                    ScopeKind::BaseTable { .. } => "b",
                    ScopeKind::UserAlias { .. } => "a",
                    ScopeKind::AnonRelation => "anon",
                    ScopeKind::Join { .. } => "j",
                    ScopeKind::PipeStage { .. } => "t",
                    ScopeKind::Wrap { .. } => "t",
                    ScopeKind::Cte { role, .. } => match role {
                        CteRole::TreeGroup => "tg",
                        CteRole::GroupCarrier => "gc",
                        CteRole::Recursive => "rec",
                        CteRole::Reachability => "reach",
                        CteRole::Materialize => "mat",
                    },
                    ScopeKind::SetArm { .. } => "arm",
                    ScopeKind::Resolution { .. } => "r",
                    ScopeKind::ErHop { .. } => "hop",
                    ScopeKind::HoCarrier { .. } => "ho",
                    ScopeKind::Scratch { .. } => "scratch",
                    ScopeKind::Interior => "int",
                };
                format!("{}_{}", prefix, n)
            }
        };
        let name = if reg.is_fixed_relation(scope) {
            // Not arbitrated: the statement writes this relation's name and
            // its references must write the same one. Reserved against every
            // later choice, authored or invented.
            let name = reg
                .emission_name(scope)
                .map(|spelling| reg.spelling_text(spelling).0)
                .unwrap_or(base);
            used.insert(canonical_key(&name));
            authored_used.insert(canonical_key(&name));
            name
        } else if fixed_witness_alias {
            base
        } else if authored.is_some() {
            // Catalog reservation means "do not invent this spelling", not
            // "rename an authored occurrence of the catalog object itself".
            // Authored scopes arbitrate only with other authored scopes, then
            // reserve their chosen spelling against every later invention.
            let name = uniquify(&mut authored_used, base);
            used.insert(canonical_key(&name));
            name
        } else {
            uniquify(&mut used, base)
        };
        scopes.insert(scope, name);
    }

    // 2. Output headings. The disambiguation convention is applied
    //    GROUP-RELATIVE within one heading, by one law, in one place.
    //
    //    A member takes the spelling the user gave it, or a minted one. Two
    //    published members carrying ONE spelling are an ambiguity, and an
    //    ambiguity poisons both sides: neither occurrence is the real one, so
    //    neither keeps the plain name. In-language both are still reached by
    //    qualified reference or by ordinal; a consumer who wants a stable
    //    wire name baptizes it with an alias.
    //
    //    A HYGIENIC member emits no output: it is a carrier the compiler
    //    put there, addressable by nothing, and it leaves the heading again
    //    before anyone reads it. It publishes no name, so it is not part of
    //    any ambiguity, and it takes its spelling AFTER the real outputs have
    //    taken theirs. Letting it contend by position made a ground-slotted
    //    column, which publishes nothing at all, consume `status` and push
    //    the one real `status` to `status_2`.
    for stmt in &bundle.statements {
        for heading in &stmt.headings {
            let published = |c: &ColId| reg.published(*c).map(|sp| reg.spelling_text(sp));
            let mut carried: HashMap<Vec<u8>, u32> = HashMap::new();
            for c in heading
                .iter()
                .filter(|c| reg.addressing(**c) != Addressing::Hygienic)
            {
                if let Some((text, _)) = published(c) {
                    *carried.entry(canonical_key(&text)).or_insert(0) += 1;
                }
            }
            let mut seen: HashMap<Vec<u8>, u32> = HashMap::new();
            let order = heading
                .iter()
                .filter(|c| reg.addressing(**c) != Addressing::Hygienic)
                .chain(
                    heading
                        .iter()
                        .filter(|c| reg.addressing(**c) == Addressing::Hygienic),
                );
            for c in order {
                // A hygienic member's spelling is the compiler's own and
                // reaches no heading, so it is neither poisoned nor part of
                // an ambiguity — it is the internal carrier's name and stays
                // legible in the SQL that carries it.
                let contested = |text: &str| {
                    reg.addressing(*c) != Addressing::Hygienic
                        && (carried.get(&canonical_key(text)).copied().unwrap_or(0) > 1
                            // AUTHORED-NAME LOSS IS MONOTONIC. An
                            // occurrence whose name lost an ambiguity
                            // upstream is drawn HERE too, even where
                            // nothing in this heading collides — otherwise
                            // a projection that leaves one of the repeated
                            // positions standing publishes a name the
                            // repetition took away.
                            || reg.name_lost(*c))
                };
                match published(c) {
                    Some((text, stropped)) if !contested(&text) => {
                        // The authored spelling arbitrates for it, so a
                        // hygienic carrier that wants the same characters
                        // takes them after and moves.
                        let count = seen.entry(canonical_key(&text)).or_insert(0);
                        *count += 1;
                        let name = if *count == 1 {
                            text
                        } else {
                            format!("{}_{}", text, count)
                        };
                        cols.insert(*c, (name, stropped));
                    }
                    _ => {
                        // Drawn once per port, never suffixed: the suffix is
                        // arbitration between two spellings, and there is only
                        // ever one occurrence of one value to arbitrate for.
                        let value = *c;
                        let name = match minted.get(&value) {
                            Some(drawn) => drawn.clone(),
                            None => {
                                // A drawn spelling an authored name owns is
                                // skipped: the author got there first.
                                let drawn = loop {
                                    invented += 1;
                                    let candidate = mint.spell(invented);
                                    if !authored_reserved.contains(&canonical_key(&candidate)) {
                                        break candidate;
                                    }
                                };
                                minted.insert(value, drawn.clone());
                                drawn
                            }
                        };
                        seen.entry(canonical_key(&name)).or_insert(1);
                        drawn.insert(*c);
                        cols.insert(*c, (name, false));
                    }
                }
            }
        }
    }

    // 3. Refuse anything referenced but never named.
    for stmt in &bundle.statements {
        for c in &stmt.refs {
            let scope = reg.scope_of(*c);
            if !scopes.contains_key(&scope) {
                return Err(BaptismError::DanglingScope { col: *c, scope });
            }
        }
    }

    Ok(Baptised {
        reg,
        scopes,
        cols,
        reports,
        drawn,
    })
}

fn uniquify(used: &mut HashSet<Vec<u8>>, base: String) -> String {
    if used.insert(canonical_key(&base)) {
        return base;
    }
    let mut i = 2u32;
    loop {
        let candidate = format!("{}_{}", base, i);
        if used.insert(canonical_key(&candidate)) {
            return candidate;
        }
        i += 1;
    }
}

fn canonical_key(value: &str) -> Vec<u8> {
    value
        .bytes()
        .map(|byte| byte.to_ascii_lowercase())
        .collect()
}
