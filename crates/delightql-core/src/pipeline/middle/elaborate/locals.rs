// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Query-local blocks: the claim ledger the front end built, each
//! query-local relation family (elaborated in authored order), and the
//! parameterized, value and truth definitions the block declares
//! (instantiated at each use). A family whose clauses read their own
//! subject is a fixpoint; its verdict is the family's constructor's.

use super::{Elaborator, Scope, Term};
use crate::pipeline::middle::core::heading::correspondence::{answers_to, lost};
use crate::pipeline::middle::core::heading::{Name, NameState, Visibility};
use crate::pipeline::middle::core::ids::{BinderId, ExprId, RelId};
use crate::pipeline::middle::core::node::rel::ClauseSpec;
use crate::pipeline::middle::core::node::run::{MergeRequest, OpenRun};
use crate::pipeline::middle::core::node::{Item, Naming, PipeOp, Route};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{
    self, CfeDefinition, CteBinding, CteSubjectView, HeadItems, HoDefinition, LexicalHorizon,
    QueryLocalDemand, QueryLocalKind, QueryLocalNames, Query, SigmaDefinition,
};

pub(super) struct Block {
    pub(super) id: usize,
    /// Whether a declaration environment entered after the block opened
    /// leaves it out of reach: a definition's body sees only the blocks
    /// that were open where it was declared.
    pub(super) hidden: bool,
    claims: QueryLocalNames,
    /// The horizon judgments against this block are made at: every
    /// declaration for the body, a clause's own for that clause.
    pub(super) horizon: LexicalHorizon,
    /// The instance frames open where the block was declared: the formals
    /// every definition the block declares can see.
    pub(super) frames: usize,
    /// Each relation family of the block by its subject: its clauses until
    /// it is first read or the block's declarations end, then its relation.
    families: Vec<(Name, Family)>,
    pub(super) hos: Vec<HoDefinition>,
    pub(super) cfes: Vec<CfeDefinition>,
    pub(super) sigmas: Vec<SigmaDefinition>,
}

/// A relation family of a block.
enum Family {
    /// Declared, not yet elaborated: its clauses in authored order.
    Pending(Vec<CteBinding>),
    /// Its clauses are being elaborated: the family stands on the stack of
    /// definitions under construction, which holds its frontier.
    Building,
    Closed(RelId),
}

/// What a name denotes among the query-local blocks.
pub(super) enum Local {
    Body(RelId),
    Frontier(BinderId),
}

fn subject_name(binding: &CteBinding) -> Option<Name> {
    match binding.subject() {
        CteSubjectView::Authored { name, .. } | CteSubjectView::Generated { name } => {
            Some(name.clone())
        }
        CteSubjectView::Frontier => None,
    }
}

impl Elaborator<'_, '_> {
    /// Open a query's block and elaborate its relation families: each when
    /// it is first read, and every other one when the declarations end.
    pub(super) fn open_block(&mut self, query: &Query) -> Result<(), Refusal> {
        let mut families: Vec<(Name, Family)> = Vec::new();
        for binding in query.ctes() {
            let name = subject_name(binding)
                .ok_or_else(|| refuse::outside("a compiler-built query-local binding"))?;
            match families.iter_mut().find(|(n, _)| *n == name) {
                Some((_, Family::Pending(clauses))) => clauses.push(binding.clone()),
                Some((_, Family::Building | Family::Closed(_))) | None => {
                    families.push((name, Family::Pending(vec![binding.clone()])))
                }
            }
        }
        let names: Vec<Name> = families.iter().map(|(n, _)| n.clone()).collect();
        let id = self.next_block;
        self.next_block += 1;
        self.blocks.push(Block {
            id,
            hidden: false,
            claims: query.local_names().clone(),
            horizon: self.input.horizon_all(),
            frames: self.frames.len(),
            families,
            hos: query.hos().to_vec(),
            cfes: query.cfes().to_vec(),
            sigmas: query.sigmas().to_vec(),
        });
        let block = self.blocks.len() - 1;
        // Every parameterized, truth and value family the block declares
        // agrees on one parameter row and badge, used or not.
        super::effect::judge_declared_query_locals(query)?;
        // An effect CTE is elaborated at each demand, never here.
        let effect = |state: Option<&Family>| match state {
            Some(Family::Pending(clauses)) => Some(clauses.iter().any(|c| c.subject().declares_effect())),
            _ => None,
        };
        let acts_pending = names.iter().any(|name| effect(self.family_state(block, name)) == Some(true));
        for name in names {
            if effect(self.family_state(block, &name)) == Some(false) {
                let first = self.b.built().count();
                self.before_acts += usize::from(acts_pending);
                let built = self.family(block, &name);
                self.before_acts -= usize::from(acts_pending);
                built?;
                self.block_bodies.push(first..self.b.built().count());
            }
        }
        // A parameterized definition's heads are judged where the block
        // declares it, whether or not anything applies it.
        let hos: Vec<HoDefinition> = self.blocks[block].hos.iter().filter(|h| !h.declares_effect()).cloned().collect();
        for ho in &hos {
            let definition = self.local_definition(block, ho, ho.name().to_string());
            self.declared_heads(definition)?;
        }
        Ok(())
    }

    /// An effect CTE's declared clauses, as its block holds them.
    pub(super) fn pending_effect_family(&self, block: usize, name: &Name) -> Result<Vec<CteBinding>, Refusal> {
        match self.family_state(block, name) {
            Some(Family::Pending(clauses)) => Ok(clauses.clone()),
            Some(Family::Building | Family::Closed(_)) | None => {
                Err(refuse::elaboration_contract("an effect CTE its block holds no clauses for"))
            }
        }
    }

    /// The key a query-local definition is built and re-entered under: its
    /// block and its declared name. A reference's spelling folds to the same
    /// identifier but not to the same bytes (`COUNTER` reads the family
    /// `counter`), so it never forms the key.
    pub(super) fn local_key(&self, block: usize, declared: &Name) -> String {
        format!("L{}:{declared}", self.blocks[block].id)
    }

    /// A block's relation family's name as its clauses declare it.
    fn declared_family(&self, block: usize, name: &Name) -> Result<Name, Refusal> {
        self.blocks[block]
            .families
            .iter()
            .find(|(n, _)| n == name)
            .map(|(n, _)| n.clone())
            .ok_or_else(|| refuse::elaboration_contract("a query-local relation family its block does not declare"))
    }

    fn family_state(&self, block: usize, name: &Name) -> Option<&Family> {
        self.blocks[block].families.iter().find(|(n, _)| n == name).map(|(_, f)| f)
    }

    fn set_family(&mut self, block: usize, name: &Name, state: Family) -> Option<Family> {
        let families = &mut self.blocks[block].families;
        families
            .iter_mut()
            .find(|(n, _)| n == name)
            .map(|(_, s)| std::mem::replace(s, state))
    }

    /// One relation family of a block, elaborated as a definition under
    /// construction: its heads judged first by the one assembler every
    /// definition crosses, then its clauses in authored order, the first (the
    /// anchor) before the frontier exists, then the family closed by the
    /// core, which judges agreement and recursion. A reference that returns
    /// to it while it is built is judged by the one re-entry judgment of
    /// every definition kind.
    fn family(&mut self, block: usize, name: &Name) -> Result<(), Refusal> {
        let Some(Family::Pending(clauses)) = self.set_family(block, name, Family::Building) else {
            return Err(refuse::elaboration_contract("a query-local relation family elaborated twice"));
        };
        let heads: Vec<_> = clauses.iter().map(|c| &c.authority().head).collect();
        super::instances::judge_signatures(name.as_str(), heads.iter().copied())?;
        self.input.assemble_heads(name.as_str(), &heads)?;
        let definition = self.local_key(block, &self.declared_family(block, name)?);
        self.building.push(super::Building {
            definition,
            display: name.to_string(),
            local: true,
            form: super::Form::Relation,
            actuals: Vec::new(),
            stands_in: false,
            frontier: None,
        });
        let result = self.family_clauses(block, name, &clauses);
        self.building.pop();
        let rel = result?;
        self.set_family(block, name, Family::Closed(rel));
        Ok(())
    }

    fn family_clauses(&mut self, block: usize, name: &Name, clauses: &[CteBinding]) -> Result<RelId, Refusal> {
        let anchor = self.clause(block, &clauses[0])?;
        let frontier = match clauses.len() > 1 {
            true => Some(self.b.frontier(anchor)?),
            false => None,
        };
        if let Some(top) = self.building.last_mut() {
            top.frontier = frontier;
        }
        let mut all = vec![ClauseSpec {
            guard: None,
            body: anchor,
            badged: clauses[0].authority().head.fixpoint.is_badged(),
            closed: clauses[0].authority().head.items.listed().is_some(),
            fact: false,
        }];
        for binding in &clauses[1..] {
            all.push(ClauseSpec {
                guard: None,
                body: self.clause(block, binding)?,
                badged: binding.authority().head.fixpoint.is_badged(),
                closed: binding.authority().head.items.listed().is_some(),
                fact: false,
            });
        }
        self.b.close_family(name.as_str(), name.as_str(), frontier, all)
    }

    /// One clause: its body read at its horizon and projected through its
    /// head, both in the block's declaration environment.
    pub(super) fn clause(&mut self, block: usize, binding: &CteBinding) -> Result<RelId, Refusal> {
        let frames = self.blocks[block].frames;
        let horizon = binding.authority().horizon;
        let saved = std::mem::replace(&mut self.blocks[block].horizon, horizon);
        let body = self.apart(block + 1, frames, |e| {
            let body = e.chain(binding.body())?;
            e.head(body, &binding.authority().head.items)
        });
        self.blocks[block].horizon = saved;
        body
    }

    /// A head is an ordered projection of its body's heading; a glob head
    /// is the body itself. A head name answers only to a position of its
    /// own body: never to a formal, never to anything around the body.
    pub(super) fn head(&mut self, body: RelId, items: &HeadItems) -> Result<RelId, Refusal> {
        let HeadItems::Listed(items) = items else {
            return Ok(body);
        };
        self.scopes.push(Scope {
            run: OpenRun::new(),
            arms: Vec::new(),
            reversed: false,
            qualified_only: false,
            pending: Some(Term {
                rel: body,
                scope: None,
                route: Route::Plain,
                merge: MergeRequest::None,
                names_scope: false,
                requalifies: true,
                born: crate::pipeline::middle::core::node::run::Born::Written,
            }),
        });
        let result = self.head_items(items);
        match result {
            Ok(out) => {
                let input = self.close_top()?;
                self.b.pipe(input, PipeOp::Project(out))
            }
            Err(e) => {
                self.scopes.pop();
                Err(e)
            }
        }
    }

    fn head_items(&mut self, items: &[facade::HeadItem]) -> Result<Vec<Item>, Refusal> {
        self.materialize()?;
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            // SUPPLY IS ELABORATION: a ground term supplies its constant to
            // every row, named by its label; unlabeled, it abstains.
            let (expr, unlabeled) = match &item.supply {
                facade::Supply::Ref(name) => (self.body_column(name)?, Naming::Reference),
                facade::Supply::Ground(literal) => (self.b.constant(literal.clone()), Naming::Abstain),
            };
            out.push(Item {
                expr,
                naming: match &item.label {
                    Some(label) => Naming::As(label.clone()),
                    None => unlabeled,
                },
            });
        }
        Ok(out)
    }

    /// The body position a head name answers to (heads-law: THE PROJECTION
    /// LAW, a head name the body lacks refuses).
    fn body_column(&mut self, name: &Name) -> Result<ExprId, Refusal> {
        let top = self.scopes.last().expect("the head's scope");
        let heading = top.run.heading();
        match answers_to(heading, name).as_slice() {
            [one] => {
                let cell = top.run.outputs()[*one].1;
                Ok(self.cell_value(cell))
            }
            [_, _, ..] => Err(refuse::ambiguous_column(name)),
            [] if !lost(heading, name).is_empty() => Err(refuse::ambiguous_column(name)),
            [] if heading.positions().iter().any(|p| {
                p.visibility == Visibility::Latent
                    && matches!(&p.name, NameState::Authored(n) | NameState::Catalog(n) if n == name)
            }) =>
            {
                Err(refuse::latent_name(name.as_str()))
            }
            [] => Err(refuse::head_name_absent(name)),
        }
    }

    /// The innermost block claiming `name` for `demand`, with the kind it
    /// claims; `None` when no block claims it.
    pub(super) fn claim(
        &mut self,
        name: &Name,
        demand: QueryLocalDemand,
    ) -> Result<Option<(usize, QueryLocalKind)>, Refusal> {
        for i in (0..self.blocks.len()).rev() {
            let block = &self.blocks[i];
            if block.hidden {
                continue;
            }
            if let Some(kind) = self.input.query_local(&block.claims, name, block.horizon, demand)? {
                return Ok(Some((i, kind)));
            }
        }
        Ok(None)
    }

    /// The kind the innermost block claiming `name` claims it as.
    pub(super) fn claimed_kind(&self, name: &Name) -> Option<QueryLocalKind> {
        let input = self.input;
        self.blocks
            .iter()
            .rev()
            .filter(|block| !block.hidden)
            .find_map(|block| input.query_local_claim(&block.claims, name))
    }

    /// What an unqualified relation name denotes among the query-local
    /// blocks; `None` when no block claims it as a relation. The family is
    /// the claiming block's own: a fixpoint's frontier is found through the
    /// declaration that defines it, never by its spelling elsewhere. A
    /// family not yet elaborated is elaborated where it is first read; a
    /// family under construction is re-entered: its own clause reads its
    /// frontier, and a return through another definition is a cycle.
    pub(super) fn local_relation(&mut self, name: &Name) -> Result<Option<Local>, Refusal> {
        let Some((i, kind)) = self.claim(name, QueryLocalDemand::Relation)? else {
            return Ok(None);
        };
        match kind {
            QueryLocalKind::Relation => {
                if matches!(self.family_state(i, name), Some(Family::Pending(_))) {
                    self.family(i, name)?;
                }
                match self.family_state(i, name) {
                    Some(Family::Closed(rel)) => Ok(Some(Local::Body(*rel))),
                    Some(Family::Building) => {
                        let key = self.local_key(i, &self.declared_family(i, name)?);
                        let at = self
                            .reentry(&key, name.as_str())?
                            .ok_or_else(|| refuse::elaboration_contract("a query-local family built off the definition stack"))?;
                        match self.building[at].frontier {
                            Some(frontier) => Ok(Some(Local::Frontier(frontier))),
                            None => Err(refuse::anchor_unresolved(name.as_str())),
                        }
                    }
                    Some(Family::Pending(_)) | None => Err(refuse::elaboration_contract(
                        "a query-local relation read before its family is elaborated",
                    )),
                }
            }
            // Inside its own body a parameterized definition's mention with
            // no parameter row is its self-reference (recursion-contract-law:
            // a self-reference is any reference to the target's name inside
            // its own body).
            QueryLocalKind::HigherOrder => {
                let declared = self.blocks[i]
                    .hos
                    .iter()
                    .find(|h| h.name() == name)
                    .map(|h| h.name().clone())
                    .unwrap_or_else(|| name.clone());
                let key = self.local_key(i, &declared);
                match self.reentry(&key, name.as_str())? {
                    Some(at) => match self.building[at].frontier {
                        Some(frontier) => Ok(Some(Local::Frontier(frontier))),
                        None => Err(refuse::anchor_first(name.as_str(), true)),
                    },
                    None => Err(refuse::outside("a query-local definition of another kind")),
                }
            }
            QueryLocalKind::Value
            | QueryLocalKind::Sigma
            | QueryLocalKind::EffectRelation
            | QueryLocalKind::EffectHigherOrder => {
                Err(refuse::outside("a query-local definition of another kind"))
            }
        }
    }
}
