// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! WHAT A NAME REFERS TO: one judgment per mention, `World::refer`, made
//! from where the mention stands (a [`Standpoint`]) over the catalog state
//! its statement reads (a [`World`]). Its answer names the identity and the
//! road of the namespace law that reached it (BARE SELECTION, ADDRESSING
//! ROUTES, THE TWO AXES, THE CROSSINGS); consumers build from that answer
//! and never select again.

pub(crate) mod imprint;

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{
    self, CandidateFact, CatalogRows, EntityType, MentionFact, NamespaceKind, Qualifier, QualifierRoute,
};

/// One namespace row: its identity and its path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Namespace {
    pub(crate) id: i64,
    pub(crate) fq: String,
}

/// An authored definition family, and the activation its body stands at.
#[derive(Clone, Debug)]
pub(crate) struct Family {
    name: Name,
    entity: i64,
    namespace: Namespace,
    load: i64,
    definition: String,
}

impl Family {
    pub(crate) fn name(&self) -> &Name {
        &self.name
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace.fq
    }

    pub(crate) fn entity_id(&self) -> i64 {
        self.entity
    }

    /// The family's stored source: its clauses in authored order.
    pub(crate) fn definition(&self) -> &str {
        &self.definition
    }
}

/// A declared edge: its pair's canonical spellings (in the stored order),
/// its context, and the family holding its clause.
#[derive(Clone, Debug)]
pub(crate) struct Edge {
    pub(crate) left: String,
    pub(crate) right: String,
    pub(crate) context: String,
    pub(crate) family: Family,
}

/// An entity the engine serves: a stored relation, a session object, a
/// bin. It has no authored body.
#[derive(Clone, Debug)]
pub(crate) struct Served {
    name: Name,
    kind: EntityType,
    entity: i64,
    namespace: String,
}

impl Served {
    pub(crate) fn name(&self) -> &Name {
        &self.name
    }

    pub(crate) fn kind(&self) -> EntityType {
        self.kind
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn entity_id(&self) -> i64 {
        self.entity
    }
}

/// An object an imprint will create, read by a sibling entity's rule
/// before it exists: its columns and where it will be read.
#[derive(Clone, Debug)]
pub(crate) struct Declared {
    name: Name,
    namespace: String,
    columns: Vec<(String, Option<String>)>,
    connection: i64,
    schema: Option<String>,
}

impl Declared {
    pub(crate) fn name(&self) -> &Name {
        &self.name
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Each column's name and declared type, in the object's order.
    pub(crate) fn columns(&self) -> &[(String, Option<String>)] {
        &self.columns
    }

    pub(crate) fn connection(&self) -> i64 {
        self.connection
    }

    pub(crate) fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }
}

/// The identity a mention selects.
#[derive(Clone, Debug)]
pub(crate) enum Referent {
    Family(Family),
    Served(Served),
    Declared(Declared),
}

impl Referent {
    pub(crate) fn name(&self) -> &Name {
        match self {
            Referent::Family(f) => f.name(),
            Referent::Served(s) => s.name(),
            Referent::Declared(d) => d.name(),
        }
    }

    pub(crate) fn namespace(&self) -> &str {
        match self {
            Referent::Family(f) => f.namespace(),
            Referent::Served(s) => s.namespace(),
            Referent::Declared(d) => d.namespace(),
        }
    }

    /// The identity a candidate row names: an authored definition, whose
    /// body stands at the load that activated it, or an entity the engine
    /// serves.
    fn of(c: CandidateFact) -> Referent {
        let name = match c.stropped {
            true => Name::stropped(c.name),
            false => Name::new(c.name),
        };
        if !c.kind.is_authored_definition() {
            return Referent::Served(Served {
                name,
                kind: c.kind,
                entity: c.entity_id,
                namespace: c.namespace,
            });
        }
        Referent::Family(Family {
            name,
            entity: c.entity_id,
            load: c.cartridge_id,
            definition: c.definition.unwrap_or_default(),
            namespace: Namespace {
                id: c.namespace_id,
                fq: c.namespace,
            },
        })
    }

    /// The catalog identity; an object an imprint will create has none yet.
    pub(crate) fn entity_id(&self) -> Option<i64> {
        match self {
            Referent::Family(f) => Some(f.entity_id()),
            Referent::Served(s) => Some(s.entity_id()),
            Referent::Declared(_) => None,
        }
    }
}

/// The judgment of one mention, and the road of the law that decided it.
#[derive(Clone, Debug)]
pub(crate) enum Referred {
    /// A bare name the standpoint's own tiers answer: a lexical link.
    Linked(Referent),
    /// A qualified name, answered in the one namespace its route reaches.
    Routed(Referent),
    /// A definition body's free name, answered in the data world its
    /// namespace's grounding bound.
    Grounded(Referent),
    /// Nothing answers. `free` names the declaring namespace when the name
    /// is a definition body's free data name.
    Unanswered { free: Option<String> },
    /// The deciding tier holds several identities; the refusal names them.
    Ambiguous(Refusal),
    /// A name under the reserved target provider `sys::target`: the target
    /// answers it, and no DQL identity does (THE TARGET SURFACE IS OPEN,
    /// step 4).
    Provider,
}

impl Referred {
    /// The identity selected, if any; ambiguity is the caller's refusal.
    pub(crate) fn referent(self) -> Result<Option<Referent>, Refusal> {
        match self {
            Referred::Linked(r) | Referred::Routed(r) | Referred::Grounded(r) => Ok(Some(r)),
            Referred::Unanswered { .. } | Referred::Provider => Ok(None),
            Referred::Ambiguous(refusal) => Err(refusal),
        }
    }
}

/// What a free name of the standpoint's text reads once its tiers miss.
#[derive(Clone, Debug)]
enum Free {
    /// A statement's own text: a miss is a miss.
    Statement,
    /// A definition body: its free names read the data world its
    /// namespace's grounding bound, when one is bound.
    Body { data: Option<Namespace> },
}

/// Where a mention stands (THE TWO AXES): its primary definition context,
/// the namespaces whose entities answer bare after it (closed under
/// exposure), its qualifier aliases, the grounded closure the primary stands
/// in, and what a free name reads. Read once from the catalog; immutable.
#[derive(Clone, Debug)]
pub(crate) struct Standpoint {
    primary: Namespace,
    imports: Vec<i64>,
    aliases: Vec<(String, i64)>,
    derivatives: BTreeMap<i64, i64>,
    free: Free,
}

impl Standpoint {
    /// The one namespace a written qualifier reaches (ADDRESSING ROUTES): a
    /// one-segment alias of this standpoint, else the exact path, or the
    /// primary's child for `.::`; never a search. Inside a grounded closure a
    /// source reads as its derivative.
    pub(crate) fn route(&self, world: &World<'_>, qualifier: &Qualifier) -> Option<Namespace> {
        let alias = match (qualifier.route(), qualifier.segments()) {
            (QualifierRoute::Exact, [one]) => self.aliases.iter().find(|(alias, _)| alias == one),
            _ => None,
        };
        let reached = match alias {
            Some((_, target)) => *target,
            None => world.tree.named(&qualifier.fq_in(&self.primary.fq))?.id,
        };
        world.tree.namespace(self.derivatives.get(&reached).copied().unwrap_or(reached))
    }

    fn body_of(&self) -> Option<&str> {
        match self.free {
            Free::Body { .. } => Some(&self.primary.fq),
            Free::Statement => None,
        }
    }
}

/// One namespace row as the world reads it.
#[derive(Debug)]
struct Row {
    id: i64,
    fq: String,
    kind: NamespaceKind,
    bound: Option<String>,
}

/// Every namespace row of the world, read once.
#[derive(Debug)]
struct Tree {
    rows: Vec<Row>,
}

impl Tree {
    fn named(&self, fq: &str) -> Option<&Row> {
        self.rows.iter().find(|r| r.fq == fq)
    }

    fn namespace(&self, id: i64) -> Option<Namespace> {
        self.rows.iter().find(|r| r.id == id).map(|r| Namespace {
            id: r.id,
            fq: r.fq.clone(),
        })
    }

    /// The archive root that makes `fq` inert (THE CROSSINGS: an imprinted
    /// source is archived, inert, under a `blueprint`-kind root).
    fn archive_over(&self, fq: &str) -> Option<&str> {
        self.rows
            .iter()
            .find(|r| r.kind == NamespaceKind::Blueprint && facade::is_within(fq, &r.fq))
            .map(|r| r.fq.as_str())
    }
}

/// The catalog state one statement reads: its rows, an imprint's forward
/// declarations when its bodies are being compiled, and the one identity a
/// judgment of the world after a retraction leaves out.
#[derive(Clone)]
pub(crate) struct World<'c> {
    rows: CatalogRows<'c>,
    tree: Rc<Tree>,
    imprint: Option<Rc<imprint::Imprint>>,
    without: Option<i64>,
}

impl<'c> World<'c> {
    pub(crate) fn open(rows: CatalogRows<'c>) -> facade::Result<Self> {
        let rows_read = rows.namespaces()?;
        let tree = Tree {
            rows: rows_read
                .into_iter()
                .filter_map(|n| {
                    n.fq.map(|fq| Row {
                        id: n.id,
                        fq,
                        kind: n.kind,
                        bound: n.default_data_ns,
                    })
                })
                .collect(),
        };
        Ok(World {
            rows,
            tree: Rc::new(tree),
            imprint: None,
            without: None,
        })
    }

    /// This world with an imprint's forward declarations in force.
    pub(crate) fn imprinting(mut self, imprint: Rc<imprint::Imprint>) -> Self {
        self.imprint = Some(imprint);
        self
    }

    /// This world as it stands once `entity` is gone.
    pub(crate) fn without(&self, entity: i64) -> Self {
        let mut world = self.clone();
        world.without = Some(entity);
        world
    }

    /// The session's standpoint: `home`, the live enlist set, the session's
    /// aliases.
    pub(crate) fn session(&self) -> facade::Result<Standpoint> {
        let home = self.must("home")?;
        let imports = self.rows.session_enlists(home.id)?;
        let aliases = self.rows.session_aliases()?;
        self.standpoint(home, imports, aliases, Free::Statement)
    }

    /// A namespace's own standpoint, for text that stands in it outside any
    /// definition (a consulted file's goal): its captured edges.
    pub(crate) fn within(&self, fq: &str) -> facade::Result<Standpoint> {
        let namespace = self.must(fq)?;
        let imports = self.rows.namespace_imports(namespace.id)?;
        let aliases = self.rows.namespace_aliases(namespace.id)?;
        self.standpoint(namespace, imports, aliases, Free::Statement)
    }

    /// A definition family's body: its own namespace, under the imports and
    /// aliases the load that admitted it captured.
    pub(crate) fn body_of(&self, family: &Family) -> facade::Result<Standpoint> {
        self.body(family.namespace.clone(), family.load)
    }

    /// The body of the activated entity `entity`, read from its activation.
    pub(crate) fn body_of_entity(&self, entity: i64) -> facade::Result<Standpoint> {
        let (fq, load) = self
            .rows
            .activation(entity)?
            .ok_or_else(|| refuse::contract("a recorded mention whose definition has no activation"))?;
        let namespace = self.must(&fq)?;
        self.body(namespace, load)
    }

    fn body(&self, namespace: Namespace, load: i64) -> facade::Result<Standpoint> {
        let imports = self.rows.load_imports(load)?;
        let aliases = self.rows.load_aliases(load)?;
        let data = self
            .tree
            .rows
            .iter()
            .find(|r| r.id == namespace.id)
            .and_then(|r| r.bound.as_deref())
            .and_then(|fq| self.tree.named(fq))
            .and_then(|r| self.tree.namespace(r.id));
        self.standpoint(namespace, imports, aliases, Free::Body { data })
    }

    fn standpoint(
        &self,
        primary: Namespace,
        imports: Vec<i64>,
        aliases: Vec<(String, i64)>,
        free: Free,
    ) -> facade::Result<Standpoint> {
        // Exposure is re-export, closed transitively; the primary's own
        // exposures are part of its declared graph, beneath its own entities.
        let exposures = self.rows.exposures()?;
        let mut reach: BTreeSet<i64> = imports.into_iter().collect();
        // The prelude is in every lexical world, a consulted body's included.
        if let Some(prelude) = self.tree.named(PRELUDE) {
            reach.insert(prelude.id);
        }
        reach.insert(primary.id);
        let mut frontier: Vec<i64> = reach.iter().copied().collect();
        while let Some(at) = frontier.pop() {
            for (_, exposed) in exposures.iter().filter(|(exposing, _)| *exposing == at) {
                if reach.insert(*exposed) {
                    frontier.push(*exposed);
                }
            }
        }
        reach.remove(&primary.id);
        let derivatives = self.rows.grounding_closure(primary.id)?.into_iter().collect();
        Ok(Standpoint {
            primary,
            imports: reach.into_iter().collect(),
            aliases,
            derivatives,
            free,
        })
    }

    fn must(&self, fq: &str) -> facade::Result<Namespace> {
        self.tree
            .named(fq)
            .and_then(|r| self.tree.namespace(r.id))
            .ok_or_else(|| refuse::contract("a standpoint whose namespace has no catalog row"))
    }

    /// Every edge declared where `at` stands: in its primary namespace or a
    /// namespace it imports (`&` holds only declared edges, and only the
    /// ones in view).
    /// An edge's keys are selected as a bare name is (BARE SELECTION): the
    /// primary tier's edge under a pair and context decides before the
    /// import tier's.
    pub(crate) fn edges(&self, at: &Standpoint) -> facade::Result<Vec<Edge>> {
        let mut edges = self.declared_edges(&[at.primary.id])?;
        let imported = self.declared_edges(&at.imports)?;
        let held: Vec<(String, String, String)> =
            edges.iter().map(|e| (e.left.clone(), e.right.clone(), e.context.clone())).collect();
        edges.extend(
            imported
                .into_iter()
                .filter(|e| !held.contains(&(e.left.clone(), e.right.clone(), e.context.clone()))),
        );
        Ok(edges)
    }

    /// Every edge the namespace `qualifier` routes to from `at` declares,
    /// enlisted or not (EVERY NAME IS TOTAL); none where it routes nowhere.
    pub(crate) fn edges_declared_by(&self, at: &Standpoint, qualifier: &Qualifier) -> facade::Result<Vec<Edge>> {
        match at.route(self, qualifier) {
            Some(namespace) => self.declared_edges(&[namespace.id]),
            None => Ok(Vec::new()),
        }
    }

    fn declared_edges(&self, namespaces: &[i64]) -> facade::Result<Vec<Edge>> {
        self.rows
            .edge_declarations(namespaces)?
            .into_iter()
            .filter(|e| self.visible(&e.candidate))
            .map(|e| match Referent::of(e.candidate) {
                Referent::Family(family) => Ok(Edge {
                    left: e.left,
                    right: e.right,
                    context: e.context,
                    family,
                }),
                Referent::Served(_) | Referent::Declared(_) => {
                    Err(refuse::contract("an edge declaration whose entity is no authored definition"))
                }
            })
            .collect()
    }

    /// What `name`, written under `qualifier`, refers to at `at`.
    /// Ambiguity is a refusal.
    pub(crate) fn refer(
        &self,
        at: &Standpoint,
        name: &Name,
        qualifier: Option<&Qualifier>,
    ) -> facade::Result<Option<Referent>> {
        self.judge(at, name, qualifier)?.referent()
    }

    /// THE ONE JUDGMENT of a mention at a standpoint.
    pub(crate) fn judge(&self, at: &Standpoint, name: &Name, qualifier: Option<&Qualifier>) -> facade::Result<Referred> {
        let key = name.canonical();
        if qualifier.is_some_and(target_provider) {
            return Ok(Referred::Provider);
        }
        let referred = match qualifier {
            Some(qualifier) => match at.route(self, qualifier) {
                None => Referred::Unanswered { free: None },
                Some(namespace) => {
                    if let Some(archive) = self.tree.archive_over(&namespace.fq) {
                        return Err(refuse::blueprint_inert(archive));
                    }
                    match self.decide(name, facts(self.held(&key, &[namespace.id])?))? {
                        Decided::One(r) => Referred::Routed(r),
                        Decided::None | Decided::Made(_) => Referred::Unanswered { free: None },
                        Decided::Many(refusal) => Referred::Ambiguous(refusal),
                    }
                }
            },
            None => self.bare(at, name, &key)?,
        };
        match &self.imprint {
            Some(imprint) if imprint.covers(at.body_of()) => imprint.answer(name, qualifier.is_none(), referred),
            _ => Ok(referred),
        }
    }

    /// BARE SELECTION with the statement's own creations among the
    /// candidates: each object the statement creates (`made`, in the order
    /// the statement creates them) stands in the tier of its durable owner
    /// as the owner's entity of that name — a session object as the owner's
    /// overlay, which hides the owner's other entities of the name, a
    /// durable one as the owner's own — the later of one owner and kind
    /// replacing the earlier. The first nonempty tier decides; more than
    /// one identity in it refuses as ambiguous, whichever statement made
    /// them. `Catalog` when the deciding tier holds no creation: the
    /// catalog's own judgment stands.
    pub(crate) fn bare_among(&self, at: &Standpoint, name: &Name, made: &[Made<'_>]) -> facade::Result<BareChoice> {
        for tier in self.bare_tiers(at, &name.canonical(), made)? {
            match self.decide(name, tier)? {
                Decided::None => continue,
                Decided::One(_) => return Ok(BareChoice::Catalog),
                Decided::Made(k) => return Ok(BareChoice::Made(k)),
                Decided::Many(refusal) => return Err(refusal),
            }
        }
        Ok(BareChoice::Catalog)
    }

    /// The two tiers of BARE SELECTION for `key`: the primary's own
    /// entities, then the import tier owner by owner (a data namespace's
    /// session objects under the name when it owns any, else its own
    /// entities), the statement's creations (`made`) standing as their
    /// owners' entities.
    fn bare_tiers(&self, at: &Standpoint, key: &str, made: &[Made<'_>]) -> facade::Result<[Vec<Candidate>; 2]> {
        let mut by_owner: BTreeMap<i64, (Option<usize>, Option<usize>)> = BTreeMap::new();
        for (k, m) in made.iter().enumerate() {
            let owner = self
                .tree
                .named(m.owner)
                .map(|r| r.id)
                .ok_or_else(|| refuse::contract("a created object whose owner has no catalog row"))?;
            let entry = by_owner.entry(owner).or_default();
            match m.session {
                true => entry.0 = Some(k),
                false => entry.1 = Some(k),
            }
        }
        let made_candidate = |k: usize| Candidate::Made(k, made[k].owner.to_string());
        let mut primary: Vec<Candidate> = self.held(key, &[at.primary.id])?.into_iter().map(Candidate::Fact).collect();
        if let Some((_, Some(k))) = by_owner.get(&at.primary.id) {
            primary.push(made_candidate(*k));
        }
        let mut imported: Vec<Candidate> = Vec::new();
        let mut catalog = self.imported_by_owner(key, &at.imports)?;
        for owner in at.imports.iter().filter(|o| by_owner.contains_key(o)) {
            catalog.entry(*owner).or_insert((false, Vec::new()));
        }
        for (owner, (overlay, facts)) in catalog {
            match (by_owner.get(&owner).copied().unwrap_or((None, None)), overlay) {
                ((Some(k), _), _) => imported.push(made_candidate(k)),
                ((None, _), true) => imported.extend(facts.into_iter().map(Candidate::Fact)),
                ((None, durable), false) => {
                    imported.extend(facts.into_iter().map(Candidate::Fact));
                    imported.extend(durable.map(made_candidate));
                }
            }
        }
        Ok([primary, imported])
    }

    /// BARE SELECTION: the primary tier, then the import tier; the first
    /// nonempty tier decides. A body's free name then reads its bound data
    /// world.
    fn bare(&self, at: &Standpoint, name: &Name, key: &str) -> facade::Result<Referred> {
        for tier in self.bare_tiers(at, key, &[])? {
            match self.decide(name, tier)? {
                Decided::One(r) => return Ok(Referred::Linked(r)),
                Decided::Many(refusal) => return Ok(Referred::Ambiguous(refusal)),
                Decided::Made(_) => return Err(refuse::contract("a creation selected where the statement creates none")),
                Decided::None => {}
            }
        }
        Ok(match &at.free {
            Free::Statement => Referred::Unanswered { free: None },
            Free::Body { data: None } => Referred::Unanswered {
                free: Some(at.primary.fq.clone()),
            },
            Free::Body { data: Some(data) } => match self.decide(name, facts(self.held(key, &[data.id])?))? {
                Decided::One(r) => Referred::Grounded(r),
                Decided::Many(refusal) => Referred::Ambiguous(refusal),
                Decided::None | Decided::Made(_) => Referred::Unanswered {
                    free: Some(at.primary.fq.clone()),
                },
            },
        })
    }

    /// The candidates activated under the name in `namespaces`.
    fn held(&self, key: &str, namespaces: &[i64]) -> facade::Result<Vec<CandidateFact>> {
        let found = self.rows.candidates(key, namespaces)?;
        Ok(found.into_iter().filter(|c| self.visible(c)).collect())
    }

    /// The import tier owner by owner: each data namespace's session
    /// objects under the name when it owns any (`true`), else its own
    /// entities.
    fn imported_by_owner(&self, key: &str, namespaces: &[i64]) -> facade::Result<BTreeMap<i64, (bool, Vec<CandidateFact>)>> {
        let mut out: BTreeMap<i64, (bool, Vec<CandidateFact>)> = BTreeMap::new();
        if namespaces.is_empty() {
            return Ok(out);
        }
        for (owner, candidate) in self.rows.overlay_candidates(key, namespaces)? {
            if self.visible(&candidate) {
                let entry = out.entry(owner).or_insert((true, Vec::new()));
                entry.1.push(candidate);
            }
        }
        for candidate in self.held(key, namespaces)? {
            match out.get_mut(&candidate.namespace_id) {
                Some((true, _)) => {}
                Some((false, facts)) => facts.push(candidate),
                None => {
                    out.insert(candidate.namespace_id, (false, vec![candidate]));
                }
            }
        }
        Ok(out)
    }

    fn visible(&self, candidate: &CandidateFact) -> bool {
        Some(candidate.entity_id) != self.without && self.tree.archive_over(&candidate.namespace).is_none()
    }

    /// Zero identities is a miss, one selects, more refuse naming every
    /// candidate: the catalog's identities by namespace, then the
    /// statement's creations by owner.
    fn decide(&self, name: &Name, tier: Vec<Candidate>) -> facade::Result<Decided> {
        let mut seen = BTreeSet::new();
        let mut facts: Vec<CandidateFact> = Vec::new();
        let mut made: Vec<(usize, String)> = Vec::new();
        for candidate in tier {
            match candidate {
                Candidate::Fact(c) => {
                    if seen.insert(c.entity_id) {
                        facts.push(c);
                    }
                }
                Candidate::Made(k, owner) => made.push((k, owner)),
            }
        }
        facts.sort_by(|a, b| (a.namespace.as_str(), a.entity_id).cmp(&(b.namespace.as_str(), b.entity_id)));
        made.sort_by(|a, b| a.1.cmp(&b.1));
        match (facts.len(), made.len()) {
            (0, 0) => Ok(Decided::None),
            (1, 0) => Ok(Decided::One(Referent::of(facts.remove(0)))),
            (0, 1) => Ok(Decided::Made(made[0].0)),
            _ => {
                let listed = facts
                    .iter()
                    .map(|c| {
                        let kind = c.kind.variant_name();
                        match &c.source_uri {
                            Some(uri) => format!("{} ({kind}, from {uri})", c.namespace),
                            None => format!("{} ({kind})", c.namespace),
                        }
                    })
                    .chain(made.iter().map(|(_, owner)| format!("{owner} (an object this statement creates)")))
                    .collect::<Vec<_>>();
                Ok(Decided::Many(refuse::ambiguous_entity(name.as_str(), &listed)))
            }
        }
    }
}

enum Decided {
    None,
    One(Referent),
    Made(usize),
    Many(Refusal),
}

/// Catalog identities as tier candidates.
fn facts(found: Vec<CandidateFact>) -> Vec<Candidate> {
    found.into_iter().map(Candidate::Fact).collect()
}

/// An object the statement creates, as bare selection reads it: the
/// durable namespace that owns it and whether it is a session object.
pub(crate) struct Made<'a> {
    pub(crate) owner: &'a str,
    pub(crate) session: bool,
}

/// What a bare name selects among the catalog's identities and the
/// statement's creations: one creation (its index), or the catalog's own
/// judgment.
pub(crate) enum BareChoice {
    Made(usize),
    Catalog,
}

/// A candidate of a tier: a catalog identity, or a creation of the
/// statement (its index and owner).
enum Candidate {
    Fact(CandidateFact),
    Made(usize, String),
}


/// The namespace every lexical world imports.
const PRELUDE: &str = "std::prelude";

/// Whether a written qualifier is the reserved target provider
/// `sys::target`: a virtual provider, not a namespace, reached by its exact
/// path alone.
fn target_provider(qualifier: &Qualifier) -> bool {
    qualifier.route() == QualifierRoute::Exact && qualifier.segments() == ["sys", "target"]
}

/// THE DEPENDENTS OF A REMOVAL (namespace-law: no remover leaves a
/// published definition depending on what it removes): the definitions
/// holding a recorded mention that, judged at the standpoint of the body
/// recording it, `reaches` what is removed. Each is named once, as
/// `namespace.entity`, in name order.
fn depending(
    world: &World<'_>,
    mentions: Vec<MentionFact>,
    reaches: impl Fn(&Standpoint, &MentionFact) -> facade::Result<bool>,
) -> facade::Result<Vec<String>> {
    let mut named = BTreeSet::new();
    for mention in mentions {
        if reaches(&world.body_of_entity(mention.entity_id)?, &mention)? {
            named.insert(format!("{}.{}", mention.namespace, mention.entity));
        }
    }
    Ok(named.into_iter().collect())
}

/// RETRACTING: the definitions whose recorded mention of `name` still
/// selects the identity `identity`.
pub(crate) fn dependents(rows: CatalogRows<'_>, identity: i64, name: &str) -> facade::Result<Vec<String>> {
    let world = World::open(rows)?;
    let mentions = rows.mentions_of(&Name::new(name).canonical(), identity)?;
    depending(&world, mentions, |at, mention| {
        let qualifier = mention.qualifier.as_deref().map(Qualifier::from_spelled);
        let selected = world.judge(at, &Name::new(mention.name.as_str()), qualifier.as_ref())?.referent();
        Ok(selected.ok().flatten().and_then(|r| r.entity_id()) == Some(identity))
    })
}

/// The definitions outside `namespace` whose recorded qualifier routes into
/// it (ADDRESSING ROUTES at the body's own standpoint).
pub(crate) fn routed_into(rows: CatalogRows<'_>, namespace: &str) -> facade::Result<Vec<String>> {
    let world = World::open(rows)?;
    let outside: Vec<MentionFact> = rows.qualified_mentions()?.into_iter().filter(|m| m.namespace != namespace).collect();
    depending(&world, outside, |at, mention| {
        let routed = mention
            .qualifier
            .as_deref()
            .and_then(|spelled| at.route(&world, &Qualifier::from_spelled(spelled)));
        Ok(routed.is_some_and(|reached| reached.fq == namespace))
    })
}
