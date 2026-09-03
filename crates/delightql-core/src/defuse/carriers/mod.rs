// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE CARRIER AUTHORITY — the one module that constructs structural
//! carriers, and the only holder of the relationship between a landing
//! and the carrier it names.
//!
//! Three constructions exist, and each is one operation of this module:
//! a higher-order call binds its relation actuals, its piped source and
//! the caller row it absorbs ([`resolve_carriers`]); a residual's
//! construction binds the row it captured with the configured values
//! beside it ([`prepare_residual_prefix`]); an effect's residual stands
//! over the plan scratch that is its evaluation row
//! ([`construct_effect_residual`]). Everything those operations do to a
//! record — binding a body, choosing the part it is bound as, recording
//! what a capture stands in place of and the token it publishes, settling
//! which carrier the caller row became — is private to this module. The
//! rest of the crate cannot name those transitions.
//!
//! Outside, a record is transformed only whole: a nested call inherits a
//! carrier by naming the landing of the record that holds it; a record
//! absorbs, is seeded by, or is crossed by another record. What leaves the
//! record are readings — the formals as the row a scalar actual stands
//! over, the definitions emitted ahead of the body, the realized row a
//! residual stands over — and the lexical authority mints frames from
//! those readings alone.
//!
//! The identity authority binds a carrier only for this module: its
//! `bind_carrier` takes a [`CarrierBind`] witness that only this module
//! constructs, and returns the landing and the carrier as one value. The
//! binding authority mints the definition from the same body and returns
//! it whole ([`crate::pipeline::bindings::BoundCarrier`]); the record
//! stores it whole and never takes it apart.

mod call;
mod residual;

pub(in crate::defuse) use call::resolve_carriers;
pub(in crate::defuse) use residual::{
    construct_effect_residual, prepare_residual_prefix, PreparedResidualPrefix, ResidualCapture,
    ResidualEvaluationRow,
};

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::resolved::CteBinding;
use crate::pipeline::bindings::BoundCarrier;
use crate::pipeline::query_features::HoParamBindings;
use crate::relation::form::HoPart;
use crate::relation::{CarrierRow, Planning, ScratchRow, SemanticRelation, StructuralRelation};

/// THE WITNESS THAT A CARRIER IS BEING BOUND BY THIS AUTHORITY. The
/// identity authority reserves a landing and instantiates a body under it
/// only when handed one, and only this module constructs one.
pub struct CarrierBind(());

/// A ROW THE COMPILER OWNS, REALIZED — THE PROOF a position stands over.
/// A carrier some record bound, or a scratch some plan allocated; minted
/// only here from the record's own carrier, or from the receipt of a
/// scratch allocation. Its identity is read inside the carrier authority
/// and nowhere else: the lexical authority reads the row through the
/// proof, under a witness only it constructs, so the proof stays attached
/// until the frame is minted and no identity leaves this value on the way.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CompilerRow {
    relation: SemanticRelation,
}

impl CompilerRow {
    fn carrier(row: CarrierRow) -> Self {
        CompilerRow {
            relation: row.relation(),
        }
    }

    /// A scratch row the plan allocated, as the proof a position stands
    /// over: the receipt is the plan's own product, never an identity.
    pub(crate) fn scratch(row: ScratchRow) -> Self {
        CompilerRow {
            relation: row.relation(),
        }
    }

    /// The row's identity, for the carrier authority's own judgments.
    pub(in crate::defuse) fn relation(&self) -> SemanticRelation {
        self.relation
    }

    /// THE AUTHORITY'S GROUND READ OF THE ROW, for the lexical authority
    /// alone: the preserve law, so the read publishes the row's own
    /// positions. The identity is spent inside; what leaves is the read.
    pub(crate) fn read(
        &self,
        _witness: crate::pipeline::resolver::RowRead,
        identities: &Planning,
    ) -> Result<crate::pipeline::ast_resolved::Chain> {
        identities.authority().ground_read(
            crate::pipeline::ast_resolved::Access::All,
            false,
            self.relation,
        )
    }
}

/// A REALIZED ROW A RESIDUAL STANDS OVER: a carrier the record holds — a
/// prepared pipe landing, a capture a preceding sibling crossed — or an
/// effect invocation's own scratch. Minted by the record from its own
/// entries, or from a scratch receipt; the definitions the row already
/// emitted travel with it, and whether it absorbs the caller row is the
/// record's own fact.
#[derive(Clone)]
pub(in crate::defuse) struct ResidualCaptureSource {
    row: CompilerRow,
    leading_ctes: Vec<CteBinding>,
    absorbs_join_input: bool,
}

impl ResidualCaptureSource {
    /// An effect's evaluation row: the plan scratch it was staged into.
    fn scratch(row: ScratchRow) -> Self {
        ResidualCaptureSource {
            row: CompilerRow::scratch(row),
            leading_ctes: Vec::new(),
            absorbs_join_input: true,
        }
    }
}

/// One structural carrier a record holds: the bound row, the definition
/// the binding authority minted for it (none when the carrier is inherited
/// from the record that bound it), whether THIS record emits that
/// definition (a receiver that took the residual's list emits it instead),
/// and whether the body may address it — an intermediate the construction
/// reads on its own is emitted and not addressed.
#[derive(Clone)]
struct Carrier {
    row: CarrierRow,
    definition: Option<BoundCarrier>,
    emits: bool,
    formal: bool,
}

impl Carrier {
    fn emitted(&self) -> Option<&CteBinding> {
        self.definition
            .as_ref()
            .filter(|_| self.emits)
            .map(BoundCarrier::binding)
    }
}

/// One item the body's leading `WITH` list carries, in emission order: a
/// carrier this record bound, or a binding a residual's construction
/// resolved from the caller's own locals ahead of its body.
#[derive(Clone)]
enum Leading {
    Carrier(Carrier),
    Extra(CteBinding),
}

/// THE CAPTURE A RESIDUAL'S CONSTRUCTION RECORDED: the landing that
/// realized the evaluation row with the configured values beside it, the
/// carrier those values are read from (the landing itself where the
/// landing is not addressed), the occurrence the landing stands in place
/// of, whether it absorbs the caller row, and the construction token the
/// landing publishes.
#[derive(Clone, Copy)]
struct Capture {
    landing: StructuralRelation,
    values: StructuralRelation,
    source: SemanticRelation,
    absorbs_join_input: bool,
    row_token: Option<crate::relation::PortId>,
}

/// What a crossing decided, for the construction that asked for it.
pub(in crate::defuse) struct Crossed {
    /// Whether the residual had emitted anything before the receiver took
    /// it.
    pub(in crate::defuse) moved_leading: bool,
    /// Whether the residual's capture absorbs the caller row.
    pub(in crate::defuse) absorbs_join_input: bool,
    /// The row the capture realized, as a later sibling stands over it:
    /// present where the capture replaced a formal of the receiver or
    /// absorbs the caller row.
    pub(in crate::defuse) captured: Option<ResidualCaptureSource>,
}

/// THE CARRIERS ONE USE BOUND — the record of the act, complete.
///
/// Private fields. A record is born empty and grows only through this
/// module's operations, which bind bodies into it, or through inheriting
/// a carrier another record bound and absorbing another record whole.
/// Its formals are what the body addresses by landing; its leading list
/// is what is emitted ahead of the body; its options name which carrier
/// the caller row became, decided by the acts that bound or crossed it.
#[derive(Clone, Default)]
pub struct CarrierRecord {
    leading: Vec<Leading>,
    join_input: Option<StructuralRelation>,
    absorbed_join_input: Option<StructuralRelation>,
    capture: Option<Capture>,
}

impl std::fmt::Debug for CarrierRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CarrierRecord")
            .field("formals", &self.formals().collect::<Vec<_>>())
            .field("join_input", &self.join_input)
            .field("absorbed_join_input", &self.absorbed_join_input)
            .finish()
    }
}

impl CarrierRecord {
    fn bound(
        &mut self,
        part: HoPart,
        body: crate::pipeline::resolver::ResolvedRelation,
        formal: bool,
        ahead: bool,
        identities: &Planning,
    ) -> Result<CarrierRow> {
        let definition =
            crate::pipeline::bindings::bind_carrier(CarrierBind(()), part, body, identities)?;
        let row = definition.row();
        let entry = Leading::Carrier(Carrier {
            row,
            definition: Some(definition),
            emits: true,
            formal,
        });
        if ahead {
            self.leading.insert(0, entry);
        } else {
            self.leading.push(entry);
        }
        Ok(row)
    }

    /// BIND A CARRIER the body addresses as a relation formal: an admitted
    /// actual, a piped source, a staged scalar. The body is spent; the
    /// landing and the carrier are born together.
    fn bind(
        &mut self,
        part: HoPart,
        body: crate::pipeline::resolver::ResolvedRelation,
        identities: &Planning,
    ) -> Result<CarrierRow> {
        self.bound(part, body, true, false, identities)
    }

    /// THE CALLER ROW BECOMES A CARRIER: the standing row the call absorbed
    /// is spent into its structural binding, and the record names it as
    /// the join input — the carrier a bare actual stands over.
    fn bind_join_input(
        &mut self,
        body: crate::pipeline::resolver::ResolvedRelation,
        identities: &Planning,
    ) -> Result<CarrierRow> {
        let row = self.bound(HoPart::ScalarInput, body, true, false, identities)?;
        self.join_input = Some(row.landing());
        Ok(row)
    }

    /// A RESIDUAL'S CAPTURE LANDING: the evaluation row republished with
    /// the configured values beside it, bound ahead of everything recorded
    /// so the bindings after it read it. It is addressed by the body only
    /// where it absorbs the caller row. `source` is the occurrence the
    /// landing stands in place of — the row the construction spent; a
    /// crossing asks the identity authority which formal of the receiver
    /// is that occurrence.
    fn bind_capture_landing(
        &mut self,
        augmented: crate::pipeline::resolver::ResolvedRelation,
        source: SemanticRelation,
        absorbs_join_input: bool,
        identities: &Planning,
    ) -> Result<CarrierRow> {
        let row = self.bound(
            HoPart::ScalarInput,
            augmented,
            absorbs_join_input,
            true,
            identities,
        )?;
        self.capture = Some(Capture {
            landing: row.landing(),
            values: row.landing(),
            source,
            absorbs_join_input,
            row_token: None,
        });
        Ok(row)
    }

    /// THE CAPTURE'S CONSTRUCTION TOKEN, once the authority has marked it
    /// on the carrier the values are read from: a receiver formal that
    /// publishes it is the capture's own row.
    fn capture_token(&mut self, row_token: crate::relation::PortId) {
        if let Some(capture) = self.capture.as_mut() {
            capture.row_token = Some(row_token);
        }
    }

    /// THE VALUE CARRIER OF A CAPTURE: the configured values read over the
    /// capture landing, bound directly after it, and the carrier the
    /// values are read from thereafter.
    fn bind_capture_value(
        &mut self,
        scalar: crate::pipeline::resolver::ResolvedRelation,
        identities: &Planning,
    ) -> Result<CarrierRow> {
        let capture = self
            .capture
            .expect("a capture's value carrier is bound after its landing");
        let definition = crate::pipeline::bindings::bind_carrier(
            CarrierBind(()),
            HoPart::ScalarInput,
            scalar,
            identities,
        )?;
        let row = definition.row();
        // The value carrier reads the landing, so it follows it directly.
        let after_landing = self
            .leading
            .iter()
            .position(|item| matches!(item, Leading::Carrier(entry) if entry.row.landing() == capture.landing))
            .map(|index| index + 1)
            .expect("a record holds the capture landing it bound");
        self.leading.insert(
            after_landing,
            Leading::Carrier(Carrier {
                row,
                definition: Some(definition),
                emits: true,
                formal: true,
            }),
        );
        self.capture = Some(Capture {
            values: row.landing(),
            ..capture
        });
        Ok(row)
    }

    /// AN EFFECT'S CAPTURE IS ITS JOIN INPUT: an effect invocation has no
    /// join to its left, so the carrier its configured values are read
    /// from is the row its clauses join.
    fn effect_capture_is_join_input(&mut self) {
        if let Some(capture) = self.capture {
            self.join_input = Some(capture.values);
        }
    }

    /// INHERIT A CARRIER the record `from` bound, as this call's own
    /// formal: the row travels whole from that record, and its definition
    /// stays where it was emitted.
    pub(in crate::defuse) fn inherit(
        &mut self,
        from: &CarrierRecord,
        landing: StructuralRelation,
    ) -> Result<()> {
        if self.holds(landing) {
            return Ok(());
        }
        let row = from.formal_row(landing).ok_or_else(|| {
            DelightQLError::transformation_error(
                "a forwarded relation lost its structural carrier",
                "higher-order relation forwarding",
            )
        })?;
        self.leading.push(Leading::Carrier(Carrier {
            row,
            definition: None,
            emits: false,
            formal: true,
        }));
        Ok(())
    }

    /// Bindings resolved from the caller's locals that the body needs
    /// ahead of it, appended once each by subject.
    fn append_extra(&mut self, bindings: Vec<CteBinding>) {
        for binding in bindings {
            if !self.subjects().any(|subject| subject == *binding.subject()) {
                self.leading.push(Leading::Extra(binding));
            }
        }
    }

    /// Bindings a realized row already emitted, placed ahead of everything
    /// recorded — the landing that reads them follows them — once each by
    /// subject.
    fn prepend_extra(&mut self, bindings: Vec<CteBinding>) {
        let mut front = Vec::new();
        for binding in bindings {
            if !self.subjects().any(|subject| subject == *binding.subject())
                && !front
                    .iter()
                    .any(|item: &Leading| matches!(item, Leading::Extra(seen) if seen.subject() == binding.subject()))
            {
                front.push(Leading::Extra(binding));
            }
        }
        front.append(&mut self.leading);
        self.leading = front;
    }

    fn carriers(&self) -> impl Iterator<Item = &Carrier> {
        self.leading.iter().filter_map(|item| match item {
            Leading::Carrier(entry) => Some(entry),
            Leading::Extra(_) => None,
        })
    }

    fn subjects(&self) -> impl Iterator<Item = SemanticRelation> + '_ {
        self.leading.iter().filter_map(|item| match item {
            Leading::Carrier(entry) => entry.emitted().map(|binding| *binding.subject()),
            Leading::Extra(binding) => Some(*binding.subject()),
        })
    }

    fn holds(&self, landing: StructuralRelation) -> bool {
        self.carriers().any(|entry| entry.row.landing() == landing)
    }

    /// Whether the body may address a carrier at this landing.
    pub(in crate::defuse) fn holds_formal(&self, landing: StructuralRelation) -> bool {
        self.carriers()
            .any(|entry| entry.formal && entry.row.landing() == landing)
    }

    /// Whether the record carries no entry at all.
    pub(in crate::defuse) fn is_empty(&self) -> bool {
        self.leading.is_empty()
    }

    /// Whether anything is emitted ahead of the body.
    pub(in crate::defuse) fn has_leading(&self) -> bool {
        self.leading_ctes().next().is_some()
    }

    /// What is emitted ahead of the body, in order.
    pub(in crate::defuse) fn leading_ctes(&self) -> impl Iterator<Item = &CteBinding> {
        self.leading.iter().filter_map(|item| match item {
            Leading::Carrier(entry) => entry.emitted(),
            Leading::Extra(binding) => Some(binding),
        })
    }

    /// THE LEADING LIST, MOVED to a receiver that will emit it: the
    /// carriers stay recorded as formals, and this record no longer emits
    /// their definitions.
    fn take_leading(&mut self) -> Vec<Leading> {
        let mut taken = Vec::new();
        let mut kept = Vec::new();
        for item in std::mem::take(&mut self.leading) {
            match item {
                Leading::Carrier(mut entry) => {
                    if entry.emits && entry.definition.is_some() {
                        taken.push(Leading::Carrier(entry.clone()));
                        entry.emits = false;
                    }
                    kept.push(Leading::Carrier(entry));
                }
                Leading::Extra(binding) => taken.push(Leading::Extra(binding)),
            }
        }
        self.leading = kept;
        taken
    }

    /// The formals the body addresses, each as the one value its bind
    /// produced.
    pub(in crate::defuse) fn formals(&self) -> impl Iterator<Item = CarrierRow> + '_ {
        self.carriers()
            .filter(|entry| entry.formal)
            .map(|entry| entry.row)
    }

    /// The formal bound at a landing.
    pub(in crate::defuse) fn formal_row(&self, landing: StructuralRelation) -> Option<CarrierRow> {
        self.carriers()
            .find(|entry| entry.formal && entry.row.landing() == landing)
            .map(|entry| entry.row)
    }

    /// THE PROOF OF THE FORMAL AT A LANDING, for the world that answers a
    /// body's mention of it: what the resolver stands over, never an
    /// identity it could stand over by itself.
    pub(in crate::defuse) fn compiler_row(&self, landing: StructuralRelation) -> Option<CompilerRow> {
        self.formal_row(landing).map(CompilerRow::carrier)
    }

    /// THE REALIZED ROW A FORMAL IS, for a residual to stand over: the
    /// carrier at the landing, with the definitions this record emits
    /// ahead of it. A landed formal absorbs the caller row: it is the exact
    /// row the enclosing consumer would otherwise bind separately, and the
    /// capture that augments it replaces that landing for the whole
    /// consumer.
    pub(in crate::defuse) fn realized_formal(
        &self,
        landing: StructuralRelation,
    ) -> Option<ResidualCaptureSource> {
        self.formal_row(landing).map(|row| ResidualCaptureSource {
            row: CompilerRow::carrier(row),
            leading_ctes: self.leading_ctes().cloned().collect(),
            absorbs_join_input: true,
        })
    }

    /// THE RECORD AS A BODY WORLD HOLDS IT: its formals and the roles the
    /// caller row plays, without the definitions — those are emitted by
    /// the use that bound them, not by the body that addresses them.
    pub(in crate::defuse) fn formals_only(&self) -> CarrierRecord {
        CarrierRecord {
            leading: self
                .carriers()
                .filter(|entry| entry.formal)
                .map(|entry| {
                    Leading::Carrier(Carrier {
                        row: entry.row,
                        definition: None,
                        emits: false,
                        formal: true,
                    })
                })
                .collect(),
            join_input: self.join_input,
            absorbed_join_input: self.absorbed_join_input,
            capture: None,
        }
    }

    fn unformal(&mut self, landing: StructuralRelation) {
        for item in &mut self.leading {
            if let Leading::Carrier(entry) = item {
                if entry.row.landing() == landing {
                    entry.formal = false;
                }
            }
        }
    }

    pub(in crate::defuse) fn join_input(&self) -> Option<StructuralRelation> {
        self.join_input
    }

    pub(in crate::defuse) fn absorbed_join_input(&self) -> Option<StructuralRelation> {
        self.absorbed_join_input
    }

    /// ABSORB A LATER RECORD: its leading items follow this record's, once
    /// each by subject; its formals join by landing; where it names a
    /// carrier the caller row became, that naming wins.
    pub(in crate::defuse) fn absorb(&mut self, other: CarrierRecord) {
        let CarrierRecord {
            leading,
            join_input,
            absorbed_join_input,
            capture: _,
        } = other;
        for item in leading {
            self.absorb_item(item);
        }
        if join_input.is_some() {
            self.join_input = join_input;
        }
        if absorbed_join_input.is_some() {
            self.absorbed_join_input = absorbed_join_input;
        }
    }

    /// THE FORMAL OF THIS RECORD THAT IS THE OCCURRENCE A CAPTURE STANDS IN
    /// PLACE OF, by the identity authority's judgment: a formal publishing
    /// the capture's own construction token, else the exact occurrence,
    /// else its recorded rebuild. Sharing a heading is not being the
    /// occurrence; two candidates is a construction defect.
    fn formal_standing_for(
        &self,
        capture: &Capture,
        identities: &Planning,
    ) -> Result<Option<StructuralRelation>> {
        let mut token_landings = Vec::new();
        let mut exact_landings = Vec::new();
        let mut rebuilt_landings = Vec::new();
        for row in self.formals() {
            let token_matches = crate::relation::published_ports(identities, &row.relation())?
                .into_iter()
                .filter(|port| {
                    capture.row_token.is_some()
                        && identities.authority().residual_row_token(*port) == capture.row_token
                })
                .count();
            if token_matches > 1 {
                return Err(DelightQLError::transformation_error(
                    "one receiver relation carries a residual construction token more than once",
                    "closed residual crossing",
                ));
            }
            if token_matches == 1 {
                token_landings.push(row.landing());
            }
            if row.relation() == capture.source {
                exact_landings.push(row.landing());
            } else if identities
                .authority()
                .continues_exactly(capture.source, row.relation())?
            {
                rebuilt_landings.push(row.landing());
            }
        }
        match (
            token_landings.as_slice(),
            exact_landings.as_slice(),
            rebuilt_landings.as_slice(),
        ) {
            ([landing], _, _) => Ok(Some(*landing)),
            ([], [landing], _) => Ok(Some(*landing)),
            ([], [], [landing]) => Ok(Some(*landing)),
            ([], [], []) | ([], [], [_, _, ..]) => Ok(None),
            ([_, _, ..], _, _) | ([], [_, _, ..], _) => Err(DelightQLError::transformation_error(
                "a residual construction row is carried by more than one receiver relation",
                "closed residual crossing",
            )),
        }
    }

    /// A RESIDUAL'S RECORD CROSSES INTO THIS RECEIVER. Its leading list
    /// moves here to be emitted; its formals join this record's; it keeps
    /// its formals without emitting their definitions. Where the residual
    /// captured the occurrence one of this record's formals is, the
    /// capture's landing stands in that formal's place: the bindings that
    /// addressed the formal address the landing, the formal stops being
    /// one here, and the roles the caller row plays are settled — on the
    /// residual, for the use it will seed; on this record, for the use it
    /// completes. What comes back for a later sibling to stand over is the
    /// captured row as this record now holds it.
    pub(in crate::defuse) fn cross(
        &mut self,
        residual: &mut CarrierRecord,
        bindings: &mut HoParamBindings,
        identities: &Planning,
    ) -> Result<Crossed> {
        let replaceable = match residual.capture {
            Some(capture) => self.formal_standing_for(&capture, identities)?,
            None => None,
        };
        let moved_leading = residual.has_leading();
        for item in residual.take_leading() {
            self.absorb_item(item);
        }
        for entry in residual.carriers().cloned().collect::<Vec<_>>() {
            if entry.formal && !self.holds(entry.row.landing()) {
                self.leading.push(Leading::Carrier(Carrier {
                    definition: None,
                    emits: false,
                    ..entry
                }));
            }
        }
        if residual.join_input.is_some() {
            self.join_input = residual.join_input;
        }
        if residual.absorbed_join_input.is_some() {
            self.absorbed_join_input = residual.absorbed_join_input;
        }
        let Some(capture) = residual.capture else {
            return Ok(Crossed {
                moved_leading,
                absorbs_join_input: false,
                captured: None,
            });
        };
        if !capture.absorbs_join_input {
            residual.join_input = Some(capture.values);
        }
        let mut replaced = false;
        if let Some(source) = replaceable {
            for scope in bindings.table_scope_params.values_mut() {
                if *scope == source {
                    *scope = capture.landing;
                    replaced = true;
                }
            }
            if let Some((_, scope)) = &mut bindings.pipe_carrier {
                if *scope == source {
                    *scope = capture.landing;
                    replaced = true;
                }
            }
            if replaced {
                self.unformal(source);
            }
        }
        if capture.absorbs_join_input {
            if replaced {
                residual.absorbed_join_input = Some(capture.landing);
            } else {
                residual.join_input = Some(capture.landing);
            }
            self.absorbed_join_input = Some(capture.landing);
        }
        let captured = (replaced || capture.absorbs_join_input).then(|| {
            let row = residual
                .carriers()
                .find(|entry| entry.row.landing() == capture.landing)
                .map(|entry| entry.row)
                .expect("a record holds the capture landing it bound");
            ResidualCaptureSource {
                row: CompilerRow::carrier(row),
                leading_ctes: self.leading_ctes().cloned().collect(),
                absorbs_join_input: self.absorbed_join_input.is_some(),
            }
        });
        Ok(Crossed {
            moved_leading,
            absorbs_join_input: capture.absorbs_join_input,
            captured,
        })
    }

    fn absorb_item(&mut self, item: Leading) {
        match item {
            Leading::Carrier(entry) => {
                let subject = entry.emitted().map(|binding| *binding.subject());
                if let Some(mine) = self.leading.iter_mut().find_map(|item| match item {
                    Leading::Carrier(mine) if mine.row.landing() == entry.row.landing() => {
                        Some(mine)
                    }
                    _ => None,
                }) {
                    if mine.emitted().is_none() && entry.emits {
                        mine.definition = entry.definition;
                        mine.emits = true;
                    }
                    mine.formal |= entry.formal;
                    return;
                }
                if subject.is_some_and(|subject| self.subjects().any(|seen| seen == subject)) {
                    self.leading.push(Leading::Carrier(Carrier {
                        emits: false,
                        ..entry
                    }));
                    return;
                }
                self.leading.push(Leading::Carrier(entry));
            }
            Leading::Extra(binding) => {
                if !self.subjects().any(|seen| seen == *binding.subject()) {
                    self.leading.push(Leading::Extra(binding));
                }
            }
        }
    }

    /// SEEDED BY AN EARLIER RECORD: a residual's own record stands ahead
    /// of the record completing it; what this record already names wins.
    pub(in crate::defuse) fn seeded_by(&mut self, seed: &CarrierRecord) {
        let own = std::mem::take(&mut self.leading);
        self.leading = seed.leading.clone();
        for item in own {
            self.absorb_item(item);
        }
        if self.join_input.is_none() {
            self.join_input = seed.join_input;
        }
        if self.absorbed_join_input.is_none() {
            self.absorbed_join_input = seed.absorbed_join_input;
        }
    }

    /// THE ROW A SCALAR ACTUAL STANDS OVER: every formal, as one row of
    /// proofs. The lexical authority mints the frame from them and reads
    /// each through its proof.
    pub(crate) fn formal_rows(&self) -> Vec<CompilerRow> {
        self.formals().map(CompilerRow::carrier).collect()
    }

    /// THE CARRIER THE CALLER ROW BECAME — the join-input carrier when the
    /// standing row was absorbed into one, else the one pipe-source
    /// carrier the call landed. A bare actual stands over exactly it, by
    /// its proof.
    pub(crate) fn landing_row(&self) -> Option<CompilerRow> {
        if let Some(landing) = self.join_input.or(self.absorbed_join_input) {
            return self.compiler_row(landing);
        }
        let mut sources = self
            .formals()
            .filter(|row| row.landing().part() == HoPart::PipeSource);
        match (sources.next(), sources.next()) {
            (Some(row), None) => Some(CompilerRow::carrier(row)),
            _ => None,
        }
    }
}
