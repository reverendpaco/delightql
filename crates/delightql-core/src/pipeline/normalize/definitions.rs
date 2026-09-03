// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Definitions — a subject, a head, and a body.
//!
//! What leaves here is one CLAUSE per authored definition. Grouping clauses by
//! subject and running the arity, name-offer, Ground-Position and heading
//! laws over them is the assembler's job: there is exactly one place a
//! subject's clauses meet, and it is not this one.
//!
//! Rule bodies come in exactly three positions — RELATIONAL (`fo_rule`,
//! `ho_rule`), VALUE (`function_rule`, and `constant_rule` its nullary), and
//! TRUTH (`sigma_rule`). The grammar already sorted them, so the body's
//! category is read off the production rather than off the body.

use super::Normalizer;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::definitions::{
    name_conflict, Fixpoint, HeadItem, HeadItems, HoParam, Offered, ResidualMode, ResidualSignature,
};
use crate::pipeline::asts::core::{
    AnonRelation, AnonTable, Chain, ContextMode, DomainExpression, FunctionApplication, GroundForm,
    HeaderItem, Query, TabularBody, TabularRow, Unresolved,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::ddl::{ClauseDecl, DdlBody, DefKind, DefSubject, DefinitionFront, Head};
use crate::pipeline::syntax::cst;
use delightql_types::SqlIdentifier;

impl<'t> Normalizer<'t> {
    pub(crate) fn entity_definition(
        &mut self,
        node: cst::EntityDefinition<'t>,
    ) -> Result<ClauseDecl> {
        match node {
            cst::EntityDefinition::RuleForm(rule) => self.rule_form(rule),
            cst::EntityDefinition::FactLike(fact) => self.fact_like(fact),
            cst::EntityDefinition::FactFunction(function) => self.fact_function(function),
            cst::EntityDefinition::EdgeDeclaration(edge) => self.edge_declaration(edge),
        }
    }

    // -----------------------------------------------------------------
    // Rules
    // -----------------------------------------------------------------

    /// A definition's subject name, ADMITTED: a defining head is a naming
    /// position, so the position law runs (exact `_` and bare reserved
    /// words refuse; a strop stays an exact name) and the spelling is
    /// reserved against the compilation's invented names.
    fn definition_subject(
        &self,
        node: cst::PredicateIdentifier<'t>,
    ) -> Result<crate::pipeline::asts::core::QualifiedName> {
        let mut name = self.qualified_name(node)?;
        name.name = self.admit_definition(name.name)?;
        Ok(name)
    }

    fn rule_form(&mut self, node: cst::RuleForm<'t>) -> Result<ClauseDecl> {
        match node {
            cst::RuleForm::FoRule(rule) => {
                let name = self.require(rule.name(), "a rule names its subject")?;
                let head = self.require(rule.head(), "a rule has a head")?;
                let body = self.require(rule.body(), "a rule has a body")?;
                let name = self.definition_subject(name)?;
                let heading = self.heading(head.into())?;
                let fixpoint = heading.fixpoint;
                let head = heading.head;
                let query = self.relex_query(body)?;
                let doc = self.doc_slot(rule.children().filter_map(doc_slot_of))?;
                Ok(self.clause(
                    DefKind::View,
                    DefSubject::Named(name.name.clone()),
                    head,
                    fixpoint,
                    DdlBody::Relational(query),
                    self.text(rule),
                    doc,
                ))
            }
            cst::RuleForm::HoRule(rule) => {
                let name = self.require(rule.name(), "a rule names its subject")?;
                let body = self.require(rule.body(), "a rule has a body")?;
                let mut params = Vec::new();
                for child in rule.children() {
                    if let cst::HoRuleChild::HoParam(param) = child {
                        params.push(self.ho_param(param)?);
                    }
                }
                let mut items = Vec::new();
                let mut glob = false;
                for item in rule.head() {
                    match item {
                        cst::HoRuleHead::HeadTerm(term) => items.push(self.head_term(term)?),
                        cst::HoRuleHead::Glob(_) => glob = true,
                        cst::HoRuleHead::CommaSigil(_) => {}
                    }
                }
                let parameterized = !params.is_empty();
                let head = Head::higher_order(
                    params,
                    if glob {
                        HeadItems::Glob
                    } else {
                        HeadItems::Listed(items)
                    },
                );
                let relational = self.deferrable(parameterized, self.text(body), |n| {
                    Ok(DdlBody::Relational(n.relex_query(body)?))
                })?;
                let doc = self.doc_slot(rule.children().filter_map(ho_rule_doc))?;
                Ok(self.clause(
                    DefKind::HoView,
                    DefSubject::Named(self.definition_subject(name)?.name.clone()),
                    head,
                    Fixpoint::Bag,
                    relational,
                    self.text(rule),
                    doc,
                ))
            }
            cst::RuleForm::FunctionRule(rule) => {
                let name = self.require(rule.name(), "a rule names its subject")?;
                let body = self.require(rule.body(), "a rule has a body")?;
                let mut params = Vec::new();
                // CCAFE: the capture is declared by the MARKER'S SHAPE, and it
                // is not a parameter — it names where the arguments come from,
                // not one of them.
                let mut context = ContextMode::None;
                for child in rule.children() {
                    if let cst::FunctionRuleChild::FunctionParam(param) = child {
                        match param {
                            cst::FunctionParam::ContextMarker(marker) => {
                                // ONE capture per signature: a second marker
                                // would silently overwrite the first, so it
                                // refuses instead of last-wins.
                                if context != ContextMode::None {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "ddl/head/duplicate_context_marker",
                                        "a signature declares its capture once — a second \
                                         context marker has nothing to add and would silently \
                                         replace the first. Keep one marker",
                                        "one context capture per signature",
                                    ));
                                }
                                // THE MARKER LEADS. A context call supplies
                                // `..` first and a positional call binds the
                                // captures first, so a marker declared after
                                // a parameter would silently reorder every
                                // call.
                                if !params.is_empty() {
                                    return Err(Self::context_marker_position_refusal());
                                }
                                context = self.context_mode(marker)
                            }
                            param => params.push(self.function_param(param)?),
                        }
                    }
                }
                let parameterized = !params.is_empty();
                let head = Head::signature(params).with_context(context);
                let scalar = self.deferrable(parameterized, self.text(body), |n| {
                    Ok(DdlBody::Scalar(n.domain_expression(body)?))
                })?;
                let doc = self.doc_slot(rule.children().filter_map(function_rule_doc))?;
                Ok(self.clause(
                    DefKind::Function,
                    DefSubject::Named(self.definition_subject(name)?.name.clone()),
                    head,
                    Fixpoint::Bag,
                    scalar,
                    self.text(rule),
                    doc,
                ))
            }
            // The NULLARY function rule, paren-less; the citation `:pi` is
            // its consumer.
            cst::RuleForm::ConstantRule(rule) => {
                let name = self.require(rule.name(), "a constant names its subject")?;
                let body = self.require(rule.body(), "a constant has a body")?;
                let expression = self.domain_expression(body)?;
                let doc = self.doc_slot(rule.children().filter_map(constant_doc))?;
                Ok(self.clause(
                    DefKind::Function,
                    DefSubject::Named(self.identifier(name)),
                    Head::signature(Vec::new()),
                    Fixpoint::Bag,
                    DdlBody::Scalar(expression),
                    self.text(rule),
                    doc,
                ))
            }
            // The PREDICATE rule — truth category. The body's category is a
            // parse-level constraint, so `p(x) :- users` never reaches here.
            cst::RuleForm::SigmaRule(rule) => {
                let name = self.require(rule.name(), "a rule names its subject")?;
                let body = self.require(rule.body(), "a sigma rule has a body")?;
                let condition = self.require(body.child(), "a sigma body is a truth expression")?;
                let condition = self.truth_expression(condition)?;
                let mut params = Vec::new();
                for child in rule.children() {
                    if let cst::SigmaRuleChild::Identifier(identifier) = child {
                        params.push(HoParam::Scalar {
                            name: self.identifier(identifier),
                            guard: None,
                            callable: false,
                        });
                    }
                }
                let doc = self.doc_slot(rule.children().filter_map(sigma_doc))?;
                Ok(self.clause(
                    DefKind::Sigma,
                    DefSubject::Named(self.definition_subject(name)?.name.clone()),
                    Head::signature(params),
                    Fixpoint::Bag,
                    DdlBody::Truth(condition),
                    self.text(rule),
                    doc,
                ))
            }
            cst::RuleForm::EffectRule(rule) => {
                let name = self.require(rule.name(), "a rule names its subject")?;
                let body = self.require(rule.body(), "an effect rule has a body")?;
                let mut params = Vec::new();
                for child in rule.children() {
                    if let cst::EffectRuleChild::HoParam(param) = child {
                        params.push(self.ho_param(param)?);
                    }
                }
                let parameterized = !params.is_empty();
                let head = Head::higher_order(params, HeadItems::Glob);
                // The effectual twin of the relational rule's `relex_query`:
                // a let block, when one is written, and the chain it feeds.
                let relational = self.deferrable(parameterized, self.text(body), |n| {
                    Ok(DdlBody::Relational(n.effrelex_query(body)?))
                })?;
                let doc = self.doc_slot(rule.children().filter_map(effect_rule_doc))?;
                Ok(self.clause(
                    DefKind::Effect,
                    // THE CATALOG SPELLING CARRIES THE MARK. An effect rule is
                    // registered under the name it is invoked by — the
                    // BinPseudoPredicate convention, `consult!` — so the
                    // `foo`/`foo!` collision rule and every demand that names a
                    // directive read the same bytes.
                    DefSubject::Named(delightql_types::SqlIdentifier::new(
                        self.effect_subject_name(name)?,
                    )),
                    head,
                    Fixpoint::Bag,
                    relational,
                    self.text(rule),
                    doc,
                ))
            }
        }
    }

    /// A body whose head declares parameters cannot always be READ before a
    /// call site supplies them: a bound naming a scalar formal has no integer
    /// to be. That body is DEFERRED — the authored characters, held as such —
    /// rather than fabricated with a stand-in value, and invocation
    /// normalizes them again with the bindings in hand.
    ///
    /// A SEMANTIC refusal is not a deferral. A rule the body breaks is broken
    /// whatever the arguments turn out to be, so it propagates eagerly and the
    /// definition never loads. The one thing that waits is a term whose value
    /// is not there yet, and its identity is what says so.
    ///
    /// NOTHING WAITS ONCE THE ARGUMENTS ARE HERE. With a call site's bindings
    /// in hand, a body that still cannot read a term has been told what the
    /// term is — so the refusal is the answer, and deferring it again would
    /// hide the call site's own mistake behind a second wait.
    fn deferrable(
        &mut self,
        parameterized: bool,
        source: &str,
        body: impl FnOnce(&mut Self) -> Result<DdlBody>,
    ) -> Result<DdlBody> {
        let source = source.to_string();
        let unbound = self.bindings().is_none();
        match body(self) {
            Ok(built) => Ok(built),
            Err(error) if parameterized && unbound && awaits_substitution(&error) => {
                Ok(DdlBody::Deferred { source })
            }
            Err(error) => Err(error),
        }
    }

    fn effect_subject_name(&self, node: cst::EffectIdentifier<'t>) -> Result<String> {
        for child in node.children() {
            if let cst::EffectIdentifierChild::PredicateIdentifier(inner) = child {
                let name = self.require(inner.name(), "a subject has a name")?;
                return Ok(format!("{}!", self.identifier(name).as_str()));
            }
        }
        Err(DelightQLError::parse_error(
            "an effect identifier has a predicate identifier",
        ))
    }

    /// A parameter a signature BAPTIZES. A ground constant is the clause's
    /// input-side match pattern, and its AUTHORED token is the match key —
    /// which is why a mention canonicalizes on the way in.
    pub(crate) fn ho_param(&mut self, node: cst::HoParam<'t>) -> Result<HoParam> {
        match node {
            cst::HoParam::RuleParam(param) => {
                let name = self.require(param.name(), "a rule parameter has a name")?;
                let mut remaining = Vec::new();
                for child in param.children() {
                    let cst::RuleParamChild::RuleModeParam(mode) = child else {
                        continue;
                    };
                    remaining.push(match mode {
                        cst::RuleModeParam::OpenRelationParam(param) => {
                            let name = self
                                .require(param.name(), "a residual relation position has a name")?;
                            ResidualMode::Relation {
                                name: self.identifier(name),
                                cols: HeadItems::Glob,
                            }
                        }
                        cst::RuleModeParam::DeclaredRelationParam(param) => {
                            let name = self
                                .require(param.name(), "a residual relation position has a name")?;
                            let cols = param
                                .children()
                                .filter_map(|child| match child {
                                    cst::DeclaredRelationParamChild::Identifier(column) => {
                                        Some(HeadItem::plumb(self.identifier(column)))
                                    }
                                    cst::DeclaredRelationParamChild::CommaSigil(_) => None,
                                })
                                .collect();
                            ResidualMode::Relation {
                                name: self.identifier(name),
                                cols: HeadItems::Listed(cols),
                            }
                        }
                        cst::RuleModeParam::ScalarParam(param) => {
                            let name = self
                                .require(param.child(), "a residual scalar position has a name")?;
                            ResidualMode::Scalar {
                                name: self.identifier(name),
                            }
                        }
                    });
                }
                let mut output = Vec::new();
                let mut glob = false;
                for item in param.head() {
                    match item {
                        cst::RuleParamHead::Identifier(name) => {
                            output.push(HeadItem::plumb(self.identifier(name)))
                        }
                        cst::RuleParamHead::Glob(_) => glob = true,
                        cst::RuleParamHead::CommaSigil(_) => {}
                    }
                }
                Ok(HoParam::Rule {
                    name: self.identifier(name),
                    signature: ResidualSignature {
                        remaining,
                        output: if glob {
                            HeadItems::Glob
                        } else {
                            HeadItems::Listed(output)
                        },
                    },
                })
            }
            cst::HoParam::OpenRelationParam(param) => {
                let name = self.require(param.name(), "a relation parameter has a name")?;
                Ok(HoParam::Relation {
                    name: self.identifier(name),
                    cols: HeadItems::Glob,
                })
            }
            cst::HoParam::DeclaredRelationParam(param) => {
                let name = self.require(param.name(), "a relation parameter has a name")?;
                let cols = param
                    .children()
                    .filter_map(|child| match child {
                        cst::DeclaredRelationParamChild::Identifier(column) => {
                            Some(HeadItem::plumb(self.identifier(column)))
                        }
                        cst::DeclaredRelationParamChild::CommaSigil(_) => None,
                    })
                    .collect();
                Ok(HoParam::Relation {
                    name: self.identifier(name),
                    cols: HeadItems::Listed(cols),
                })
            }
            cst::HoParam::ScalarParam(param) => {
                let name = self.require(param.child(), "a scalar parameter has a name")?;
                Ok(HoParam::Scalar {
                    name: self.identifier(name),
                    guard: None,
                    callable: false,
                })
            }
            cst::HoParam::Ground(ground) => {
                let value = self.ground(ground)?;
                Ok(HoParam::Ground {
                    name: SqlIdentifier::new(format!("_ground_{}", value)),
                    text: value.stored_ground(),
                })
            }
        }
    }

    /// `..` captures implicitly; `..{…}` declares what it captures, including
    /// nothing. The shape is the declaration.
    pub(crate) fn context_mode(&mut self, marker: cst::ContextMarker<'t>) -> ContextMode {
        match marker.child() {
            None => ContextMode::Implicit,
            Some(capture) => ContextMode::Explicit(
                capture
                    .children()
                    .filter_map(|child| match child {
                        cst::ContextCaptureChild::Identifier(name) => Some(self.identifier(name)),
                        cst::ContextCaptureChild::CommaSigil(_) => None,
                    })
                    .collect(),
            ),
        }
    }

    /// THE MARKER LEADS. The context capture is the signature's first
    /// declaration at both faces: a context call supplies `..` first, and a
    /// positional call binds the captures first — so a marker declared after
    /// a parameter would silently reorder every call.
    pub(crate) fn context_marker_position_refusal() -> DelightQLError {
        DelightQLError::validation_error_categorized(
            "ddl/head/context_position",
            "the context capture leads the signature — a `..` declared after a \
             parameter would silently reorder every call",
            "declare the capture first: `f:(..{cols}, rest…)`",
        )
    }

    fn function_param(&mut self, node: cst::FunctionParam<'t>) -> Result<HoParam> {
        match node {
            // Handled by the head: a capture is not a parameter.
            cst::FunctionParam::ContextMarker(_) => Err(DelightQLError::parse_error(
                "a context marker declares a capture, not a parameter",
            )),
            cst::FunctionParam::CallableParam(param) => {
                let name = self.require(param.name(), "a callable parameter has a name")?;
                Ok(HoParam::Scalar {
                    name: self.identifier(name),
                    guard: None,
                    callable: true,
                })
            }
            // The guard is a filter the clause applies to its OWN argument.
            cst::FunctionParam::GuardedParam(param) => {
                let name = self.require(param.name(), "a guarded parameter has a name")?;
                let guard = self.require(param.child(), "a guarded parameter has a guard")?;
                let condition = self.guard(guard)?;
                Ok(HoParam::Scalar {
                    name: self.identifier(name),
                    guard: Some(condition),
                    callable: false,
                })
            }
            cst::FunctionParam::PlainParam(param) => {
                let name = self.require(param.child(), "a parameter has a name")?;
                Ok(HoParam::Scalar {
                    name: self.identifier(name),
                    guard: None,
                    callable: false,
                })
            }
        }
    }

    // -----------------------------------------------------------------
    // Facts
    // -----------------------------------------------------------------

    /// FACT ELABORATION — a fact is not a distinct body kind: it elaborates
    /// ONCE, during definition assembly, into ordinary ground relational
    /// clause bodies. What is built here is the fact's authored shape — the
    /// data table and the heading offers its rows make — for the assembler's
    /// one settlement.
    fn fact_like(&mut self, node: cst::FactLike<'t>) -> Result<ClauseDecl> {
        match node {
            cst::FactLike::FactForm(fact) => {
                let name = self.require(fact.name(), "a fact names its subject")?;
                let body = self.require(fact.body(), "a fact has a body")?;
                let subject_ident = self.definition_subject(name)?.name.clone();
                let subject = subject_ident.as_str().to_string();
                let (table, row_offers) = self.fact_body(&subject, body)?;
                let mut decl = self.clause(
                    DefKind::Fact,
                    DefSubject::Named(subject_ident),
                    Head::glob(),
                    Fixpoint::Bag,
                    DdlBody::Relational(Query::relational(Chain::authored(GroundForm::Literal(
                        AnonRelation::plain(table),
                    )))),
                    self.text(fact),
                    None,
                );
                decl.fact_row_offers = row_offers;
                Ok(decl)
            }
            // The PARAMETERIZED fact: a fact body behind a signature. Sugar
            // for the necked ho_rule-with-fact-body; ONE ELABORATION — so the
            // head is the one the verbose form writes. The body's header names
            // the output positions, and the sugar's whole claim is that
            // `f(g)(a, b ---- …)` and `f(g)(a, b) :- _(a, b ---- …)` are the
            // same definition; a glob head here would make them two, and the
            // squished schema would put the enumeration column on the other
            // side.
            cst::FactLike::HoFactForm(fact) => {
                let name = self.require(fact.name(), "a fact names its subject")?;
                let body = self.require(fact.body(), "a fact has a body")?;
                let mut params = Vec::new();
                for child in fact.children() {
                    if let cst::HoFactFormChild::HoParam(param) = child {
                        params.push(self.ho_param(param)?);
                    }
                }
                let items = self.fact_header_items(body)?;
                let subject_ident = self.definition_subject(name)?.name.clone();
                let subject = subject_ident.as_str().to_string();
                let (table, row_offers) = self.fact_body(&subject, body)?;
                // A parameterized fact's heading lives in its header; a datum
                // label with no header has no verbose-form equivalent to
                // elaborate to, so it refuses toward the header spelling
                // rather than silently disappearing.
                if row_offers.iter().flatten().any(Option::is_some) {
                    return Err(DelightQLError::validation_error_categorized(
                        "ddl/head/parameterized_fact_offer",
                        format!(
                            "'{subject}': a parameterized fact names its output positions \
                             in its header, not on its data — write \
                             `{subject}(…)(name, … ---- rows)`"
                        ),
                        "a parameterized fact's heading is its header",
                    ));
                }
                Ok(self.clause(
                    DefKind::HoView,
                    DefSubject::Named(subject_ident),
                    Head::higher_order(params, items),
                    Fixpoint::Bag,
                    DdlBody::Relational(Query::relational(Chain::authored(GroundForm::Literal(
                        AnonRelation::plain(table),
                    )))),
                    self.text(fact),
                    None,
                ))
            }
        }
    }

    /// The output positions a parameterized fact's header names.
    ///
    /// A header column becomes a PLUMBED head item, exactly as the verbose
    /// spelling writes it. A headerless body names no positions, so the head
    /// is the glob.
    fn fact_header_items(&mut self, node: cst::FactBody<'t>) -> Result<HeadItems> {
        let Some(header) = node.header() else {
            return Ok(HeadItems::Glob);
        };
        let mut items = Vec::new();
        for child in header.children() {
            let cst::HeaderRowChild::HeaderItem(item) = child else {
                continue;
            };
            for part in item.children() {
                let cst::HeaderItemChild::Slot(slot) = part else {
                    continue;
                };
                // Every header item NAMES an output position here: a slot
                // that names nothing (`_`, a constraint term) would be
                // silently dropped from the declared head while the table
                // keeps its width — so it refuses instead.
                let cst::Slot::NamedReference(reference) = slot else {
                    return Err(DelightQLError::validation_error_categorized(
                        "ddl/head/fact_header",
                        "a parameterized fact's header names its output positions — \
                         every header item is a column name",
                        "a fact header item is a column name",
                    ));
                };
                let column = self.authored_column(reference)?;
                items.push(HeadItem::plumb(column.name));
            }
        }
        Ok(if items.is_empty() {
            HeadItems::Glob
        } else {
            HeadItems::Listed(items)
        })
    }

    /// ONE SHAPE FOR EVERY TABULAR INTERIOR. A fact declares its columns with
    /// the same `header_row` an anonymous table does — sparse marks included —
    /// and its rows are assembled, judged for width, and checked against
    /// their offers by the SAME algorithm. What differs is the CELL: a fact
    /// datum is ground with an optional heading offer, and an anonymous
    /// datum is a domain expression, each read by its own body before the
    /// shared assembly sees it.
    /// Returns the table and, for a HEADERLESS body, the per-row heading
    /// offers the assembler's fact elaboration spends. A header consumes its
    /// rows' offers here (they must agree with it), so a headered body hands
    /// back none.
    #[allow(clippy::type_complexity)]
    fn fact_body(
        &mut self,
        subject: &str,
        node: cst::FactBody<'t>,
    ) -> Result<(AnonTable<Unresolved>, Vec<Vec<Option<SqlIdentifier>>>)> {
        let (column_headers, sparse) = self.tabular_heading(node.header())?;
        let mut rows = Vec::new();
        let mut row_offers: Vec<Vec<Option<SqlIdentifier>>> = Vec::new();
        for child in node.children() {
            if let cst::FactBodyChild::FactRow(row) = child {
                let (positional, offers, fills) = self.fact_row_parts(row)?;
                row_offers.push(offers.clone());
                rows.push((positional, fills, offers));
            }
        }
        let rows = self.tabular_rows("fact", Some(subject), &column_headers, &sparse, rows)?;
        let unconsumed = if column_headers.is_some() {
            Vec::new()
        } else {
            row_offers
        };
        Ok((
            AnonTable {
                body: TabularBody {
                    header: column_headers,
                    rows,
                },
            },
            unconsumed,
        ))
    }

    /// A fact row's written cells, split the way a data row's are: ground
    /// values in order, and fills paired with the column each names.
    #[allow(clippy::type_complexity)]
    fn fact_row_parts(
        &mut self,
        row: cst::FactRow<'t>,
    ) -> Result<(
        Vec<DomainExpression<Unresolved>>,
        Vec<Option<SqlIdentifier>>,
        Vec<(SqlIdentifier, DomainExpression<Unresolved>)>,
    )> {
        let mut values = Vec::new();
        let mut offers = Vec::new();
        let mut fills = Vec::new();
        for cell in row.children() {
            let cst::FactRowChild::FactDatum(datum) = cell else {
                continue;
            };
            let mut value = None;
            let mut fill = None;
            for part in datum.children() {
                match part {
                    cst::FactDatumChild::Ground(ground) => value = Some(self.ground(ground)?),
                    cst::FactDatumChild::SparseFill(node) => fill = Some(node),
                    cst::FactDatumChild::AsKeyword(_) => {}
                }
            }
            if let Some(node) = fill {
                fills.extend(self.sparse_fill_parts(node)?);
                continue;
            }
            // `as` on a datum is a HEADING OFFER: the literal still supplies,
            // the label only names the POSITION — so it travels beside the
            // value, where a heading is decided, and not inside it.
            values.push(DomainExpression::Application(FunctionApplication::Ground(
                self.require(value, "a fact datum is a ground term")?,
            )));
            offers.push(datum.alias().map(|alias| self.identifier(alias)));
        }
        Ok((values, offers, fills))
    }

    /// THE FACT-FUNCTION: a fact whose `->` declares the functional mode.
    ///
    /// ONE CARRIER, ONE ALGORITHM, EVERY NONEMPTY WIDTH. The declaration IS
    /// the compression, so width one is not an anchored case that happens to
    /// be one row — it is the same declared mode with one input and one
    /// output, read by the same code and stored in the same shape.
    ///
    /// Every arm is validated against the declared widths HERE, at the
    /// definition authority. A row that does not match the head is a defect
    /// in the definition, and refusing it later — through a call that found
    /// the wrong number of things, or through SQL that came out malformed —
    /// would blame the caller for what the author wrote.
    fn fact_function(&mut self, node: cst::FactFunction<'t>) -> Result<ClauseDecl> {
        use crate::pipeline::asts::core::{FactFunctionArm, FactFunctionMode};
        use crate::pipeline::asts::vocabulary::Vec1;

        let name = self.require(node.name(), "a fact function names its subject")?;
        let inputs = self.require(
            Vec1::try_from_vec(node.inputs().map(|input| self.identifier(input)).collect()),
            "a fact function declares an input",
        )?;
        let outputs = self.require(
            Vec1::try_from_vec(node.outputs().map(|out| self.identifier(out)).collect()),
            "a fact function declares an output",
        )?;
        let subject = self.definition_subject(name)?.name.clone();
        // A DECLARED NAME IS DECLARED ONCE, OVER THE WHOLE HEADING. The
        // finite relational face publishes the inputs followed by the outputs
        // as ONE heading, so the two lists are not two namespaces: a name
        // repeated anywhere in them occupies two positions of that heading,
        // and on the callable side leaves either a binder or a pick with two
        // answers.
        unique_heading(&subject, &inputs, &outputs)?;

        let mut arms = Vec::new();
        let mut default = None;
        for child in node.children() {
            match child {
                cst::FactFunctionChild::FactArm(arm) => {
                    let mut matched = Vec::new();
                    for term in arm.inputs() {
                        matched.push(self.ground(term)?);
                    }
                    let mut produced = Vec::new();
                    for result in arm.outputs() {
                        produced.push(self.fact_function_output(&inputs, result)?);
                    }
                    width(&subject, "arm's match row", inputs.len(), matched.len())?;
                    width(&subject, "arm's output row", outputs.len(), produced.len())?;
                    arms.push(FactFunctionArm {
                        inputs: self.require(Vec1::try_from_vec(matched), "an arm matches")?,
                        outputs: self.require(Vec1::try_from_vec(produced), "an arm produces")?,
                    });
                }
                cst::FactFunctionChild::FactDefault(node) => {
                    let mut produced = Vec::new();
                    for result in node.outputs() {
                        produced.push(self.fact_function_output(&inputs, result)?);
                    }
                    width(
                        &subject,
                        "default output row",
                        outputs.len(),
                        produced.len(),
                    )?;
                    default =
                        Some(self.require(Vec1::try_from_vec(produced), "a default produces")?);
                }
                cst::FactFunctionChild::Arrow(_)
                | cst::FactFunctionChild::CommaSigil(_)
                | cst::FactFunctionChild::Separator(_) => {}
            }
        }
        let arms = self.require(Vec1::try_from_vec(arms), "a fact function has an arm")?;
        Ok(self.clause(
            DefKind::FactFunction,
            DefSubject::Named(subject),
            Head::glob(),
            Fixpoint::Bag,
            DdlBody::FactFunction(
                crate::pipeline::asts::core::FactFunctionDefinition::assemble(FactFunctionMode {
                    inputs,
                    outputs,
                    arms,
                    default,
                }),
            ),
            self.text(node),
            None,
        ))
    }

    /// THE DECLARED INPUTS ARE THE OUTPUT CELLS' BINDERS.
    ///
    /// INPUTS DETERMINE OUTPUTS is what the `->` declares, so the names it
    /// declares on the left are exactly the names an output cell may read.
    /// They are the only ones: there is no enclosing row here — in a finite
    /// relational face these cells are a fact's data, and in the callable face
    /// the supplied argument row is all there is — so any other name addresses
    /// nothing, and a qualifier addresses a relation that does not exist.
    ///
    /// The two faces stay one meaning because the binding is spent the same
    /// way in both: the relational face substitutes each arm's own ground
    /// match row, and the callable face substitutes the call's arguments. An
    /// arm that reads `a` publishes the `a` it matched and answers with the
    /// `a` it was called with, and those are the same value.
    fn fact_function_output(
        &mut self,
        inputs: &crate::pipeline::asts::vocabulary::Vec1<SqlIdentifier>,
        node: cst::DomainExpression<'t>,
    ) -> Result<DomainExpression<Unresolved>> {
        let value = self.domain_expression(node)?;
        if let Some(column) = first_unbound_reference(inputs, &value) {
            let declared = inputs
                .iter()
                .map(SqlIdentifier::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(DelightQLError::validation_error_categorized(
                "fact_function/output_reads_no_input",
                format!(
                    "a fact function output cell reads '{column}', which is not one of its \
                     declared inputs — those are {declared}"
                ),
                "an output is determined by the declared inputs: read one of them, or write \
                 an expression over constants",
            ));
        }
        Ok(value)
    }

    // -----------------------------------------------------------------
    // Edges
    // -----------------------------------------------------------------

    /// AN EDGE DECLARATION IS A GROUND HEAD: sugar for
    /// `&(:a-term, :b-term, ::ctx)(*) :- body`, all three parameters ground
    /// on mentions. An edge names a PAIR and baptizes nothing, which is why
    /// its subject is the one deliberately non-name shape.
    fn edge_declaration(&mut self, node: cst::EdgeDeclaration<'t>) -> Result<ClauseDecl> {
        let left = self.require(node.left(), "an edge declares a left term")?;
        let right = self.require(node.right(), "an edge declares a right term")?;
        let body = self.require(node.body(), "an edge declaration has a body")?;
        let context = match node.context() {
            Some(context) => {
                let symbol = self.require(context.child(), "an edge context is a symbol")?;
                self.text(symbol).trim_start_matches("::").to_string()
            }
            None => {
                return Err(DelightQLError::validation_error_categorized(
                    "grounding/er/context_required",
                    "an edge declaration names its context",
                    "write the context as a symbol: `a(*) &(::normal) b(*) :- …`",
                ))
            }
        };
        // IDENTITY IS THE CANONICAL SPELLING: the stored keys are the
        // canonical bytes, never the authored ones.
        let left_spelling = crate::term_spec::canonicalize_term(self.text(left))?;
        let right_spelling = crate::term_spec::canonicalize_term(self.text(right))?;
        let query = self.relex_query(body)?;
        let doc = self.doc_slot(node.children().filter_map(edge_doc))?;
        Ok(self.clause(
            DefKind::Edge,
            DefSubject::edge(left_spelling, right_spelling, context),
            Head::glob(),
            Fixpoint::Bag,
            DdlBody::Relational(query),
            self.text(node),
            doc,
        ))
    }

    // -----------------------------------------------------------------
    // Shared clause assembly
    // -----------------------------------------------------------------

    /// `fixpoint` is the flavor the AUTHORED head badged. Only a relational
    /// rule head has a badge position — recursion is relation-form only — so
    /// every other form states `Bag`, which is what an absent badge claims.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn clause(
        &self,
        kind: DefKind,
        subject: DefSubject,
        head: Head,
        fixpoint: Fixpoint,
        body: DdlBody,
        full_source: &str,
        doc: Option<String>,
    ) -> ClauseDecl {
        DefinitionFront {
            kind,
            subject,
            head,
            fixpoint,
        }
        .into_clause_decl(body, full_source.to_string(), doc)
    }

    /// The doc slot after a neck, read WHOLE: the clause's documentation, and
    /// the annotations standing beside it.
    ///
    /// `doc_slot = definition_annotation+ | definition_annotation*
    /// definition_doc definition_annotation*`, so both inhabit it and both are
    /// answered here. Documentation is the ONE `(~~docs … ~~)` body — the
    /// delimiters are not part of it — and it is what the catalog keeps on the
    /// entity; a second document has no derivation, so nothing here counts
    /// them. An annotation is not documentation: it decorates the definition
    /// and reaches the same collector it reaches anywhere else, so a danger
    /// gate acknowledged over a definition is the acknowledgment the
    /// compilation already knows how to weigh. A slot member that reached
    /// neither would be authored syntax nobody consumed.
    fn doc_slot(
        &mut self,
        slots: impl Iterator<Item = cst::DocSlot<'t>>,
    ) -> Result<Option<String>> {
        let mut doc = None;
        for slot in slots {
            for child in slot.children() {
                match child {
                    cst::DocSlotChild::DefinitionDoc(node) => {
                        let text = node
                            .body()
                            .map(|body| self.text(body).trim())
                            .filter(|body| !body.is_empty());
                        doc = doc.or_else(|| text.map(str::to_string));
                    }
                    cst::DocSlotChild::DefinitionAnnotation(annotation) => {
                        self.definition_annotation(annotation)?
                    }
                }
            }
        }
        Ok(doc)
    }
}

/// TWO OFFERS, ONE POSITION. A stacked fact's header offers a name, and
/// FN.35's `ground as identifier` offers one too; both are heading offers,
/// and the elaboration law says they must agree.
///
/// Agreement is the IDENTIFIER law's, not a character comparison — `TAG` and
/// `tag` are one name, `` `Tag` `` and `Tag` are two — because `SqlIdentifier`
/// folds iff unstropped and that is the only place stroppedness is decided.
///
/// This runs where the fact is READ, so a definition that reaches the mixed
/// fact/view rewrite and one that does not are judged by the same rule at the
/// same moment. A dense cell lands at the heading's next non-sparse position;
/// a sparse column is reached by a fill, which carries no `as`, so it offers
/// nothing to contest.
pub(crate) fn offers_agree_with_header(
    subject: &str,
    headers: &TabularRow<HeaderItem<Unresolved>>,
    sparse: &[(usize, SqlIdentifier)],
    offers: &[Option<SqlIdentifier>],
    row: usize,
) -> Result<()> {
    let dense =
        (0..headers.len()).filter(|at| !sparse.iter().any(|(sparse_at, _)| sparse_at == at));
    for (position, offer) in dense.zip(offers) {
        let Some(offer) = offer else {
            continue;
        };
        // A header item that is not a plain column reference declares no name
        // to contest; `header_parts` has already refused the shapes that
        // cannot stand beside a `?`.
        let Some(DomainExpression::Reference(Reference::Named(NamedReference(column)))) =
            headers.0[position].term()
        else {
            continue;
        };
        if column.name != *offer {
            return Err(name_conflict(
                subject,
                position,
                (&column.name, Offered::Header),
                (offer, Offered::Row(row)),
            ));
        }
    }
    Ok(())
}

/// Whether a refusal says "not yet", rather than "never". Only the
/// substituted-term identity does: everything else is a rule the body broke.
pub(crate) fn awaits_substitution(error: &DelightQLError) -> bool {
    matches!(
        error,
        DelightQLError::ValidationError {
            subcategory: Some(subcategory),
            ..
        } if *subcategory == crate::uri_registry::subcat::LIMIT_VALUE
    )
}

fn doc_slot_of(child: cst::FoRuleChild<'_>) -> Option<cst::DocSlot<'_>> {
    match child {
        cst::FoRuleChild::DocSlot(slot) => Some(slot),
        cst::FoRuleChild::DefinitionNeck(_) => None,
    }
}

fn ho_rule_doc(child: cst::HoRuleChild<'_>) -> Option<cst::DocSlot<'_>> {
    match child {
        cst::HoRuleChild::DocSlot(slot) => Some(slot),
        _ => None,
    }
}

fn function_rule_doc(child: cst::FunctionRuleChild<'_>) -> Option<cst::DocSlot<'_>> {
    match child {
        cst::FunctionRuleChild::DocSlot(slot) => Some(slot),
        _ => None,
    }
}

fn constant_doc(child: cst::ConstantRuleChild<'_>) -> Option<cst::DocSlot<'_>> {
    match child {
        cst::ConstantRuleChild::DocSlot(slot) => Some(slot),
        cst::ConstantRuleChild::DefinitionNeck(_) => None,
    }
}

fn sigma_doc(child: cst::SigmaRuleChild<'_>) -> Option<cst::DocSlot<'_>> {
    match child {
        cst::SigmaRuleChild::DocSlot(slot) => Some(slot),
        _ => None,
    }
}

fn effect_rule_doc(child: cst::EffectRuleChild<'_>) -> Option<cst::DocSlot<'_>> {
    match child {
        cst::EffectRuleChild::DocSlot(slot) => Some(slot),
        _ => None,
    }
}

fn edge_doc(child: cst::EdgeDeclarationChild<'_>) -> Option<cst::DocSlot<'_>> {
    match child {
        cst::EdgeDeclarationChild::DocSlot(slot) => Some(slot),
        _ => None,
    }
}

/// A DECLARED WIDTH IS A DECLARED WIDTH. The head's two lists say how wide
/// every match row and every output row is; a row that disagrees is refused
/// where it was written.
fn width(subject: &str, position: &str, declared: usize, written: usize) -> Result<()> {
    if declared == written {
        return Ok(());
    }
    Err(DelightQLError::validation_error_categorized(
        "fact_function/width",
        format!("'{subject}' declares {declared} for its {position}, and one row writes {written}"),
        "every arm's match row is as wide as the declared inputs, and every output row \
         as wide as the declared outputs",
    ))
}

/// A DECLARED NAME IS DECLARED ONCE, over the whole declared heading.
///
/// The roles are kept so the teaching can say WHICH collision this is — two
/// inputs, two outputs, or an input and an output — because the three read
/// as different mistakes even though the law is one.
fn unique_heading(
    subject: &str,
    inputs: &crate::pipeline::asts::vocabulary::Vec1<SqlIdentifier>,
    outputs: &crate::pipeline::asts::vocabulary::Vec1<SqlIdentifier>,
) -> Result<()> {
    let declared: Vec<(&str, &SqlIdentifier)> = inputs
        .iter()
        .map(|name| ("input", name))
        .chain(outputs.iter().map(|name| ("output", name)))
        .collect();
    for (position, (role, name)) in declared.iter().enumerate() {
        // Exact identifier agreement, stropping included: two spellings that
        // differ are two names, and two that agree are one.
        let Some((earlier, _)) = declared[..position].iter().find(|(_, seen)| seen == name) else {
            continue;
        };
        let collision = if earlier == role {
            format!("twice as an {role}")
        } else {
            format!("once as an {earlier} and once as an {role}")
        };
        return Err(DelightQLError::validation_error_categorized(
            "fact_function/duplicate_name",
            format!("'{subject}' declares '{name}' {collision}"),
            "the inputs and the outputs are ONE heading: each position holds its own name, \
             and a repeated one leaves a reader with two answers",
        ));
    }
    Ok(())
}

/// The first reference inside a value that is NOT one of the declared inputs,
/// by its written spelling. A qualified reference is never a binder — it
/// addresses somebody else's relation — so it is reported whatever it names.
fn first_unbound_reference(
    inputs: &crate::pipeline::asts::vocabulary::Vec1<SqlIdentifier>,
    value: &DomainExpression<Unresolved>,
) -> Option<String> {
    use crate::pipeline::ast_visit::{walk_visit_domain, AstVisit, Descent};

    struct Found<'a> {
        inputs: &'a crate::pipeline::asts::vocabulary::Vec1<SqlIdentifier>,
        unbound: Option<String>,
    }
    impl AstVisit<Unresolved> for Found<'_> {
        /// A NESTED RELATION IS ITS OWN SCOPE. The names inside a scalarized
        /// relation, an existence, or a relational membership address that
        /// relation's columns first, and resolution — not this walk — is
        /// what says which of them correlate outward. The cell's own reads
        /// are the references standing outside every interior.
        fn enter_relational(
            &mut self,
            _: &crate::pipeline::asts::core::Chain<Unresolved>,
        ) -> Result<Descent> {
            Ok(Descent::SkipSubtree)
        }

        fn enter_domain(&mut self, e: &DomainExpression<Unresolved>) -> Result<Descent> {
            if self.unbound.is_none() {
                if let DomainExpression::Reference(Reference::Named(NamedReference(column))) = e {
                    let bound = column.qualifier.is_none()
                        && self.inputs.iter().any(|input| input == &column.name);
                    if !bound {
                        self.unbound = Some(match &column.qualifier {
                            Some(qualifier) => format!("{qualifier}.{}", column.name),
                            None => column.name.to_string(),
                        });
                    }
                }
            }
            Ok(Descent::Continue)
        }
    }

    let mut found = Found {
        inputs,
        unbound: None,
    };
    walk_visit_domain(&mut found, value).ok()?;
    found.unbound
}
