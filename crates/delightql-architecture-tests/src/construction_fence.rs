//! Durable compile-fail coverage for invariant-bearing construction doors.
//!
//! The runner builds a temporary source overlay and appends one probe inside
//! the module that owns the private field or phase payload. Each child Cargo
//! check must fail at the final prohibited operation; ordinary Cargo feature
//! configurations remain compilable. This lives outside `delightql-core` so
//! an ordinary Core unit-test run never launches child Cargo processes.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::Instant;

    use tempfile::TempDir;

    struct Probe {
        source_path: &'static str,
        source: &'static str,
        expected: &'static [&'static str],
    }

    const PROBES: &[Probe] = &[
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::names::CallableId;

    const FORGED: CallableId = CallableId(0);
}
"#,
            expected: &["CallableId", "private fields"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{ColumnOrdinal, NamespacePath, Reference, Resolved};

    fn forge() {
        let _ = Reference::<Resolved>::Ordinal(ColumnOrdinal {
            position: 1,
            reverse: false,
            qualifier: None,
            namespace_path: NamespacePath::empty(),
            glob: false,
        });
    }
}
"#,
            expected: &["Ordinal", "expected `Never`"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::operators::HoArgument;
    use crate::pipeline::asts::core::{AtSign, Resolved};

    fn forge() {
        // A LANDING IS SPENT BEFORE A CLOSED RESOLVED QUERY EXISTS. The
        // invocation that reads it substitutes its relation there, so no
        // resolved argument row can still be carrying the mark.
        let _ = HoArgument::<Resolved>::Landing(AtSign);
    }
}
"#,
            expected: &["Landing", "expected `Never`"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::operators::ScalarArgument;
    use crate::pipeline::asts::core::{ContextMarker, Resolved};

    fn forge() {
        // A CONTEXT MARKER IS CONSUMED WHERE THE CALL INSTANTIATES. The
        // captured context stands as ordinary resolved arguments, so no
        // resolved argument row can still be carrying the mark.
        let _ = ScalarArgument::<Resolved>::Context(ContextMarker);
    }
}
"#,
            expected: &["Context", "expected `Never`"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Glob, OneOut, OutItem, Spread, Unresolved};

    fn forge() {
        // A ONE-VALUE ITEM ADMITS ONE VALUE. An enumeration standing here
        // would be a name published across several columns.
        let _: OutItem<Unresolved> = OutItem::One(OneOut {
            expr: Spread::Glob(Glob::whole()),
            naming: None,
            output: (),
        });
    }
}
"#,
            expected: &["expected", "DomainExpression", "Spread"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Glob, OutItem, Spread, Unresolved};

    fn forge() {
        // A SPREAD HAS NO NAME. The alternative that carries one carries
        // the spread and nothing beside it.
        let _: OutItem<Unresolved> = OutItem::Many(
            Spread::Glob(Glob::whole()),
            Some(delightql_types::SqlIdentifier::new("named")),
        );
    }
}
"#,
            expected: &["Many", "argument"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{ArgumentValue, Path, PathStep, Unresolved};

    fn forge() {
        // A PATH IS A SPEC, NOT A VALUE. It never stands where one value
        // is computed — an argument's value included.
        let path = Path::try_from_steps(vec![PathStep::Key("a".to_string())])
            .expect("one step");
        let _: ArgumentValue<Unresolved> = ArgumentValue::plain(path);
    }
}
"#,
            expected: &["expected", "DomainExpression", "Path"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Path, PathStep, SelectorItem, Spread, Unresolved};

    fn forge() {
        // A path is not an enumeration either: it reaches one value, so no
        // selector or spread position admits one.
        let path = Path::try_from_steps(vec![PathStep::Key("a".to_string())])
            .expect("one step");
        let _: SelectorItem<Unresolved> = SelectorItem::Spread(Spread::Glob(path));
    }
}
"#,
            expected: &["expected", "Glob", "Path"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Membership, MembershipSource, Probe, Unresolved};

    fn forge(probe: Probe<Unresolved>) {
        // MEMBERSHIP IN NOTHING IS NOT A TRUTH. The grammar's candidate grid
        // supplies a row, so no value of this carrier holds none and no
        // lowering has to give the empty set a truth value.
        let _: Membership<Unresolved> = Membership {
            probe,
            negated: false,
            rows: Vec::new(),
            source: MembershipSource::In,
        };
    }
}
"#,
            expected: &["expected", "Vec1", "rows"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{TruthExpression, Unresolved};

    fn forge(one: TruthExpression<Unresolved>) {
        // A CONJUNCTION HAS AT LEAST TWO MEMBERS. One conjunct is that
        // conjunct, so there is no one-member composition to build and no
        // consumer has to unwrap one.
        let _: TruthExpression<Unresolved> = TruthExpression::Conjunction(Box::new(vec![one]));
    }
}
"#,
            expected: &["expected", "Vec2", "Vec<"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{DomainExpression, Unresolved};

    fn forge() {
        // PARENS ARE ADMISSION, NOT MEANING. They decide which expression
        // nests inside which and are spent at normalization; no receipt
        // survives for a later pass to consult, and lowering reads the
        // grouping back off the structure.
        let inner = DomainExpression::<Unresolved>::Reference(unimplemented!());
        let _ = DomainExpression::<Unresolved>::Parenthesized {
            inner: Box::new(inner),
        };
    }
}
"#,
            expected: &["Parenthesized", "not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{DomainExpression, Unresolved};

    fn forge() {
        // THE SUBSTITUTION LAW IS SPENT AT NORMALIZATION. A function pipe
        // becomes the nested application it denotes, so a piped call and a
        // directly written call are one shape and there is no receipt for a
        // later pass to branch on.
        let _ = DomainExpression::<Unresolved>::PipedExpression {
            value: unimplemented!(),
            transforms: Vec::new(),
        };
    }
}
"#,
            expected: &["PipedExpression", "not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{DomainExpression, Unresolved};

    fn forge() {
        // A BRACKETED VALUE IS AN ENCLYPH; A MEMBERSHIP ROW IS A PROBE.
        // Two positions, two carriers, and no tuple variant on the value
        // enum for either of them to be mistaken for.
        let _ = DomainExpression::<Unresolved>::Tuple {
            elements: Vec::new(),
        };
    }
}
"#,
            expected: &["Tuple", "not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{TruthExpression, Unresolved};

    fn forge() {
        // THERE IS NO SYNTHETIC TRUTH LEAF. `true` in source is a ground
        // VALUE; a rewrite needing a logical identity owns it in its plan
        // or SQL AST, and a test needing a truth writes a comparison.
        let _: TruthExpression<Unresolved> = TruthExpression::BooleanLiteral { value: true };
    }
}
"#,
            expected: &["BooleanLiteral", "variant not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{TruthExpression, Unresolved};

    fn forge() {
        // USING ACCEPTS NO ROW. It directs an access or a member's
        // correspondence, so it is not a truth and no truth position
        // admits one.
        let _: TruthExpression<Unresolved> = TruthExpression::Using {
            columns: Vec::new(),
        };
    }
}
"#,
            expected: &["Using", "variant not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{TruthExpression, Unresolved};

    fn forge(left: delightql_types::SqlIdentifier, right: delightql_types::SqlIdentifier) {
        // A WHOLE-HEADING CORRELATION NAMES TWO ARMS and cannot be
        // evaluated against one row, so it is not a truth either. Its home
        // is the comma continuation and the pair-scoped correlation.
        let _: TruthExpression<Unresolved> =
            TruthExpression::GlobCorrelation { left, right };
    }
}
"#,
            expected: &["GlobCorrelation", "variant not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Correspondence, MemberCorrelation, Unresolved};

    fn forge(correspondence: Correspondence) {
        // A CORRESPONDENCE IS READ OFF THE ACCESS at resolution, so the
        // authored phase holds `Never` and this variant has no inhabitant
        // before the access that directs it is resolved.
        let _: MemberCorrelation<Unresolved> = MemberCorrelation::Correspond(correspondence);
    }
}
"#,
            expected: &["expected", "Never", "Correspondence"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::transformer::Mutation;

    fn forge() {
        // The parameter is NAMED so inference cannot fail first: an E0282
        // would end the compile before the privacy fence is reached, and the
        // probe would report a pass it never proved.
        let _: Mutation<()> = Mutation {
            callable: unsafe { std::mem::zeroed() },
            category: unsafe { std::mem::zeroed() },
            source: unsafe { std::mem::zeroed() },
            target: unsafe { std::mem::zeroed() },
            target_relation: unsafe { std::mem::zeroed() },
            stage: unsafe { std::mem::zeroed() },
            receipt: unsafe { std::mem::zeroed() },
        };
    }
}
"#,
            expected: &["Mutation", "private"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{
        Comparison, DomainExpression, DomainHole, FunctionApplication, TruthExpression,
    };

    fn forge() {
        // THE CANONICAL CONSTRUCTORS ARE `all` AND `any`. A binary builder
        // beside them takes two members and makes a two-member node, which
        // is the same-operator nesting the n-ary carrier exists to prevent:
        // handed a conjunction, it produces a conjunction of one.
        let leaf = || {
            TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                left: Box::new(DomainExpression::Application(FunctionApplication::Open(DomainHole::Disregarded))),
                right: Box::new(DomainExpression::Application(FunctionApplication::Open(DomainHole::Disregarded))),
            })
        };
        let _ = TruthExpression::and(leaf(), leaf());
    }
}
"#,
            expected: &["`and`", "not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Unresolved, ValueTemplate, ValueTemplatePart};

    fn forge() {
        // A TEMPLATE THAT INTERPOLATES NOTHING IS A GROUND STRING. The
        // constructor is the only door and it is fallible, so the
        // no-interpolation shape has no representation here at all.
        let _: ValueTemplate<Unresolved> =
            ValueTemplate { parts: crate::pipeline::asts::vocabulary::Vec1::new(ValueTemplatePart::Text("hi".to_string())) };
    }
}
"#,
            expected: &["private", "parts"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{RecordMember, Unresolved, WrittenBinder};
    use crate::pipeline::asts::core::metadata::NamespacePath;

    fn forge() {
        // A CONSTRUCTOR HOLDS NO PATTERN MEMBER. A binder BINDS a heading;
        // a record member BUILDS a value. Shared punctuation is not shared
        // meaning, so no record member enum admits one.
        let _: RecordMember<Unresolved> = RecordMember::Binder(WrittenBinder {
            name: "k".into(),
            namespace_path: NamespacePath::empty(),
        });
    }
}
"#,
            expected: &["Binder", "not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{
        Enclyph, Record, RecordPattern, RecordPatternMember, Unresolved,
    };
    use crate::pipeline::asts::vocabulary::Vec1;

    fn forge() {
        // A PATTERN IS NOT A CONSTRUCTOR. The two families mirror each other
        // member for member and never trade places: a record is built from
        // record members, and a pattern's members bind.
        let pattern = RecordPattern::<Unresolved> {
            members: Vec1::new(RecordPatternMember::Disregarded),
        };
        let _: Enclyph<Unresolved> = Enclyph::Record(Record::plain(pattern.members));
    }
}
"#,
            expected: &["expected", "RecordMember"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{RecordPattern, RecordPatternMember, Unresolved};

    fn forge(members: Vec<RecordPatternMember<Unresolved>>) {
        // A PATTERN IS NONEMPTY BY CONSTRUCTION. `pattern_member (','
        // pattern_member)*` binds at least one name, so a pattern that binds
        // nothing is unspellable rather than checked.
        let _ = RecordPattern::<Unresolved> { members };
    }
}
"#,
            expected: &["expected", "Vec1"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{FunctionApplication, MetadataGroup, Unresolved};

    fn forge(group: MetadataGroup<Unresolved>) {
        // METADATA IS NOT AN ORDINARY FUNCTION EXPRESSION EITHER. A metadata
        // group yields an interior record keyed by DATA, which only a
        // reduction compresses — so no value position reaches one at all.
        let _: FunctionApplication<Unresolved> = FunctionApplication::Metadata(group);
    }
}
"#,
            expected: &["Metadata", "not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{MetadataOut, GroupSpec, ReductionItem, Unresolved};

    fn forge(metadata: MetadataOut<Unresolved>) {
        // THE REDUCTION SPEC IS THE ONE DOOR. Projection, embed and the
        // group's KEYS take out items; only the reduction admits both, and
        // it says so in its own type.
        let _: GroupSpec<Unresolved> = GroupSpec::Distinct {
            keys: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Metadata(metadata)),
        };
    }
}
"#,
            expected: &["expected", "OutItem"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{NameTarget, RenameSource, RenameSpec, Resolved};

    fn forge(from: RenameSource<Resolved>) {
        // A TEMPLATE IS SPENT AT RESOLUTION. The bound phase carries the
        // spelling resolution minted, so an authored target cannot stand in
        // a resolved rename.
        let _: RenameSpec<Resolved> = RenameSpec {
            from,
            to: NameTarget::Identifier("x".to_string()),
        };
    }
}
"#,
            expected: &["NameTarget", "Spelling"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{
        DomainExpression, DomainHole, FunctionApplication, Resolved,
    };

    fn forge() {
        // THE LEAF IS SPENT AT RESOLUTION. The position that applies an
        // open body spends its slot before any closed resolved tree is
        // minted, so a resolved expression cannot carry one.
        let _: DomainExpression<Resolved> = DomainExpression::Application(
            FunctionApplication::Open(DomainHole::CompositionInput),
        );
    }
}
"#,
            expected: &["DomainHole", "OpenLeaf"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{PipeOp, Unresolved};

    fn forge() {
        // AN EMPTY PROJECTION IS UNSPELLABLE. The grammar refuses `()`,
        // and the carrier says it too: the payload is nonempty by type,
        // not by a check somewhere upstream.
        let _: PipeOp<Unresolved> = PipeOp::Project(vec![]);
    }
}
"#,
            expected: &["Vec1", "Vec"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{MetadataGroup, Resolved};

    fn forge(group: MetadataGroup<Resolved>) {
        // ONE METADATA CARRIER, PHASE-SELECTED. The key is `P::Col`, so a
        // resolved group holds a bound occurrence and there is no authored
        // twin standing beside it to drift.
        let _: delightql_types::SqlIdentifier = group.key;
    }
}
"#,
            expected: &["expected", "SqlIdentifier"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{PipeOp, SealedCall, Unresolved};

    fn forge(call: SealedCall<Unresolved>) {
        // A CALL IS NOT AN OPERATOR. A directive call is a relation-position
        // call — authored and synthesized alike — so no pipe operator
        // carries one and no second resolution or lowering road exists for
        // an operator to take.
        let _: PipeOp<Unresolved> = PipeOp::FunctorCall(call);
    }
}
"#,
            expected: &["FunctorCall", "not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::expressions::chain::{RunForm, RunStep};
    use crate::pipeline::asts::core::Unresolved;

    fn forge(form: RunForm<Unresolved>) {
        // A RUN STEP IS THE PARTITION'S OWN PAIR. Holding one IS the
        // membership proof, so no phase can assemble a run step from a form
        // it chose and a relation it chose.
        let _: RunStep<Unresolved> = RunStep { form, result: () };
    }
}
"#,
            expected: &["private"],
        },
        // ---------------------------------------------------- the atomic
        // result. Three doors the R4.5 repair closed, one probe each.
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Continuation, Resolved, Step};
    use crate::relation::SemanticRelation;

    // A RELATION CANNOT BE ATTACHED TO A NODE. `Step` pairs a continuation
    // with what it publishes and its fields are private, so an arm's
    // carrier — or any other valid relation — has no road into the position
    // another step occupies.
    fn forge(form: Continuation<Resolved>, arm: SemanticRelation) {
        let _: Step<Resolved> = Step {
            form,
            result: arm,
        };
    }
}
"#,
            expected: &["private"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::relation::port::Interface;
    use crate::relation::SemanticRelation;

    // AN INTERFACE CANNOT BE ATTACHED TO A RELATION. There is no entrance
    // that takes a relation and a heading side by side, and no way to
    // assemble the heading to offer: `Interface` has no public constructor.
    fn forge(relation: SemanticRelation) {
        let _ = Interface::of(Vec::new());
        let _ = relation;
    }
}
"#,
            expected: &["Interface", "private"],
        },
        // -------------------------------------------- the capability, the
        // refinement outcome and the head, one probe each.
        Probe {
            source_path: "crates/delightql-core/src/pipeline/transformer/mod.rs",
            source: r#"
mod construction_fence_probe {
    use crate::names::Registry;
    use crate::relation::SemanticBuilder;

    // LOWERING HOLDS NO CONSTRUCTION CAPABILITY. There is no function from
    // a registry — shared or borrowed — to a builder, so a phase past the
    // semantic epoch cannot recover one from what it holds.
    fn forge(registry: &Registry) {
        let _ = SemanticBuilder::new(registry);
    }
}
"#,
            expected: &["private"],
        },
        Probe {
            source_path: "crates/delightql-core/src/pipeline/transformer/mod.rs",
            source: r#"
mod construction_fence_probe {
    use crate::names::Registry;
    use std::rc::Rc;

    // THE CAPABILITY IS NOT REACHABLE FROM A SHARED REGISTRY. Its one
    // producer takes the registry BY VALUE, so a lowering context holding
    // the shared handle has nothing to open an epoch with.
    fn forge(registry: Rc<Registry>) {
        let _ = crate::relation::Planning::open(registry);
    }
}
"#,
            expected: &["expected", "Registry"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::relation::{RelationId, TotalPortMap};

    // A TOTAL MAP CANNOT BE ASSEMBLED FROM PARTS. The only producer is the
    // refinement road, which reads the lineage a rebuild wrote down — so a
    // caller cannot hand one old relation, one new relation and a list of
    // pairs to anything.
    fn forge(from: RelationId, to: RelationId) {
        let _ = TotalPortMap {
            from,
            to,
            pairs: Vec::new(),
        };
    }
}
"#,
            expected: &["private"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{GroundForm, Grelex, Resolved};
    use crate::relation::SemanticRelation;

    // A HEAD CANNOT BE PAIRED WITH A CHOSEN RELATION. The ground form and
    // what it publishes are one value with private fields, so a valid
    // relation has no road onto another valid head.
    fn forge(form: GroundForm<Resolved>, result: SemanticRelation) {
        let _: Grelex<Resolved> = Grelex { form, result };
    }
}
"#,
            expected: &["private"],
        },
        Probe {
            source_path: "crates/delightql-core/src/pipeline/mod.rs",
            source: r#"
mod construction_fence_probe {
    use crate::names::ColId;
    use crate::relation::PortId;

    // PUBLISHING IS THE AUTHORITY'S. A phase holding a port cannot take the
    // column out of it and it cannot put one in: the payload is private to
    // `crate::relation`, so a position is an output position only where the
    // authority said so.
    fn unwrap(port: PortId) -> ColId {
        port.0
    }
}
"#,
            expected: &["field `0`", "private"],
        },
        Probe {
            source_path: "crates/delightql-core/src/pipeline/mod.rs",
            source: r#"
mod construction_fence_probe {
    use crate::relation::Planning;

    // THE CAPABILITY IS NOT COPYABLE. Sealing a copy would leave the
    // original open beside the reader it produced, which is the state the
    // affine transition exists to make unrepresentable.
    fn forge(planning: Planning) -> (Planning, Planning) {
        let twin = planning.clone();
        (planning, twin)
    }
}
"#,
            expected: &["clone", "Planning"],
        },
        Probe {
            source_path: "crates/delightql-core/src/pipeline/mod.rs",
            source: r#"
mod construction_fence_probe {
    use crate::relation::{Planning, Relations};

    // AND IT CANNOT SURVIVE THE TRANSITION. `seal` takes `self`, so a road
    // that hands back a reader while keeping the capability does not
    // typecheck.
    fn forge(planning: Planning) -> (Planning, Relations) {
        let relations = planning.seal();
        (planning, relations)
    }
}
"#,
            expected: &["moved", "planning"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Chain, Resolved, Step};

    // A BOUND STEP CANNOT BE APPENDED BEHIND ANOTHER PREFIX. What a step
    // publishes is an answer about the operand it was derived over;
    // putting it behind a different one keeps an answer to a question
    // nobody asked.
    fn forge(chain: Chain<Resolved>, step: Step<Resolved>) -> Chain<Resolved> {
        chain.then(step)
    }
}
"#,
            expected: &["then", "trait bounds"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Continuation, Resolved, Step};

    // A BOUND STEP'S PAYLOAD HAS NO SETTER. Swapping what a step DOES
    // while what it publishes stays is the mismatch this arc removed.
    fn forge(step: &mut Step<Resolved>) -> &mut Continuation<Resolved> {
        step.form_mut()
    }
}
"#,
            expected: &["form_mut", "trait bounds"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{DomainExpression, OneOut, Resolved};
    use crate::relation::PortId;

    // A PUBLICATION POSITION CANNOT BE PAIRED WITH A CHOSEN PORT. The
    // bound constructor takes the authority's token, which is unforgeable
    // outside semantic construction.
    fn forge(expr: DomainExpression<Resolved>, output: PortId) -> OneOut<Resolved> {
        OneOut {
            expr,
            naming: None,
            output,
        }
    }
}
"#,
            expected: &["private"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::sql_ast::{DomainExpression, SelectItem};

    // AN EMITTED POSITION CANNOT HIDE WHAT IT REALIZES. There is no
    // variant with an optional alias to fall back on: a position either
    // states the occurrence it realizes or is scaffolding that publishes
    // nothing, and the two are different types.
    fn forge(expr: DomainExpression) -> SelectItem {
        SelectItem::Expression { expr, alias: None }
    }
}
"#,
            expected: &["Expression", "SelectItem"],
        },
    ];

    #[test]
    fn construction_fences_reach_their_prohibited_operations() {
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace = workspace
            .parent()
            .and_then(Path::parent)
            .expect("core crate is nested under workspace crates directory");

        let overlay = TemporaryWorkspace::new(workspace)
            .unwrap_or_else(|error| panic!("create construction-fence overlay: {error}"));
        let started = Instant::now();
        // Most type errors coexist in one compiler run. Privacy checking is
        // later and is suppressed when an earlier type error already makes
        // the crate ill-formed, so its probe owns one diagnostic-complete
        // shard instead of receiving a false negative from error recovery.
        // PRIVACY CHECKING RUNS AFTER TYPE CHECKING and is suppressed once
        // an earlier type error makes the crate ill-formed, so every probe
        // whose prohibition IS a visibility gets a shard of its own or one
        // shared only with other privacy probes.
        let shards = [
            (0, 9),
            (9, 17),
            (17, 18),
            (18, 19),
            (19, 20),
            (20, 30),
            (30, 31),
            (31, 32),
            (32, 33),
            (33, 34),
            (34, 35),
            (35, 36),
            (36, 37),
            (37, 38),
            (38, 42),
            // A PRIVACY PROBE OWNS ITS SHARD. See above: a type error
            // anywhere in the crate suppresses the check this one is for.
            (42, 43),
            (43, PROBES.len()),
        ];
        for (shard_index, &(first, end)) in shards.iter().enumerate() {
            let shard = &PROBES[first..end];
            let installed = overlay
                .install_shard(first, shard)
                .unwrap_or_else(|error| panic!("shard {shard_index}: install probes: {error}"));
            let shard_started = Instant::now();
            let output = overlay
                .run_check()
                .unwrap_or_else(|error| panic!("shard {shard_index}: run Cargo check: {error}"));
            let diagnostics = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );

            assert!(
                !output.status.success(),
                "shard {shard_index} unexpectedly compiled"
            );
            assert!(
                !diagnostics.contains("unresolved import")
                    && !diagnostics.contains("could not find")
                    && !(diagnostics.contains("module `") && diagnostics.contains("is private")),
                "shard {shard_index} failed during setup:\n{diagnostics}"
            );
            for installed_probe in &installed {
                let probe_diagnostics = diagnostics_in_range(&output.stdout, installed_probe);
                assert!(
                    !probe_diagnostics.is_empty(),
                    "{} produced no error whose primary span reaches its injected source:\n{}",
                    installed_probe.label,
                    diagnostics
                );
                for expected in installed_probe.expected {
                    assert!(
                        probe_diagnostics.contains(expected),
                        "{} missed diagnostic fragment {expected:?}:\n{}",
                        installed_probe.label,
                        probe_diagnostics
                    );
                }
            }
            eprintln!(
                "construction-fence shard {}/{} ({} probes): {:.2?}",
                shard_index + 1,
                shards.len(),
                shard.len(),
                shard_started.elapsed()
            );
        }
        eprintln!(
            "construction-fence total ({} probes, {} child checks): {:.2?}",
            PROBES.len(),
            shards.len(),
            started.elapsed()
        );
    }

    struct InstalledProbe {
        label: String,
        source_path: &'static str,
        start_line: u64,
        end_line: u64,
        expected: &'static [&'static str],
    }

    fn diagnostics_in_range(stdout: &[u8], probe: &InstalledProbe) -> String {
        let mut diagnostics = String::new();
        for line in String::from_utf8_lossy(stdout).lines() {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if value.get("reason").and_then(|v| v.as_str()) != Some("compiler-message") {
                continue;
            }
            let Some(message) = value.get("message") else {
                continue;
            };
            if message.get("level").and_then(|v| v.as_str()) != Some("error") {
                continue;
            }
            let reaches_probe =
                message
                    .get("spans")
                    .and_then(|v| v.as_array())
                    .is_some_and(|spans| {
                        spans.iter().any(|span| {
                            span.get("is_primary").and_then(|v| v.as_bool()) == Some(true)
                                && span
                                    .get("file_name")
                                    .and_then(|v| v.as_str())
                                    .is_some_and(|path| path.ends_with(probe.source_path))
                                && span.get("line_start").and_then(|v| v.as_u64()).is_some_and(
                                    |line| (probe.start_line..=probe.end_line).contains(&line),
                                )
                        })
                    });
            if reaches_probe {
                if let Some(rendered) = message.get("rendered").and_then(|v| v.as_str()) {
                    diagnostics.push_str(rendered);
                } else if let Some(text) = message.get("message").and_then(|v| v.as_str()) {
                    diagnostics.push_str(text);
                    diagnostics.push('\n');
                }
            }
        }
        diagnostics
    }

    fn probe_label(index: usize, probe: &Probe) -> String {
        let summary: String = probe.expected[0]
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
            .collect();
        format!("probe-{index:02}-{summary}")
    }

    struct TemporaryWorkspace {
        root: TempDir,
        original: PathBuf,
    }

    impl TemporaryWorkspace {
        fn new(original: &Path) -> io::Result<Self> {
            let root = tempfile::tempdir()?;
            let workspace = Self {
                root,
                original: original.to_path_buf(),
            };
            workspace.install_workspace()?;
            Ok(workspace)
        }

        fn install_workspace(&self) -> io::Result<()> {
            copy_file(
                &self.original.join("Cargo.toml"),
                &self.root.path().join("Cargo.toml"),
            )?;
            copy_file(
                &self.original.join("Cargo.lock"),
                &self.root.path().join("Cargo.lock"),
            )?;

            for entry in fs::read_dir(&self.original)? {
                let entry = entry?;
                let name = entry.file_name();
                if name == "crates"
                    || name == "target"
                    || name == ".git"
                    || name == ".jj"
                    || name == "Cargo.toml"
                    || name == "Cargo.lock"
                {
                    continue;
                }
                symlink_or_copy(&entry.path(), &self.root.path().join(name))?;
            }

            let crates = self.root.path().join("crates");
            fs::create_dir(&crates)?;
            for entry in fs::read_dir(self.original.join("crates"))? {
                let entry = entry?;
                let destination = crates.join(entry.file_name());
                if entry.file_name() == "delightql-core" {
                    copy_tree(&entry.path(), &destination)?;
                } else {
                    symlink_or_copy(&entry.path(), &destination)?;
                }
            }
            Ok(())
        }

        fn install_shard(
            &self,
            first_index: usize,
            probes: &[Probe],
        ) -> io::Result<Vec<InstalledProbe>> {
            // EVERY probe path is restored, not only this shard's: a stale
            // probe left in another file by an earlier shard is a type
            // error in the same crate, and privacy checking does not run
            // once one of those has made the crate ill-formed. A shard's
            // compile sees its own probes and nothing else.
            let mut paths: Vec<&str> = PROBES.iter().map(|probe| probe.source_path).collect();
            paths.sort_unstable();
            paths.dedup();
            for source_path in paths {
                copy_file(
                    &self.original.join(source_path),
                    &self.root.path().join(source_path),
                )?;
            }

            let mut installed = Vec::with_capacity(probes.len());
            for (offset, probe) in probes.iter().enumerate() {
                let index = first_index + offset;
                let path = self.root.path().join(probe.source_path);
                let before = fs::read_to_string(&path)?;
                let start_line = before.lines().count() as u64 + 1;
                let source = probe.source.replacen(
                    "construction_fence_probe",
                    &format!("construction_fence_probe_{index:02}"),
                    1,
                );
                use std::io::Write;
                let mut file = fs::OpenOptions::new().append(true).open(&path)?;
                file.write_all(source.as_bytes())?;
                let end_line = fs::read_to_string(&path)?.lines().count() as u64 + 1;
                installed.push(InstalledProbe {
                    label: probe_label(index, probe),
                    source_path: probe.source_path,
                    start_line,
                    end_line,
                    expected: probe.expected,
                });
            }
            Ok(installed)
        }

        fn run_check(&self) -> io::Result<std::process::Output> {
            Command::new("cargo")
                .current_dir(self.root.path())
                .args([
                    "check",
                    "-p",
                    "delightql-core",
                    "--lib",
                    "--jobs",
                    "2",
                    "--message-format=json",
                ])
                .env("CARGO_TARGET_DIR", self.root.path().join("target"))
                .output()
        }
    }

    fn copy_file(source: &Path, destination: &Path) -> io::Result<()> {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source, destination).map(|_| ())
    }

    fn copy_tree(source: &Path, destination: &Path) -> io::Result<()> {
        fs::create_dir_all(destination)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let child = destination.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                copy_tree(&entry.path(), &child)?;
            } else {
                copy_file(&entry.path(), &child)?;
            }
        }
        Ok(())
    }

    fn symlink_or_copy(source: &Path, destination: &Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(source, destination)
        }
        #[cfg(not(unix))]
        {
            if source.is_dir() {
                copy_tree(source, destination)
            } else {
                copy_file(source, destination)
            }
        }
    }
}
