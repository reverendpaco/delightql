//! Durable compile-fail coverage for invariant-bearing construction doors.
//!
//! The runner builds a temporary source overlay and appends one probe inside
//! the module that owns the private field or phase payload. Each child Cargo
//! check must fail at the final prohibited operation; ordinary Cargo feature
//! configurations remain compilable.

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::process::Command;

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
            expected: &["expected", "OutValue", "Spread"],
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
            target_scope: unsafe { std::mem::zeroed() },
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
    use crate::pipeline::asts::core::{Comparison, DomainExpression, TruthExpression, Unresolved};

    fn forge() {
        // THE TRUTH BOUNDARY. No domain expression is a truth read as a
        // value: the crossing stands at an out item's value, an argument,
        // or a slot constraint, each in its own type, and the broad wrapper
        // that let any value position hold one no longer exists.
        let _ = DomainExpression::<Unresolved>::Predicate {
            expr: Box::new(TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                left: Box::new(DomainExpression::Application(FunctionApplication::Open(DomainHole::Disregarded))),
                right: Box::new(DomainExpression::Application(FunctionApplication::Open(DomainHole::Disregarded))),
            })),
        };
    }
}
"#,
            expected: &["Predicate", "variant not found"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{
        Comparison, DomainExpression, Resolved, SlotConstraint, TruthAsValue, TruthExpression,
    };

    fn forge() {
        // A RESOLVED CROSSED SLOT NAMES ITS COLUMN. The unification is built
        // at lowering from the pair, so a resolved crossing without one
        // would drop the constraint the author wrote.
        let _ = SlotConstraint::<Resolved>::Truth {
            column: (),
            value: TruthAsValue(TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                left: Box::new(DomainExpression::Application(FunctionApplication::Open(DomainHole::Disregarded))),
                right: Box::new(DomainExpression::Application(FunctionApplication::Open(DomainHole::Disregarded))),
            })),
        };
    }
}
"#,
            expected: &["expected", "ColId"],
        },
        Probe {
            source_path: "crates/delightql-core/src/lib.rs",
            source: r#"
mod construction_fence_probe {
    use crate::pipeline::asts::core::{Comparison, DomainExpression, TruthExpression};

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
    use crate::pipeline::asts::core::expressions::functions::MatchArm;
    use crate::pipeline::asts::core::{
        Comparison, DomainExpression, DomainHole, FunctionApplication, LiteralValue, TruthAsValue,
        TruthExpression, Unresolved,
    };

    fn forge() {
        // AN AUTHORED CASE RESULT IS A DOMAIN EXPRESSION. A multi-clause
        // value rule's selection carries `OutValue` because a clause's
        // result is its BODY — one of the crossing's licensed positions —
        // and that is a different carrier, so this one does not widen.
        let _: MatchArm<Unresolved> = MatchArm {
            term: LiteralValue::Null,
            result: Box::new(TruthAsValue(TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                left: Box::new(DomainExpression::Application(FunctionApplication::Open(DomainHole::Disregarded))),
                right: Box::new(DomainExpression::Application(FunctionApplication::Open(DomainHole::Disregarded))),
            }))),
        };
    }
}
"#,
            expected: &["expected", "DomainExpression"],
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
    use crate::pipeline::asts::core::{
        Enclyph, MetadataGroup, MetadataTarget, RecordMember, Unresolved,
    };

    fn forge(group: MetadataGroup<Unresolved>, enclyph: Enclyph<Unresolved>) {
        // METADATA IS NOT A CONSTRUCTION MEMBER. Its home is the reduction
        // spec: data values become KEYS, which only a reduction can
        // compress, and `meta_target` chains are the only way down.
        let _ = MetadataTarget::Enclyph(enclyph);
        let _: RecordMember<Unresolved> = RecordMember::Metadata(group);
    }
}
"#,
            expected: &["Metadata", "not found"],
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
            expected: &["DomainHole", "Never"],
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
    use crate::pipeline::asts::core::expressions::chain::RunStep;
    use crate::pipeline::asts::core::{Continuation, Unresolved};

    fn forge(step: Continuation<Unresolved>) {
        // A STRUCTURAL RUN STEP IS THE EXACT FAMILY. The partition moves a
        // StructuralStep whole; the broad continuation enum cannot ride the
        // run, so no phase can classify a structural step again.
        let _: RunStep<Unresolved> = RunStep::Structural(step);
    }
}
"#,
            expected: &["expected", "StructuralStep", "Continuation"],
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
        for (index, probe) in PROBES.iter().enumerate() {
            overlay
                .install_probe(probe)
                .unwrap_or_else(|error| panic!("probe {index}: install probe: {error}"));
            let output = overlay
                .run_check()
                .unwrap_or_else(|error| panic!("probe {index}: run isolated Cargo check: {error}"));
            let diagnostics = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );

            assert!(
                !output.status.success(),
                "probe {index} unexpectedly compiled"
            );
            assert!(
                !diagnostics.contains("unresolved import")
                    && !diagnostics.contains("could not find")
                    && !(diagnostics.contains("module `") && diagnostics.contains("is private")),
                "probe {index} failed during setup:\n{diagnostics}"
            );
            for expected in probe.expected {
                assert!(
                    diagnostics.contains(expected),
                    "probe {index} missed diagnostic fragment {expected:?}:\n{diagnostics}"
                );
            }
        }
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

        fn install_probe(&self, probe: &Probe) -> io::Result<()> {
            copy_file(
                &self.original.join(probe.source_path),
                &self.root.path().join(probe.source_path),
            )?;
            let path = self.root.path().join(probe.source_path);
            use std::io::Write;
            let mut file = fs::OpenOptions::new().append(true).open(path)?;
            file.write_all(probe.source.as_bytes())
        }

        fn run_check(&self) -> io::Result<std::process::Output> {
            Command::new("cargo")
                .current_dir(self.root.path())
                .args(["check", "-p", "delightql-core", "--lib"])
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
