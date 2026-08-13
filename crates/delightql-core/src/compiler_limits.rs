// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! What the compiler's bounded resources ARE, said once.
//!
//! A limit is a RESOURCE policy, never a rule of the language: nothing here
//! says what a valid query may be, only how much of this process one may
//! spend. Each resource is described exactly once — its name, its default,
//! the ceiling ordinary runtime configuration cannot raise past, what the
//! number counts, the identity its refusal wears, and the environment knob
//! that sets it.
//!
//! ONE DESCRIPTION, TWO READERS. The guard that spends the resource and the
//! catalog that publishes it read the same [`CompilerLimit`]. A second copy —
//! constants on one side and literal rows in the bootstrap schema on the
//! other — is how a later safety adjustment makes `sys::execution` and the
//! guard disagree while both still compile.
//!
//! ONE MEMBERSHIP DECLARATION. `bounded_resources!` emits the kind, the
//! mapping to its description, and the iterable membership from a single list,
//! so a resource cannot be described and answered for while remaining absent
//! from what publication walks.
//!
//! The process cells here are POLICY: what a compilation started from now on
//! will arm with. What a compilation ALREADY RUNNING is bounded by is its
//! [`ArmedLimits`], and [`Running`] — the extent of its EXECUTION, not of the
//! object that drives it — is how work too deep to be handed anything still
//! finds them.

use crate::refinement_budget::RefinementBudget;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One bounded compiler resource: everything a guard enforces and a catalog
/// publishes about it, plus the process cell holding what a host has asked
/// for.
pub struct CompilerLimit {
    name: &'static str,
    default_value: usize,
    ceiling: usize,
    unit: &'static str,
    refusal: &'static str,
    knob: &'static str,
    /// Zero means "not yet read"; the environment is consulted once, lazily.
    /// It is also why zero can never be STORED: a stored zero would read back
    /// as unread, and the next reader would silently get the default.
    cell: AtomicUsize,
}

/// Levels of authored tree nesting a compilation may use.
///
/// MEASURED on this tree, with the debug binary every validation lane runs
/// (debug frames are the fat ones, so this is the conservative floor):
///
/// - The accepted corpus's deepest real query is **263**
///   (`sef_core/filters_after_pipes--ultimate_50_pipe_test_no_comments`;
///   next are 211, 112, 111, then a long flat tail at 54).
/// - A parenthesis ladder aborted the process at tree depth ~500 before the
///   walks below it were made stack-safe, and above **1013** after. The
///   three that overflowed — grounding's inliners, the bubble collector,
///   and the resolver fold's domain walk — descend once per nesting level,
///   which is why a ladder reaches exactly them.
///
/// The default 500 sits ~1.9x above what real queries ask for and ~2x below
/// the measured abort floor. Both margins are deliberate: the corpus side
/// because a query is allowed to be bigger than today's biggest, the crash
/// side because a host thread can be smaller than this process's main one
/// and because shapes cost different amounts of stack per level.
///
/// The ceiling sits just under the 1013 the ladder aborted at, and at twice
/// the default. What it bounds is CONFIGURATION, not physics: a host thread
/// smaller than this process's main one can abort below either number, and no
/// value here changes that. Its margin is thinner than the refinement
/// budget's (2x its default, where that one has 8x) and the failure past it
/// is an abort rather than gradual pressure. Both are reasons this number is
/// small, not reasons it is absent.
pub static NESTING: CompilerLimit = CompilerLimit::new(
    "nesting",
    500,
    1000,
    "authored tree levels",
    crate::uri_registry::subcat::RESOURCE_NESTING,
    "DELIGHTQL_MAX_NESTING",
);

/// Active refinement frames a compilation may hold at once.
///
/// MEASURED over the full SQLite corpus, one ball and one server worker at a
/// time — 3,535 executions, 9,108 refinement roots (one submitted query
/// refines definitions, CTEs, assertions and other compiler-built relations
/// besides its own relation):
///
/// - chain transformation: p50 1, p99 6, second-highest 15, maximum **101**;
/// - rebuilder re-entry: p50 1, p99 3, second-highest 7, maximum **50**.
///
/// Both maxima are the deliberately extreme fifty-stage pipe test
/// (`sef_core/filters_after_pipes--ultimate_50_pipe_test_no_comments`), a
/// lawful stress query rather than evidence of a cycle. The default 512 is
/// five times the observed maximum and thirty-four times the second-highest,
/// and still small enough to stop a repeating compiler state before it
/// threatens the process.
///
/// A far larger number would not be a safer default — it would be an
/// ineffective one: the walk clones and retains enough state per frame to
/// exhaust memory long before a counter in the hundred-thousands is reached.
///
/// The default is POLICY and belongs to the host. The ceiling is PROCESS
/// SAFETY and belongs to the build: supporting a deeper walk requires a new
/// bounded measurement and a code decision, not a bigger environment
/// variable.
pub static REFINEMENT_DEPTH: CompilerLimit = CompilerLimit::new(
    "refinement-depth",
    512,
    4096,
    "active refiner frames",
    crate::uri_registry::subcat::RESOURCE_REFINEMENT_DEPTH,
    "DELIGHTQL_MAX_REFINEMENT_DEPTH",
);

/// The resource set, declared ONCE.
///
/// The kind, the mapping to its description, and the iterable membership all
/// come out of this one list, so a resource cannot be described and answered
/// for while remaining absent from what publication walks — there is no second
/// place to add it to, and no second place to forget.
macro_rules! bounded_resources {
    ($($kind:ident => $descriptor:ident),+ $(,)?) => {
        /// WHICH bounded resource. Everything that has to answer per-resource
        /// matches on this, so a kind added to the declaration is a compile
        /// error at each such place until it is answered.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum LimitKind {
            $($kind),+
        }

        /// Every bounded resource, in the order the catalog publishes them.
        pub const ALL: &[LimitKind] = &[$(LimitKind::$kind),+];

        impl LimitKind {
            /// What this resource IS.
            pub fn descriptor(self) -> &'static CompilerLimit {
                match self {
                    $(LimitKind::$kind => &$descriptor),+
                }
            }
        }
    };
}

bounded_resources!(Nesting => NESTING, RefinementDepth => REFINEMENT_DEPTH);

impl CompilerLimit {
    const fn new(
        name: &'static str,
        default_value: usize,
        ceiling: usize,
        unit: &'static str,
        refusal: &'static str,
        knob: &'static str,
    ) -> Self {
        CompilerLimit {
            name,
            default_value,
            ceiling,
            unit,
            refusal,
            knob,
            cell: AtomicUsize::new(0),
        }
    }

    /// The catalog's name for this limit.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// What an unconfigured process uses.
    pub fn default_value(&self) -> usize {
        self.default_value
    }

    /// The value ordinary runtime configuration cannot raise past.
    pub fn ceiling(&self) -> usize {
        self.ceiling
    }

    /// What the number counts. Published beside the value so an operator
    /// reading the catalog is not left to infer whether 512 means frames,
    /// bytes, or seconds.
    pub fn unit(&self) -> &'static str {
        self.unit
    }

    /// The subcategory this limit's refusal is raised under.
    pub fn refusal(&self) -> &'static str {
        self.refusal
    }

    /// The identity the refusal wears, as an operator reads it off the badge
    /// and looks it up in the catalog.
    pub fn error_identity(&self) -> String {
        format!(
            "{}{}",
            crate::uri_registry::UriKind::Error.scheme(),
            self.refusal
        )
    }

    /// The environment knob that sets this limit when the host is a process.
    pub fn knob(&self) -> &'static str {
        self.knob
    }

    /// The value in force for compilations started from now on.
    ///
    /// A compilation already running keeps what it armed with; see
    /// [`ArmedLimits`].
    pub fn effective(&self) -> usize {
        let current = self.cell.load(Ordering::Relaxed);
        if current != 0 {
            return current;
        }
        let from_env = self.read(std::env::var(self.knob).ok().as_deref());
        self.cell.store(from_env, Ordering::Relaxed);
        from_env
    }

    /// Ask this process to carry a different value, and learn what it did.
    ///
    /// Three outcomes, and they are three because a host cannot act on two:
    /// a request above the ceiling MUTATES the process to the ceiling, and a
    /// request of zero mutates nothing. A single boolean would report both as
    /// failure while only one of them left the process where it was.
    pub fn set(&self, requested: usize) -> LimitOutcome {
        let Some(accepted) = self.accepted(requested) else {
            return LimitOutcome::Invalid {
                requested,
                effective: self.effective(),
            };
        };
        self.cell.store(accepted, Ordering::Relaxed);
        if accepted == requested {
            LimitOutcome::Exact {
                effective: accepted,
            }
        } else {
            LimitOutcome::Clamped {
                requested,
                effective: accepted,
            }
        }
    }

    /// What [`set`](Self::set) would store, and `None` for the value it
    /// refuses.
    ///
    /// Zero is not a spelling for "unlimited": a budget of zero would refuse
    /// every query, and it is the cell's own "unread" mark besides. A caller
    /// reaching for "no limit" wants a large number, and gets the ceiling.
    ///
    /// Split from the store because the cell is process-wide and every
    /// compilation reads it, so a test exercising the arithmetic through the
    /// store would be changing what another thread is reading.
    fn accepted(&self, requested: usize) -> Option<usize> {
        (requested > 0).then(|| requested.min(self.ceiling))
    }

    /// The value a raw knob asks for, and gets.
    ///
    /// A value that is not a positive integer reads as the default: a
    /// malformed knob must not turn every query into a refusal, and — the
    /// property that matters — it must not turn the guard off. A value above
    /// the ceiling is CLAMPED rather than refused, because a host asking for
    /// more than the process can survive should get the most it can survive,
    /// never no guard.
    fn read(&self, raw: Option<&str>) -> usize {
        raw.and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(self.default_value)
            .min(self.ceiling)
    }
}

/// What a request to change a limit did, and what is in force afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitOutcome {
    /// Applied as asked.
    Exact { effective: usize },
    /// Above the ceiling, so the ceiling took effect. The process DID change.
    Clamped { requested: usize, effective: usize },
    /// Not a budget. The process did NOT change, and `effective` is the value
    /// that was already in force.
    Invalid { requested: usize, effective: usize },
}

impl LimitOutcome {
    /// The value in force after the call — whichever outcome this is.
    pub fn effective(self) -> usize {
        match self {
            LimitOutcome::Exact { effective }
            | LimitOutcome::Clamped { effective, .. }
            | LimitOutcome::Invalid { effective, .. } => effective,
        }
    }
}

/// The authored-depth allowance ONE compilation runs under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NestingBudget(usize);

impl NestingBudget {
    /// The budget in force where the caller stands: the compilation
    /// EXECUTING on this thread answers with what it armed, and process
    /// policy answers when none is.
    pub fn current() -> Self {
        ArmedLimits::in_force().nesting()
    }

    /// Read the process policy now, ignoring any running compilation. What a
    /// NEW compilation arms from.
    pub fn from_policy() -> Self {
        NestingBudget(NESTING.effective())
    }

    pub fn levels(self) -> usize {
        self.0
    }
}

thread_local! {
    /// The limits of the compilations EXECUTING on this thread, innermost
    /// last. A compilation may run another inside itself — a consulted
    /// definition's instantiation,
    /// a stored view's body — and each answers for its own extent.
    static RUNNING: std::cell::RefCell<Vec<(u64, Rc<ArmedLimits>)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static NEXT_RUNNING: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// The extent over which one compilation is EXECUTING.
///
/// Held across compiler work, never across mere object retention. That
/// distinction is the whole point: a `Pipeline` may be kept alive so its
/// cached stages can be read while an unrelated compilation is built and run
/// on the same thread, and a retained object must not answer for the work
/// happening beside it. Held around execution instead, the ambient answer is
/// exactly "the compilation whose work this is" — so an arena minted while one
/// runs is NESTED and shares its limits, and an arena minted while one is only
/// retained is INDEPENDENT and arms from policy.
pub struct Running {
    id: u64,
}

impl Running {
    /// Enter the extent of a compilation running under `limits`.
    pub fn under(limits: Rc<ArmedLimits>) -> Running {
        let id = NEXT_RUNNING.with(|next| {
            let id = next.get().wrapping_add(1);
            next.set(id);
            id
        });
        RUNNING.with(|running| running.borrow_mut().push((id, limits)));
        Running { id }
    }

    fn innermost() -> Option<Rc<ArmedLimits>> {
        RUNNING.with(|running| running.borrow().last().map(|(_, limits)| Rc::clone(limits)))
    }
}

impl Drop for Running {
    /// Removed BY IDENTITY, not by position: two compilations' extents may
    /// overlap without nesting, and popping the top would then retire the
    /// wrong one's limits.
    fn drop(&mut self) {
        let id = self.id;
        RUNNING.with(|running| running.borrow_mut().retain(|(open, _)| *open != id));
    }
}

/// The limits ONE compilation runs under — BOTH of them, together.
///
/// They are armed together and inherited together. Nested compiler work does
/// not synthesize its own pair out of one inherited half and one fresh read:
/// it receives THIS object, so the depth it is judged against and the frames
/// it spends both belong to the compilation that caused it. Refinement frames
/// especially, because they are STATE — a walk holds them and gives them back
/// — and a re-entry handed a fresh allowance is exactly the unbounded cycle
/// the budget exists to stop.
///
/// A host that raises a budget mid-compilation changes the NEXT compilation,
/// not the one already walking. That is also why publication is HANDED these
/// and does not consult the process cells: a catalog reporting a number the
/// running compilation is not bounded by would be a lie its reader has no way
/// to detect.
pub struct ArmedLimits {
    nesting: NestingBudget,
    refinement: RefinementBudget,
}

impl ArmedLimits {
    /// Arm a NEW compilation, both budgets, from process policy.
    pub fn from_policy() -> Self {
        ArmedLimits {
            nesting: NestingBudget::from_policy(),
            refinement: RefinementBudget::new(REFINEMENT_DEPTH.effective()),
        }
    }

    /// The limits in force here.
    ///
    /// SHARED with the compilation executing on this thread when there is
    /// one — the same object, so nested work spends the same frames — and a
    /// fresh arming from policy when there is not.
    pub fn in_force() -> Rc<ArmedLimits> {
        Running::innermost().unwrap_or_else(|| Rc::new(ArmedLimits::from_policy()))
    }

    /// The authored depth this compilation's parse is measured against.
    pub fn nesting(&self) -> NestingBudget {
        self.nesting
    }

    /// The refinement frames this compilation may hold, and how many it
    /// currently holds.
    pub fn refinement(&self) -> &RefinementBudget {
        &self.refinement
    }

    /// What this compilation armed for one resource — the value the catalog
    /// publishes for it. Exhaustive: a new kind does not compile until this
    /// answers for it.
    pub fn effective(&self, kind: LimitKind) -> usize {
        match kind {
            LimitKind::Nesting => self.nesting.levels(),
            LimitKind::RefinementDepth => self.refinement.max(),
        }
    }
}

/// Exclusive use of the process cells for one test, restored on drop.
///
/// The cells are process-wide and every parse and every refinement reads
/// them, so two tests setting them at once would be reading each other's
/// numbers. Restoration happens on DROP, so a failed assertion still hands
/// the process back the way it found it.
///
/// A lease is not a licence to set anything: whatever else the harness is
/// running reads these cells too, so a test must only ever store a value AT
/// OR ABOVE the default. A smaller one is a spurious refusal in whatever
/// query happens to compile in that instant.
#[cfg(test)]
pub(crate) struct ProcessLimitLease {
    _exclusive: std::sync::MutexGuard<'static, ()>,
    /// Read from [`ALL`], so a resource the enumeration gains is restored
    /// without anyone remembering to add it here.
    restore: Vec<(LimitKind, usize)>,
}

#[cfg(test)]
impl ProcessLimitLease {
    pub(crate) fn take() -> Self {
        static EXCLUSIVE: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let guard = EXCLUSIVE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ProcessLimitLease {
            _exclusive: guard,
            restore: ALL
                .iter()
                .map(|kind| (*kind, kind.descriptor().effective()))
                .collect(),
        }
    }
}

#[cfg(test)]
impl Drop for ProcessLimitLease {
    fn drop(&mut self) {
        for (kind, value) in &self.restore {
            kind.descriptor().cell.store(*value, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The knob's reading, checked where it is a pure function of the raw
    /// value. Nothing here can disable a guard: not zero, not a word, not a
    /// number above the ceiling.
    #[test]
    fn no_knob_value_can_disable_a_guard() {
        for limit in ALL.iter().copied().map(LimitKind::descriptor) {
            for silent in [
                None,
                Some(""),
                Some("0"),
                Some("-3"),
                Some("lots"),
                Some(" "),
                Some("1.5"),
            ] {
                assert_eq!(
                    limit.read(silent),
                    limit.default_value(),
                    "{} should read {silent:?} as its default",
                    limit.name()
                );
            }
            assert_eq!(limit.read(Some("64")), 64, "{}", limit.name());
            assert_eq!(
                limit.read(Some("18446744073709551615")),
                limit.ceiling(),
                "{} clamps the largest spelling there is",
                limit.name()
            );
        }
    }

    /// The setter honours the same ceiling the environment road does. A
    /// published ceiling true of only one of the two roads would be false of
    /// whichever session used the other.
    #[test]
    fn neither_road_reaches_past_the_ceiling() {
        for limit in ALL.iter().copied().map(LimitKind::descriptor) {
            assert_eq!(
                limit.accepted(0),
                None,
                "{}: zero is not a budget",
                limit.name()
            );
            assert_eq!(limit.accepted(1), Some(1), "{}", limit.name());
            assert_eq!(
                limit.accepted(limit.ceiling()),
                Some(limit.ceiling()),
                "{}",
                limit.name()
            );
            assert_eq!(
                limit.accepted(limit.ceiling() + 1),
                Some(limit.ceiling()),
                "{}",
                limit.name()
            );
            assert_eq!(
                limit.accepted(usize::MAX),
                Some(limit.ceiling()),
                "{}",
                limit.name()
            );
        }
    }

    /// Every published field is stated, and a default is never at or above
    /// the ceiling that is supposed to bound it.
    #[test]
    fn every_limit_describes_itself_completely() {
        for limit in ALL.iter().copied().map(LimitKind::descriptor) {
            assert!(!limit.name().is_empty());
            assert!(!limit.unit().is_empty());
            assert!(!limit.knob().is_empty());
            assert!(
                limit.default_value() > 0 && limit.default_value() < limit.ceiling(),
                "{}: the default must sit inside the ceiling it is bounded by",
                limit.name()
            );
            assert_eq!(
                limit.error_identity(),
                format!("delightql-error://{}", limit.refusal()),
                "{}",
                limit.name()
            );
        }
        let names: Vec<_> = ALL.iter().map(|kind| kind.descriptor().name()).collect();
        assert_eq!(
            names,
            ["nesting", "refinement-depth"],
            "the two budgets measure different objects at different times and \
             must stay two rows"
        );
    }

    /// A compilation arms from policy and then stops listening. The
    /// process moving afterwards is the NEXT compilation's business.
    #[test]
    fn an_armed_compilation_does_not_hear_a_later_setting() {
        let _lease = ProcessLimitLease::take();
        assert_eq!(NESTING.set(700).effective(), 700);
        assert_eq!(REFINEMENT_DEPTH.set(1024).effective(), 1024);

        let armed = ArmedLimits::from_policy();

        assert_eq!(NESTING.set(900).effective(), 900);
        assert_eq!(REFINEMENT_DEPTH.set(2048).effective(), 2048);

        assert_eq!(
            armed.nesting().levels(),
            700,
            "the parse keeps its allowance"
        );
        assert_eq!(
            armed.refinement().max(),
            1024,
            "the refiner keeps its allowance"
        );
    }

    /// Every kind answers, and answers with what this compilation armed —
    /// which is what publication writes into the row.
    #[test]
    fn armed_limits_answer_for_every_kind() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(700);
        REFINEMENT_DEPTH.set(1024);
        let armed = ArmedLimits::from_policy();

        assert_eq!(armed.effective(LimitKind::Nesting), 700);
        assert_eq!(armed.effective(LimitKind::RefinementDepth), 1024);
        for kind in ALL.iter().copied() {
            assert!(
                armed.effective(kind) > 0,
                "{} must have an armed value to publish",
                kind.descriptor().name()
            );
        }
    }

    /// Work done INSIDE a running compilation shares that compilation's
    /// limits — BOTH of them, and the refinement budget as the same object.
    ///
    /// Half-inheriting is the bug this pins: a nested arena that took the
    /// outer depth but a fresh read of refinement policy would hand a
    /// re-entry an allowance its caller never armed, which is the unbounded
    /// cycle the budget exists to stop.
    #[test]
    fn nested_work_shares_the_running_compilation_s_whole_allowance() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(700);
        REFINEMENT_DEPTH.set(1024);
        let outer = Rc::new(ArmedLimits::from_policy());
        let _running = Running::under(Rc::clone(&outer));

        // The host moves BOTH policies after the outer compilation armed.
        NESTING.set(1000);
        REFINEMENT_DEPTH.set(2048);

        let nested = ArmedLimits::in_force();
        assert_eq!(nested.nesting().levels(), 700, "the depth is the outer's");
        assert_eq!(
            nested.refinement().max(),
            1024,
            "and so is the frame allowance"
        );
        assert!(
            Rc::ptr_eq(&outer, &nested),
            "nested work receives the outer limits rather than a synthesized pair"
        );

        // The frames are the SAME state, so a re-entry cannot spend them twice.
        let _frame = outer.refinement().enter().expect("a frame is affordable");
        assert_eq!(
            nested.refinement().active(),
            1,
            "the nested view sees the frame the outer walk is holding"
        );
    }

    /// A compilation that begins while nothing is running arms from policy —
    /// both budgets, and its own frames.
    #[test]
    fn an_independent_compilation_arms_from_policy() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(700);
        REFINEMENT_DEPTH.set(1024);
        let retained = Rc::new(ArmedLimits::from_policy());

        // Retained, but NOT running: policy moves and the next compilation
        // gets the policy, not the retained object's allowance.
        NESTING.set(1000);
        REFINEMENT_DEPTH.set(2048);

        let independent = ArmedLimits::in_force();
        assert_eq!(independent.nesting().levels(), 1000);
        assert_eq!(independent.refinement().max(), 2048);
        assert!(
            !Rc::ptr_eq(&retained, &independent),
            "an independent compilation does not share a retained one's frames"
        );
    }

    /// The extent ends where the work ends.
    #[test]
    fn policy_answers_again_once_no_compilation_is_running() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(700);
        {
            let _running = Running::under(Rc::new(ArmedLimits::from_policy()));
            NESTING.set(1000);
            assert_eq!(NestingBudget::current().levels(), 700);
        }
        assert_eq!(NestingBudget::current().levels(), 1000);
    }

    /// Extents may overlap without nesting, so an ending one retires its own
    /// limits and not whichever was pushed last.
    #[test]
    fn an_ending_extent_retires_its_own_limits() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(700);
        let first = Running::under(Rc::new(ArmedLimits::from_policy()));
        NESTING.set(1000);
        let second = Running::under(Rc::new(ArmedLimits::from_policy()));

        drop(first);
        assert_eq!(
            NestingBudget::current().levels(),
            1000,
            "the one still open still answers"
        );
        drop(second);
        assert_eq!(NestingBudget::current().levels(), 1000);
    }
}
