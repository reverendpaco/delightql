// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The core's identities. Each is an index into one arena of the graph; the
//! constructors are visible only inside `core`, where the arena push and the
//! binder mint are the only callers.

use std::fmt;

macro_rules! identity {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub(crate) struct $name(u32);

        impl $name {
            pub(super) fn at(index: usize) -> Self {
                $name(u32::try_from(index).expect("an arena holds fewer than 2^32 nodes"))
            }

            pub(crate) fn index(self) -> usize {
                self.0 as usize
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}{}", $prefix, self.0)
            }
        }
    };
}

identity!(BinderId, "b");
identity!(RelId, "r");
identity!(ExprId, "e");
identity!(TruthId, "t");
identity!(MergeId, "m");
identity!(InstanceId, "i");
identity!(PassengerId, "p");
