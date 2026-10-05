# Language Limitations {.dqlh}

Delightql has limitations on its expressive power and its expressive guarantees.

The source of these limitations can be any of the following:

 - a missing feature in SQL itself
 - a feature that is inconsistently implemented across SQL targets
 - a feature from logic that has no expressive abstraction at all in SQL
 - a feature that is possible in SQL but requires an admixture of DDL and DML and querying
 - any feature that does not *yet* have a satisfactory semantics

The last point expresses the fact that delightql may choose to not support a
feature at the current time while leaving the door open for its support upon
future semantic clarity.  This is a statement that can apply to all SQL targets or
to selective SQL targets.

The current section aims to enumerate all known limitations.
