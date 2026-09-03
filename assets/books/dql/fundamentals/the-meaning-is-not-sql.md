# Operational semantics of Directives {.dqlh}

Directives (i.e. effects) are the impure entities within
the delightql language.  They **cannot** have a semantic
that is defined by a single query.  In fact, with the
lack of certain branching and looping primitives in
the SQL standard for DDL, some primitives cannot
have a semantic consisting of a sequence of
DDL and DML.


![Inward Operations Only](images/directive-semantics.svg)

Delightql therefore absolves  directives as a category from
the obligation to return back from the target whatever was
returned.  The delightql runtime must interpret
and act accordingly on different conditions.
