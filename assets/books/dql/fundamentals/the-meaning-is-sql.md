
# Operational semantics of Query Expressions{.dqlh}

Delightql operates by transforming valid delightql queries into
valid SQL queries and submitting it to the relevant SQL engine.

![Inward Operations Only](images/delightql-sola-sql.svg)

Delightql makes a non-modification guarantee to the results that are returned
-- in other words, it passes back the raw bytes produced by the target engine.

By abstaining from any transformation to the data returned to the user,
delightql ties the meaning of its code (query expressions ) to the meaning of
the SQL it produces -- that is to say, meaning (i.e. semantics) is transitively
mapped from a delightql string to a SQL string, and then to a resulting table.

Any questions about delightql's semantics are ultimately answered by the
combination of its transpilation choices and what the targeted SQL engine
returns.

Delightql may choose to arrange SQL in any of various forms if it can guarantee
that the target SQL engine will return the exact same table.  This choice
affords flexibility in what SQL to produce that is orthogonal to performance or
formatting.
