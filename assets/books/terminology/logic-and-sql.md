# Logic and Sql Terminology {.dqlh}

Delightql takes inspiration from the logic programming world as well as from
SQL, so there will be many places in this reference where terminology from
both may be used.

As a mini-tutorial for those unfamiliar,
consider the following table of terminology equivalences:

SQL                     | logic programming
--                      | ----
database                | database
query                   | query
query                   | goal
query execution         | unification and resolution
table                   | relation
table                   | predicate
table                   | relation/predicate of only ground terms (extensional)
view                    | predicate rule (intensional)
view with recursive CTE | predicate rule with multiple clauses and recursion
column                  | dimension
column                  | logic variable (LVar)
row                     | fact
tuple                   | fact
value                   | ground term
table definition        | rule
view definition         | rule
join                    | logical and
join                    | conjunction
exists (semijoin)       | provable
not exists (antijoin)   | not provable
union                   | logical or
union                   | disjunction
union                   | predicate rule with multiple clauses
insert row              | assert a fact
delete row              | retract a fact
update row              | retract and assert fact
number of columns       | predicate arity
number of columns       | predicate dimensionality
table-valued function   | higher order predicate
: SQL and logic programming terminology equivalences


This table is neither complete nor free of caveats.

The historic justification for this semi-equivalence is
unassailable: Codd and Kowalski showed how relational algebra query languages and
Prolog respectively could be centered on a *predicate calculus* -- the first-order
language of logic.

