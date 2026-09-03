# Predicate vs Relation vs Table {.dqlh}

The terms predicate, relation, and table lack any
universal authority, yet each carries emphasis depending on whom you're talking
to. This section exists not for philosophy but because Prolog, SQL, and
therefore delightql all take different positions at different times on whether
these are separate concepts, and how strongly to emphasize the chosen mental
model.

A predicate/relation/table can be viewed as:

 - An arbitrary subset of a Cartesian product (the mathematical view)
 - A named storage entity for data queries (the database view)
 - A named evaluator of truth in logical statements (the logic view)
 - A generalization of functions (the relational view delightql emphasizes)

SQL treats *relation* and *table* as interchangeable, embracing the first two
definitions. It reserves *predicate* for built-in truth evaluators over
traditional domains like numbers and strings -- as in `where age>20 and last_name like
"%son"`, where two predicates compose into one.

Prolog favors *predicate*, with occasional references to *relation* and rare
references to *table*. But it maps its term to all three definitions above. A
Prolog predicate can be stored data, a logical assertion, or a computable
relationship.

Delightql uses these terms interchangeably, with some deliberate emphasis. It
borrows from Prolog in favoring *predicate* for tables (extensional data), views
(intensional data derived from extensional data), and built-in domain
predicates like `=`, `<`, `>`, and `like`.


Like Prolog, delightql recognizes an operational difference between predicates
resolvable from ground truths (finitary -- backed by actual stored tuples) and
those expressing relations over infinities. To wit, there is no table enumerating all
pairs where one number is less than another. Prolog restricts such non-Herbrand
expressions via moding. Delightql similarly won't let you write the equivalent
of `select left_hand from ">" where right_hand=3`.

There is one more definition worth surfacing:

- A predicate is a function of truth -- given a tuple, it returns membership (true/false)

This is just a restatement of "named evaluator of truth," but the framing
emphasizes the boolean nature of predicates. In such situations, we are asking not what data the predicate
contains, but whether a given tuple belongs. Delightql utilizes this framing
for what SQL calls semi-joins (`EXISTS` queries) and what Prolog expresses
through provability contexts like negation-as-failure (`\+`). In these cases, we
don't care what the predicate returns  -- only whether it succeeds or not.



