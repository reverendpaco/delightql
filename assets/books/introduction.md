# Introduction {.dqlh}

![](images/logo-text-fancy.svg)

Delightql is a logic-inspired query language that transpiles to SQL.
This document provides a reference to the language via an enumeration of all of its features.

Each feature comes with at least one example and a short description to
motivate usage.  While structured like an introduction by beginning with foundations
and growing from there, this reference is not a beginner's text and
presupposes a familiarity with SQL. A reader with such a background will find
features easier to locate and to understand.

The reference is organized into sections familiar to SQL users: DQL, DDL, and DML.

  - **Data Querying Language (DQL)**:  Any feature that is used to return a
    result via querying. All examples here may be directly run in the delightql
    read-eval-print loop (REPL).
  - **Data Definition Language (DDL)**: Any feature that is used to create or
    drop tables, views or functions.  These correspond to SQL DDL statements
    and to Prolog assertion-mode rules.
  - **Data Manipulation Language (DML)**:  Any feature that is used to modify
    an existing set of data, i.e. *`update`*, *`delete`*, or *`insert`*.
  - **Keywords, Identifiers, and Ground Terms**:  An index of lexicographical syntax
    needed to identify identifiers, constant (ground) values, and keywords.
  - **Precedence**:  The precedence rules for parsing operators and
    expressions, both at the level of *table expressions* and of *domain
    expressions*.
  - **Directives**: An index of the special pseudo-relations and operators used to
    configure the language, and attach databases.
  - **System Tables**: A listing of the system tables available to introspect code,
    execution, and the state of the databases.




Throughout this reference, examples of SQL illustrate how a
delightql expression *could* be transpiled.  Such SQL examples should be
read as both descriptive of translation semantics and rigorous to final
results.  Actual SQL from these delightql examples may differ --
for one,  different SQL targets [ The term *target*
indicates the dialect of SQL which will be the transpile target for the
delightql transpiler. ]{.sidenote} will likely have different implementations.


As an example of this point, consider the following SQL for an `OUTER JOIN`.

```SQL
  SELECT *
  FROM employee FULL OUTER JOIN department
    ON employee.DepartmentId = department.DepartmentId;
```

Some SQL dialects do not have this native syntax,  but
can achieve the same result, with a lot of
ceremony -- example copied from:`tpt:#fc(<enwiki:join>)`.

```SQL
SELECT
  employee.LastName,
  employee.DepartmentId,
  department.DepartmentName,
  department.DepartmentId
FROM employee
JOIN department
  ON employee.DepartmentId = department.DepartmentId
UNION ALL
SELECT
  employee.LastName,
  employee.DepartmentId,
  CAST(NULL AS varchar(20)),
  CAST(NULL AS integer)
FROM employee
WHERE
  NOT EXISTS (
    SELECT *
    FROM department
    WHERE
      employee.DepartmentId = department.DepartmentId
  )
UNION ALL
SELECT
  CAST(NULL AS varchar(20)),
  CAST(NULL AS integer),
  department.DepartmentName,
  department.DepartmentId
FROM department
WHERE
  NOT EXISTS (
    SELECT *
    FROM employee
    WHERE
      employee.DepartmentId = department.DepartmentId
  );
```

Both the above queries execute a *full outer join* with significantly different syntax
while capturing the same semantics[It is operationally easy to
define *semantic equality* in our discussions:  if two different
queries under **all** circumstances produce the exact same table (same schema,
same cardinality), then they are semantically equivalent]{.sidenote}. In such
circumstances, this reference will choose the simplest and clearest SQL.



## Terminology {.dqlh}

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


This table is neither complete nor free of caveats.  There is no novelty[in the
*nothing new under the sun* sense]{.sidenote} to this congruence as Codd and Kowalski
both showed how relational algebra query languages and Prolog respectively
could be built from *predicate calculus*. Many disciplines
starting in the 1970s continue to examine the ways in which these
technologies are two sides of the same coin, but may still yet inform the
other.[A notable example of this cross-pollination is how the academic
research from deductive databases and Datalog provided the semantics -- and
permission -- for SQL to adopt recursive common table expressions]{.sidenote}

Some features from one area have no
equivalence in the other.  For instance, the SQL *`NULL`* has
no logic programming analog, and features that derive from this distinction
(like outer joins) likewise find no purchase.

### Core Terminology {.dqlh}

Some terms in this reference are used often enough to warrant the
following few early paragraphs. These terms are

  * *domain*
  * *functor*
  * *predicate* and *relation* and their distinction
  * *sigilization*

#### Domain {.dqlh}

The term domain is used throughout this book much in the same way it is used throughout
mathematics, as the set of possible values for a given quantity.  In SQL parlance, this is not to be confused
with type, as the domain of heights should not be confused with the domain of Kelvin temperatures
even though both could be modeled as positive real numbers.

Highlighting the term domain is done to create an emphasis on non-domain things.  Delightql
makes this distinction by asserting that things are either domain values or predicate values,
either domain expressions or predicate expressions.

This is a somewhat artificial delineation as tables/predicates being cartesian products have
their own very real input space and domain.


To be concrete:

  - **Domain values** include `1`, `"roger"`, and `true`; domain
expressions include `1+2` and `upper("roger")`.
  - **Predicate values** include
`employee`; predicate expressions include `generate_series(1,10)` or `(select
i_am_a_subquery from inner) as newtable`.

#### Functor Form, or just Functor {.dqlh}

A functor form is an identifier followed by a pair of parentheses and the
arguments contained within. This definition is syntactic only, and has no
assigned semantics until we establish a context. With
apologies to Prolog, we shorten the term **functor form**
to just **functor**.


Within the context of delightql a functor is *usually* understood to be a **table**
and **always** understood to be a *predicate* (as predicates generalize tables).

```delightql
// Access the table foo and its three columns
// which we call a,b, and c
foo(a,b,c)
```

```delightql
// Access the table bar and all its columns
bar(*)
```

In the context of delightql and most logic languages, the functor notation is self-denoting.
This means several things, but practically we can say the following:

  - With functor notation, traditional notions of inputs and outputs are blurred, and often meaningless (each argument may be either input or output).
  - A functor cannot be assigned to a (scalar) variable because it denotes itself **as a relation**, and not as a scalar (domain value).


In contrast, most other programming languages use functor syntax
to denote a *function* and/or (perhaps) a *subroutine*.

```
    foo(a,b,c)
```

implies

  - if a subroutine: an execution/call only
  - if a function: an execution/call and substitution by the domain value that it produces.

Under these languages, arguments are passed _into_ a function (`a`, `b`, and
`c`) and are consumed. Here, the function syntax
is operationally equivalent to the value it returns, and thus can be assigned
to domain variables.

That delightql, like Prolog, assigns the concept of a
**predicate**/**relation** to the functor does not mean the absence of
functions.

In Prolog, functions re-use the predicate functor syntax via the designation of
a known argument (called a mode) to be the output. Prolog has additional
nuances about functions which we will sidestep.

Delightql provides a special syntax that remains reminiscent of a
relation: a functor with a colon `:` between the identifier and the pair of
parentheses, which we will call a **function functor**.

```delightql
    count:(*)
    length:(last_name)
    foo:(x,y,z)
```

  The colon asks us to read `foo of x
and y and z` or `length of last_name`.

#### Predicate vs Relation vs Table {.dqlh}

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
foregrounds the boolean nature of predicates. In such situations, we ask not what data they
contain, but whether a given tuple belongs. Delightql emphasizes this framing
for what SQL calls semi-joins (`EXISTS` queries) and what Prolog expresses
through provability contexts like negation-as-failure (`\+`). In these cases, we
don't care what the predicate returns  -- only whether it succeeds.



#### Sigilization {.dqlh}

Delightql has few keywords compared to SQL. Where SQL uses words, delightql uses symbols.

Throughout this reference, the term *sigilization* describes delightql's practice
of representing operators with non-alphanumeric symbols rather than keywords.

For example, delightql sigilizes the **DISTINCT** relational operator with a `%` symbol
followed by parentheses:

```delightql
users(*) |> %(last_name)
// SQL: SELECT DISTINCT last_name FROM users
```

To continue this example, delightql recognizes that `GROUP BY` is simply `DISTINCT`
extended with aggregation. The same `%` sigil handles both  -- separate grouping
columns from aggregate functions with `~>` to get the equivalent of SQL's `GROUP
BY`:

```delightql
users(*) |> %(last_name ~> count:(*), sum:(salary) as salary_by_last_name)
// SQL: SELECT count(*), sum(salary) as salary_by_last_name FROM users GROUP BY last_name
```



