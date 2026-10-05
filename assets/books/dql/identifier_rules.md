# Case insensitivity {.dqlh}

Delightql is case-insensitive. [In contrast to Prolog, where capitalization
distinguishes variables from atoms.]{.sidenote} The following all refer to the
same identifier:

 - `employee_id`
 - `Employee_Id`
 - `EMPLOYEE_ID`


## Stropping {.dqlh}

Names containing characters outside the bare identifier shape (spaces, for
instance) are delimited with backticks: `` `employee Id` ``. Keyword spellings
do not require stropping in identifier positions: `from(*)` may name a table,
and `as:(x)` may name a function. Keywords still perform their grammatical
roles where an operator or literal is expected.

The keyword-name ban has been ruled out; implementation of its removal is
still owed. Until that cut lands, some bare keyword names still refuse.
