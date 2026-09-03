# Introduction {.dqlh}

![](images/logo-text-fancy.svg)

Delightql is a logic-inspired query language that transpiles to SQL.
This document provides a reference to the language.

The breakdown for reference should seem familiar to anyone who knows SQL:

  - **Data Querying Language (DQL)**:  Any feature that is used to return a
    result via querying. All examples here may be directly run in the delightql
    read-eval-print loop (REPL).
  - **Data Definition Language (DDL)**: Any feature that is used to create or
    drop views or functions.  These correspond to SQL DDL statements
    and to Prolog assertion-mode rules.
  - **Data Manipulation Language (DML)**:  Any feature that is used to modify
    an existing set of data, i.e. *`update`*, *`delete`*, or *`insert`*.
  - **Fundamentals**: How the precedence rules for parsing operators and
    expressions, both at the level of *table expressions* and of *domain
    expressions*, affect semantics.  How LVars create unification.
  - **Namespace and Directives**: The runtime environment for loading and scoping data and DDL.
  - **Scripting**:
  - **System**: A listing of system tables available to introspect code,
    execution, and the state of the databases.


Throughout this reference, examples of SQL illustrate how a delightql
expression *could* be transpiled.  Such SQL examples should be viewed as both
descriptive of the translation semantics and accurate as to the final results.



