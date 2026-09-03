# Data Definition Language (DDL) {.dqlh}


The definition of SQL DDL is the set of features that create, modify, or
delete schema entities.

Delightql's DDL encompasses SQL's DDL and extra features with no
direct SQL mapping (higher-order predicates, ER-context rules). The thematic
concern is **reusability**: definitions, tables, and data that can be authored,
loaded, and referenced.

This section covers:

  - **Relational rules**. Views, higher-order views, ER-context rules, and sigma predicates.
  - **Function rules**. Reusable domain functions.
  - **Facts**. Axiomatic ground data.
  - **Namespaces**. Organization and visibility of all the above.


## Basics {.dqlh}

> **Assertion mode vs query mode**. Delightql, like Prolog, distinguishes two programming modes:
>
> - **Query mode**. Expressions entered into a REPL that execute immediately.
>   This was the subject of the first book in this reference.
> - **Assertion mode**. Files that contain definitions for later use.
>
> The features in this DDL section are assertion-mode constructs.

Assertion mode has two general syntactic forms: *rules* and *facts*.

### Rules {.dqlh}

The general form of a rule is

```
  <HEAD>  <NECK>  <BODY>
```


```dql
  young_users(*)
    :- adults(*), age < 20
```

In the above example:

  - `young_users(*)`{.delightql} is the `HEAD`
  - `:-`{.delightql} is the `NECK`
  - `adults(*), age <20`{.delightql} is the `BODY`. The body may use any DQL feature--the entire previous book applies here.

### Facts {.dqlh}

Facts are functor forms with grounded data.

```delightql
parent("Abraham", "Isaac")
```

This *looks* like a query.  The syntax
is context dependent.  In assertion mode, i.e. in
a file with rules and definitions, this syntax
defines extensional data -- axiomatic truths with no derivation.
