# Docs {.dqlh}

Definitions may carry structured documentation between the neck and the
body.  The docs block uses the annotation delimiters with the `docs`
identifier:

```text
  <HEAD>  <NECK>  (~~docs ... ~~)  <BODY>
```

## Syntax {.dqlh}

```{.delightql .am}
long_tracks(*) :-
  (~~docs
    Tracks longer than ten minutes.

    Returns:
      columns: inherited from track
      cardinality: variable
  ~~)
  track(*), milliseconds > 600000
```

The docs block is a `(~~docs ... ~~)` annotation.  The body is raw
text -- no DQL parsing is applied.  Line breaks, indentation, and blank
lines are preserved as written.

The block must appear immediately after the neck, before the first
expression of the body.  Only one docs block per definition is permitted.

## Applicability {.dqlh}

The docs block is valid on any rule-form definition:

**Views:**
```{.delightql .am}
usa_customers(*) :-
  (~~docs
    Customers whose country is USA.
  ~~)
  customer(*), country = "USA"
```

**Functions:**
```{.delightql .am}
tax_amount:(price, rate) :-
  (~~docs
    Computes tax as price times rate, rounded to two decimal places.

    Returns:
      type: numeric
  ~~)
  round:(price * rate, 2)
```

**Higher-order rules:**
```{.delightql .am}
same_schema(T(*), V(*))(*) :-
  (~~docs
    Compares T's and V's schema for
    equality. Equality is reached if the
    column names match exactly and are
    in the same ordinal position.

    Returns:
      column: pass
      column type: boolean
      cardinality: 1
  ~~)
  first_md(*)  : T(^)
  second_md(*) : V(^)
  together(*)  :
    second_md(*),
      first_md(*.(column_name, ordinal))
  together(~> count:() as a),
    first_md(~> count:() as b),
    second_md(~> count:() as c)
      |> ( (a = b) and (b = c) as pass)
```

**Sigma predicates:**
```{.delightql .am}
is_recent(invoice_date, threshold) :-
  (~~docs
    Filters to rows where invoice_date is
    within threshold days of today.
  ~~)
  invoice_date > date:("now", ("-" ++ threshold) ++ " days")
```

The docs block is not valid on facts (which have no neck) or on
shadow-neck definitions (which are query-scoped and ephemeral).

## Storage {.dqlh}

When a definition is loaded via `consult!`, the docs text is extracted
at parse time and stored alongside the entity in the system catalog.

The docs are queryable through the system metadata:

```delightql
sys::entities.entity(*)
  |> ( name, doc )
```
