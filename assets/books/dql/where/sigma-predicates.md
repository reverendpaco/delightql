
# Sigma Predicates {.dqlh}

Predicates can be defined and reused. A *sigma predicate* is a rule that expands
into selection criteria: [Defining sigma predicates is covered in
DDL.]{.sidenote}

```{.delightql .numberLines .am}
 no_data("NA";"N/A";"UNKNOWN")

 empty(column) :- null=column
 empty(column) :- trim:(column)=""
 empty(column) :- +no_data(upper:(column))
```

Use with semi-join or anti-join syntax:[Delightql applies De Morgan's laws,
distributing negation across disjunctive clauses.]{.sidenote}

```delightql
customer(*),
  \+empty(company),
  \+empty(state)
```


```sql
WITH no_data(value) AS (VALUES ('NA'), ('N/A'), ('UNKNOWN'))
SELECT *
FROM customer
WHERE
  (company IS NULL
    OR trim(company) IS NOT DISTINCT FROM ''
    OR EXISTS (SELECT 1 FROM no_data WHERE upper(company) = no_data.value)
  ) IS NOT TRUE
  AND (state IS NULL
    OR trim(state) IS NOT DISTINCT FROM ''
    OR EXISTS (SELECT 1 FROM no_data WHERE upper(state) = no_data.value)
  ) IS NOT TRUE;
```


## *`Like`* and `Between` {.dqlh}


SQL's `LIKE` and `BETWEEN` have special syntax. Delightql maps functor notation to these constructs:

```delightql
track(*), +like(name,"%Love%"), \+between(milliseconds,120000,600000)
```

The above delightql transpiles to the following Sql.

```sql
select
  *
from track
  where
    (name like '%Love%') is true and
    (milliseconds between 120000 and 600000) is not true;
```

To match a value against several patterns at once, `like_any` takes the value,
then `&`, then the patterns as rows. Under `+` it keeps the rows whose value
matches at least one pattern; under `\+`, the rows whose value matches none:

```delightql
customer(*), \+like_any(email & "%.com"; "%.org")(*)
```

It is equivalent to:

```sql
select *
from customer
where not exists (
  select 1
  from (values ('%.com'), ('%.org')) as p(pattern)
  where email like p.pattern
);
```

The patterns can be any one-column relation — `+like_any(email, domains(*))(*)`.
Without the `+` it joins instead of filtering: each row gains the `pattern` it
matched, once per match. A NULL value matches no pattern.


## Disjunction {.dqlh}

Two syntaxes express `OR`:

**Keyword form (recommended)**. The `or` keyword binds predicates within a sigma clause:

```delightql
invoice(*)
  , trim:(lower:(billing_country)) = "usa"
        or total > 10
  , billing_state != "CA"
  |> %( billing_country
          ~>
        count:(*) as invoice_count,
        avg:(total) )
```


**Sigil form**. The **SEMI-OR** sigil `;`{.delightql .sigil} requires parentheses to capture scope:


```delightql
invoice(*)
  , (trim:(lower:(billing_country)) = "usa"
        ; total > 10 )
  , billing_state != "CA"
  |> %( billing_country
          ~>
        count:(*) as invoice_count,
        avg:(total) )
```

Prefer the keyword form -- it reads more clearly and avoids parenthesis errors.
See "Precedence and Scoping" for details.
