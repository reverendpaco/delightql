# Conditional Covering (If-Only) {.dqlh}

The IF-ONLY sigil `|`{.delightql .sigil} constrains which rows a cover
applies to. Rows not matching the predicate pass through unchanged.

**Map-cover with if-only:**

```delightql
employee(*)
  |> $(upper:())(last_name, first_name | title = "IT Staff")
```

```sql
SELECT
  employee_id,
  CASE
    WHEN title = 'IT Staff' THEN upper(
      last_name
    )
    ELSE last_name
  END AS last_name,
  CASE
    WHEN title = 'IT Staff' THEN upper(
      first_name
    )
    ELSE first_name
  END AS first_name,
  title,
  reports_to,
  birth_date,
  hire_date,
  address,
  city,
  state,
  country,
  postal_code,
  phone,
  fax,
  email
FROM employee;
```

The predicate follows the column list, separated by `|`. This mirrors
the aggregate if-only `count:(col | pred)` -- the `|` always sits between
the operands and the condition.

Without if-only, the function applies to all rows. With if-only, the
function applies only to matching rows; non-matching rows retain their
original values.

**Basic-cover with if-only:**

```delightql
employee(*)
  |> $$("REDACTED" as phone, "---" as fax | title = "IT Staff")
```

```sql
select
    employee_id,
    last_name,
    first_name,
    title,
    reports_to,
    birth_date,
    hire_date,
    address,
    city,
    state,
    country,
    postal_code,
    case when title = 'IT Staff'
         then 'REDACTED' else phone end as phone,
    case when title = 'IT Staff'
         then '---' else Fax end        as Fax,
    email
from employee;
```

The predicate goes at the end of the item list, after the last `as` target.

**Composability**. If-only composes with callable nesting and chaining:

```delightql
customer(*)
  |> $(trim:(upper:(@)))(first_name, last_name | country = "USA")
```

If-only is syntactic sugar over CASE expressions. The equivalent without
if-only:

```delightql
employee(*)
  |> $$( _:(title = "IT Staff" -> upper:(last_name);  _ -> last_name)  as last_name,
         _:(title = "IT Staff" -> upper:(first_name); _ -> first_name) as first_name)
```
