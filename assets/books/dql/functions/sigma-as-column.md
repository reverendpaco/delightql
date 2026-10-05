# Boolean Expressions as Columns {.dqlh}


Predicates -- what delightql calls *sigma clauses* -- can appear in column position,
returning boolean values. [Some SQL dialects lack a boolean type; these
transpile to 1 and 0.]{.sidenote}


```delightql
invoice(*)
    |> +( billing_country="USA"
            and billing_state!="CA"
                AS usa_outside_ca,
          billing_country="USA"
                AS usa,
          total > 15
            or invoice_date >= "2025-01-01"
                AS big_or_recent,
          billing_state!="CA"
                AS outside_ca)
```


:::::{.widen}
```sql
  select
    *,
    billing_country is not distinct from 'USA'
      and billing_state is distinct from 'CA' as usa_outside_ca,
    billing_country is not distinct from 'USA' as usa,
    total > 15 or invoice_date >= '2025-01-01' as big_or_recent,
    billing_state is distinct from 'CA' as outside_ca
  from invoice;
```
::::::


Compound predicates must use keywords (`and`, `or`) rather than sigils (
`,`{.delightql .sigil} , `;`{.delightql .sigil}) when appearing as column
expressions.


**Existence tests**. Semi-joins and anti-joins also return booleans when used in column position:

```delightql
employee(*)
 |> +( +customer(,
         customer.support_rep_id
          =employee.employee_id),
      \+ customer(,
         customer.support_rep_id
          =employee.employee_id),
      +between(hire_date,"2002-01-01","2002-12-31"))
```


:::::{.widen}
```sql
  select
    *,
    --
    exists (select 1 from customer
      where customer.support_rep_id=employee.employee_id),
    --
    not exists (select 1 from customer
      where customer.support_rep_id=employee.employee_id),
    --
    hire_date between '2002-01-01' and '2002-12-31'
  from employee;
```
:::::::
