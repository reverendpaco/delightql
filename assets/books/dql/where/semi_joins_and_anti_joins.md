
# Semi-Joins and Anti-Joins {.dqlh}

Semi-joins (∃ or ⋉) and anti-joins (∄ or ▷) test for existence without contributing
columns. They ask "can you prove this?" rather than "give me this data."

The **PROVE** sigil `+`{.delightql .sigil} prefixes a semi-join:

```{.delightql .numberLines}
employee(*) as e, +customer(, e.employee_id=support_rep_id)
```


```sql
SELECT *
FROM employee AS e
WHERE
  EXISTS (
    SELECT 1
    FROM customer
    WHERE
      support_rep_id = e.employee_id
  );
```


The DISPROVE sigil `\+`{.delightql .sigil} prefixes an anti-join: [This syntax comes directly from
Prolog's negation-as-failure.]{.sidenote}

```delightql
employee(*) as e, \+ customer(, e.employee_id=customer.support_rep_id)
```


```sql
select
  *
from employee e
  where not exists (select 1 from customer
                      where support_rep_id = e.employee_id);
```



The join condition(s) appears *inside* the parentheses -- this is called *interior notation*.
The relation is tested for provability, not joined for data.
The correlated row match still has join equality: two NULL keys do not
correspond. `+` and `\+` change how matching rows are observed, not how
they match. A comparison confined to one interior row, such as
`customer.support_rep_id = null`, remains a local null-safe test.
