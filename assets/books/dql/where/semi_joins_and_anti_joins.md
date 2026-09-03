
# Semi-Joins and Anti-Joins {.dqlh}

Semi-joins (∃ or ⋉) and anti-joins (∄ or ▷) test for existence without contributing
columns. They ask "can you prove this?" rather than "give me this data."

The **PROVE** sigil `+`{.delightql .sigil} prefixes a semi-join:

```{.delightql .numberLines}
employee(*) as e, +fired_employees(, e.EmployeeId=id)
```


```sql
SELECT *
FROM employee AS e
WHERE
  EXISTS (
    SELECT 1
    FROM fired_employees
    WHERE
      id IS NOT DISTINCT FROM e.EmployeeId
  );
```


The DISPROVE sigil `\+`{.delightql .sigil} prefixes an anti-join: [This syntax comes directly from
Prolog's negation-as-failure.]{.sidenote}

```delightql
employee(*) as e, \+ fired_employees(, e.EmployeeId=f.id)
```


```sql
select
  *
from employee e
  where not exists (select 1 from fired_employees
                      where id IS NOT DISTINCT FROM e.EmployeeId);
```



The join condition(s) appears *inside* the parentheses -- this is called *interior notation*.
The relation is tested for provability, not joined for data.

