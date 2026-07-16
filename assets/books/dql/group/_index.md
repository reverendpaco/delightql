# Distinct and Group By {.dqlh}


Distinct and group by are congruent operations. Their similarity is often a source of confusion:

```sql
select
  Department
from employee
  group by Department;

-- Produces the same result as:

select
  distinct Department
from employee;
```

Delightql reflects this congruence with unified syntax.
