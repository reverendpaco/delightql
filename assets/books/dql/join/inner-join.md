
# Inner Join {.dqlh}

```delightql
employee(*), department(*), employee.DepartmentId = department.DepartmentId
```

```sql
SELECT * FROM employee
  JOIN department ON employee.DepartmentId = department.DepartmentId;
```

The join condition follows the tables it correlates. Multiple conditions conjoin naturally:

```delightql
employee(*), department(*),
  employee.DepartmentId = department.DepartmentId,
  employee.Location = department.Location
```

**Scope is left to right.** This is an error:
```{.delightql .bad}
// INVALID: department not yet in scope
employee(*), department.DepartmentId = employee.DepartmentId, department(*)
```

