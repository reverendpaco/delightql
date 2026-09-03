
# ON vs WHERE Inference {.dqlh}

For inner joins, the placement of conditions in `ON` versus `WHERE` is
semantically equivalent. For outer joins, it differs: `ON` conditions govern
the match while preserving nulls; `WHERE` conditions filter the result and
eliminate nulls.

Delightql infers placement from column references:

- **Condition references multiple tables** → `ON`
- **Condition references one table** → `WHERE`
```delightql
employee(*), department?(*),
  employee.DepartmentId = department.DepartmentId,   -- two tables → ON
  department.Status = "active"                        -- one table → WHERE
```
```sql
SELECT * FROM employee
  LEFT OUTER JOIN department
    ON employee.DepartmentId = department.DepartmentId
WHERE department.Status = 'active';
```

The multi-table condition (`employee.DepartmentId = department.DepartmentId`) becomes the
join's `ON` clause. The single-table condition (`department.Status = 'active'`)
becomes a `WHERE` filter -- employees with null or inactive departments are
excluded.


