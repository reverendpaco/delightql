
# `USING` Shorthand {.dqlh}

The `.(cols)` operator specifies USING columns:
```delightql
employee(*), department(*.(DepartmentId))
```
```sql
SELECT * FROM employee JOIN department USING (DepartmentId);
```

Multiple columns are comma-separated:
```delightql
employee(*), department(*.(DepartmentId, LocationId))
```
```sql
SELECT * FROM employee JOIN department USING (DepartmentId, LocationId);
```

`USING` and explicit `ON` differ operationally: `USING` retains one copy of the matched column; `ON` retains both.

`USING` may combine with explicit conditions:
```delightql
employee(*), department(*.(DepartmentId)), employee.StartDate > department.Founded
```


