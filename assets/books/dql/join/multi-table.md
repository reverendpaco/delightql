
# Multi-Table Joins {.dqlh}

Joins chain left to right. Each table enters scope upon appearance:
```delightql
employee(*),
  department(*.(DepartmentId)),
  location(*.(LocationId)),
  employee.StartDate > "2020-01-01"
```
```sql
SELECT * FROM employee
  JOIN department USING (DepartmentId)
  JOIN location USING (LocationId)
WHERE employee.StartDate > '2020-01-01';
```

After the third table, columns from all three are in scope.

