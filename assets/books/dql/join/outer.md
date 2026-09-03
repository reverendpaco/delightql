
# Outer Joins {.dqlh}

The OUTER-IND sigil `?`{.delightql .sigil} marks a relation as optional -- it
may contribute nulls when no match exists.

**Left outer** (right table optional):
```delightql
employee(*), department?(*.(DepartmentId))
```
```sql
SELECT * FROM employee LEFT OUTER JOIN department USING (DepartmentId);
```

**Right outer** (left table optional):
```delightql
employee?(*), department(*.(DepartmentId))
```

**Full outer** (either optional):
```delightql
employee?(*), department?(*.(DepartmentId))
```

Outer joins work with explicit conditions:
```delightql
employee(*), department?(*), employee.DepartmentId = department.DepartmentId
```
```sql
SELECT * FROM employee
  LEFT OUTER JOIN department ON employee.DepartmentId = department.DepartmentId;
```

