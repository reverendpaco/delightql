# Self-Join {.dqlh}

Aliases distinguish multiple references to the same table:
```delightql
employee(*) as e, employee(*) as mgr, e.ManagerId = mgr.Id
  |> (e.Name as Employee, mgr.Name as Manager)
```
```sql
SELECT e.Name AS Employee, mgr.Name AS Manager
FROM employee e
  JOIN employee mgr ON e.ManagerId = mgr.Id;
```
