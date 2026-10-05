# Self-Join {.dqlh}

Aliases distinguish multiple references to the same table:
```delightql
employee(*) as e, employee(*) as mgr, e.reports_to = mgr.employee_id
  |> (e.last_name as employee, mgr.last_name as Manager)
```
```sql
SELECT e.last_name AS employee, mgr.last_name AS Manager
FROM employee e
  JOIN employee mgr ON e.reports_to = mgr.employee_id;
```
