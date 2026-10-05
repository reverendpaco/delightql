
# Multi-Table Joins {.dqlh}

Joins chain left to right. Each table enters scope upon appearance:
```delightql
invoice(*),
  invoice_line(*.(invoice_id)),
  track(*.(track_id)),
  invoice.invoice_date > "2025-01-01"
```
```sql
SELECT * FROM invoice
  JOIN invoice_line USING (invoice_id)
  JOIN track USING (track_id)
WHERE invoice.invoice_date > '2025-01-01';
```

After the third table, columns from all three are in scope.

