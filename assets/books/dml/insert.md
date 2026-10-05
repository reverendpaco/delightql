
# Insert {.dqlh}

Use the `insert!` pseudo-predicate to insert rows.
The relation entering the `insert!` pipe must contain
a subset of the schema of the mutation target.  Any extra or erroneously
named columns are an error.

```delightql
_(last_name, first_name, title @ "Eklund", "Daniel", "IT Staff")
  |> insert!(employee(*))(*)
```

```sql
INSERT INTO employee (last_name, first_name, title)
VALUES ('Eklund', 'Daniel', 'IT Staff');
```

You may union tables and prediacte their tuples to provide input tuples:

```delightql
mount!("etl.sqlite", "etl")(*)
// the target: an empty table shaped like the loads
etl.partner_sale_2025_06_30(*), #<0 |> table!(etl.partner_sale_line(*))(*)

etl.partner_sale_2025_06_30(*)
  |;| etl.partner_sale_2025_07_31(, received_at >= "2025-07-01")
  |> insert!(etl.partner_sale_line(*))(*)
```


```delightql
mount!("etl.sqlite", "etl")(*)
// the target: an empty table shaped like the loads
etl.partner_sale_2025_06_30(*), #<0 |> table!(etl.partner_sale_line(*))(*)

etl.partner_sale_2025_07_31(*),
  quantity > 0 |> (sale_id, line_no, track_id, unit_price, quantity)
  |> insert!(etl.partner_sale_line(*))(*)
```

```sql
INSERT INTO etl.partner_sale_line (sale_id, line_no, track_id, unit_price, quantity)
SELECT sale_id, line_no, track_id, unit_price, quantity
FROM etl.partner_sale_2025_07_31
WHERE quantity > 0;
```
