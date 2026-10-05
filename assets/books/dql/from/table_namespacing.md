# Table Namespacing {.dqlh}

```delightql
mount!("etl.sqlite", "etl")(*)
etl.partner_sale_2025_07_31(*) as s
```

A dot-prefixed identifier namespaces the table. Here, `partner_sale_2025_07_31` lives within the
namespace `etl`. What this namespace represents -- schema in some databases, database
in others -- is implementation-dependent.

```sql
select * from etl.partner_sale_2025_07_31 as s;
```

The namespace is the entire syntax _before_ the dot and may include nesting using `::`.  Namespaces are nested
like file-system folders.

```delightql
mount!("etl.sqlite", "client1::production::etl")(*)
client1::production::etl.partner_sale_2025_07_31(*) as s
```

In the above example, `client1::production::etl` is the namespace where `client1` contains `production` which contains `etl`.

Namespaces are elements of the delightql runtime. The delightql programmer chooses the hierarchy and maps these to
source structures. For more information, see the namespacing section of DDL.

