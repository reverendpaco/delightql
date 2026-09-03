# Table Namespacing {.dqlh}

```delightql
hr.employee(*) as e
```

A dot-prefixed identifier namespaces the table. Here, `employee` lives within the
namespace `hr`. What this namespace represents -- schema in some databases, database
in others -- is implementation-dependent.

```sql
select * from hr.employee as e;
```

The namespace is the entire syntax _before_ the dot and may include nesting using `::`.  Namespaces are nested
like file-system folders.

```delightql
client1::production::hr.employee(*) as e
```

In the above example, `client1::production::hr` is the namespace where `client1` contains `production` which contains `hr`.

Namespaces are elements of the delightql runtime. The delightql programmer chooses the hierarchy and maps these to
source structures. For more information, see the namespacing section of DDL.

