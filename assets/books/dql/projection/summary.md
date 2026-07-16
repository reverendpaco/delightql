# Projection Summary {.dqlh}

The operators introduced so far are all **unary** -- they transform a single relation. The following qualities characterize their behavior:

- **Column Cardinality Preserved**: The number of columns remains unchanged.
- **Column Ordinality Preserved**: Columns retain their relative order, with no intercalation of new columns among existing ones.
- **Column Names Preserved**: Existing columns keep their names (new columns do not affect this).
- **Table Cardinality Preserved**: The number of rows remains unchanged.
- **Table Ordinality Preserved**: Rows retain their order.

A quality is marked **Y** only if it holds under all circumstances; **N** if any case violates it.




:::::{.widen}
|                              | `( )`           | `-( )`          | `$( )( )`       | `$$( )`         |
| ---------------------------  | -----------     | ----------      | -----------     | -----------     |
|                              | Project         | Project Out     | Map Cover       | Basic Map Cover |
| Column Ordinality Preserved  |   N             |    Y            |    Y            |    Y            |
| Column Cardinality Preserved |   N             |    N            |    Y            |    Y            |
| Column Names Preserved       |   N             |    Y            |    Y            |    Y            |
| Table Cardinality Preserved  |   Y             |    Y            |    Y            |    Y            |
| Table Ordinality Preserved   |   Y             |    Y            |    Y            |    Y            |
: Preservation properties of Project, ProjectOut, MapCover, and BasicMapCover
::::::::

:::::{.widen}
|                              | `*( )`          | `#( )`          | `%( )`          |
| ---------------------------  | -----------     | -----------     | -----------     |
|                              | Rename          | Order By        | Group Modulo    |
| Column Ordinality Preserved  |  Y              |    Y            |   N             |
| Column Cardinality Preserved |  Y              |    Y            |   N             |
| Column Names Preserved       |  N              |    Y            |   N             |
| Table Cardinality Preserved  |  Y              |    Y            |   N             |
| Table Ordinality Preserved   |  Y              |    N            |   N*            |
: Preservation properties of Rename, OrderBy, and GroupModulo
::::::

|                              | `+( )`          | `+$( )`         |
| ---------------------------  | -----------     | -----------     |
|                              | Embed           | Map Embed       |
| Column Ordinality Preserved  |   Y             |    Y            |
| Column Cardinality Preserved |   N             |    N            |
| Column Names Preserved       |   Y             |    Y            |
| Table Cardinality Preserved  |   Y             |    Y            |
| Table Ordinality Preserved   |   Y             |    Y            |
: Preservation properties of Embed and MapEmbed


One operator stands apart: GROUP-MODULO `%(  )`{.delightql .sigil} preserves none of these qualities
unconditionally. It captures both `DISTINCT` and `GROUP BY`, and is the subject of
the next chapter.
