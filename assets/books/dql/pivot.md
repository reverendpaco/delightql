# Pivot and Melt {.dqlh}

Melting and pivoting are inverse transformations between two table shapes
containing the same data. The "long skinny" table stores attributes as data --
normalized, often resembling key-value pairs. The "short wide" table lifts
attributes to metadata -- they become column names. [[ ![pivot and
melt](images/melt-pivot.svg){.thumbnail} ]{.sidenote}]{.sidenote-number}

Both transformations are possible in pure SQL given:

1. Support for compound data (JSON objects, arrays)
2. Ability to join against compound data (`unnest`, `json_each`)
3. For pivoting: attribute values must be known at query-write time to become column names

## Melt {.dqlh}

Melting normalizes denormalized data. A common case: data transfers between
organizations where a single row contains multiple relations.

Consider `customer` with three contact columns that should be normalized into separate rows:
```{.delightql .numberLines}
customer(*), _( Contact, Channel, channel_number
                -------------------------------
                phone, "phone", 1;
                fax,   "fax",   2;
                email, "email", 3 )
  |> (customer_id, channel_number, Channel, Contact)
```

The anonymous table (lines 1–5) maps each source column set to a row. Joining
it to `customer` with no condition produces one output row per contact channel
per customer -- three times the original cardinality. The projection (line 6)
retains only the normalized columns.

The transpiled SQL:
```{.sql .numberLines}
SELECT
  customer_id,
  CASE arm.i WHEN 0 THEN 1       WHEN 1 THEN 2     WHEN 2 THEN 3       END AS channel_number,
  CASE arm.i WHEN 0 THEN 'phone' WHEN 1 THEN 'fax' WHEN 2 THEN 'email' END AS Channel,
  CASE arm.i WHEN 0 THEN phone   WHEN 1 THEN fax   WHEN 2 THEN email   END AS Contact
FROM customer
CROSS JOIN (
  SELECT 0 AS i
  UNION ALL SELECT 1
  UNION ALL SELECT 2
) AS arm;
```

## Pivot {.dqlh}

Pivoting is a `GROUP BY` that rotates row-oriented data into columns. The group
key defines the entity; an attribute column becomes column names; a value
column fills them.

Given `invoice` totals summed per year and billing country (lines 3–4 of
the query below):

| year | billing_country | sales  |
|------|-----------------|--------|
| 2021 | Brazil          | 37.62  |
| 2021 | Canada          | 57.42  |
| 2021 | France          | 35.64  |
| 2021 | Germany         | 53.46  |
| 2021 | USA             | 103.95 |
| 2022 | Brazil          | 41.6   |
| 2022 | Canada          | 76.26  |
| 2022 | France          | 39.6   |
| 2022 | Germany         | 25.74  |
| 2022 | USA             | 102.98 |

: Sales per year and billing country (first two years)

A pivot on `billing_country` produces:

| year | USA    | Canada | France | Brazil | Germany |
|------|--------|--------|--------|--------|---------|
| 2021 | 103.95 | 57.42  | 35.64  | 37.62  | 53.46   |
| 2022 | 102.98 | 76.26  | 39.6   | 41.6   | 25.74   |
| 2023 | 103.01 | 55.44  | 42.61  | 19.8   | 48.57   |
| 2024 | 127.98 | 42.57  | 36.66  | 53.46  | 18.81   |
| 2025 | 85.14  | 72.27  | 40.59  | 37.62  | 9.9     |

: Pivoted result -- countries become columns

`tpt:#numbering_on()`

:::::{.widen}
```{.delightql .numberLines}
invoice(*),
  billing_country in ("USA"; "Canada"; "France"; "Brazil"; "Germany")
  |> %(strftime:("%Y", invoice_date) as year, billing_country
         ~> round:(sum:(total), 2) as sales)
  |> %( year
          ~>
        sales of billing_country )
```
:::::::

`tpt:#numbering_off()`

- Line 5: `year` defines the entity (group key), determining output cardinality
- Line 2: the `in` clause constrains which attribute values become columns
- Line 7: `sales of billing_country` rotates values into attribute-named columns

**The `in` clause is required.** Pivoting has compile-time semantics -- the
output schema is determined by the query, not the data. Without a fixed set of
attribute values, the column names would be unknowable.

The transpiled SQL:

:::::{.widen}
```sql
WITH _preagg_invoice AS (
  SELECT
    strftime('%Y', invoice_date) AS year,
    billing_country,
    round(sum(total), 2) AS sales
  FROM invoice
  WHERE billing_country IN ('USA', 'Canada', 'France', 'Brazil', 'Germany')
  GROUP BY strftime('%Y', invoice_date), billing_country
)
SELECT
  year,
  max(CASE WHEN billing_country = 'USA' THEN sales END) AS USA,
  max(CASE WHEN billing_country = 'Canada' THEN sales END) AS Canada,
  max(CASE WHEN billing_country = 'France' THEN sales END) AS France,
  max(CASE WHEN billing_country = 'Brazil' THEN sales END) AS Brazil,
  max(CASE WHEN billing_country = 'Germany' THEN sales END) AS Germany
FROM _preagg_invoice
GROUP BY year;
```
:::::::

### Multiple Value Columns {.dqlh}

A second aggregate, `count:(*) as invoices`, counts each cell's invoices. Multiple `of` clauses pivot additional columns:

| year | USA    | Canada | ... | USA_invoices | Canada_invoices | ... |
|------|--------|--------|-----|--------------|-----------------|-----|
| 2021 | 103.95 | 57.42  |     | 17           | 10              |     |
| 2022 | 102.98 | 76.26  |     | 18           | 12              |     |
| 2023 | 103.01 | 55.44  |     | 19           | 11              |     |
| 2024 | 127.98 | 42.57  |     | 21           | 9               |     |
| 2025 | 85.14  | 72.27  |     | 16           | 14              |     |

: Pivot with multiple value columns

:::::{.widen}
`tpt:#numbering_on()`
```{.delightql .numberLines}
invoice(*),
  billing_country in ("USA"; "Canada"; "France"; "Brazil"; "Germany")
  |> %(strftime:("%Y", invoice_date) as year, billing_country
         ~> round:(sum:(total), 2) as sales, count:(*) as invoices)
  |> %( year
          ~>
        sales of billing_country,
        invoices of :"{billing_country}_invoices" )
```
`tpt:#numbering_off()`
:::::::

Lines 7–8 introduce two pivot column sets. The second uses a format function to
distinguish column names (`USA_invoices`, `Canada_invoices`, etc.). When pivoting
multiple value columns, the attribute expression after `of` must differ --
here, `billing_country` versus `:"{billing_country}_invoices"`.


## Pivot Syntax {.dqlh}

The `of` keyword rotates values into attribute-named columns. The grammar:
```text
<value_column> of <attribute_column>
<value_column> of :<format_string>
```

The attribute column must be constrained by an `in` clause. When pivoting
multiple value columns, each `of` expression must produce distinct column names
-- hence the format string option.
