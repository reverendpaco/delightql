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

Consider `claim_header` with four diagnosis columns that should be normalized into separate rows:
```{.delightql .numberLines}
claim_header(*), _( Diagnosis, Description, DiagnosisNumber
                    -------------------------------------
                    diag_1, diag_description1, 1;
                    diag_2, diag_description2, 2;
                    diag_3, diag_description3, 3;
                    diag_4, diag_description4, 4 )
  |> (claim_id, DiagnosisNumber, Diagnosis, Description)
```

The anonymous table (lines 1–5) maps each source column set to a row. Joining
it to `claim_header` with no condition produces one output row per diagnosis
per claim -- four times the original cardinality. The projection (line 6)
retains only the normalized columns.

The transpiled SQL uses JSON as an intermediate representation: [[The use of
JSON functions is an implementation detail -- arrays with `unnest` would work
equally. The result contains no JSON; it is a normal
table.]{.sidenote}]{.sidenote-number}
```{.sql .numberLines}
WITH
  _premelt_claim_header AS (
    SELECT
      claim_id,
      json_array(
        json_array(
          diagnosis_1,
          diagnosis_1_description,
          1
        ),
        json_array(
          diagnosis_2,
          diagnosis_2_description,
          2
        ),
        json_array(
          diagnosis_3,
          diagnosis_3_description,
          3
        ),
        json_array(
          diagnosis_4,
          diagnosis_4_description,
          4
        )
      ) AS _melt_packet
    FROM claim_header
  )
SELECT
  claim_id,
  json_extract(j.value, "$[2]") AS DiagnosisNumber,
  json_extract(j.value, "$[0]") AS Diagnosis,
  json_extract(j.value, "$[1]") AS Description
FROM _premelt_claim_header
JOIN json_each(_melt_packet) AS j;
```

## Pivot {.dqlh}

Pivoting is a `GROUP BY` that rotates row-oriented data into columns. The group
key defines the entity; an attribute column becomes column names; a value
column fills them.

Given `student_scores`:

| lastname | firstname | subject   | evaluation_result | evaluation_day |
|----------|-----------|-----------|-------------------|----------------|
| Smith    | John      | Music     | 7.0               | 2016-03-01     |
| Smith    | John      | Maths     | 4.0               | 2016-03-01     |
| Smith    | John      | History   | 9.0               | 2016-03-22     |
| Smith    | John      | Language  | 7.0               | 2016-03-15     |
| Smith    | John      | Geography | 9.0               | 2016-03-04     |
| Gabriel  | Peter     | Music     | 2.0               | 2016-03-01     |
| Gabriel  | Peter     | Maths     | 10.0              | 2016-03-01     |
| Gabriel  | Peter     | History   | 7.0               | 2016-03-22     |
| Gabriel  | Peter     | Language  | 4.0               | 2016-03-15     |
| Gabriel  | Peter     | Geography | 10.0              | 2016-03-04     |

: Sample student_scores data

A pivot on `subject` produces:

| lastname | firstname | geography | history | maths | music | language |
|----------|-----------|-----------|---------|-------|-------|----------|
| Gabriel  | Peter     | 10.0      | 7.0     | 10.0  | 2.0   | 4.0      |
| Smith    | John      | 9.0       | 9.0     | 4.0   | 7.0   | 7.0      |

: Pivoted result -- subjects become columns

`tpt:#numbering_on()`

:::::{.widen}
```{.delightql .numberLines}
student_scores(*),
  subject in ("Music"; "Maths"; "History"; "Language"; "Geography")
  |> %( firstname, lastname
          ~>
        evaluation_result of subject )
```
:::::::

`tpt:#numbering_off()`

- Line 3: `firstname, lastname` defines the entity (group key), determining output cardinality
- Line 2: the `in` clause constrains which attribute values become columns
- Line 5: `evaluation_result of subject` rotates values into attribute-named columns

**The `in` clause is required.** Pivoting has compile-time semantics -- the
output schema is determined by the query, not the data. Without a fixed set of
attribute values, the column names would be unknowable.

The transpiled SQL:

:::::{.widen}
```sql
WITH _prepivot_student_scores AS (
  SELECT
    lastname,
    firstname,
    json_group_object(
      subject,
      json_object('evaluation_result', evaluation_result)
    ) AS _pivot_packet
  FROM student_scores
  GROUP BY lastname, firstname
)
SELECT
  lastname,
  firstname,
  json_extract(_pivot_packet, '$.Geography.evaluation_result') AS geography,
  json_extract(_pivot_packet, '$.History.evaluation_result') AS history,
  json_extract(_pivot_packet, '$.Maths.evaluation_result') AS maths,
  json_extract(_pivot_packet, '$.Music.evaluation_result') AS music,
  json_extract(_pivot_packet, '$.Language.evaluation_result') AS language
FROM _prepivot_student_scores;
```
:::::::

### Multiple Value Columns {.dqlh}

The source table included `evaluation_day`, unused above. Multiple `of` clauses pivot additional columns:

| lastname | firstname | geography | geography_day | history | history_day | ... |
|----------|-----------|-----------|---------------|---------|-------------|-----|
| Gabriel  | Peter     | 10.0      | 2016-03-04    | 7.0     | 2016-03-22  |     |
| Smith    | John      | 9.0       | 2016-03-04    | 9.0     | 2016-03-22  |     |

: Pivot with multiple value columns

:::::{.widen}
`tpt:#numbering_on()`
```{.delightql .numberLines}
student_scores(*),
  subject in ("Music"; "Maths"; "History"; "Language"; "Geography")
  |> %( firstname, lastname
          ~>
        evaluation_result of subject,
        evaluation_day of :"{subject}_day" )
```
`tpt:#numbering_off()`
:::::::

Lines 5–6 introduce two pivot column sets. The second uses a format function to
distinguish column names (`Music_day`, `Maths_day`, etc.). When pivoting
multiple value columns, the attribute expression after `of` must differ --
here, `subject` versus `:"{subject}_day"`.


## Pivot Syntax {.dqlh}

The `of` keyword rotates values into attribute-named columns. The grammar:
```
<value_column> of <attribute_column>
<value_column> of :<format_string>
```

The attribute column must be constrained by an `in` clause. When pivoting
multiple value columns, each `of` expression must produce distinct column names
-- hence the format string option.
