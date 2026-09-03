
# Metadata-Oriented Tree Grouping {.dqlh}

Metadata-oriented tree grouping elevates data values to JSON keys. A column's
distinct values become the keys of a single object rather than elements of an
array.

The syntax uses `:~>` after a bare identifier:
```delightql
employee(*)
  ~> Title: ~> {FirstName, LastName} as people_by_title
```

The result is an interior record (one object), not an interior table (array of
objects):
```json
{
  "General Manager": [
    { "FirstName": "Andrew", "LastName": "Adams" }
  ],
  "IT Manager": [
    { "FirstName": "Michael", "LastName": "Mitchell" }
  ],
  "Sales Manager": [
    { "FirstName": "Nancy", "LastName": "Edwards" }
  ]
}
```

**Distinguishing syntax:**

- Normal keys are quoted strings: `"people":`
- Metadata keys are bare identifiers followed by `:~>`{.delightql}: `Title: ~>`{.delightql}


**Restriction:** Only one column can serve as a metadata key per level -- the
object can have only one set of keys. This constraint reflects JSON's
structure: two metadata-keyed objects with the same key type would create
ambiguous destructuring. Metadata-oriented trees satisfy TNF-M. (See Appendix
A.)


**Within a regular group by:**
```delightql
employee(*)
  |> %( State
          ~>
        Title: ~> {FirstName, LastName} as people_by_title )
```

Returns one row per state, each containing an object keyed by title.
