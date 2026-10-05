
# Metadata-Oriented Tree Grouping {.dqlh}

Metadata-oriented tree grouping elevates data values to JSON keys. A column's
distinct values become the keys of a single object rather than elements of an
array.

The syntax uses `:~>` after a bare identifier:
```delightql
employee(*)
  ~> title: ~> {first_name, last_name} as people_by_title
```

The result is an interior record (one object), not an interior table (array of
objects):
```json
{
  "General Manager": [
    { "first_name": "Andrew", "last_name": "Adams" }
  ],
  "IT Manager": [
    { "first_name": "Michael", "last_name": "Mitchell" }
  ],
  "IT Staff": [
    { "first_name": "Robert", "last_name": "King" },
    { "first_name": "Laura", "last_name": "Callahan" }
  ],
  "Sales Manager": [
    { "first_name": "Nancy", "last_name": "Edwards" }
  ],
  "Sales Support Agent": [
    { "first_name": "Jane", "last_name": "Peacock" },
    { "first_name": "Margaret", "last_name": "Park" },
    { "first_name": "Steve", "last_name": "Johnson" }
  ]
}
```

**Distinguishing syntax:**

- Normal keys are quoted strings: `"people":`
- Metadata keys are bare identifiers followed by `:~>`{.delightql}: `title: ~>`{.delightql}


**Restriction:** Only one column can serve as a metadata key per level -- the
object can have only one set of keys. This constraint reflects JSON's
structure: two metadata-keyed objects with the same key type would create
ambiguous destructuring. Metadata-oriented trees satisfy TNF-M. (See Appendix
A.)


**Within a regular group by:**
```delightql
employee(*)
  |> %( city
          ~>
        title: ~> {first_name, last_name} as people_by_title )
```

Returns one row per city, each containing an object keyed by title.
