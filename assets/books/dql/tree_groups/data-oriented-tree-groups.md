
# Data-Oriented Tree Grouping {.dqlh}

Data-oriented tree grouping uses `~>`{.delightql} followed by a compound
constructor. Within each stated group it collects objects or tuples from the
input rows. Nested boundaries state the grouping hierarchy; a flat collector
does not deduplicate its members merely because their values agree.

**Simple example:**
```delightql
employee(*)
  |> %( city ~> { title, state } as title_and_state)
```

The tree grouping variables above are `city` alone. Within each of these
groups `{title, state}` do not require distinctness.

**Simple example:**
```delightql
employee(*)
  ~> { title, state } as title_and_state
```

Returns one row containing an array of all `{title, state}`
combinations.  No grouping variables mean this is a whole-group
tree group and the number of rows in the starting table will
equal the number of rows in the array.

**Nested example:**

```delightql
employee(*)
  ~> { title,
       "people": ~> {first_name, last_name},
       city } as people_by_title_and_city
```

Returns a single-row, single-column table:


+----------------------------------------------------------------+
| people_by_title_and_city                                       |
+================================================================+
|  ```json                                                       |
|     [                                                          |
|      { "title": "General Manager",                             |
|        "city": "Edmonton",                                     |
|        "people": [                                             |
|          { "first_name": "Andrew", "last_name": "Adams" }      |
|        ]                                                       |
|      },                                                        |
|      { "title": "IT Manager",                                  |
|        "city": "Calgary",                                      |
|        "people": [                                             |
|          { "first_name": "Michael", "last_name": "Mitchell" }  |
|        ]                                                       |
|      },                                                        |
|      { "title": "IT Staff",                                    |
|        "city": "Lethbridge",                                   |
|        "people": [                                             |
|          { "first_name": "Robert", "last_name": "King" },      |
|          { "first_name": "Laura", "last_name": "Callahan" }    |
|        ]                                                       |
|      },                                                        |
|      { "title": "Sales Manager",                               |
|        "city": "Calgary",                                      |
|        "people": [                                             |
|          { "first_name": "Nancy", "last_name": "Edwards" }     |
|        ]                                                       |
|      },                                                        |
|      { "title": "Sales Support Agent",                         |
|        "city": "Calgary",                                      |
|        "people": [                                             |
|          { "first_name": "Jane", "last_name": "Peacock" },     |
|          { "first_name": "Margaret", "last_name": "Park" },    |
|          { "first_name": "Steve", "last_name": "Johnson" }     |
|        ]                                                       |
|      }                                                         |
|    ]                                                           |
|   ```                                                          |
+----------------------------------------------------------------+
: {#tbl:array-tree-group}

**Transpilation.** Tree grouping uses JSON aggregation functions as
intermediates:
```sql
SELECT
  json_group_array(
    json_object(
      'title', title,
      'city', city,
      'people', people
    )
  ) AS people_by_title_and_city
FROM (
  SELECT
    title,
    city,
    json_group_array(
      json_object('first_name', first_name, 'last_name', last_name)
    ) AS people
  FROM employee
  GROUP BY title, city
);
```

The nested `GROUP BY` mirrors the nested `~>`. Each tree group level becomes a
subquery with its own grouping and JSON aggregation. The JSON functions are
implementation details -- the result is a standard column containing structured
data.

**Three-level example:**
```delightql
customer(*)
  ~> { country,
       "customers_by_city":
         ~> { city,
              "customers": ~> {first_name, last_name} } }
    as customers_by_city_within_country
```

Groups first by `country`, then within each country by `city`, then collects
customers within each city.

**Sibling tree groups:**


Multiple nested groups at the same level share their parent's context but are
otherwise independent:
```delightql
customer(*)
  ~> { country,
       "customers_by_city": ~> { city, "customers": ~> {first_name, last_name} },
       "reps": ~> [support_rep_id] }
    as nested_with_siblings
```

The `customers_by_city` and `reps` tree groups are siblings -- both nested
within `country`, neither containing the other.

Sibling tree groups share their parent's context but aggregate independently.
The relationship between siblings---which customer had which support rep -- is not
preserved. This is inherent to the structure: siblings represent independent
projections of the grouped data. [Trees with siblings satisfy TNF-G but not
TNF-R; they cannot round-trip losslessly. (See Appendix A.)]{.sidenote}

