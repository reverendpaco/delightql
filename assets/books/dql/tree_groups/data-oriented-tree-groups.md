
# Data-Oriented Tree Grouping {.dqlh}

Data-oriented tree grouping uses `~>`{.delightql} followed by a compound constructor. The
result is an array of objects (or tuples), one per distinct combination of tree
grouping variables.

**Simple example:**
```delightql
employee(*)
  %( Gender ,"people": ~> { Title, State } as title_and_state)
```

The tree grouping variables above are `Gender` alone. Within each of these
groups `{Title, State}` do not require distinctness.

**Simple example:**
```delightql
employee(*)
  ~> { Title, State } as title_and_state
```

Returns one row containing an array of all `{Title, State}`
combinations.  No grouping variables mean this is a whole-group
tree group and the number of rows in the starting table will
equal the number of rows in the array.

**Nested example:**

```delightql
employee(*)
  ~> { Title,
       "people": ~> {FirstName, LastName},
       State } as people_by_title_and_state
```

Returns a single-row, single-column table:


+----------------------------------------------------------------+
| people_by_title_and_state                                      |
+================================================================+
|  ```                                                           |
|     [                                                          |
|      { "Title": "Account Representative",                      |
|        "State": "PA",                                          |
|        "people": [                                             |
|          { "FirstName": "Stafani", "LastName": "Hurton" },     |
|          { "FirstName": "Jenda", "LastName": "Bownd" }         |
|        ]                                                       |
|      },                                                        |
|      { "Title": "Programmer",                                  |
|        "State": "PA",                                          |
|        "people": [                                             |
|          { "FirstName": "Clareta", "LastName": "Cuss" }        |
|        ]                                                       |
|      },                                                        |
|      { "Title": "Programmer",                                  |
|        "State": "GA",                                          |
|        "people": [                                             |
|          { "FirstName": "Anita", "LastName": "Aburrow" }       |
|        ]                                                       |
|      },                                                        |
|      { "Title": "VP",                                          |
|        "State": "OH",                                          |
|        "people": [                                             |
|          { "FirstName": "Drusi", "LastName": "Sachno" }        |
|        ]                                                       |
|      },                                                        |
|      { "Title": "VP",                                          |
|        "State": "PA",                                          |
|        "people": [                                             |
|          { "FirstName": "Frazer", "LastName": "Vido" },        |
|      { "FirstName": "Corney", "LastName": "Treherne" }         |
|                               ]                                |
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
      'Title', Title,
      'State', State,
      'people', people
    )
  ) AS people_by_title_and_state
FROM (
  SELECT
    Title,
    State,
    json_group_array(
      json_object('FirstName', FirstName, 'LastName', LastName)
    ) AS people
  FROM employee
  GROUP BY Title, State
);
```

The nested `GROUP BY` mirrors the nested `~>`. Each tree group level becomes a
subquery with its own grouping and JSON aggregation. The JSON functions are
implementation details -- the result is a standard column containing structured
data.

**Three-level example:**
```delightql
employee(*)
  ~> { Title,
       "people_by_state":
         ~> { State,
              "people": ~> {FirstName, LastName} } }
    as people_by_state_within_title
```

Groups first by `Title`, then within each title by `State`, then collects
people within each state.

**Sibling tree groups:**


Multiple nested groups at the same level share their parent's context but are
otherwise independent:
```delightql
employee(*)
  ~> { Title,
       "people_by_state": ~> { State, "people": ~> {FirstName, LastName} },
       "cities": ~> [City] }
    as nested_with_siblings
```

The `people_by_state` and `cities` tree groups are siblings -- both nested
within `Title`, neither containing the other.

Sibling tree groups share their parent's context but aggregate independently.
The relationship between siblings---which person was in which city -- is not
preserved. This is inherent to the structure: siblings represent independent
projections of the grouped data. [Trees with siblings satisfy TNF-G but not
TNF-R; they cannot round-trip losslessly. (See Appendix A.)]{.sidenote}


