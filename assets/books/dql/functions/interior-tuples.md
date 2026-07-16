# Compound Data Constructors {.dqlh}

The enclyphs `{ }`{.delightql .sigil} and `[ ]`{.delightql .sigil} construct compound data--records and tuples. They are
functions, though they look like syntax. Their behavior depends on context:

| Position | `{ }` | `[ ]` |
|----------|-------|-------|
| Scalar (non-reduction) | interior record | interior tuple |
| Aggregate (after `~>`) | table of records | table of tuples |
: Compound data constructor behavior by position

```delightql
-- scalar constructors
employee(*) |>
  ( [LastName,FirstName] as interior_tuple )
employee(*) |>
  ( {LastName,FirstName} as interior_record )

-- aggregate constructors
employee(*)
  |> ( Department
        ~>
      [LastName,FirstName] as table_of_tuples )
employee(*)
  |> ( Department
        ~>
      { LastName,FirstName } as table_of_records )
```

These constructors transpile to JSON in most SQL dialects. [SQLite and Postgres
both provide JSON as a data type with supporting functions. See SQLite
JSON1.]{.sidenote} But the concept is not about JSON per se but about nested
structure.

> The compound data types introduced here provide groundwork for pivots, melts,
> and tree-grouping (covered later). Programmers needing arbitrary JSON
> manipulation can call SQL's JSON functions directly:
>
>    `json_array:(last_name, first_name)`.

## Scalar Interior Record {.dqlh}

The INTERIOR-RECORD enclyph `{ }`{.delightql .sigil} creates a nested row addressable by name:

```delightql
employee(*)
  |> (Department , { LastName,FirstName } as name  )
```

+--------------------------+-------------------------------------------------+
| Department               | name                                            |
+--------------------------+-------------------------------------------------+
| Accounting               | `{"FirstName":"Erhard","LastName":"Moorrud"}`   |
+--------------------------+-------------------------------------------------+
| Product Management       | `{"FirstName":"Anson","LastName":"Woodall"}`    |
+--------------------------+-------------------------------------------------+

: Scalar interior record result

Column names become keys. To specify different keys:

```delightql
employee(*)
  |> (Department ,
      { "FirstName": FirstName ,
        "LastName" : LastName} as name  )
```

Access nested fields with JSON-access notation (see next section).

```delightql
employee(*)
  |> (Department ,
      { "FirstName": FirstName ,
        "LastName" : LastName} as name  )
  |> ( Department, name:{.FirstName})
```

```sql
with
    _cpr0 as (
        select
          Department,
          json_object('FirstName',FirstName,
                      'LastName' ,LastName) as name
        from employee)
    select
        Department,
        name ->> "$.FirstName" as FirstName
    from _cpr0;
```

## Aggregate Interior Record {.dqlh}

In a reduction position, `{ }`{.delightql .sigil} collects multiple records into a table:

```delightql
employee(*)
  |> %(Department ~> { LastName,FirstName } as name )
```

+---------------+-----------------------------------------------------+
|  Department   | name                                                |
+===============+=====================================================+
| ```           | ```                                                 |
|   Accounting  |   [                                                 |
| ```           |     {"LastName":"Moorrud","FirstName":"Erhard"},    |
|               |     {"LastName":"Cowwell","FirstName":"Orlando"},   |
|               | {"LastName":"Tuley","FirstName":"Hanan"},           |
|               | {"LastName":"Unstead","FirstName":"Gretchen"}       |
|               |   ]                                                 |
|               | ```                                                 |
+---------------+-----------------------------------------------------+
| ```           | ```                                                 |
|   Business    |   [                                                 |
|   Development |     {"LastName":"Marcone","FirstName":"Dinnie"},    |
| ```           |     {"LastName":"Tuffell","FirstName":"Mathias"},   |
|               |     {"LastName":"Harbord","FirstName":"Venita"},    |
|               |     {"LastName":"Hinstock","FirstName":"Ashli"}     |
|               |   ]                                                 |
|               | ```                                                 |
+---------------+-----------------------------------------------------+
: Aggregate interior record result -- grouped by Department

The outer `[ ]`{.delightql .sigil} in the JSON represents multiplicity -- a
list of rows. The interior table has a uniform schema of named columns
represented as objects.

## Scalar Interior Tuple {.dqlh}

The **INTERIOR-TUPLE** enclyph `[ ]`{.delightql .sigil} creates a nested row addressable by position:

```delightql
employee(*)
  |> (Department , [LastName,FirstName] as name )
```

+--------------------------+----------------------------+
| Department               | name                       |
+--------------------------+----------------------------+
| Accounting               |  `["Erhard","Moorrud"]`    |
+--------------------------+----------------------------+
| Product Management       |  `["Anson","Woodall"]`     |
+--------------------------+----------------------------+

: Scalar interior tuple result

Access elements by index:

```delightql
employee(*)
  |> (Department , [LastName,FirstName] as name )
  |> ( Department, name:[1] as first_name)
```


## Aggregate Interior Tuple {.dqlh}

In a reduction position, `[ ]`{.delightql .sigil} collects multiple tuples into a table:

```delightql
employee(*)
  |> %(Department ~> [ LastName,FirstName ] as name )
```

+---------------+----------------------------------------------+
| Department    | name                                         |
+===============+==============================================+
|               | ```                                          |
|   Accounting  |    [                                         |
|               |     ["Erhard","Moorrud"],                    |
|               |     ["Orlando","Cowwell"],                   |
|               |     ["Hanan","Tuley"],                       |
|               |     ["Gretchen","Unstead"]                   |
|               |    ]                                         |
|               | ```                                          |
+---------------+----------------------------------------------+
|               | ```                                          |
|   Business    |   [                                          |
|   Development |     ["Dinnie","Marcone"],                    |
|               |     ["Mathias","Tuffell"],                   |
|               |     ["Venita","Harbord"],                    |
|               |     ["Ashli","Hinstock"]                     |
|               |   ]                                          |
|               | ```                                          |
+---------------+----------------------------------------------+

: Aggregate interior tuple result -- grouped by Department

JSON's `[ ]` does double duty here: the outer brackets indicate multiple rows;
the inner brackets indicate tuples. This is a syntactic limitation of JSON, not
a semantic ambiguity. [If JSON had a distinct tuple syntax -- perhaps
parentheses -- this overloading would not exist.]{.sidenote}

## Nesting {.dqlh}

The power of these constructors emerges when nested:

```delightql
employee(*)
  ~>  {  Title ,
           "people_by_state":
             ~>{ State ,
                "people" : ~>{FirstName, LastName} } }
                  as people_by_state_within_title
```

This tree-structured output is covered in detail in the **Tree Groups** section.
