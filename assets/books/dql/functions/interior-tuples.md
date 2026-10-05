# Compound Data Constructors {.dqlh}

The enclyphs `{ }`{.delightql .sigil} and `[ ]`{.delightql .sigil} construct compound data--records and tuples. They are
functions, though they look like syntax. Their behavior depends on context:

| Position | `{ }` | `[ ]` |
|----------|-------|-------|
| Scalar (non-reduction) | interior record | interior tuple |
| Aggregate (after `~>`) | table of records | table of tuples |
: Compound data constructor behavior by position

```delightql
// scalar constructors
employee(*) |>
  ( [last_name,first_name] as interior_tuple )
employee(*) |>
  ( {last_name,first_name} as interior_record )

// aggregate constructors
employee(*)
  |> %( title
        ~>
      [last_name,first_name] as table_of_tuples )
employee(*)
  |> %( title
        ~>
      { last_name,first_name } as table_of_records )
```

These constructors transpile to JSON in most SQL dialects. [SQLite and Postgres
both provide JSON as a data type with supporting functions. See SQLite
JSON1.]{.sidenote} But the concept is not about JSON per se but about nested
structure.

> The compound data types introduced here provide groundwork for pivots, melts,
> and tree grouping (covered later). Programmers needing arbitrary JSON
> manipulation can call SQL's JSON functions directly:
>
>    `json_array:(last_name, first_name)`.

## Scalar Interior Record {.dqlh}

The INTERIOR-RECORD enclyph `{ }`{.delightql .sigil} creates a nested row addressable by name:

```delightql
employee(*)
  |> (title , { last_name,first_name } as name  )
```

+--------------------------+-------------------------------------------------+
| title                    | name                                            |
+--------------------------+-------------------------------------------------+
| General Manager          | `{"last_name":"Adams","first_name":"Andrew"}`   |
+--------------------------+-------------------------------------------------+
| Sales Manager            | `{"last_name":"Edwards","first_name":"Nancy"}`  |
+--------------------------+-------------------------------------------------+

: Scalar interior record result

Column names become keys. To specify different keys:

```delightql
employee(*)
  |> (title ,
      { "first_name": first_name ,
        "last_name" : last_name} as name  )
```

Access nested fields with JSON-access notation (see next section).

```delightql
employee(*)
  |> (title ,
      { "first_name": first_name ,
        "last_name" : last_name} as name  )
  |> ( title, name:{.first_name})
```

```sql
with
    _cpr0 as (
        select
          title,
          json_object('first_name',first_name,
                      'last_name' ,last_name) as name
        from employee)
    select
        title,
        name ->> "$.first_name" as first_name
    from _cpr0;
```

## Aggregate Interior Record {.dqlh}

In a reduction position, `{ }`{.delightql .sigil} collects multiple records into a table:

```delightql
employee(*)
  |> %(title ~> { last_name,first_name } as name )
```

+---------------+-----------------------------------------------------+
|  title        | name                                                |
+===============+=====================================================+
| ```text       | ```json                                             |
|   IT Staff    |   [                                                 |
| ```           |     {"last_name":"King","first_name":"Robert"},     |
|               |     {"last_name":"Callahan","first_name":"Laura"}   |
|               |   ]                                                 |
|               | ```                                                 |
+---------------+-----------------------------------------------------+
| ```text       | ```json                                             |
|   Sales       |   [                                                 |
|   Support     |     {"last_name":"Peacock","first_name":"Jane"},    |
|   Agent       |     {"last_name":"Park","first_name":"Margaret"},   |
| ```           |     {"last_name":"Johnson","first_name":"Steve"}    |
|               |   ]                                                 |
|               | ```                                                 |
+---------------+-----------------------------------------------------+
: Aggregate interior record result -- grouped by `title`

The outer `[ ]`{.delightql .sigil} in the JSON represents multiplicity -- a
list of rows. The interior table has a uniform schema of named columns
represented as objects.

## Scalar Interior Tuple {.dqlh}

The **INTERIOR-TUPLE** enclyph `[ ]`{.delightql .sigil} creates a nested row addressable by position:

```delightql
employee(*)
  |> (title , [last_name,first_name] as name )
```

+--------------------------+----------------------------+
| title                    | name                       |
+--------------------------+----------------------------+
| General Manager          |  `["Adams","Andrew"]`      |
+--------------------------+----------------------------+
| Sales Manager            |  `["Edwards","Nancy"]`     |
+--------------------------+----------------------------+

: Scalar interior tuple result

Access elements by index:

```delightql
employee(*)
  |> (title , [last_name,first_name] as name )
  |> ( title, name:{.1} as first_name)
```


## Aggregate Interior Tuple {.dqlh}

In a reduction position, `[ ]`{.delightql .sigil} collects multiple tuples into a table:

```delightql
employee(*)
  |> %(title ~> [ last_name,first_name ] as name )
```

+---------------+----------------------------------------------+
| title         | name                                         |
+===============+==============================================+
|               | ```json                                      |
|   IT Staff    |    [                                         |
|               |     ["King","Robert"],                       |
|               |     ["Callahan","Laura"]                     |
|               |    ]                                         |
|               | ```                                          |
+---------------+----------------------------------------------+
|               | ```json                                      |
|   Sales       |   [                                          |
|   Support     |     ["Peacock","Jane"],                      |
|   Agent       |     ["Park","Margaret"],                     |
|               |     ["Johnson","Steve"]                      |
|               |   ]                                          |
|               | ```                                          |
+---------------+----------------------------------------------+

: Aggregate interior tuple result -- grouped by `title`

JSON's `[ ]` does double duty here: the outer brackets indicate multiple rows;
the inner brackets indicate tuples. This is a syntactic limitation of JSON, not
a semantic ambiguity. [If JSON had a distinct tuple syntax -- perhaps
parentheses -- this overloading would not exist.]{.sidenote}

## Nesting {.dqlh}

The power of these constructors emerges when nested:

```delightql
customer(*)
  ~>  {  country ,
           "people_by_state":
             ~>{ state ,
                "people" : ~>{first_name, last_name} } }
                  as people_by_state_within_country
```

This tree-structured output is covered in detail in the **Tree Groups** section.
