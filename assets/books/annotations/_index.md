# Annotations {.dqlh}


Annotations appear at continuation points in a query pipeline, and
permit language directives to operate as inline syntactic
elements.

## The Annotation Framework {.dqlh}

Annotations are distinguished by matching **PARENOTATES** `(~~ ~~)`{.delightql .sigil}
followed immediately (no space) by an `<identifier>`.

```delightql
(~~<identifier> body ~~)             -- annotation with body
(~~<identifier>:instance body ~~)    -- annotation with instance name
```

The identifier after the `(~~` is the **ANNOTATION-TYPE**.

How the body is parsed within an annotation is a function of the specific
**ANNOTATION-TYPE**.  Each recognized type has its own grammar rule:

  - `(~~error`: no body -- just an optional URI which matches with well-known errors
  - `(~~danger`: a URI naming the acknowledged danger

`(~~docs` shares the delimiters but is not an annotation. It is the dedicated
definition-document form, and its body is raw text.

A colon form `identifier:instance` names a specific annotation instance
and is also a function of the particular annotation type in question.

```delightql
users(*), age > 0
  (~~danger://semantics/example ~~)
```

Only the annotation types listed above are recognized by the grammar.
Unknown annotation names produce a parse error.

## Placement {.dqlh}

Annotations may appear at any continuation point -- before a pipe, before
a comma, or at the end of an expression:

```delightql
users(*), age > 30
  (~~danger://foo/bar ~~)
  |> (first_name, email)
```

Annotations are usually transparent to SQL generation and so the pipeline above
produces identical SQL to:

```delightql
users(*), age > 30 |> (first_name, email)
```

Multiple annotations at the same point are permitted. They appear as siblings
in the CST and are processed in order.

## Annotation Types {.dqlh}

- `error` -- expects compilation to fail, matches the error URI
  (see **Error Assertions**)
- `danger` -- acknowledges a named safety gate for the current
  query (see **Danger Gates**)
  (see **Options**)

The dedicated `docs` form attaches documentation to a definition; see
**Docs**.

