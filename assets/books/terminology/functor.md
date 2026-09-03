# Functor Form, or just Functor {.dqlh}

A functor form is an identifier followed by a pair of parentheses and the
arguments contained within. This definition is syntactic only, and has no
assigned semantics until we establish a context. With
apologies to Prolog, we shorten the term **functor form**
to just **functor**.

Within the context of delightql a functor is *usually* understood to be a **table**
and **always** understood to be a *predicate* (as predicates generalize tables).

```delightql
// Access the table foo and its three columns
// which we call a,b, and c
foo(a,b,c)
```

```delightql
// Access the table bar and all its columns
bar(*)
```

In the context of delightql and most logic languages, the functor notation is self-denoting.
This means several things, but practically we can say the following:

  - With functor notation, traditional notions of inputs and outputs are blurred, and often meaningless (each argument may be either input or output).
  - A functor cannot be assigned to a (scalar) variable because it denotes itself **as a relation**, and not as a scalar (domain value).


In contrast, most other programming languages use functor syntax
to denote a *function* and/or (perhaps) a *subroutine*.

```
    foo(a,b,c)
```

implies

  - if a subroutine: an execution/call only
  - if a function: an execution/call and substitution by the domain value that it produces.

Under these languages, arguments are passed _into_ a function (`a`, `b`, and
`c`) and are consumed. Here, the function syntax
is operationally equivalent to the value it returns, and thus can be assigned
to domain variables.

That delightql, like Prolog, assigns the concept of a
**predicate**/**relation** to the functor does not mean the absence of
functions.

In Prolog, functions re-use the predicate functor syntax via the designation of
a known argument (called a mode) to be the output. Prolog has additional
nuances about functions which we will sidestep.

Delightql provides a special syntax that remains reminiscent of a
relation: a functor with a colon `:` between the identifier and the pair of
parentheses, which we will call a **function functor**.

```delightql
    count:(*)
    length:(last_name)
    foo:(x,y,z)
```

  The colon asks us to read `foo of x
and y and z` or `length of last_name`.

