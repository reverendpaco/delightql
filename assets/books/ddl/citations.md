# Citation Rules {.dqlh}

A **citation** is the shorthand invocation of a zero-argument value function:
`:greeting` reads the function `greeting`, whether it was defined as
`greeting:() :- "hello"` or as the paren-less `greeting :- "hello"`. It is the
same application as `greeting:()`; nothing distinguishes them after parsing.

```delightql
esc :- char:(27)
hue :- 31
red:(s) :- :"{:esc}[{:hue}m{s}{:esc}[0m"
```

A citation composes wherever a value does — in arithmetic, in a template
hole, as an argument. Qualified, the mark belongs to the leaf, after the
namespace and its dot: `ansi.:esc`, the same function as `ansi.esc:()`. The
mark never precedes the path: `:ansi.esc` is not a spelling.

A citation is a reference to a defined function and can alias another
citation's value; contrast the symbol `::name`, which is a self-valued
constant needing no definition.
