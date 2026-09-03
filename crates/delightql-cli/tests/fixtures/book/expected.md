---
title: Emitter Invariants
---

# Root {.dqlh}

Depth 0: the root atom's headings do not shift.

## Child {#anchor .dqlh}

Depth 1: marked headings shift by one, and a merged attribute block
(id + marker) is preserved intact.

## Unmarked Section

The heading above carries no marker: it passes through verbatim.

  #### Spaced {.dqlh}

The heading above is indented 1-3 spaces: still a heading, and the
added hashes go after the indentation.

    # indented code, marked but literal {.dqlh}

The line above is indented 4+ spaces: literal code, never shifted.

### Grandchild {.dqlh}

Depth 2: shifts compound across nesting levels.

````markdown
```
# quoted heading inside a nested fence {.dqlh}
```
````

The quoted heading above sits inside a ```` block: a ``` inside it
must not close the fence, and nothing inside shifts.

#### Tail {.dqlh}

## Plain {.dqlh}

Depth 1 again, AFTER the deeper subtree: preorder emission returns to
the shallower level correctly.

