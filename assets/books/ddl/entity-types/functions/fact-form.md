


# Fact Form {.dqlh}

Individual facts define point mappings:
```delightql
title_kind:("IT Manager" -> "tech")
title_kind:("IT Staff" -> "tech")
title_kind:(_ -> "other")
```

This is equivalent to the stacked form but spread across statements. Use it when mappings are added incrementally or loaded from external sources.

## Restrictions {.dqlh}

- Stacked notation and rule form cannot be mixed for the same function
- Stacked notation and individual facts cannot be mixed for the same function
- Disjunctive clauses (multiple rules with the same head) must be co-located in the source
- Textual order determines evaluation order for disjunctive clauses
