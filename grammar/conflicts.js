// Declared GLR forks. Each entry names a set of productions that share a
// prefix and are told apart by a LATER token — never by classifying content.
module.exports = $ => [
  // The subject now stands OUTSIDE the heading, so every named form shares the
  // bare `predicate_identifier` prefix and forks on the token after it.
  [$.fact_form, $.fact_function, $.fo_rule, $.ho_fact_form, $.ho_rule, $.relation_name, $.sigma_rule],
  // A query-scoped binding's head and a relation read share the bare name;
  // the group after it — heading, or parameter group — tells them apart.
  [$.relation_name, $.standard_cte, $.ho_cte],
  [$.declared_relation_param, $.open_relation_param, $.predicate_identifier],
  [$.rule_param, $.declared_relation_param, $.open_relation_param, $.predicate_identifier],
  [$.callee, $.relation_name],
  [$.mutation_source],
  [$.namespace],
  [$.namespace, $.qualifier_name],
  [$.effect_chain, $.relex],
  [$.let_free_relex],
  [$.let_block],
  // A definition's body may open with a let block, and a `(~~ddl …~~)` stands
  // in both the doc slot and the preamble; the token after it decides which.
  [$.definition_annotation, $.let_block],
  [$.doc_slot],
  [$.binary_connective, $.comma_continuation],
  [$.binary_connective, $.corresponding_union_continuation],
  [$.binary_connective, $.minus_continuation],
  [$.binary_connective, $.positional_union_continuation],
  [$.binary_connective, $.smart_union_continuation],
  [$.reference, $.slot],
  [$.constraint_term, $.domain_expression],
  // Two segments or three is decided by the token AFTER the second name, so
  // the refusal witness and the ordinary qualified reference fork here.
  [$.named_reference, $.qualifier],
  // `f(*)…` — a glob supplying a parameter and a glob HEAD share their whole
  // prefix; the token after the group tells them apart.
  [$.ho_argument, $.effect_glob_head],
  [$.ho_argument, $.spread],
  [$.ho_argument_reference, $.named_reference],
  // `f(_(x …` — a sparse fill opening a lifted row and an anonymous relation
  // opening an argument list share `_(` and the name after it; the token after
  // THAT one decides.
  [$.named_reference, $.sparse_fill],
  // `f(a.b)…` — an argument that names a column and a positional slot that
  // binds one share their whole prefix; the group AFTER them tells them apart.
  [$.ho_argument_reference, $.slot],
  [$.ho_argument_reference, $.reference],
  // `+f(|1|)` — an ordinal argument to a sigma application and to an
  // existence's ho_part share their whole prefix; the group after them tells
  // them apart.
  [$.ho_argument_reference, $.domain_expression],
  [$.ho_argument, $.non_infix_application],
  [$.residual_designator, $.argumentative_functor, $.interior_functor],
  [$.head_term, $.named_reference],
  // `f!(1 as x` — a CTE head naming its supplied constant and a slot the
  // ruled teaching refuses share the term and the `as`; the group's role is
  // settled by what closes it.
  [$.head_term, $.non_infix_application],
  [$.head_term, $.scalar_param, $.sigma_rule],
  // `p(x)…` in a let block: a heading term and a scalar formal share the
  // name; the group after the parens decides.
  [$.head_term, $.scalar_param],
  // `p(x)…`, `p(1)…`, `p(f(x))…` in a let block: the first group of a
  // query-scoped binding is either a heading (standard_cte) or a parameter
  // group (ho_cte), and its terms are heading terms, formals, or — read as a
  // relation's own argument row — a call's arguments; the group after the
  // parens decides.
  [$.head_term, $.ho_param],
  [$.head_term, $.scalar_param, $.named_reference],
  [$.ho_argument, $.head_term, $.ho_param, $.non_infix_application],
  [$.head_term, $.ho_argument_reference, $.named_reference],
  [$.head_term, $.ho_argument, $.non_infix_application],
  [$.fact_datum, $.head_term],
  [$.fact_datum, $.head_term, $.ho_param],
  [$.fact_datum, $.head_term, $.non_infix_application],
  [$.fact_datum, $.head_term, $.ho_param, $.non_infix_application],
  [$.fact_function, $.head_term, $.named_reference, $.scalar_param, $.sigma_rule],
  [$.fact_function, $.sigma_rule],
  [$.named_out_item],
  // `~> k:~> {…} as n` — the name belongs to what the reduction publishes,
  // not to the stage the chain is in. Both readings parse; the fork is here.
  [$.metadata_group],
  [$.domain_expression, $.operand],
  [$.function_application, $.operand],
  [$.infix_operator],
  [$.comparison, $.infix_operator],
  [$.conjunction_expression],
  [$.disjunction_expression],
  [$.conjunction_expression, $.disjunction_expression],
  [$.out_value, $.parenthesized_operand],
  [$.out_value, $.probe_row],
  // THE CROSSING'S TWO STRATA. After a complete non-infix truth (`+f(x)`)
  // the parser does not know whether it is the whole truth or the left
  // operand of a comparison; after `(x > 1)` it does not know whether the
  // parens group a truth or cross a value. The token after decides.
  [$.crossed_truth, $.truth_expression],
  [$._infix_crossing, $.truth_expression],
  [$.disregarded, $.skipped],
  [$.composition_input, $.landing],
  [$.domain_hole, $.non_infix_application],
  [$.domain_activate, $.glob],
  [$.identifier, $.scalar_parameter_reference],
];
